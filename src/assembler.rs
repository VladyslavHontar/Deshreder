use solana_entry::entry::Entry;
use solana_ledger::shred::{ReedSolomonCache, Shred};
use std::collections::{HashMap, HashSet};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

struct SlotBuffer {
    data_shreds:   HashMap<u32, Shred>,
    coding_shreds: HashMap<u32, Shred>,
    /// FEC sets where we actually recovered ≥1 data shred.
    recovered_fec_sets: HashSet<u32>,
    /// FEC sets that failed last time we tried — skip until new shreds arrive.
    failed_fec_sets: HashSet<u32>,
    created_at: Instant,

    // Streaming state: index of the next data shred that begins the next entry batch.
    next_stream_index: u32,
    /// Running count of entries emitted so far (diagnostics).
    entries_emitted:   u64,

    // Latency tracking (nanoseconds since created_at).
    first_entry_ns: Option<u64>,
    last_entry_ns:  Option<u64>,
}

impl SlotBuffer {
    fn new() -> Self {
        Self {
            data_shreds:        HashMap::new(),
            coding_shreds:      HashMap::new(),
            recovered_fec_sets: HashSet::new(),
            failed_fec_sets:    HashSet::new(),
            created_at:         Instant::now(),
            next_stream_index:  0,
            entries_emitted:    0,
            first_entry_ns:     None,
            last_entry_ns:      None,
        }
    }

    fn is_expired(&self, timeout_ms: u64) -> bool {
        self.created_at.elapsed().as_millis() as u64 >= timeout_ms
    }
}

pub(crate) struct SlotAssembler {
    slots:              HashMap<u64, SlotBuffer>,
    slot_timeout_ms:    u64,
    reed_solomon_cache: ReedSolomonCache,
}

impl SlotAssembler {
    pub fn new(slot_timeout_ms: u64) -> Self {
        Self {
            slots: HashMap::new(),
            slot_timeout_ms,
            reed_solomon_cache: ReedSolomonCache::default(),
        }
    }

    /// Returns `(slot, newly_emitted_entries, slot_complete)`. `slot_complete`
    /// is true only when this call processed the LAST_SHRED_IN_SLOT batch
    /// (the whole slot is reconstructed), not merely an intermediate batch.
    pub fn add_shred(&mut self, shred: Shred) -> (u64, Vec<Entry>, bool) {
        let slot    = shred.slot();
        let fec_idx = shred.fec_set_index();

        {
            let buf = self.slots.entry(slot).or_insert_with(SlotBuffer::new);
            if shred.is_data() {
                buf.data_shreds.insert(shred.index(), shred);
            } else if shred.is_code() {
                buf.coding_shreds.insert(shred.index(), shred);
            }
            buf.failed_fec_sets.remove(&fec_idx);
        }

        if !self.slots[&slot].recovered_fec_sets.contains(&fec_idx) {
            self.try_fec_recovery(slot, fec_idx);
        }

        let (slot, entries) = self.try_stream_entries(slot);
        // The buffer is removed only when LAST_SHRED_IN_SLOT is processed, so a
        // now-absent slot means the whole slot was just reconstructed.
        let complete = !self.slots.contains_key(&slot);
        (slot, entries, complete)
    }

    /// Reconstruct entries one ENTRY BATCH at a time. Solana produces multiple
    /// batches per slot (one shredding call each); every batch is its own
    /// length-prefixed `bincode(Vec<Entry>)` spanning a contiguous run of data
    /// shreds ending at a shred with the DATA_COMPLETE flag. The slot is finished
    /// only at the shred carrying LAST_SHRED_IN_SLOT. (Treating the whole slot as
    /// a single blob dropped every batch after the first.)
    fn try_stream_entries(&mut self, slot: u64) -> (u64, Vec<Entry>) {
        let mut emitted: Vec<Entry> = Vec::new();

        loop {
            let start = match self.slots.get(&slot) {
                Some(b) => b.next_stream_index,
                None    => return (slot, emitted),
            };

            // Walk forward to the first present DATA_COMPLETE shred, recovering
            // gaps via FEC as we go. The batch is the contiguous run [start..=end].
            let mut idx = start;
            let batch_end = loop {
                let present = self.slots.get(&slot)
                    .map_or(false, |b| b.data_shreds.contains_key(&idx));
                if present {
                    if self.slots[&slot].data_shreds[&idx].data_complete() {
                        break idx;
                    }
                    idx += 1;
                } else {
                    // Gap at idx: try FEC on any unrecovered set, then retry idx.
                    let pre = self.slots[&slot].data_shreds.len();
                    for fec in self.unrecovered_fec_sets(slot) {
                        self.try_fec_recovery(slot, fec);
                    }
                    let post = self.slots.get(&slot).map_or(0, |b| b.data_shreds.len());
                    if post <= pre {
                        return (slot, emitted); // can't fill the gap yet — wait for more shreds
                    }
                }
            };

            // Concatenate the batch's data payloads into one bincode Vec<Entry> blob.
            let blob: Vec<u8> = {
                let b = &self.slots[&slot];
                let mut v = Vec::new();
                for i in start..=batch_end {
                    match shred_data_bytes(&b.data_shreds[&i]) {
                        Some(bytes) => v.extend_from_slice(bytes),
                        None        => return (slot, emitted), // malformed shred — wait
                    }
                }
                v
            };

            // deserialize_from tolerates trailing bytes (last-shred padding bounded by `size`).
            let mut cur = std::io::Cursor::new(&blob[..]);
            let batch_entries = match bincode::deserialize_from::<_, Vec<Entry>>(&mut cur) {
                Ok(es) => es,
                Err(_) => return (slot, emitted), // incomplete/corrupt — wait for more shreds
            };

            let last_in_slot = self.slots[&slot].data_shreds[&batch_end].last_in_slot();
            let n = batch_entries.len() as u64;
            emitted.extend(batch_entries);

            {
                let b = self.slots.get_mut(&slot).unwrap();
                b.next_stream_index = batch_end + 1;
                b.entries_emitted  += n;
                if n > 0 {
                    let el = b.created_at.elapsed().as_nanos() as u64;
                    if b.first_entry_ns.is_none() {
                        b.first_entry_ns = Some(el);
                        let ts_ms = SystemTime::now().duration_since(UNIX_EPOCH)
                            .unwrap_or_default().as_millis();
                        eprintln!("[first_tx] slot={slot} ts={ts_ms} elapsed={}µs", el / 1_000);
                    }
                    b.last_entry_ns = Some(el);
                }
            }

            if last_in_slot {
                let b = &self.slots[&slot];
                if let (Some(first_ns), Some(last_ns)) = (b.first_entry_ns, b.last_entry_ns) {
                    eprintln!(
                        "[metrics] slot={slot} ts={} first_tx={}µs complete={}µs entries={}",
                        SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis(),
                        first_ns / 1_000, last_ns / 1_000, b.entries_emitted,
                    );
                }
                eprintln!("[assembler] slot {slot}: complete, {} entries", b.entries_emitted);
                self.slots.remove(&slot);
                return (slot, emitted);
            }
            // Otherwise more batches may already be available — loop for the next one.
        }
    }

    fn unrecovered_fec_sets(&self, slot: u64) -> Vec<u32> {
        let buf = match self.slots.get(&slot) {
            Some(b) => b,
            None    => return vec![],
        };
        buf.coding_shreds
            .values()
            .map(|s| s.fec_set_index())
            .collect::<HashSet<_>>()
            .into_iter()
            .filter(|fec| {
                !buf.recovered_fec_sets.contains(fec) && !buf.failed_fec_sets.contains(fec)
            })
            .collect()
    }

    fn try_fec_recovery(&mut self, slot: u64, fec_set_index: u32) {
        let buf = match self.slots.get_mut(&slot) {
            Some(b) => b,
            None    => return,
        };

        // Collect both data and coding shreds for this FEC set.
        let all: Vec<Shred> = buf
            .data_shreds
            .values()
            .filter(|s| s.fec_set_index() == fec_set_index)
            .chain(
                buf.coding_shreds
                    .values()
                    .filter(|s| s.fec_set_index() == fec_set_index),
            )
            .cloned()
            .collect();

        if all.is_empty() {
            return;
        }

        // Use the public `shred::recover` (wraps merkle::recover internally).
        match solana_ledger::shred::recover(all, &self.reed_solomon_cache) {
            Ok(iter) => {
                let mut count = 0usize;
                for result in iter {
                    if let Ok(shred) = result {
                        if shred.is_data() {
                            let idx = shred.index();
                            buf.data_shreds.entry(idx).or_insert(shred);
                            count += 1;
                        }
                    }
                }
                if count > 0 {
                    buf.recovered_fec_sets.insert(fec_set_index);
                } else {
                    buf.failed_fec_sets.insert(fec_set_index);
                }
            }
            Err(_) => {
                buf.failed_fec_sets.insert(fec_set_index);
            }
        }
    }

    pub fn evict_expired(&mut self) -> usize {
        let timeout = self.slot_timeout_ms;
        let before  = self.slots.len();
        self.slots.retain(|slot, buf| {
            let expired = buf.is_expired(timeout);
            if expired {
                eprintln!(
                    "[assembler] evict slot {slot}: data={} coding={} emitted={} next_idx={}",
                    buf.data_shreds.len(),
                    buf.coding_shreds.len(),
                    buf.entries_emitted,
                    buf.next_stream_index,
                );
            }
            !expired
        });
        before - self.slots.len()
    }

    pub fn active_slot_count(&self) -> usize {
        self.slots.len()
    }
}

/// Extract the entry data bytes from a data shred using the public payload API.
///
/// Solana data shred layout (both legacy and Merkle):
///   [0..64)   = Signature
///   [64..83)  = CommonShredHeader (variant:1, slot:8, index:4, version:2, fec_set_index:4)
///   [83..85)  = parent_slot_offset (DataShredHeader)
///   [85..86)  = flags (DataShredHeader)
///   [86..88)  = data size, LE u16 (DataShredHeader)
///   [88..88+size) = actual entry data
///
/// For Merkle shreds the proof is appended AFTER the data; `size` bounds it correctly.
fn shred_data_bytes(shred: &Shred) -> Option<&[u8]> {
    if !shred.is_data() {
        return None;
    }
    let raw: &[u8] = &shred.payload().bytes;
    if raw.len() < 88 {
        return None;
    }
    let size = u16::from_le_bytes([raw[86], raw[87]]) as usize;
    let end  = 88 + size;
    if end > raw.len() {
        return None;
    }
    Some(&raw[88..end])
}

#[cfg(test)]
mod tests {
    use super::*;
    use solana_entry::entry::Entry;
    use solana_hash::Hash;
    use solana_keypair::Keypair;
    use solana_ledger::shred::{ProcessShredsStats, ReedSolomonCache, Shredder};

    pub fn make_test_shreds(slot: u64) -> (Vec<Shred>, Vec<Shred>) {
        let parent_slot = slot.saturating_sub(1);
        let keypair = Keypair::new();
        let entries = vec![Entry {
            num_hashes: 0,
            hash: Hash::default(),
            transactions: vec![],
        }];
        let shredder = Shredder::new(slot, parent_slot, 0, 0).unwrap();
        let cache    = ReedSolomonCache::default();
        let (data, coding) = shredder.entries_to_merkle_shreds_for_tests(
            &keypair,
            entries.as_slice(),
            true,
            Hash::default(),
            0, 0,
            &cache,
            &mut ProcessShredsStats::default(),
        );
        (data, coding)
    }

    #[test]
    fn test_assembles_slot_from_data_shreds() {
        let slot = 100u64;
        let (data_shreds, _) = make_test_shreds(slot);
        assert!(!data_shreds.is_empty());

        let mut assembler = SlotAssembler::new(400);
        let mut all_entries: Vec<Entry> = Vec::new();
        for shred in data_shreds {
            let (_, entries, _) = assembler.add_shred(shred);
            all_entries.extend(entries);
        }
        assert!(!all_entries.is_empty());
    }

    /// Reproduces the real-slot failure: a slot is produced as MULTIPLE entry
    /// batches (separate shredding calls), each its own length-prefixed
    /// `Vec<Entry>` blob, with contiguous data-shred indices and DATA_COMPLETE
    /// flags marking batch boundaries. We feed ALL shreds (no gaps), so any
    /// shortfall is purely the assembler's batch handling — not coverage.
    #[test]
    fn test_assembles_multi_batch_slot() {
        let slot = 300u64;
        let parent = slot - 1;
        let keypair = Keypair::new();
        let cache = ReedSolomonCache::default();
        let shredder = Shredder::new(slot, parent, 0, 0).unwrap();

        let batch1 = vec![Entry { num_hashes: 1, hash: Hash::default(), transactions: vec![] }; 3];
        let batch2 = vec![Entry { num_hashes: 2, hash: Hash::default(), transactions: vec![] }; 4];

        let (d1, c1) = shredder.entries_to_merkle_shreds_for_tests(
            &keypair, &batch1, false, Hash::default(), 0, 0, &cache,
            &mut ProcessShredsStats::default());
        let (d2, _c2) = shredder.entries_to_merkle_shreds_for_tests(
            &keypair, &batch2, true, Hash::default(), d1.len() as u32, c1.len() as u32,
            &cache, &mut ProcessShredsStats::default());

        let mut all_data: Vec<Shred> = d1;
        all_data.extend(d2);
        all_data.sort_by_key(|s| s.index());

        let mut assembler = SlotAssembler::new(10_000);
        let mut entries: Vec<Entry> = Vec::new();
        for s in all_data {
            let (_, e, _) = assembler.add_shred(s);
            entries.extend(e);
        }

        assert_eq!(
            entries.len(), 7,
            "slot has 3+4=7 entries across two batches; assembler returned {}",
            entries.len()
        );
    }

    #[test]
    fn test_evicts_expired_slots() {
        let mut assembler = SlotAssembler::new(0);
        let (_, coding_shreds) = make_test_shreds(200);
        let coding = coding_shreds.into_iter().next().expect("expected a coding shred");
        assembler.add_shred(coding);

        assert_eq!(assembler.slots.len(), 1);
        let evicted = assembler.evict_expired();
        assert_eq!(evicted, 1);
        assert_eq!(assembler.slots.len(), 0);
    }
}
