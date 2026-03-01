use solana_entry::entry::Entry;
use solana_ledger::shred::{
    shred_data::ShredData,
    traits::ShredData as ShredDataTrait,
    merkle,
    ReedSolomonCache,
    Shred,
};
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

    // Streaming state.
    prefix_bytes:      Vec<u8>,
    next_stream_index: u32,
    stream_cursor_pos: usize,
    entries_total:     Option<u64>,
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
            prefix_bytes:       Vec::new(),
            next_stream_index:  0,
            stream_cursor_pos:  0,
            entries_total:      None,
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
    slots:            HashMap<u64, SlotBuffer>,
    slot_timeout_ms:  u64,
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

    pub fn add_shred(&mut self, shred: Shred) -> (u64, Vec<Entry>) {
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

        self.try_stream_entries(slot)
    }

    fn try_stream_entries(&mut self, slot: u64) -> (u64, Vec<Entry>) {
        'extend: loop {
            let next_idx = match self.slots.get(&slot) {
                Some(b) => b.next_stream_index,
                None    => return (slot, vec![]),
            };

            if self.slots[&slot].data_shreds.contains_key(&next_idx) {
                let data = {
                    let shred = &self.slots[&slot].data_shreds[&next_idx];
                    shred_data_bytes(shred).map(|b| b.to_vec())
                };
                match data {
                    Ok(bytes) => {
                        let buf = self.slots.get_mut(&slot).unwrap();
                        buf.prefix_bytes.extend_from_slice(&bytes);
                        buf.next_stream_index += 1;
                    }
                    Err(_) => break 'extend,
                }
            } else {
                let fec_sets = self.unrecovered_fec_sets(slot);
                if fec_sets.is_empty() {
                    break 'extend;
                }
                let pre = self.slots[&slot].data_shreds.len();
                for fec in fec_sets {
                    self.try_fec_recovery(slot, fec);
                }
                let post = self.slots.get(&slot).map_or(0, |b| b.data_shreds.len());
                if post <= pre {
                    break 'extend;
                }
            }
        }

        let buf = match self.slots.get_mut(&slot) {
            Some(b) => b,
            None    => return (slot, vec![]),
        };

        if buf.entries_total.is_none() {
            if buf.prefix_bytes.len() < 8 {
                return (slot, vec![]);
            }
            let count = u64::from_le_bytes(buf.prefix_bytes[0..8].try_into().unwrap());
            buf.entries_total     = Some(count);
            buf.stream_cursor_pos = 8;
            eprintln!("[assembler] slot {slot}: stream start, total_entries={count}");
        }

        let total = buf.entries_total.unwrap();
        let mut newly_emitted: Vec<Entry> = Vec::new();

        while buf.entries_emitted < total {
            let pos = buf.stream_cursor_pos;
            let result = {
                let slice  = &buf.prefix_bytes[pos..];
                let mut c  = std::io::Cursor::new(slice);
                bincode::deserialize_from::<_, Entry>(&mut c)
                    .map(|e| (e, c.position() as usize))
            };
            match result {
                Ok((entry, consumed)) => {
                    buf.stream_cursor_pos += consumed;
                    buf.entries_emitted   += 1;
                    newly_emitted.push(entry);
                }
                Err(_) => break,
            }
        }

        if !newly_emitted.is_empty() {
            let elapsed = buf.created_at.elapsed().as_nanos() as u64;
            if buf.first_entry_ns.is_none() {
                buf.first_entry_ns = Some(elapsed);
                let ts_ms = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis();
                eprintln!("[first_tx] slot={slot} ts={ts_ms} elapsed={}µs", elapsed / 1_000);
            }
            buf.last_entry_ns = Some(elapsed);
        }

        if buf.entries_emitted == total && total > 0 {
            if let (Some(first_ns), Some(last_ns)) = (buf.first_entry_ns, buf.last_entry_ns) {
                eprintln!(
                    "[metrics] slot={slot} ts={} first_tx={}µs complete={}µs entries={total}",
                    SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis(),
                    first_ns / 1_000,
                    last_ns  / 1_000,
                );
            }
            eprintln!("[assembler] slot {slot}: complete, {total} entries");
            self.slots.remove(&slot);
        }

        (slot, newly_emitted)
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

        let all: Vec<merkle::Shred> = buf
            .data_shreds
            .values()
            .filter(|s| s.fec_set_index() == fec_set_index)
            .chain(
                buf.coding_shreds
                    .values()
                    .filter(|s| s.fec_set_index() == fec_set_index),
            )
            .filter_map(|s| merkle::Shred::try_from(s.clone()).ok())
            .collect();

        if all.is_empty() {
            return;
        }

        match merkle::recover(all, &self.reed_solomon_cache) {
            Ok(iter) => {
                let mut count = 0usize;
                for result in iter {
                    if let Ok(merkle_shred) = result {
                        let shred = Shred::from(merkle_shred);
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
                    "[assembler] evict slot {slot}: data={} coding={} entries={}/{}",
                    buf.data_shreds.len(),
                    buf.coding_shreds.len(),
                    buf.entries_emitted,
                    buf.entries_total.unwrap_or(0),
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

fn shred_data_bytes(shred: &Shred) -> Result<&[u8], solana_ledger::shred::Error> {
    match shred {
        Shred::ShredData(sd) => match sd {
            ShredData::Legacy(inner)  => inner.data(),
            ShredData::Merkle(inner)  => inner.data(),
        },
        Shred::ShredCode(_) => Err(solana_ledger::shred::Error::InvalidShredType),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use solana_entry::entry::Entry;
    use solana_ledger::shred::{ProcessShredsStats, ReedSolomonCache, Shredder};
    use solana_sdk::{hash::Hash, signature::Keypair};

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
        let (data, coding) = shredder.entries_to_shreds(
            &keypair,
            entries.as_slice(),
            true,
            None,
            0, 0, true,
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
            let (_, entries) = assembler.add_shred(shred);
            all_entries.extend(entries);
        }
        assert!(!all_entries.is_empty());
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
