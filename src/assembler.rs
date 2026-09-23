//! Raw shreds in, block components out.
//!
//! Per slot, data shreds are kept by index and coding shreds by FEC set. A
//! batch is a contiguous run of data shreds ending at DATA_COMPLETE; the
//! moment such a run is present it is decoded as one `BlockComponent` and
//! emitted — nothing waits for the rest of the slot. When a set has lost
//! data shreds but holds at least `num_data` shards in total, the missing
//! data shards are rebuilt with Reed–Solomon before the run is checked again.
//!
//! The struct is pure: no clock, no I/O, no logging. Time enters only through
//! [`Assembler::evict_below`], which the caller drives from its slot clock.

use crate::wire::{self, Header, Kind};
use reed_solomon_erasure::galois_8::ReedSolomon;
use solana_entry::block_component::{
    BlockComponent, BlockMarkerV1, VersionedBlockFooter, VersionedBlockMarker,
};
use solana_entry::entry::Entry;
use solana_hash::Hash;
use std::collections::{BTreeMap, BTreeSet};

/// What falls out of [`Assembler::push`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Event {
    /// One entry batch, emitted as soon as its shreds were all present.
    Entries { slot: u64, entries: Vec<Entry> },
    /// The block footer marker (Alpenglow). Carries what a consumer used to
    /// wait for votes to learn: the bank hash and the producer's clock.
    Footer { slot: u64, bank_hash: Hash, producer_time_nanos: u64 },
    /// The LAST_SHRED_IN_SLOT batch was processed; every batch of this slot
    /// has been emitted and its state dropped. Later packets are ignored.
    SlotComplete { slot: u64 },
    /// A complete run of shreds did not decode as a `BlockComponent`. It is
    /// skipped so the slot cannot stall forever; the consumer decides what a
    /// hole means. This should never fire on honest leader output.
    UndecodableBatch { slot: u64, first_index: u32, last_index: u32 },
}

struct DataShred {
    header: Header,
    /// The erasure shard: headers + data. One buffer serves both Reed–Solomon
    /// and entry extraction, so a shred is copied exactly once.
    shard: Vec<u8>,
}

struct FecSet {
    num_data: u16,
    num_code: u16,
    shard_len: usize,
    code: BTreeMap<u16, Vec<u8>>,
    recovered: bool,
}

#[derive(Default)]
struct SlotState {
    data: BTreeMap<u32, DataShred>,
    fec: BTreeMap<u32, FecSet>,
    /// First index of the next batch not yet emitted.
    next: u32,
}

#[derive(Default)]
pub struct Assembler {
    slots: BTreeMap<u64, SlotState>,
    /// Slots whose LAST_SHRED_IN_SLOT batch was emitted. A late duplicate
    /// must not resurrect them; pruned by `evict_below`.
    finished: BTreeSet<u64>,
    /// Everything below is history: dropped on sight.
    floor: u64,
}

impl Assembler {
    pub fn new() -> Self {
        Self::default()
    }

    /// Feed one packet. Events, if any, are appended to `out`.
    pub fn push(&mut self, packet: &[u8], out: &mut Vec<Event>) {
        let Some(h) = wire::parse_packet(packet) else { return };
        if h.slot < self.floor || self.finished.contains(&h.slot) {
            return;
        }
        let state = self.slots.entry(h.slot).or_default();
        let shard = wire::shard(packet, &h);
        match h.kind {
            Kind::Data { .. } => {
                // First arrival wins; a duplicate is a no-op.
                state.data.entry(h.index).or_insert_with(|| DataShred {
                    header: h,
                    shard: shard.to_vec(),
                });
            }
            Kind::Code { num_data, num_code, position } => {
                let set = state.fec.entry(h.fec_set_index).or_insert_with(|| FecSet {
                    num_data,
                    num_code,
                    shard_len: h.shard_len(),
                    code: BTreeMap::new(),
                    recovered: false,
                });
                if set.shard_len == shard.len() && !set.recovered {
                    set.code.entry(position).or_insert_with(|| shard.to_vec());
                }
            }
        }
        recover(state, h.slot, h.fec_set_index);
        stream(state, h.slot, out);
        if self.slots[&h.slot].next == u32::MAX {
            self.slots.remove(&h.slot);
            self.finished.insert(h.slot);
        }
    }

    /// Drop every slot below `slot` and forget it was ever seen. Returns how
    /// many live slots were dropped. Drive this from the consumer's tip.
    pub fn evict_below(&mut self, slot: u64) -> usize {
        let before = self.slots.len();
        self.slots = self.slots.split_off(&slot);
        self.finished = self.finished.split_off(&slot);
        self.floor = self.floor.max(slot);
        before - self.slots.len()
    }

    pub fn active_slots(&self) -> usize {
        self.slots.len()
    }
}

/// Rebuild missing data shards of one FEC set once enough shards are held.
fn recover(state: &mut SlotState, slot: u64, fec_set_index: u32) {
    let Some(set) = state.fec.get_mut(&fec_set_index) else { return };
    if set.recovered {
        return;
    }
    let num_data = usize::from(set.num_data);
    let num_code = usize::from(set.num_code);
    let range = fec_set_index..fec_set_index.saturating_add(set.num_data.into());
    let present = state.data.range(range.clone()).count();
    if present == num_data {
        set.recovered = true;
        set.code.clear();
        return;
    }
    if present + set.code.len() < num_data {
        return; // not enough shards yet
    }

    let mut shards: Vec<Option<Vec<u8>>> = Vec::with_capacity(num_data + num_code);
    for index in range.clone() {
        shards.push(state.data.get(&index).map(|d| d.shard.clone()));
    }
    for position in 0..set.num_code {
        shards.push(set.code.get(&position).cloned());
    }
    let Ok(rs) = ReedSolomon::new(num_data, num_code) else { return };
    if rs.reconstruct_data(&mut shards).is_err() {
        return;
    }

    for (i, shard) in shards.into_iter().take(num_data).enumerate() {
        let index = fec_set_index + i as u32;
        if state.data.contains_key(&index) {
            continue;
        }
        let Some(shard) = shard else { continue };
        // Reed–Solomon output is only trusted if it reads back as the shred
        // it claims to be; a mismatched coding shred yields garbage here.
        let Some(header) = wire::parse_data_shard(&shard) else { continue };
        if header.slot != slot || header.index != index || header.fec_set_index != fec_set_index {
            continue;
        }
        state.data.insert(index, DataShred { header, shard });
    }
    set.recovered = true;
    set.code.clear();
}

/// Emit every batch whose shreds are now all present, in order.
fn stream(state: &mut SlotState, slot: u64, out: &mut Vec<Event>) {
    loop {
        let start = state.next;
        // Walk the contiguous run from `start` to the first DATA_COMPLETE.
        let mut end = start;
        let end = loop {
            match state.data.get(&end) {
                None => return,
                Some(d) if d.header.data_complete() => break end,
                Some(_) => end += 1,
            }
        };

        let blob: Vec<u8> = (start..=end)
            .flat_map(|i| {
                let d = &state.data[&i];
                wire::data_in_shard(&d.shard, &d.header).iter().copied()
            })
            .collect();

        match wincode::deserialize::<BlockComponent>(&blob) {
            Ok(BlockComponent::EntryBatch(entries)) => out.push(Event::Entries { slot, entries }),
            Ok(BlockComponent::BlockMarker(VersionedBlockMarker::V1(BlockMarkerV1::BlockFooter(f)))) => {
                let VersionedBlockFooter::V1(footer) = f.inner();
                out.push(Event::Footer {
                    slot,
                    bank_hash: footer.bank_hash,
                    producer_time_nanos: footer.block_producer_time_nanos,
                });
            }
            Ok(BlockComponent::BlockMarker(_)) => {} // header / update-parent / genesis
            Err(_) => out.push(Event::UndecodableBatch { slot, first_index: start, last_index: end }),
        }

        let last = state.data[&end].header.last_in_slot();
        // Emitted batches are never read again; drop them so a slow slot
        // does not hold every shred until completion.
        for i in start..=end {
            state.data.remove(&i);
        }
        state.next = end + 1;
        if last {
            out.push(Event::SlotComplete { slot });
            state.next = u32::MAX; // sentinel: slot is done
            return;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use solana_entry::block_component::{BlockFooterV1, LengthPrefixed};
    use solana_keypair::Keypair;
    use solana_ledger::shred::{ProcessShredsStats, ReedSolomonCache, Shred, Shredder};

    fn entries(n: u64) -> Vec<Entry> {
        (0..n).map(|i| Entry { num_hashes: i, hash: Hash::default(), transactions: vec![] }).collect()
    }

    /// Real agave shreds for one component; `(data, code)` sorted by index.
    fn shred_component(slot: u64, c: &BlockComponent, last: bool, first_index: u32) -> (Vec<Shred>, Vec<Shred>) {
        let shredder = Shredder::new(slot, slot - 1, 0, 0).unwrap();
        shredder.component_to_merkle_shreds_for_tests(
            &Keypair::new(), c, last, Hash::default(), first_index, first_index,
            &ReedSolomonCache::default(), &mut ProcessShredsStats::default())
    }

    fn feed(a: &mut Assembler, shreds: impl IntoIterator<Item = Shred>) -> Vec<Event> {
        let mut out = Vec::new();
        for s in shreds {
            a.push(&s.payload().bytes, &mut out);
        }
        out
    }

    fn collected(events: &[Event]) -> Vec<Entry> {
        events.iter().filter_map(|e| match e {
            Event::Entries { entries, .. } => Some(entries.clone()),
            _ => None,
        }).flatten().collect()
    }

    #[test]
    fn wire_offsets_match_agave_parser() {
        let (data, code) = shred_component(500, &BlockComponent::EntryBatch(entries(100)), true, 0);
        for s in data.iter().chain(&code) {
            let h = wire::parse_packet(&s.payload().bytes).expect("real shred must parse");
            assert_eq!((h.slot, h.index, h.fec_set_index, h.is_data()),
                       (s.slot(), s.index(), s.fec_set_index(), s.is_data()));
            if s.is_data() {
                assert_eq!(h.data_complete(), s.data_complete());
                assert_eq!(h.last_in_slot(), s.last_in_slot());
            }
        }
        // Equal shard lengths across a set is what makes Reed–Solomon valid.
        let lens: BTreeSet<usize> = data.iter().chain(&code)
            .map(|s| wire::parse_packet(&s.payload().bytes).unwrap().shard_len()).collect();
        assert_eq!(lens.len(), 1, "data and code shards must be the same length");
    }

    /// The bug the rewrite exists for: one batch spanning several shreds.
    #[test]
    fn multi_shred_batch_roundtrips() {
        let want = entries(100);
        let (data, _) = shred_component(400, &BlockComponent::EntryBatch(want.clone()), true, 0);
        assert!(data.len() >= 3, "fixture must span several shreds, got {}", data.len());
        let ev = feed(&mut Assembler::new(), data);
        assert_eq!(collected(&ev), want);
        assert_eq!(ev.last(), Some(&Event::SlotComplete { slot: 400 }));
    }

    #[test]
    fn multi_batch_slot_streams_each_batch_as_it_completes() {
        let (b1, b2) = (entries(3), entries(4));
        let (d1, c1) = shred_component(300, &BlockComponent::EntryBatch(b1.clone()), false, 0);
        let (d2, _) = shred_component(300, &BlockComponent::EntryBatch(b2.clone()), true, d1.len() as u32);
        let _ = c1;
        let mut a = Assembler::new();
        let ev1 = feed(&mut a, d1);
        assert_eq!(collected(&ev1), b1, "first batch emitted before the slot ends");
        assert_eq!(a.active_slots(), 1);
        let ev2 = feed(&mut a, d2);
        assert_eq!(collected(&ev2), b2);
        assert_eq!(ev2.last(), Some(&Event::SlotComplete { slot: 300 }));
        assert_eq!(a.active_slots(), 0);
    }

    #[test]
    fn recovers_lost_data_shreds_from_coding() {
        let want = entries(100);
        let (data, code) = shred_component(600, &BlockComponent::EntryBatch(want.clone()), true, 0);
        // Drop as many data shreds per FEC set as that set has coding shreds:
        // the maximum recoverable loss.
        let mut sets: BTreeMap<u32, u16> = BTreeMap::new();
        for c in &code {
            let h = wire::parse_packet(&c.payload().bytes).unwrap();
            if let Kind::Code { num_code, .. } = h.kind { sets.insert(h.fec_set_index, num_code); }
        }
        let mut dropped = 0;
        let kept: Vec<Shred> = data.into_iter().filter(|d| {
            let set = d.fec_set_index();
            let budget = sets.get_mut(&set).unwrap();
            if *budget > 0 { *budget -= 1; dropped += 1; false } else { true }
        }).collect();
        assert!(dropped > 0);
        let mut a = Assembler::new();
        assert!(collected(&feed(&mut a, kept)).is_empty(), "nothing decodable with holes");
        let ev = feed(&mut a, code);
        assert_eq!(collected(&ev), want, "recovered {dropped} data shreds");
        assert!(ev.contains(&Event::SlotComplete { slot: 600 }));
    }

    #[test]
    fn waits_for_enough_shards_then_recovers() {
        let want = entries(40);
        let (mut data, code) = shred_component(700, &BlockComponent::EntryBatch(want.clone()), true, 0);
        data.remove(1);
        data.remove(0);
        let mut a = Assembler::new();
        let mut ev = feed(&mut a, data);
        ev.extend(feed(&mut a, code.iter().take(1).cloned()));
        assert!(collected(&ev).is_empty(), "two holes, one parity shard: must wait");
        let ev = feed(&mut a, code.into_iter().skip(1).take(1));
        assert_eq!(collected(&ev), want);
    }

    #[test]
    fn footer_marker_yields_footer_event() {
        let footer = BlockFooterV1 {
            bank_hash: Hash::new_unique(),
            block_producer_time_nanos: 1_726_000_000_123_456_789,
            block_user_agent: b"agave".to_vec(),
            block_final_cert: None,
            skip_reward_cert: None,
            notar_reward_cert: None,
        };
        let marker = VersionedBlockMarker::V1(BlockMarkerV1::BlockFooter(
            LengthPrefixed::new(VersionedBlockFooter::V1(footer.clone()))));
        let (data, _) = shred_component(800, &BlockComponent::BlockMarker(marker), true, 0);
        let ev = feed(&mut Assembler::new(), data);
        assert_eq!(ev, vec![
            Event::Footer { slot: 800, bank_hash: footer.bank_hash, producer_time_nanos: footer.block_producer_time_nanos },
            Event::SlotComplete { slot: 800 },
        ]);
    }

    #[test]
    fn late_packet_for_finished_slot_is_ignored() {
        let (data, _) = shred_component(900, &BlockComponent::EntryBatch(entries(5)), true, 0);
        let mut a = Assembler::new();
        let first = data[0].clone();
        feed(&mut a, data);
        assert_eq!(a.active_slots(), 0);
        assert!(feed(&mut a, [first]).is_empty());
        assert_eq!(a.active_slots(), 0, "a finished slot must not be resurrected");
    }

    #[test]
    fn evict_below_drops_live_state_and_memory() {
        let (data, _) = shred_component(1000, &BlockComponent::EntryBatch(entries(50)), true, 0);
        let mut a = Assembler::new();
        feed(&mut a, data[..1].to_vec());
        assert_eq!(a.active_slots(), 1);
        assert_eq!(a.evict_below(1001), 1);
        assert!(feed(&mut a, data).is_empty(), "history stays dropped");
        assert_eq!(a.active_slots(), 0);
    }

    #[test]
    fn rejects_short_and_legacy_packets() {
        assert!(wire::parse_packet(&[0u8; 87]).is_none());
        let mut legacy = vec![0u8; wire::CODE_PAYLOAD];
        legacy[64] = 0xa5;
        assert!(wire::parse_packet(&legacy).is_none());
        let mut out = Vec::new();
        Assembler::new().push(&legacy, &mut out);
        assert!(out.is_empty());
    }
}
