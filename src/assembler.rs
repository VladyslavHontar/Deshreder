//! Raw shreds in, block components out.
//!
//! Per slot, data shreds are kept by index and coding shreds by FEC set. A
//! FEC set is one Merkle tree, every shred of it signed by the leader through
//! one root, and the sets of one block tile its indices. Shreds that
//! contradict that (a set under two roots, sets that overlap, a data shred
//! outside its set) are two versions of the block from an equivocating
//! leader: the slot is given up whole (`Event::Equivocated`), since which
//! version is the block cannot be told from the shreds. A
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
use solana_transaction::versioned::VersionedTransaction;
use std::collections::BTreeMap;

/// What falls out of [`Assembler::push`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Event {
    /// Entries of one batch, in order: those whole in the shreds from the
    /// batch's start as soon as those are in, the rest when the batch is.
    /// Like everything read off shreds they are what the leader sent, before
    /// any node ran it: the block may yet be skipped, and `Equivocated` takes
    /// back a slot's whole stream.
    /// `announced` is how many transactions at the front of the first entry
    /// already went out in `Transactions`, read before the entry was whole.
    Entries { slot: u64, entries: Vec<Entry>, announced: usize },
    /// Transactions of an entry not yet whole, in order, each as soon as its
    /// bytes are in: an entry's first transactions need not wait for its
    /// last. The entry itself follows in `Entries`, its `announced` saying
    /// how many of its transactions went out here.
    Transactions { slot: u64, transactions: Vec<VersionedTransaction> },
    /// A batch went out whole: everything in it is emitted. What streamed
    /// earlier did so ahead of this moment.
    BatchEnd { slot: u64 },
    /// The block footer marker (Alpenglow). Carries what a consumer used to
    /// wait for votes to learn: the bank hash and the producer's clock.
    Footer { slot: u64, bank_hash: Hash, producer_time_nanos: u64 },
    /// The LAST_SHRED_IN_SLOT batch was processed; every batch of this slot
    /// has been emitted and its state dropped. Later packets are ignored.
    SlotComplete { slot: u64 },
    /// A complete run of shreds did not decode as a `BlockComponent`. It is
    /// skipped so the slot cannot stall forever; the consumer decides what a
    /// hole means. Entries read whole from its start may already have gone
    /// out. This should never fire on honest leader output.
    UndecodableBatch { slot: u64, first_index: u32, last_index: u32 },
    /// The slot's shreds contradict one another: its leader signed two
    /// versions of the block. Which one is the block cannot be told from the
    /// shreds, so neither is assembled: the slot's state is dropped and its
    /// later packets are ignored. Emitted once per slot; whatever of the slot
    /// went out before, early entries included, is to be taken back.
    Equivocated { slot: u64 },
}

struct DataShred {
    header: Header,
    /// The erasure shard: headers + data. One buffer serves both Reed–Solomon
    /// and entry extraction, so a shred is copied exactly once.
    shard: Vec<u8>,
}

struct FecSet {
    /// The Merkle root every shred of the set proves its place under.
    root: [u8; 32],
    /// `(num_data, num_code, shard_len)`, as its first coding shred says;
    /// `None` until one arrives, and with it no recovery.
    coding: Option<(u16, u16, usize)>,
    code: BTreeMap<u16, Vec<u8>>,
    recovered: bool,
}

#[derive(Default)]
struct SlotState {
    data: BTreeMap<u32, DataShred>,
    fec: BTreeMap<u32, FecSet>,
    /// The sets whose size a coding shred told, by first index: their data
    /// shreds' count. They never overlap, so one lookup finds the one an
    /// index falls in, however many sets of unknown size lie between.
    sized: BTreeMap<u32, u16>,
    /// First index of the next batch not yet emitted.
    next: u32,
    /// Highest data index seen so far (`received` in agave's SlotMeta).
    received: Option<u32>,
    /// Index of the LAST_SHRED_IN_SLOT data shred, once seen.
    last_index: Option<u32>,
    /// The slot this block builds on (`slot − parent_offset`), from the first data shred.
    parent: Option<u64>,
    /// How far the batch at `next` was read before its last shreds came.
    early: Option<Early>,
}

/// Entries already emitted from the start of an unfinished batch.
struct Early {
    /// The batch's first index.
    start: u32,
    /// The first index past the shreds held in a row when it was last read.
    upto: u32,
    /// The bytes of the shreds `start..upto`, each shred's added once.
    blob: Vec<u8>,
    /// Bytes of the batch read: its entry count, then whole entries.
    offset: usize,
    /// Entries emitted.
    emitted: usize,
    /// Of the entry at `offset`, not yet whole: where its next transaction
    /// begins, and how many of its transactions went out.
    tx_offset: usize,
    announced: usize,
}

#[derive(Default)]
pub struct Assembler {
    slots: BTreeMap<u64, SlotState>,
    /// Slots whose LAST_SHRED_IN_SLOT batch was emitted, with their parent.
    /// A late duplicate must not resurrect them; pruned by `evict_below`.
    finished: BTreeMap<u64, Option<u64>>,
    /// Entries emitted before their batch was whole.
    early_entries: u64,
    /// Slots found to hold two versions of their block; pruned by `evict_below`.
    equivocated: std::collections::BTreeSet<u64>,
    /// Everything below is history: dropped on sight.
    floor: u64,
    /// Shreds dropped for proving a root other than their set's.
    conflicts: u64,
    /// Shreds that sat off SIMD-0504's shape.
    off_spec: u64,
    /// Drop such shreds instead of only counting them.
    strict: bool,
}

impl Assembler {
    pub fn new() -> Self {
        Self::default()
    }

    /// Drops the shreds that break SIMD-0504's rules (see `off_spec`) instead
    /// of counting them and going on. Agave does not enforce the rules yet:
    /// count first, to see whether the network already keeps them.
    pub fn strict(mut self) -> Self {
        self.strict = true;
        self
    }

    /// Shreds seen off SIMD-0504's shape: a set that is not 32 data and 32
    /// coding, or a shred outside its set's positions.
    pub fn off_spec(&self) -> u64 {
        self.off_spec
    }

    /// Feed one packet. Events, if any, are appended to `out`.
    pub fn push(&mut self, packet: &[u8], out: &mut Vec<Event>) {
        let Some(h) = wire::parse_packet(packet) else { return };
        if h.slot < self.floor || self.finished.contains_key(&h.slot) || self.equivocated.contains(&h.slot) {
            return;
        }
        let Some(root) = wire::merkle_root(packet) else { return };
        if h.off_spec() {
            self.off_spec += 1;
            if self.strict {
                return;
            }
        }
        let state = self.slots.entry(h.slot).or_default();
        // What one honest block never holds, each a sign of a second version:
        // a set under two roots; a data shred naming another set than the
        // sized one its index falls in, or a sized set it falls outside of; a
        // coding shred sizing a set over a sized one, a set begun inside it,
        // or a data shred there or past its end naming another set or it.
        let contradicts = state.fec.get(&h.fec_set_index).is_some_and(|set| set.root != root)
            || match h.kind {
                Kind::Data { parent_offset, .. } => {
                    // A leader that named two parents for one slot made two blocks of it.
                    state.parent.is_some_and(|p| p != h.slot.saturating_sub(u64::from(parent_offset)))
                        || covering(state, h.index).is_some_and(|start| start != h.fec_set_index)
                        || state.sized.get(&h.fec_set_index).is_some_and(|&n| h.index >= h.fec_set_index.saturating_add(n.into()))
                }
                Kind::Code { num_data, .. } => match state.sized.get(&h.fec_set_index) {
                    Some(&n) => n != num_data,
                    None => overlaps(state, h.fec_set_index, num_data),
                },
            };
        if contradicts {
            // Neither version is the block for certain: the slot is given up whole.
            self.conflicts += 1;
            self.slots.remove(&h.slot);
            self.equivocated.insert(h.slot);
            out.push(Event::Equivocated { slot: h.slot });
            return;
        }
        let set = state.fec.entry(h.fec_set_index).or_insert_with(|| FecSet { root, coding: None, code: BTreeMap::new(), recovered: false });
        let shard = wire::shard(packet, &h);
        match h.kind {
            Kind::Data { parent_offset, .. } => {
                state.received = Some(state.received.map_or(h.index, |r| r.max(h.index)));
                state.parent.get_or_insert(h.slot.saturating_sub(u64::from(parent_offset)));
                if h.last_in_slot() {
                    state.last_index = Some(h.index);
                }
                // First arrival wins; a duplicate is a no-op.
                state.data.entry(h.index).or_insert_with(|| DataShred {
                    header: h,
                    shard: shard.to_vec(),
                });
            }
            Kind::Code { num_data, num_code, position } => {
                let (_, _, shard_len) = *set.coding.get_or_insert((num_data, num_code, h.shard_len()));
                if shard_len == shard.len() && !set.recovered {
                    set.code.entry(position).or_insert_with(|| shard.to_vec());
                }
                state.sized.entry(h.fec_set_index).or_insert(num_data);
            }
        }
        recover(state, h.slot, h.fec_set_index);
        self.early_entries += stream(state, h.slot, out) as u64;
        if self.slots[&h.slot].next == u32::MAX {
            let done = self.slots.remove(&h.slot).expect("slot state");
            self.finished.insert(h.slot, done.parent);
        }
    }

    /// Drop every slot below `slot` and forget it was ever seen. Returns how
    /// many live slots were dropped. Drive this from the consumer's tip.
    pub fn evict_below(&mut self, slot: u64) -> usize {
        let before = self.slots.len();
        self.slots = self.slots.split_off(&slot);
        self.finished = self.finished.split_off(&slot);
        self.equivocated = self.equivocated.split_off(&slot);
        self.floor = self.floor.max(slot);
        before - self.slots.len()
    }

    pub fn active_slots(&self) -> usize {
        self.slots.len()
    }

    /// Entries emitted before the batch they are in was whole.
    pub fn early_entries(&self) -> u64 {
        self.early_entries
    }

    /// Shreds that contradicted their slot's others, each giving the slot up
    /// as `Equivocated`.
    pub fn conflicts(&self) -> u64 {
        self.conflicts
    }

    // ── what a repair driver needs to know, and nothing more ─────────────
    //
    // The assembler reports its own gaps; filling them (peers, requests,
    // timeouts, retries) is the caller's job. This mirrors agave's split
    // between the blockstore's SlotMeta / find_missing_data_indexes and
    // repair_service, which only reads them.

    /// Every batch of `slot` has been emitted (until `evict_below` forgets it).
    pub fn is_complete(&self, slot: u64) -> bool {
        self.finished.contains_key(&slot)
    }

    /// The slot this block builds on, once any data shred has arrived. Slots
    /// strictly between it and `slot` were never produced.
    pub fn parent_slot(&self, slot: u64) -> Option<u64> {
        match self.slots.get(&slot) {
            Some(state) => state.parent,
            None => self.finished.get(&slot).copied().flatten(),
        }
    }

    /// Highest data index seen for `slot`, if any shred has arrived.
    pub fn received(&self, slot: u64) -> Option<u32> {
        self.slots.get(&slot)?.received
    }

    /// Index of the LAST_SHRED_IN_SLOT shred, once it has been seen. `None`
    /// means the slot's length is still unknown.
    pub fn last_index(&self, slot: u64) -> Option<u32> {
        self.slots.get(&slot)?.last_index
    }

    /// Data indices still missing for `slot`, in order, at most `limit`.
    ///
    /// Everything below the next unemitted batch is already assembled and
    /// dropped, so gaps are searched only in `[next, end]`, where `end` is
    /// `last_index` if known, else the highest index received so far. Once
    /// the slot is complete this is empty.
    pub fn missing(&self, slot: u64, limit: usize) -> Vec<u32> {
        let Some(state) = self.slots.get(&slot) else { return Vec::new() };
        let Some(end) = state.last_index.or(state.received) else { return Vec::new() };
        (state.next..=end)
            .filter(|i| !state.data.contains_key(i))
            .take(limit)
            .collect()
    }

    /// The data indices worth asking for to finish `slot`, in order, at most
    /// `limit`: of a set whose size a coding shred told, only as many of its
    /// missing ones as it still lacks for Reed–Solomon to rebuild the rest
    /// (any shred of a set will do, so `num_data` held of either kind is
    /// enough); of any other, every missing one.
    pub fn needed(&self, slot: u64, limit: usize) -> Vec<u32> {
        let Some(state) = self.slots.get(&slot) else { return Vec::new() };
        let Some(end) = state.last_index.or(state.received) else { return Vec::new() };
        let mut out = Vec::new();
        // The set the last index fell in, and how many more of it to take.
        let mut current: Option<(u32, usize)> = None;
        for i in (state.next..=end).filter(|i| !state.data.contains_key(i)) {
            if out.len() == limit {
                break;
            }
            let set = covering(state, i).and_then(|start| Some((start, &state.fec[&start], state.fec[&start].coding?.0)));
            let Some((start, set, num_data)) = set else {
                out.push(i);
                continue;
            };
            if current.is_none_or(|(s, _)| s != start) {
                let held = state.data.range(start..start.saturating_add(num_data.into())).count() + set.code.len();
                current = Some((start, usize::from(num_data).saturating_sub(held)));
            }
            if let Some((_, left)) = current.as_mut().filter(|(_, left)| *left > 0) {
                *left -= 1;
                out.push(i);
            }
        }
        out
    }
}

/// The first index of the set, of known size, that `index` falls in. Sized
/// sets never overlap (`overlaps` keeps them apart), so the nearest one at or
/// below `index` is the only one that can hold it.
fn covering(state: &SlotState, index: u32) -> Option<u32> {
    let (&start, &num_data) = state.sized.range(..=index).next_back()?;
    (index < start.saturating_add(num_data.into())).then_some(start)
}

/// Whether a set of `num_data` data shreds starting at `start` contradicts
/// what is held: it lies in a sized set, a set begins inside it, a data shred
/// in its range names another set, or one past its end names it.
fn overlaps(state: &SlotState, start: u32, num_data: u16) -> bool {
    let end = start.saturating_add(num_data.into());
    covering(state, start).is_some()
        || state.fec.range(start.saturating_add(1)..end).next().is_some()
        || state.data.range(start..end).any(|(_, d)| d.header.fec_set_index != start)
        || state.data.range(end..).any(|(_, d)| d.header.fec_set_index == start)
}

/// Rebuild missing data shards of one FEC set once enough shards are held.
fn recover(state: &mut SlotState, slot: u64, fec_set_index: u32) {
    let Some(set) = state.fec.get_mut(&fec_set_index) else { return };
    let Some((data_shreds, code_shreds, _)) = set.coding.filter(|_| !set.recovered) else { return };
    let num_data = usize::from(data_shreds);
    let num_code = usize::from(code_shreds);
    let range = fec_set_index..fec_set_index.saturating_add(data_shreds.into());
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
    for position in 0..code_shreds {
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
        if header.last_in_slot() {
            state.last_index = Some(index);
        }
        state.data.insert(index, DataShred { header, shard });
    }
    set.recovered = true;
    set.code.clear();
}

/// Emit every batch whose shreds are now all present, in order.
/// Returns how many entries went out before their batch was whole.
fn stream(state: &mut SlotState, slot: u64, out: &mut Vec<Event>) -> usize {
    loop {
        let start = state.next;
        // Walk the contiguous run from `start` to the first DATA_COMPLETE.
        let mut end = start;
        let end = loop {
            match state.data.get(&end) {
                None => return read_early(state, slot, start, end, out),
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

        let (early, announced) = state.early.take().filter(|e| e.start == start).map_or((0, 0), |e| (e.emitted, e.announced));
        match wincode::deserialize::<BlockComponent>(&blob) {
            Ok(BlockComponent::EntryBatch(mut entries)) => {
                // Those read from the batch's start before it was whole went out then.
                entries.drain(..early.min(entries.len()));
                if !entries.is_empty() {
                    out.push(Event::Entries { slot, entries, announced });
                }
            }
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

        out.push(Event::BatchEnd { slot });
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
            return 0;
        }
    }
}

/// Emit the entries whole in the shreds held in a row from `start` up to
/// `upto`, the first one missing, that were not emitted yet: a batch's first
/// transactions need not wait for its last shreds. A block marker waits for
/// its whole batch, and bytes that do not read as a whole entry wait for more.
fn read_early(state: &mut SlotState, slot: u64, start: u32, upto: u32, out: &mut Vec<Event>) -> usize {
    let fresh = || Early { start, upto: start, blob: Vec::new(), offset: ENTRY_COUNT, emitted: 0, tx_offset: 0, announced: 0 };
    let early = state.early.get_or_insert_with(fresh);
    if early.start != start {
        *early = fresh();
    }
    if upto <= early.upto {
        return 0; // no shred joined the run since it was last read
    }
    // Only the shreds that joined the run are copied: a long batch is not
    // copied again for each of its shreds.
    for i in early.upto..upto {
        let d = &state.data[&i];
        early.blob.extend_from_slice(wire::data_in_shard(&d.shard, &d.header));
    }
    early.upto = upto;
    let blob = &early.blob;
    let Some(count) = blob.get(..ENTRY_COUNT).map(|b| u64::from_le_bytes(b.try_into().expect("8 bytes"))) else { return 0 };
    // A block marker waits for its whole batch, and so does a count the whole
    // batch's decoder would refuse (its length bound, `MaxDataShredsLen`).
    if count == 0 || count > solana_entry::entry::MAX_DATA_SHREDS_SIZE as u64 {
        return 0;
    }
    let mut rest = &blob[early.offset.min(blob.len())..];
    let mut entries = Vec::new();
    while ((early.emitted + entries.len()) as u64) < count {
        let before = rest;
        match wincode::deserialize_from::<Entry>(&mut rest) {
            Ok(entry) => entries.push(entry),
            Err(_) => {
                rest = before; // cut short: the rest of it is in shreds still to come
                break;
            }
        }
    }
    let emitted = entries.len();
    if !entries.is_empty() {
        out.push(Event::Entries { slot, entries, announced: early.announced });
        (early.tx_offset, early.announced) = (0, 0);
    }
    early.offset = early.blob.len() - rest.len();
    early.emitted += emitted;
    // The entry that is not whole yet: its transactions, one by one, past its
    // header (`num_hashes`, `hash`, and how many transactions follow).
    if (early.emitted as u64) < count && rest.len() >= ENTRY_HEADER {
        let txs = u64::from_le_bytes(rest[ENTRY_HEADER - 8..ENTRY_HEADER].try_into().expect("8 bytes"));
        if early.tx_offset == 0 {
            early.tx_offset = early.offset + ENTRY_HEADER;
        }
        let mut at = &early.blob[early.tx_offset..];
        let mut transactions = Vec::new();
        while ((early.announced + transactions.len()) as u64) < txs {
            match wincode::deserialize_from::<VersionedTransaction>(&mut at) {
                Ok(tx) => transactions.push(tx),
                Err(_) => break, // the rest of it is in shreds still to come
            }
            early.tx_offset = early.blob.len() - at.len();
        }
        early.announced += transactions.len();
        if !transactions.is_empty() {
            out.push(Event::Transactions { slot, transactions });
        }
    }
    emitted
}

/// Bytes of an entry ahead of its transactions: `num_hashes`, `hash`, and their count.
const ENTRY_HEADER: usize = 8 + 32 + 8;

/// Bytes of an entry batch's leading count of entries.
const ENTRY_COUNT: usize = 8;

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
    fn real_shreds_keep_the_set_shape_and_strict_drops_what_breaks_it() {
        let (data, code) = shred_component(600, &BlockComponent::EntryBatch(entries(4000)), true, 0);
        let mut plain = Assembler::new();
        feed(&mut plain, data.iter().chain(&code).cloned());
        assert_eq!(plain.off_spec(), 0, "agave's own shreds are 32:32 and in place");
        // Two parents for one slot: a second version, whatever the mode.
        let mut ev = Vec::new();
        let mut two = Assembler::new().strict();
        two.push(&data[0].payload().bytes, &mut ev);
        // From another set, so that no root of its own contradicts it.
        let later = data.iter().find(|s| s.fec_set_index() != data[0].fec_set_index()).expect("a second set");
        let mut other = later.payload().bytes.to_vec();
        other[64 + 19] ^= 1; // parent_offset, first byte of the data header
        two.push(&other, &mut ev);
        assert!(ev.contains(&Event::Equivocated { slot: 600 }), "{ev:?}");
        // A coding shred claiming another position than its index says is off.
        let mut bad = code[0].payload().bytes.to_vec();
        let at = 64 + 19 + 4; // coding header: num_data, num_code, position
        bad[at] ^= 1;
        let h = wire::parse_packet(&bad).expect("still parses");
        assert!(h.off_spec());
        let mut lax = Assembler::new();
        lax.push(&bad, &mut Vec::new());
        assert_eq!((lax.off_spec(), lax.active_slots()), (1, 1), "counted, and kept");
        let mut strict = Assembler::new().strict();
        strict.push(&bad, &mut Vec::new());
        assert_eq!((strict.off_spec(), strict.active_slots()), (1, 0), "counted, and dropped before it opens a slot");
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
        let lens: std::collections::BTreeSet<usize> = data.iter().chain(&code)
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
            Event::BatchEnd { slot: 800 },
            Event::SlotComplete { slot: 800 },
        ]);
    }

    #[test]
    fn parent_and_completion_survive_until_eviction() {
        let (data, _) = shred_component(500, &BlockComponent::EntryBatch(entries(3)), true, 0);
        let mut a = Assembler::new();
        assert_eq!(a.parent_slot(500), None);
        assert!(!a.is_complete(500));
        let events = feed(&mut a, data);
        assert!(matches!(events.last(), Some(Event::SlotComplete { slot: 500 })));
        assert!(a.is_complete(500));
        assert_eq!(a.parent_slot(500), Some(499), "Shredder::new(slot, slot - 1, ..)");
        a.evict_below(501);
        assert!(!a.is_complete(500));
        assert_eq!(a.parent_slot(500), None);
    }

    #[test]
    fn merkle_root_matches_agave_for_data_and_code() {
        let (data, code) = shred_component(7, &BlockComponent::EntryBatch(entries(40)), true, 0);
        assert!(!code.is_empty());
        for s in data.iter().chain(&code) {
            let packet = &s.payload().bytes;
            let ours = crate::wire::merkle_root(packet).expect("merkle root");
            let theirs = solana_ledger::shred::layout::get_merkle_root(packet).expect("agave root");
            assert_eq!(ours, theirs.to_bytes(), "index {}", s.index());
        }
    }

    #[test]
    fn a_set_under_two_roots_gives_the_slot_up() {
        let (data, code) = shred_component(1300, &BlockComponent::EntryBatch(entries(40)), true, 0);
        let roots: std::collections::BTreeSet<[u8; 32]> =
            data.iter().chain(&code).map(|s| crate::wire::merkle_root(&s.payload().bytes).unwrap()).collect();
        assert_eq!(roots.len(), 1, "data and coding shreds of a set prove one root");

        // The same slot and indices, other entries: what an equivocating leader sends.
        let (other, _) = shred_component(1300, &BlockComponent::EntryBatch(entries(41)), true, 0);
        let mut a = Assembler::new();
        feed(&mut a, [data[0].clone()]);
        assert_eq!(feed(&mut a, other), vec![Event::Equivocated { slot: 1300 }], "said once");
        assert_eq!(a.conflicts(), 1);
        assert!(feed(&mut a, data[1..].iter().cloned()).is_empty(), "neither version is assembled");
        assert_eq!((a.active_slots(), a.needed(1300, 10).len()), (0, 0), "nothing kept, nothing to repair");
        a.evict_below(1301);
        assert_eq!(a.conflicts(), 1);
    }

    #[test]
    fn asks_only_what_a_set_still_lacks() {
        let (data, code) = shred_component(1400, &BlockComponent::EntryBatch(entries(300)), true, 0);
        assert!(data.len() >= 6 && code.len() >= 2, "one set with room to drop some");
        let mut a = Assembler::new();
        // The last data shred tells the length; the first five are lost.
        feed(&mut a, data[5..].iter().cloned());
        assert_eq!(a.needed(1400, 100), vec![0, 1, 2, 3, 4], "no coding shred yet: every one");
        // Two coding shreds stand for two of them: three more rebuild the set.
        feed(&mut a, code[..2].iter().cloned());
        assert_eq!(a.needed(1400, 100), vec![0, 1, 2]);
        assert_eq!(a.needed(1400, 2), vec![0, 1], "limit is honoured");
        let ev = feed(&mut a, data[..3].iter().cloned());
        assert!(a.is_complete(1400), "rebuilt with exactly what was asked for");
        assert_eq!(collected(&ev), entries(300));
        assert!(a.needed(1400, 100).is_empty());
    }

    #[test]
    fn a_shred_past_what_a_slot_holds_is_refused() {
        let (data, code) = shred_component(1500, &BlockComponent::EntryBatch(entries(5)), true, 0);
        let at = |packet: &[u8], index: u32, set: u32| {
            let mut p = packet.to_vec();
            p[73..77].copy_from_slice(&index.to_le_bytes());
            p[79..83].copy_from_slice(&set.to_le_bytes());
            p
        };
        let data = data[0].payload().bytes.to_vec();
        let code = code[0].payload().bytes.to_vec();
        let max = wire::MAX_SHREDS_PER_SLOT;
        assert!(wire::parse_packet(&at(&data, max - 1, max - 1)).is_some(), "the last index a slot holds");
        assert!(wire::parse_packet(&at(&data, max, max)).is_none(), "one past it");
        assert!(wire::parse_packet(&at(&data, u32::MAX, 0)).is_none(), "a gap report would walk 4 billion indices");
        assert!(wire::parse_packet(&at(&data, 3, 4)).is_none(), "a data shred before its own set");
        assert!(wire::parse_packet(&at(&code, 0, max - 1)).is_none(), "a set whose data would run past the slot");
    }

    #[test]
    fn a_data_shred_naming_another_set_than_its_own_gives_the_slot_up() {
        let (data, code) = shred_component(1600, &BlockComponent::EntryBatch(entries(300)), true, 0);
        assert!(data.len() >= 6, "one set with room for a stray");
        // A second version whose shreds start at index 3 as a set of their own:
        // its index 4 falls in the first version's set, which starts at 0.
        let (other, _) = shred_component(1600, &BlockComponent::EntryBatch(entries(301)), true, 3);
        let stray = || other.iter().filter(|s| s.index() == 4).cloned();
        let mut a = Assembler::new();
        feed(&mut a, [code[0].clone()]);
        assert_eq!(feed(&mut a, stray()), vec![Event::Equivocated { slot: 1600 }], "index 4 is set 0's");
        // The other way round: the stray came first, the set's size after.
        let mut b = Assembler::new();
        assert!(feed(&mut b, stray()).is_empty(), "alone it contradicts nothing yet");
        assert_eq!(feed(&mut b, [code[0].clone()]), vec![Event::Equivocated { slot: 1600 }]);
    }

    #[test]
    fn a_given_up_slot_takes_nothing_more() {
        let (data, code) = shred_component(1700, &BlockComponent::EntryBatch(entries(300)), true, 0);
        let (other, _) = shred_component(1700, &BlockComponent::EntryBatch(entries(301)), true, 3);
        let mut a = Assembler::new();
        feed(&mut a, [code[0].clone()]);
        let ev = feed(&mut a, other.iter().filter(|s| s.index() == 4 || s.index() == 5).cloned().chain(data.iter().cloned()));
        assert_eq!(ev, vec![Event::Equivocated { slot: 1700 }]);
        assert_eq!(a.conflicts(), 1, "later packets of the slot are not even looked at");
    }

    #[test]
    fn a_set_sized_over_a_known_one_gives_the_slot_up() {
        let (data, code) = shred_component(1800, &BlockComponent::EntryBatch(entries(300)), true, 0);
        let (_, other_code) = shred_component(1800, &BlockComponent::EntryBatch(entries(301)), true, 3);
        let mut a = Assembler::new();
        feed(&mut a, data[..10].iter().chain(&code[..1]).cloned());
        assert_eq!(feed(&mut a, other_code[..1].iter().cloned()), vec![Event::Equivocated { slot: 1800 }]);
        // An honest slot next to it is untouched.
        let (next, _) = shred_component(1801, &BlockComponent::EntryBatch(entries(5)), true, 0);
        assert_eq!(collected(&feed(&mut a, next)), entries(5));
    }

    #[test]
    fn a_batch_starts_streaming_before_its_last_shreds() {
        let want = entries(300);
        let (data, _) = shred_component(1900, &BlockComponent::EntryBatch(want.clone()), true, 0);
        assert!(data.len() >= 6, "a batch over many shreds: {}", data.len());
        let mut a = Assembler::new();
        let mut early = Vec::new();
        for (k, shred) in data[..data.len() - 1].iter().enumerate() {
            let got = collected(&feed(&mut a, [shred.clone()]));
            assert!(got.len() <= want.len());
            if k > 0 {
                assert!(!got.is_empty() || early.len() == want.len(), "shred {k} added entries");
            }
            early.extend(got);
        }
        assert!(!early.is_empty(), "entries out before the batch is whole");
        assert_eq!(early, want[..early.len()], "in order, from the start");
        let ev = feed(&mut a, [data[data.len() - 1].clone()]);
        let rest = collected(&ev);
        assert_eq!(a.early_entries() as usize, early.len());
        assert_eq!([early, rest].concat(), want, "the rest once whole, nothing twice");
        assert_eq!(ev.last(), Some(&Event::SlotComplete { slot: 1900 }));
    }

    #[test]
    fn nothing_streams_before_a_batch_begins() {
        let want = entries(300);
        let (data, _) = shred_component(2000, &BlockComponent::EntryBatch(want.clone()), true, 0);
        let mut a = Assembler::new();
        assert!(collected(&feed(&mut a, data[1..data.len() - 1].iter().cloned())).is_empty(), "its first shred is missing");
        let early = collected(&feed(&mut a, [data[0].clone()]));
        assert!(!early.is_empty(), "the run from the start is read at once");
        let rest = collected(&feed(&mut a, [data[data.len() - 1].clone()]));
        assert_eq!([early, rest].concat(), want);
    }

    #[test]
    fn transactions_go_out_one_by_one_ahead_of_their_entry() {
        // Few entries, each of many transactions: an entry spans several shreds.
        let tx = |k: u32| VersionedTransaction {
            signatures: vec![solana_signature::Signature::from([(k % 251) as u8; 64]); 1 + (k % 3) as usize],
            ..VersionedTransaction::default()
        };
        let want: Vec<Entry> = (0..4u32)
            .map(|e| Entry { num_hashes: e.into(), hash: Hash::default(), transactions: (0..200).map(|k| tx(e * 1000 + k)).collect() })
            .collect();
        let all: Vec<VersionedTransaction> = want.iter().flat_map(|e| e.transactions.clone()).collect();
        let (data, _) = shred_component(2100, &BlockComponent::EntryBatch(want.clone()), true, 0);
        assert!(data.len() > 8, "an entry over several shreds: {} shreds", data.len());

        let mut a = Assembler::new();
        let (mut streamed, mut ahead, mut entries) = (Vec::new(), 0, Vec::new());
        for shred in &data {
            for ev in feed(&mut a, [shred.clone()]) {
                match ev {
                    Event::Transactions { transactions, .. } => {
                        ahead += transactions.len();
                        streamed.extend(transactions);
                    }
                    Event::Entries { entries: got, announced, .. } => {
                        // What was announced is not given twice.
                        for (k, e) in got.iter().enumerate() {
                            streamed.extend(e.transactions.iter().skip(if k == 0 { announced } else { 0 }).cloned());
                        }
                        entries.extend(got);
                    }
                    _ => {}
                }
            }
        }
        assert!(ahead > 0, "some went out before their entry was whole");
        assert_eq!(streamed, all, "every transaction once, in order");
        assert_eq!(entries, want, "the entries themselves whole");
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
    fn reports_gaps_but_never_below_the_emitted_frontier() {
        let (data, _) = shred_component(1100, &BlockComponent::EntryBatch(entries(100)), true, 0);
        let last = data.len() as u32 - 1;
        let mut a = Assembler::new();
        // Nothing yet: no length, no gaps to report.
        assert_eq!(a.last_index(1100), None);
        assert_eq!(a.missing(1100, 10), Vec::<u32>::new());
        // Only the last shred: length known, everything before it missing.
        feed(&mut a, [data[last as usize].clone()]);
        assert_eq!(a.last_index(1100), Some(last));
        assert_eq!(a.received(1100), Some(last));
        assert_eq!(a.missing(1100, 3), vec![0, 1, 2], "limit is honoured");
        assert_eq!(a.missing(1100, 1000).len(), last as usize);
        // Fill all but index 1: the report is exactly that one hole.
        feed(&mut a, data.iter().enumerate().filter(|(i, _)| *i != 1 && *i != last as usize).map(|(_, s)| s.clone()));
        assert_eq!(a.missing(1100, 10), vec![1]);
        // Fill it: slot completes, state is gone, nothing to report.
        feed(&mut a, [data[1].clone()]);
        assert_eq!(a.active_slots(), 0);
        assert_eq!(a.missing(1100, 10), Vec::<u32>::new());
    }

    #[test]
    fn missing_uses_received_when_length_is_unknown() {
        let (data, _) = shred_component(1200, &BlockComponent::EntryBatch(entries(100)), true, 0);
        assert!(data.len() >= 4);
        let mut a = Assembler::new();
        feed(&mut a, [data[3].clone()]);
        assert_eq!(a.last_index(1200), None, "last shred not seen");
        assert_eq!(a.received(1200), Some(3));
        assert_eq!(a.missing(1200, 10), vec![0, 1, 2], "gaps up to the highest seen index");
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
