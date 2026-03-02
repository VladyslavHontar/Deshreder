mod assembler;
mod dedup;

use assembler::SlotAssembler;
use dedup::{Deduplicator, ShredKey};

pub use solana_entry::entry::Entry;

/// A slot together with its assembled entries and the pre-encoded bytes.
///
/// `entries_bytes` is `bincode::serialize(&entries)` — ready to stuff into
/// the Jito-compatible gRPC proto `Entry { slot, entries: entries_bytes }`.
pub struct SlotEntries {
    pub slot:          u64,
    pub entries:       Vec<Entry>,
    pub entries_bytes: Vec<u8>,
}

/// Pure shred-to-entry assembler. Zero I/O — no networking, no files, no
/// async runtime required. Feed raw UDP shred packets, receive Solana entries.
pub struct Deshredder {
    assembler: SlotAssembler,
    dedup:     Deduplicator,
}

impl Deshredder {
    /// * `slot_timeout_ms` — evict stale slots after N ms. Recommend `3000`.
    /// * `max_tracked_slots` — dedup history depth. Recommend `200`.
    pub fn new(slot_timeout_ms: u64, max_tracked_slots: usize) -> Self {
        Self {
            assembler: SlotAssembler::new(slot_timeout_ms),
            dedup:     Deduplicator::new(max_tracked_slots),
        }
    }

    /// Feed one raw UDP shred packet. Returns newly assembled entries for the
    /// completed slot, or `None` on duplicates, parse errors, or when the slot
    /// is not yet complete.
    pub fn push_raw(&mut self, bytes: &[u8]) -> Option<SlotEntries> {
        let shred = match solana_ledger::shred::Shred::new_from_serialized_shred(bytes.to_vec()) {
            Ok(s)  => s,
            Err(_) => return None,
        };

        let key = ShredKey {
            slot:    shred.slot(),
            index:   shred.index(),
            is_data: shred.is_data(),
        };
        if !self.dedup.is_new(key) {
            return None;
        }

        let (slot, entries) = self.assembler.add_shred(shred);
        if entries.is_empty() {
            return None;
        }

        let entries_bytes = match bincode::serialize(&entries) {
            Ok(b)  => b,
            Err(e) => {
                eprintln!("[deshredder] bincode::serialize failed for slot {slot}: {e}");
                return None;
            }
        };

        Some(SlotEntries { slot, entries, entries_bytes })
    }

    /// Evict stale slot buffers to bound memory. Call every ~100ms.
    pub fn evict_expired(&mut self) -> usize {
        self.assembler.evict_expired()
    }

    /// Number of slots currently buffering shreds (bounded by calling `evict_expired`).
    pub fn active_slot_count(&self) -> usize {
        self.assembler.active_slot_count()
    }

    /// Number of slots retained in the deduplication history window.
    pub fn tracked_slot_count(&self) -> usize {
        self.dedup.tracked_slot_count()
    }
}
