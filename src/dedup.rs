use std::collections::{HashMap, HashSet, VecDeque};

/// Key that uniquely identifies a shred within a slot.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct ShredKey {
    pub slot:    u64,
    pub index:   u32,
    pub is_data: bool,
}

pub(crate) struct Deduplicator {
    /// slot => set of (index, is_data) seen
    seen: HashMap<u64, HashSet<(u32, bool)>>,
    /// FIFO eviction order
    slot_order: VecDeque<u64>,
    max_slots: usize,
}

impl Deduplicator {
    pub fn new(max_slots: usize) -> Self {
        Self {
            seen: HashMap::new(),
            slot_order: VecDeque::new(),
            max_slots,
        }
    }

    pub fn is_new(&mut self, key: ShredKey) -> bool {
        let entry = self.seen.entry(key.slot).or_insert_with(|| {
            self.slot_order.push_back(key.slot);
            HashSet::new()
        });

        let pair = (key.index, key.is_data);
        if entry.contains(&pair) {
            return false;
        }
        entry.insert(pair);

        while self.slot_order.len() > self.max_slots {
            if let Some(old_slot) = self.slot_order.pop_front() {
                self.seen.remove(&old_slot);
            }
        }
        true
    }

    pub fn tracked_slot_count(&self) -> usize {
        self.seen.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(slot: u64, index: u32, is_data: bool) -> ShredKey {
        ShredKey { slot, index, is_data }
    }

    #[test]
    fn test_new_shred_passes() {
        let mut d = Deduplicator::new(100);
        assert!(d.is_new(key(1, 0, true)));
    }

    #[test]
    fn test_duplicate_shred_blocked() {
        let mut d = Deduplicator::new(100);
        assert!(d.is_new(key(1, 0, true)));
        assert!(!d.is_new(key(1, 0, true)));
    }

    #[test]
    fn test_same_index_different_type_allowed() {
        let mut d = Deduplicator::new(100);
        assert!(d.is_new(key(1, 0, true)));
        assert!(d.is_new(key(1, 0, false)));
    }

    #[test]
    fn test_evicts_oldest_slot() {
        let mut d = Deduplicator::new(2);
        d.is_new(key(100, 0, true));
        d.is_new(key(200, 0, true));
        d.is_new(key(300, 0, true));
        assert!(d.is_new(key(100, 0, true)));
    }

    #[test]
    fn test_different_slots_independent() {
        let mut d = Deduplicator::new(100);
        assert!(d.is_new(key(1, 5, true)));
        assert!(d.is_new(key(2, 5, true)));
    }
}
