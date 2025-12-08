#![warn(clippy::pedantic)]

use std::collections::BTreeMap;

pub struct LSMTree {
    // memtable - keys get written here first, and its the first place we start lookups
    // BTreeMap is a sorted map
    memtable: BTreeMap<Vec<u8>, Option<Vec<u8>>>,

    // levels - mock "disk" layout
    levels: Vec<Option<LSMLevel>>,

    // threshold for flushing memtable to disk
    memtable_flush_threshold: usize,
}

pub struct LSMLevel {
    data: Vec<(Vec<u8>, Option<Vec<u8>>)>,
}

impl LSMTree {
    #[must_use]
    pub fn new(memtable_flush_threshold: usize) -> Self {
        let memtable = BTreeMap::new();
        LSMTree {
            memtable,
            levels: vec![],
            memtable_flush_threshold,
        }
    }

    /// returns the capacity for a given level
    ///
    /// each level can hold threshold × 2 ^ level
    /// this balances write amplification with read performance and space usage
    ///
    /// examples:
    /// 1. if each level had the same capacity, on every write, we would flush through every level
    /// 2. if this was a large ratio (for example 10^level), we do compaction less frequently, but:
    ///   - waste more space - arent getting rid of stale values
    ///   - reads are slower - more in a level, slower binary search is
    fn level_capacity(&self, level: usize) -> usize {
        self.memtable_flush_threshold << level
    }

    /// insert the key-value pair into self.memtable (it's a `BTreeMap`)
    /// check if memtable size has reached `self.memtable_threshold`
    /// if threshold reached, call `self.flush_memtable()` to write it to level 0
    pub fn insert(&mut self, key: Vec<u8>, value: Option<Vec<u8>>) {
        self.memtable.insert(key, value);

        if self.memtable.len() >= self.memtable_flush_threshold {
            self.flush_memtable();
        }
    }

    /// deletes a key by inserting a tombstone (`None`) for that key
    pub fn delete(&mut self, key: Vec<u8>) {
        self.insert(key, None);
    }

    /// get a given key
    ///
    /// first checks memtable, then iterates through levels newest-to-oldest, binary searching each
    /// level
    ///
    // https://corrode.dev/blog/defensive-programming/#pattern-use-must-use-on-important-types
    #[must_use]
    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        if let Some(value) = self.memtable.get(key) {
            return value.clone();
        }

        for level in &self.levels {
            let Some(level) = level else {
                continue;
            };

            // we have a guarantee that the keys are in sorted order, because the memtable is a
            // BTreeMap. when we flush the memtable to a level, we iterate through the keys in
            // order
            if let Ok(pos) = level.data.binary_search_by(|(k, _)| k.as_slice().cmp(key)) {
                return level.data[pos].1.clone();
            }
        }

        None
    }

    /// flushes memtable data to level 0
    fn flush_memtable(&mut self) {
        let mut new_level_data = vec![];

        // std::mem::take takes ownership of the value and replaces with an empty value
        for (key, value) in std::mem::take(&mut self.memtable) {
            new_level_data.push((key, value));
        }

        self.merge_into_level(0, new_level_data);
    }

    fn merge_into_level(&mut self, level: usize, new_data: Vec<(Vec<u8>, Option<Vec<u8>>)>) {
        if level >= self.levels.len() {
            self.levels.push(Some(LSMLevel { data: new_data }));
            return;
        }

        let existing_data = self.levels[level]
            .take()
            .map(|l| l.data)
            .unwrap_or_default();

        let data = merge_sorted(&existing_data, &new_data);

        // cascading compaction - check if merged data exceeds level capacity (see `level_capacity` for notes)
        // if so, merge into the next level. if not, set current level data
        if data.len() >= self.level_capacity(level) {
            self.merge_into_level(level + 1, data);
        } else {
            self.levels[level] = Some(LSMLevel { data });
        }
    }
}

/// merge 2 sorted vecs
///
/// when merging:
/// 1. push smaller key into result
/// 2. if equal, use `new_data`
/// 3. when list runs out, go to the end of the other list
fn merge_sorted(
    old_data: &[(Vec<u8>, Option<Vec<u8>>)],
    new_data: &[(Vec<u8>, Option<Vec<u8>>)],
) -> Vec<(Vec<u8>, Option<Vec<u8>>)> {
    let mut merged = vec![];
    let mut i = 0;
    let mut j = 0;

    // while we still have data remaining in both lists
    while i < old_data.len() && j < new_data.len() {
        match old_data[i].0.cmp(&new_data[j].0) {
            std::cmp::Ordering::Less => {
                merged.push(old_data[i].clone());
                i += 1;
            }
            std::cmp::Ordering::Greater => {
                merged.push(new_data[j].clone());
                j += 1;
            }
            std::cmp::Ordering::Equal => {
                merged.push(new_data[j].clone());
                i += 1;
                j += 1;
            }
        }
    }

    // while we still have data remaining in existing
    while i < old_data.len() {
        merged.push(old_data[i].clone());
        i += 1;
    }

    // while we still have data remaining in new
    while j < new_data.len() {
        merged.push(new_data[j].clone());
        j += 1;
    }

    merged
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_insert_and_get() {
        let mut lsm = LSMTree::new(3);
        lsm.insert(b"key1".to_vec(), Some(b"value1".to_vec()));
        lsm.insert(b"key2".to_vec(), Some(b"value2".to_vec()));

        assert_eq!(lsm.get(b"key1"), Some(b"value1".to_vec()));
        assert_eq!(lsm.get(b"key2"), Some(b"value2".to_vec()));
        assert_eq!(lsm.get(b"key3"), None);
    }

    #[test]
    fn test_memtable_flush() {
        let mut lsm = LSMTree::new(2);

        lsm.insert(b"k1".to_vec(), Some(b"v1".to_vec()));
        lsm.insert(b"k2".to_vec(), Some(b"v2".to_vec()));

        assert_eq!(lsm.memtable.len(), 0);
        assert_eq!(lsm.levels.len(), 1);
        assert!(lsm.levels[0].is_some());
        assert_eq!(lsm.levels[0].as_ref().unwrap().data.len(), 2);

        assert_eq!(lsm.get(b"k1"), Some(b"v1".to_vec()));
        assert_eq!(lsm.get(b"k2"), Some(b"v2".to_vec()));
    }

    #[test]
    fn test_level_merge() {
        // Threshold 2.
        // Level 0 capacity: 2 * 2^0 = 2.
        // Level 1 capacity: 2 * 2^1 = 4.
        let mut lsm = LSMTree::new(2);

        // 1. Insert 2 items -> Flush to L0. L0 size 2.
        lsm.insert(b"a".to_vec(), Some(b"1".to_vec()));
        lsm.insert(b"b".to_vec(), Some(b"2".to_vec()));

        assert_eq!(lsm.levels.len(), 1);
        assert_eq!(lsm.levels[0].as_ref().unwrap().data.len(), 2);

        // 2. Insert 2 items -> Flush to L0.
        // Merge (L0 existing) + (New) = 4 items.
        // 4 > L0 capacity (2). So push to L1.
        lsm.insert(b"c".to_vec(), Some(b"3".to_vec()));
        lsm.insert(b"d".to_vec(), Some(b"4".to_vec()));

        assert_eq!(lsm.levels.len(), 2); // Should have created L1
        assert!(lsm.levels[0].is_none()); // L0 data moved up
        assert!(lsm.levels[1].is_some()); // L1 has the data
        assert_eq!(lsm.levels[1].as_ref().unwrap().data.len(), 4);

        assert_eq!(lsm.get(b"a"), Some(b"1".to_vec()));
        assert_eq!(lsm.get(b"d"), Some(b"4".to_vec()));
    }

    #[test]
    fn test_overwrite() {
        let mut lsm = LSMTree::new(2);
        lsm.insert(b"key1".to_vec(), Some(b"val1".to_vec()));
        lsm.insert(b"key1".to_vec(), Some(b"val2".to_vec())); // Overwrite in memtable

        assert_eq!(lsm.get(b"key1"), Some(b"val2".to_vec()));

        // Flush
        lsm.insert(b"key2".to_vec(), Some(b"val3".to_vec())); // Trigger flush (size 2)

        // Now key1 is in L0 with val2.
        assert_eq!(lsm.get(b"key1"), Some(b"val2".to_vec()));

        // Overwrite again in memtable
        lsm.insert(b"key1".to_vec(), Some(b"val3".to_vec()));
        assert_eq!(lsm.get(b"key1"), Some(b"val3".to_vec()));
    }

    #[test]
    fn test_delete() {
        let mut lsm = LSMTree::new(2);
        lsm.insert(b"key1".to_vec(), Some(b"val1".to_vec()));

        // flush
        lsm.insert(b"key2".to_vec(), Some(b"val2".to_vec()));
        lsm.delete(b"key1".to_vec());

        assert_eq!(lsm.get(b"key1"), None);
    }
}
