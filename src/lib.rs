#![warn(clippy::pedantic)]

use std::collections::BTreeMap;

pub struct LSMTree {
    // memtable - keys get written here first, and its the first place we start lookups
    memtable: BTreeMap<Vec<u8>, Vec<u8>>,

    // levels - mock "disk" layout
    levels: Vec<Option<LSMLevel>>,

    // threshold for flushing memtable to disk
    memtable_flush_threshold: usize,
}

pub struct LSMLevel {
    #[allow(dead_code)]
    data: Vec<(Vec<u8>, Vec<u8>)>,
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

    /// insert the key-value pair into self.memtable (it's a `BTreeMap`)
    /// check if memtable size has reached `self.memtable_threshold`
    /// if threshold reached, call `self.flush_memtable()`
    pub fn insert(&mut self, key: Vec<u8>, value: Vec<u8>) {
        self.memtable.insert(key, value);

        if self.memtable.len() >= self.memtable_flush_threshold {
            self.flush_memtable();
        }
    }

    // https://corrode.dev/blog/defensive-programming/#pattern-use-must-use-on-important-types
    #[must_use]
    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        if let Some(value) = self.memtable.get(key) {
            return Some(value.clone());
        }

        None
    }

    fn flush_memtable(&mut self) {
        let mut new_level_data = vec![];

        // std::mem::take takes ownership of the value and replaces with an empty value
        for (key, value) in std::mem::take(&mut self.memtable) {
            new_level_data.push((key, value));
        }

        self.merge_into_level(0, new_level_data);
    }

    fn merge_into_level(&mut self, level: usize, new_data: Vec<(Vec<u8>, Vec<u8>)>) {
        if level >= self.levels.len() {
            self.levels.push(Some(LSMLevel { data: new_data }));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_insert_and_get() {
        let mut lsm = LSMTree::new(3);
        lsm.insert(b"key1".to_vec(), b"value1".to_vec());
        lsm.insert(b"key2".to_vec(), b"value2".to_vec());

        assert_eq!(lsm.get(b"key1"), Some(b"value1".to_vec()));
        assert_eq!(lsm.get(b"key2"), Some(b"value2".to_vec()));
        assert_eq!(lsm.get(b"key3"), None);
    }

    #[test]
    #[ignore]
    fn test_memtable_flush() {
        let mut lsm = LSMTree::new(2);

        lsm.insert(b"k1".to_vec(), b"v1".to_vec());
        lsm.insert(b"k2".to_vec(), b"v2".to_vec());

        assert_eq!(lsm.memtable.len(), 0);
        assert_eq!(lsm.levels.len(), 1);
        assert!(lsm.levels[0].is_some());
        assert_eq!(lsm.levels[0].as_ref().unwrap().data.len(), 2);

        assert_eq!(lsm.get(b"k1"), Some(b"v1".to_vec()));
        assert_eq!(lsm.get(b"k2"), Some(b"v2".to_vec()));
    }
}
