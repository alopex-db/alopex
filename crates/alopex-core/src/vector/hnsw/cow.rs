//! Copy-on-write storage for graph snapshots. Payloads detach only on mutation.

use std::collections::hash_map::RandomState;
use std::collections::HashMap;
use std::hash::BuildHasher;
use std::ops::{Deref, DerefMut, Index, IndexMut};
use std::sync::Arc;

use super::types::HnswNode;

const PAGE_SIZE: usize = 128;
const KEY_SHARDS: usize = 256;

#[derive(Clone, Debug)]
pub(crate) struct SharedNode(Arc<HnswNode>);
impl From<HnswNode> for SharedNode {
    fn from(node: HnswNode) -> Self {
        Self(Arc::new(node))
    }
}
impl Deref for SharedNode {
    type Target = HnswNode;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
impl DerefMut for SharedNode {
    fn deref_mut(&mut self) -> &mut Self::Target {
        Arc::make_mut(&mut self.0)
    }
}

/// A snapshot shares the directory and pages. A write detaches one directory
/// and at most one page; node payloads have a separate copy-on-write boundary.
#[derive(Clone, Debug)]
pub(crate) struct PagedVec<T> {
    pages: Arc<Vec<Arc<Vec<T>>>>,
    len: usize,
}
impl<T> Default for PagedVec<T> {
    fn default() -> Self {
        Self {
            pages: Arc::new(Vec::new()),
            len: 0,
        }
    }
}
impl<T> PagedVec<T> {
    pub(crate) fn len(&self) -> usize {
        self.len
    }
    pub(crate) fn capacity(&self) -> usize {
        self.pages.len() * PAGE_SIZE
    }
    pub(crate) fn get(&self, index: usize) -> Option<&T> {
        self.pages.get(index / PAGE_SIZE)?.get(index % PAGE_SIZE)
    }
    pub(crate) fn iter(&self) -> impl Iterator<Item = &T> {
        self.pages.iter().flat_map(|page| page.iter())
    }
    #[cfg(all(test, not(target_arch = "wasm32")))]
    pub(crate) fn contains(&self, value: &T) -> bool
    where
        T: PartialEq,
    {
        self.iter().any(|item| item == value)
    }
}
impl<T: Clone> PagedVec<T> {
    pub(crate) fn get_mut(&mut self, index: usize) -> Option<&mut T> {
        if index >= self.len {
            return None;
        }
        let pages = Arc::make_mut(&mut self.pages);
        Arc::make_mut(&mut pages[index / PAGE_SIZE]).get_mut(index % PAGE_SIZE)
    }
    pub(crate) fn push(&mut self, value: T) {
        let pages = Arc::make_mut(&mut self.pages);
        if self.len.is_multiple_of(PAGE_SIZE) {
            pages.push(Arc::new(Vec::with_capacity(PAGE_SIZE)));
        }
        Arc::make_mut(pages.last_mut().expect("page allocated")).push(value);
        self.len += 1;
    }
    pub(crate) fn pop(&mut self) -> Option<T> {
        if self.len == 0 {
            return None;
        }
        let pages = Arc::make_mut(&mut self.pages);
        let page = Arc::make_mut(pages.last_mut().expect("nonempty pages"));
        let value = page.pop();
        if page.is_empty() {
            pages.pop();
        }
        self.len -= 1;
        value
    }
}
impl<T> Index<usize> for PagedVec<T> {
    type Output = T;
    fn index(&self, index: usize) -> &T {
        self.get(index).expect("node index in bounds")
    }
}
impl<T: Clone> IndexMut<usize> for PagedVec<T> {
    fn index_mut(&mut self, index: usize) -> &mut T {
        self.get_mut(index).expect("node index in bounds")
    }
}
impl<T: Clone> FromIterator<T> for PagedVec<T> {
    fn from_iter<I: IntoIterator<Item = T>>(values: I) -> Self {
        let mut result = Self::default();
        for value in values {
            result.push(value);
        }
        result
    }
}
impl<'a, T> IntoIterator for &'a PagedVec<T> {
    type Item = &'a T;
    type IntoIter = std::iter::FlatMap<
        std::slice::Iter<'a, Arc<Vec<T>>>,
        std::slice::Iter<'a, T>,
        fn(&'a Arc<Vec<T>>) -> std::slice::Iter<'a, T>,
    >;
    fn into_iter(self) -> Self::IntoIter {
        self.pages.iter().flat_map(|page| page.iter())
    }
}

type KeyShard = Arc<HashMap<Vec<u8>, u32>>;
/// A random shard hash is stable across snapshots, but is not serialized.
#[derive(Clone)]
pub(crate) struct KeyMap {
    shards: Arc<Vec<Option<KeyShard>>>,
    hash: RandomState,
}
impl Default for KeyMap {
    fn default() -> Self {
        Self {
            shards: Arc::new(vec![None; KEY_SHARDS]),
            hash: RandomState::new(),
        }
    }
}
impl KeyMap {
    fn shard(&self, key: &[u8]) -> usize {
        self.hash.hash_one(key) as usize % KEY_SHARDS
    }
    pub(crate) fn get(&self, key: &[u8]) -> Option<&u32> {
        self.shards[self.shard(key)].as_ref()?.get(key)
    }
    pub(crate) fn contains_key(&self, key: &[u8]) -> bool {
        self.get(key).is_some()
    }
    pub(crate) fn insert(&mut self, key: Vec<u8>, value: u32) -> Option<u32> {
        let shard = self.shard(&key);
        let shards = Arc::make_mut(&mut self.shards);
        let map = shards[shard].get_or_insert_with(|| Arc::new(HashMap::new()));
        Arc::make_mut(map).insert(key, value)
    }
    pub(crate) fn remove(&mut self, key: &[u8]) -> Option<u32> {
        let shard = self.shard(key);
        if !self.shards[shard].as_ref()?.contains_key(key) {
            return None;
        }
        let shards = Arc::make_mut(&mut self.shards);
        let map = shards[shard].as_mut().expect("existing key shard");
        let removed = Arc::make_mut(map).remove(key);
        if map.is_empty() {
            shards[shard] = None;
        }
        removed
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use std::cell::Cell;
    use std::rc::Rc;

    #[derive(Debug)]
    struct Counted(Rc<Cell<usize>>, usize);
    impl Clone for Counted {
        fn clone(&self) -> Self {
            self.0.set(self.0.get() + 1);
            Self(self.0.clone(), self.1)
        }
    }

    #[test]
    fn snapshot_copies_no_elements_and_mutation_copies_only_one_page() {
        let clones = Rc::new(Cell::new(0));
        let original: PagedVec<_> = (0..(PAGE_SIZE * 2 + 1))
            .map(|i| Counted(clones.clone(), i))
            .collect();
        let mut snapshot = original.clone();
        assert_eq!(clones.get(), 0);
        snapshot[PAGE_SIZE].1 = 999;
        assert_eq!(clones.get(), PAGE_SIZE);
        assert_eq!(original[PAGE_SIZE].1, PAGE_SIZE);
        assert_eq!(snapshot[PAGE_SIZE].1, 999);
        assert_eq!(original[0].1, 0);
        assert!(snapshot.get_mut(original.len()).is_none());
        assert_eq!(clones.get(), PAGE_SIZE);
    }

    #[test]
    fn node_page_detachment_does_not_copy_unmodified_payloads() {
        let node = || {
            SharedNode::from(HnswNode {
                key: b"a".to_vec(),
                vector: vec![1.0, 2.0],
                norm: 5.0_f64.sqrt(),
                metadata: vec![3],
                neighbors: vec![vec![]],
                deleted: false,
            })
        };
        let original: PagedVec<_> = [Some(node()), Some(node())].into_iter().collect();
        let mut copy = original.clone();
        copy[0].as_mut().unwrap().deleted = true;
        assert!(!original[0].as_ref().unwrap().deleted);
        assert!(!Arc::ptr_eq(
            &original[0].as_ref().unwrap().0,
            &copy[0].as_ref().unwrap().0
        ));
        assert!(Arc::ptr_eq(
            &original[1].as_ref().unwrap().0,
            &copy[1].as_ref().unwrap().0
        ));
    }

    #[test]
    fn pop_and_reappend_at_page_boundaries_preserve_snapshots() {
        let original: PagedVec<_> = (0..PAGE_SIZE + 1).collect();
        let mut copy = original.clone();
        assert_eq!(copy.pop(), Some(PAGE_SIZE));
        assert_eq!(copy.capacity(), PAGE_SIZE);
        copy.push(999);
        assert_eq!(copy[PAGE_SIZE], 999);
        assert_eq!(original[PAGE_SIZE], PAGE_SIZE);
        for expected in (0..PAGE_SIZE).rev() {
            if expected == PAGE_SIZE - 1 {
                assert_eq!(copy.pop(), Some(999));
            }
            assert_eq!(copy.pop(), Some(expected));
        }
        assert_eq!(copy.pop(), None);
        assert_eq!(copy.len(), 0);
        assert_eq!(copy.capacity(), 0);
        assert!(copy.get(0).is_none());
        copy.push(7);
        assert_eq!(copy[0], 7);
        assert_eq!(original.len(), PAGE_SIZE + 1);
    }

    #[test]
    fn key_snapshots_detach_only_one_shard_and_preserve_collisions() {
        let mut original = KeyMap::default();
        for key in 0..=KEY_SHARDS as u32 {
            original.insert(key.to_le_bytes().to_vec(), key);
        }
        assert!(
            original
                .shards
                .iter()
                .filter(|shard| shard.is_some())
                .count()
                > 1
        );
        let mut copy = original.clone();
        assert!(Arc::ptr_eq(&original.shards, &copy.shards));
        copy.insert(0u32.to_le_bytes().to_vec(), 999);
        let changed = original
            .shards
            .iter()
            .zip(copy.shards.iter())
            .filter(|(left, right)| match (left, right) {
                (Some(left), Some(right)) => !Arc::ptr_eq(left, right),
                _ => false,
            })
            .count();
        assert_eq!(changed, 1);
        for key in 0..=KEY_SHARDS as u32 {
            assert_eq!(original.get(&key.to_le_bytes()), Some(&key));
            assert_eq!(
                copy.get(&key.to_le_bytes()),
                Some(&if key == 0 { 999 } else { key })
            );
        }
        // 257 keys in 256 shards guarantee at least one collision.
        let collision = original
            .shards
            .iter()
            .flatten()
            .find(|shard| shard.len() > 1)
            .unwrap();
        let keys: Vec<_> = collision.keys().take(2).cloned().collect();
        let untouched = *copy.get(&keys[1]).unwrap();
        assert!(copy.remove(&keys[0]).is_some());
        assert_eq!(copy.get(&keys[1]), Some(&untouched));
        assert!(original.contains_key(&keys[0]));
        assert!(copy.remove(b"missing").is_none());
    }

    #[test]
    fn index_fork_and_transaction_before_image_share_node_payloads() {
        use super::super::{HnswConfig, HnswIndex, HnswTransactionState};
        let mut index = HnswIndex::create("cow", HnswConfig::default().with_dimension(2)).unwrap();
        for key in 0..4u8 {
            index.upsert(&[key], &[key as f32, 1.0], &[]).unwrap();
        }
        let fork = index.fork_owned();
        let original = index.graph.read().unwrap();
        let forked = fork.graph.read().unwrap();
        assert!(Arc::ptr_eq(&original.nodes.pages, &forked.nodes.pages));
        assert!(Arc::ptr_eq(
            &original.key_to_node.shards,
            &forked.key_to_node.shards
        ));
        assert!(Arc::ptr_eq(
            &original.free_list.pages,
            &forked.free_list.pages
        ));
        let mut state = HnswTransactionState::default();
        state.ensure_snapshot(&forked);
        let before = state.snapshot.as_ref().unwrap();
        assert!(Arc::ptr_eq(&before.nodes.pages, &forked.nodes.pages));
        for (node, saved) in original.nodes.iter().zip(before.nodes.iter()) {
            assert!(Arc::ptr_eq(
                &node.as_ref().unwrap().0,
                &saved.as_ref().unwrap().0
            ));
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(32))]
        #[test]
        fn paged_edits_match_vec_and_preserve_snapshots(ops in prop::collection::vec((0u8..4, 0usize..(PAGE_SIZE + 3)), 1..32)) {
            let mut actual: PagedVec<_> = (0..PAGE_SIZE + 2).collect();
            let mut expected: Vec<_> = (0..PAGE_SIZE + 2).collect();
            for (operation, value) in ops {
                let before = actual.clone();
                let before_values = expected.clone();
                match operation {
                    0 => { actual.push(value); expected.push(value); }
                    1 => { prop_assert_eq!(actual.pop(), expected.pop()); }
                    _ => {
                        if let Some(slot) = actual.get_mut(value) { *slot = operation as usize; }
                        if let Some(slot) = expected.get_mut(value) { *slot = operation as usize; }
                    }
                }
                prop_assert_eq!(actual.iter().copied().collect::<Vec<_>>(), expected.clone());
                prop_assert_eq!(before.iter().copied().collect::<Vec<_>>(), before_values);
                prop_assert_eq!(actual.len(), expected.len());
            }
        }
        #[test]
        fn sharded_edits_match_map_and_preserve_snapshots(ops in prop::collection::vec((any::<bool>(), 0u8..16, any::<u32>()), 1..32)) {
            let mut actual = KeyMap::default();
            let mut expected = HashMap::new();
            for (insert, key, value) in ops {
                let before = actual.clone();
                let before_values = expected.clone();
                if insert { prop_assert_eq!(actual.insert(vec![key], value), expected.insert(vec![key], value)); }
                else { prop_assert_eq!(actual.remove(&[key]), expected.remove(&vec![key])); }
                for key in 0..16 {
                    prop_assert_eq!(actual.get(&[key]), expected.get(&vec![key]));
                    prop_assert_eq!(actual.contains_key(&[key]), expected.contains_key(&vec![key]));
                    prop_assert_eq!(before.get(&[key]), before_values.get(&vec![key]));
                }
            }
        }
    }
}
