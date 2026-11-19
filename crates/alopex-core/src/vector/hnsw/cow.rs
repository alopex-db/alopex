//! Copy-on-write storage for graph snapshots. Payloads detach only on mutation.

use std::collections::hash_map::RandomState;
use std::collections::HashMap;
use std::hash::BuildHasher;
use std::ops::{Deref, DerefMut, Index, IndexMut};
use std::sync::Arc;

use super::types::HnswNode;

const PAGE_SIZE: usize = 128;
const KEY_SHARDS: usize = 256;
type Page<T> = Arc<[T; PAGE_SIZE]>;
type PageSlot<T> = Option<Page<T>>;

/// Contiguous payload shared by node headers; mutable access detaches it.
#[derive(Clone, Debug, Default, PartialEq)]
pub(crate) struct SharedSlice<T>(Arc<[T]>);
impl<T> From<Vec<T>> for SharedSlice<T> {
    fn from(value: Vec<T>) -> Self {
        Self(value.into())
    }
}
impl<T: Clone> From<&[T]> for SharedSlice<T> {
    fn from(value: &[T]) -> Self {
        Self(value.into())
    }
}
impl<T> SharedSlice<T> {
    pub(crate) fn as_slice(&self) -> &[T] {
        &self.0
    }
}
impl<T> Deref for SharedSlice<T> {
    type Target = [T];
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
impl<T: Clone> DerefMut for SharedSlice<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        Arc::make_mut(&mut self.0)
    }
}
impl<'a, T> IntoIterator for &'a SharedSlice<T> {
    type Item = &'a T;
    type IntoIter = std::slice::Iter<'a, T>;
    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}
impl<'a, T: Clone> IntoIterator for &'a mut SharedSlice<T> {
    type Item = &'a mut T;
    type IntoIter = std::slice::IterMut<'a, T>;
    fn into_iter(self) -> Self::IntoIter {
        self.deref_mut().iter_mut()
    }
}

/// The header lives in its page; cloning shares each separately owned payload.
#[derive(Clone, Debug)]
pub(crate) struct SharedNode(HnswNode);
impl From<HnswNode> for SharedNode {
    fn from(node: HnswNode) -> Self {
        Self(node)
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
        &mut self.0
    }
}

/// A snapshot shares the directory and pages. A write detaches one directory
/// and at most one page; node payloads have a separate copy-on-write boundary.
/// Values live inside each shared page, avoiding a separate Vec allocation and
/// data-pointer lookup on every graph read. Unused tail slots contain defaults
/// and are excluded from indexed access and iteration by the logical length.
/// The shared directory is a slice so reads do not follow a Vec data pointer.
/// Spare directory slots are empty; doubling preserves amortized page growth
/// without allocating payload pages until they are needed.
#[derive(Clone, Debug)]
pub(crate) struct PagedVec<T> {
    pages: Arc<[PageSlot<T>]>,
    len: usize,
}
impl<T> Default for PagedVec<T> {
    fn default() -> Self {
        Self {
            pages: Arc::from([]),
            len: 0,
        }
    }
}
impl<T> PagedVec<T> {
    pub(crate) fn len(&self) -> usize {
        self.len
    }
    pub(crate) fn capacity(&self) -> usize {
        self.len.div_ceil(PAGE_SIZE) * PAGE_SIZE
    }
    pub(crate) fn get(&self, index: usize) -> Option<&T> {
        if index >= self.len {
            return None;
        }
        self.pages
            .get(index / PAGE_SIZE)?
            .as_ref()?
            .get(index % PAGE_SIZE)
    }
    pub(crate) fn iter(&self) -> impl Iterator<Item = &T> {
        self.into_iter()
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
        Arc::make_mut(pages[index / PAGE_SIZE].as_mut()?).get_mut(index % PAGE_SIZE)
    }
}
impl<T: Clone + Default> PagedVec<T> {
    pub(crate) fn push(&mut self, value: T) {
        let page_index = self.len / PAGE_SIZE;
        if page_index == self.pages.len() {
            let directory_len = self
                .pages
                .len()
                .checked_mul(2)
                .expect("directory capacity")
                .max(1);
            let mut pages = self.pages.to_vec();
            pages.resize_with(directory_len, || None);
            self.pages = pages.into();
        }
        let pages = Arc::make_mut(&mut self.pages);
        if self.len.is_multiple_of(PAGE_SIZE) {
            pages[page_index] = Some(Arc::new(std::array::from_fn(|_| T::default())));
        }
        Arc::make_mut(pages[page_index].as_mut().expect("page allocated"))[self.len % PAGE_SIZE] =
            value;
        self.len += 1;
    }
    pub(crate) fn pop(&mut self) -> Option<T> {
        if self.len == 0 {
            return None;
        }
        let page_index = (self.len - 1) / PAGE_SIZE;
        let pages = Arc::make_mut(&mut self.pages);
        let page = Arc::make_mut(pages[page_index].as_mut().expect("nonempty pages"));
        self.len -= 1;
        let value = std::mem::take(&mut page[self.len % PAGE_SIZE]);
        if self.len.is_multiple_of(PAGE_SIZE) {
            pages[page_index] = None;
        }
        if self.len == 0 {
            self.pages = Arc::from([]);
        }
        Some(value)
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
impl<T: Clone + Default> FromIterator<T> for PagedVec<T> {
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
    type IntoIter = std::iter::Take<
        std::iter::FlatMap<
            std::slice::Iter<'a, PageSlot<T>>,
            std::slice::Iter<'a, T>,
            fn(&'a PageSlot<T>) -> std::slice::Iter<'a, T>,
        >,
    >;
    fn into_iter(self) -> Self::IntoIter {
        let page_iter: fn(&'a PageSlot<T>) -> std::slice::Iter<'a, T> = |page| {
            page.as_deref()
                .map_or([].as_slice(), |page| page.as_slice())
                .iter()
        };
        self.pages.iter().flat_map(page_iter).take(self.len)
    }
}

type KeyShard = Arc<HashMap<Vec<u8>, u32>>;
/// A random shard hash is stable across snapshots, but is not serialized.
/// Empty graphs allocate the shard directory only on their first insertion.
#[derive(Clone)]
pub(crate) struct KeyMap {
    shards: Arc<Vec<Option<KeyShard>>>,
    hash: RandomState,
}
impl Default for KeyMap {
    fn default() -> Self {
        Self {
            shards: Arc::new(Vec::new()),
            hash: RandomState::new(),
        }
    }
}
impl KeyMap {
    fn shard(&self, key: &[u8]) -> usize {
        self.hash.hash_one(key) as usize % KEY_SHARDS
    }
    pub(crate) fn get(&self, key: &[u8]) -> Option<&u32> {
        self.shards.get(self.shard(key))?.as_ref()?.get(key)
    }
    pub(crate) fn contains_key(&self, key: &[u8]) -> bool {
        self.get(key).is_some()
    }
    pub(crate) fn insert(&mut self, key: Vec<u8>, value: u32) -> Option<u32> {
        let shard = self.shard(&key);
        let shards = Arc::make_mut(&mut self.shards);
        if shards.is_empty() {
            shards.resize_with(KEY_SHARDS, || None);
        }
        let map = shards[shard].get_or_insert_with(|| Arc::new(HashMap::new()));
        Arc::make_mut(map).insert(key, value)
    }
    pub(crate) fn remove(&mut self, key: &[u8]) -> Option<u32> {
        let shard = self.shard(key);
        if !self.shards.get(shard)?.as_ref()?.contains_key(key) {
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

    #[derive(Debug, Default)]
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
        assert!(Arc::ptr_eq(&original.pages, &snapshot.pages));
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
                key: b"a".to_vec().into(),
                vector: vec![1.0, 2.0].into(),
                norm: 5.0_f64.sqrt(),
                metadata: vec![3].into(),
                neighbors: vec![vec![]].into(),
                deleted: false,
            })
        };
        let original: PagedVec<_> = [Some(node()), Some(node())].into_iter().collect();
        let mut copy = original.clone();
        copy[0].as_mut().unwrap().deleted = true;
        assert!(!original[0].as_ref().unwrap().deleted);
        for index in 0..2 {
            let old = original[index].as_ref().unwrap();
            let new = copy[index].as_ref().unwrap();
            assert!(Arc::ptr_eq(&old.vector.0, &new.vector.0));
            assert!(Arc::ptr_eq(&old.key.0, &new.key.0));
            assert!(Arc::ptr_eq(&old.metadata.0, &new.metadata.0));
            assert!(Arc::ptr_eq(&old.neighbors.0, &new.neighbors.0));
        }
        copy[0].as_mut().unwrap().neighbors[0].push(1);
        assert!(original[0].as_ref().unwrap().neighbors[0].is_empty());
        assert_eq!(copy[0].as_ref().unwrap().neighbors[0], vec![1]);
        assert!(Arc::ptr_eq(
            &original[0].as_ref().unwrap().vector.0,
            &copy[0].as_ref().unwrap().vector.0
        ));
        assert!(!Arc::ptr_eq(
            &original[0].as_ref().unwrap().neighbors.0,
            &copy[0].as_ref().unwrap().neighbors.0
        ));
    }

    #[test]
    fn shared_slice_mutation_detaches_once_and_preserves_snapshot() {
        let clones = Rc::new(Cell::new(0));
        let original: SharedSlice<_> = (0..3)
            .map(|i| Counted(clones.clone(), i))
            .collect::<Vec<_>>()
            .into();
        let mut changed = original.clone();
        assert_eq!(clones.get(), 0);
        changed[1].1 = 9;
        assert_eq!(clones.get(), 3);
        for item in &mut changed {
            item.1 += 1;
        }
        assert_eq!(clones.get(), 3);
        assert_eq!(
            (&original)
                .into_iter()
                .map(|item| item.1)
                .collect::<Vec<_>>(),
            vec![0, 1, 2]
        );
        assert_eq!(
            changed
                .as_slice()
                .iter()
                .map(|item| item.1)
                .collect::<Vec<_>>(),
            vec![1, 10, 3]
        );
        assert!(SharedSlice::<u8>::default().is_empty());
    }

    #[test]
    fn pop_and_reappend_at_page_boundaries_preserve_snapshots() {
        for length in [PAGE_SIZE + 1, PAGE_SIZE * 2 + 1, PAGE_SIZE * 4 + 1] {
            let original: PagedVec<_> = (0..length).collect();
            let mut copy = original.clone();
            assert_eq!(copy.pop(), Some(length - 1));
            assert_eq!(copy.capacity(), length - 1);
            copy.push(999);
            assert_eq!(copy[length - 1], 999);
            assert_eq!(original[length - 1], length - 1);
            for expected in (0..length - 1).rev() {
                if expected == length - 2 {
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
            assert_eq!(
                original.iter().copied().collect::<Vec<_>>(),
                (0..length).collect::<Vec<_>>()
            );
        }
    }

    #[test]
    fn unused_tail_slots_are_inaccessible_and_do_not_leak_into_iteration() {
        for length in [0, 1, PAGE_SIZE - 1, PAGE_SIZE, PAGE_SIZE + 1] {
            let mut actual: PagedVec<_> = (1..=length).collect();
            let snapshot = actual.clone();
            assert!(
                !actual.contains(&0),
                "unused default slots are not elements"
            );
            assert_eq!(actual.contains(&length), length > 0);
            assert_eq!(
                actual.iter().copied().collect::<Vec<_>>(),
                (1..=length).collect::<Vec<_>>()
            );
            assert_eq!(
                (&actual).into_iter().copied().collect::<Vec<_>>(),
                (1..=length).collect::<Vec<_>>()
            );
            for index in [length, actual.capacity(), usize::MAX] {
                assert!(actual.get(index).is_none());
                assert!(actual.get_mut(index).is_none());
            }
            if length > 0 {
                assert_eq!(actual.pop(), Some(length));
                assert!(actual.get(length - 1).is_none());
                assert!(actual.get_mut(length - 1).is_none());
                assert_eq!(actual.iter().count(), length - 1);
                assert_eq!(snapshot.get(length - 1), Some(&length));
                actual.push(999);
                assert_eq!(actual.get(length - 1), Some(&999));
                assert_eq!(snapshot.get(length - 1), Some(&length));
            }
        }
    }

    #[test]
    fn popped_payload_is_released_without_retaining_it_in_unused_tail() {
        let value = Arc::new(7);
        let mut actual: PagedVec<_> = [Some(value.clone()), Some(value.clone())]
            .into_iter()
            .collect();
        let snapshot = actual.clone();
        assert_eq!(Arc::strong_count(&value), 3);
        let removed = actual.pop().unwrap();
        assert_eq!(Arc::strong_count(&value), 5);
        drop(removed);
        assert_eq!(Arc::strong_count(&value), 4);
        drop(snapshot);
        assert_eq!(Arc::strong_count(&value), 2);
        assert_eq!(actual.iter().count(), 1);
        drop(actual.pop());
        assert_eq!(Arc::strong_count(&value), 1);
        assert!(actual.iter().next().is_none());

        let mut actual: PagedVec<_> = (0..PAGE_SIZE * 2 + 1)
            .map(|_| Some(value.clone()))
            .collect();
        drop(actual.pop());
        assert!(
            actual.pages[2].is_none(),
            "an empty page must not be retained by the directory"
        );
        assert_eq!(Arc::strong_count(&value), PAGE_SIZE * 2 + 1);
        actual.push(Some(value.clone()));
        assert_eq!(actual.len(), PAGE_SIZE * 2 + 1);
        assert!(actual[PAGE_SIZE * 2].is_some());
        drop(actual);
        assert_eq!(Arc::strong_count(&value), 1);
    }

    #[test]
    fn empty_key_snapshots_allocate_no_directory_until_insertion() {
        let mut original = KeyMap::default();
        let mut fork = original.clone();
        for map in [&mut original, &mut fork] {
            assert_eq!(map.shards.capacity(), 0);
            assert_eq!(map.get(b"first"), None);
            assert!(!map.contains_key(b"first"));
            assert_eq!(map.remove(b"first"), None);
            assert_eq!(map.shards.capacity(), 0);
        }
        assert!(Arc::ptr_eq(&original.shards, &fork.shards));

        assert_eq!(fork.insert(b"first".to_vec(), 7), None);
        assert_eq!(fork.get(b"first"), Some(&7));
        assert_eq!(original.get(b"first"), None);
        assert_eq!(original.shards.capacity(), 0);
        assert_eq!(original.insert(b"second".to_vec(), 9), None);
        assert_eq!(fork.get(b"second"), None);
        assert_eq!(original.get(b"second"), Some(&9));

        let saved = fork.clone();
        assert_eq!(fork.remove(b"first"), Some(7));
        assert_eq!(fork.get(b"first"), None);
        assert_eq!(saved.get(b"first"), Some(&7));
        assert_eq!(fork.insert(b"first".to_vec(), 11), None);
        assert_eq!(fork.insert(b"first".to_vec(), 13), Some(11));
        assert_eq!(fork.get(b"first"), Some(&13));
        assert_eq!(saved.get(b"first"), Some(&7));
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
            let node = node.as_ref().unwrap();
            let saved = saved.as_ref().unwrap();
            assert!(Arc::ptr_eq(&node.vector.0, &saved.vector.0));
            assert!(Arc::ptr_eq(&node.neighbors.0, &saved.neighbors.0));
            assert!(Arc::ptr_eq(&node.key.0, &saved.key.0));
            assert!(Arc::ptr_eq(&node.metadata.0, &saved.metadata.0));
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(32))]
        #[test]
        fn paged_edits_match_vec_and_preserve_snapshots(
            initial in prop::sample::select(vec![0, PAGE_SIZE - 1, PAGE_SIZE, PAGE_SIZE + 1, PAGE_SIZE * 2, PAGE_SIZE * 2 + 1]),
            ops in prop::collection::vec((0u8..4, 0usize..(PAGE_SIZE * 2 + 3)), 1..32)
        ) {
            let mut actual: PagedVec<_> = (0..initial).collect();
            let mut expected: Vec<_> = (0..initial).collect();
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
                prop_assert_eq!(actual.capacity(), expected.len().div_ceil(PAGE_SIZE) * PAGE_SIZE);
                prop_assert!(actual.get(actual.len()).is_none());
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
