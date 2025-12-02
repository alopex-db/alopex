use alopex_core::kv::memory::MemoryKV;
use alopex_core::vector::flat::{search_flat, Candidate};
use alopex_core::{KVStore, KVTransaction, Metric, TxnManager, TxnMode};

fn encode_vec(v: &[f32]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(v.len() * 4);
    for f in v {
        buf.extend_from_slice(&f.to_le_bytes());
    }
    buf
}

#[test]
fn memorykv_vector_search_cosine() {
    let store = MemoryKV::new_with_limit(Some(16 * 1024));
    let manager = store.txn_manager();

    let mut txn = manager.begin(TxnMode::ReadWrite).unwrap();
    txn.put(b"a".to_vec(), encode_vec(&[1.0, 0.0])).unwrap();
    txn.put(b"b".to_vec(), encode_vec(&[0.0, 1.0])).unwrap();
    txn.put(b"c".to_vec(), encode_vec(&[1.0, 1.0])).unwrap();
    manager.commit(txn).unwrap();

    let pairs: Vec<_> = manager
        .snapshot()
        .into_iter()
        .map(|(k, v)| {
            let mut vec = Vec::with_capacity(v.len() / 4);
            for chunk in v.chunks_exact(4) {
                vec.push(f32::from_le_bytes(chunk.try_into().unwrap()));
            }
            (k, vec)
        })
        .collect();

    let candidates = pairs.iter().map(|(k, v)| Candidate {
        key: k,
        vector: v,
    });

    let res = search_flat(&[1.0, 0.0], Metric::Cosine, 3, candidates, None::<fn(&_) -> bool>)
        .unwrap();
    assert_eq!(res.len(), 3);
    assert_eq!(res[0].key, b"a");
    assert_eq!(res[1].key, b"c"); // mixed vector ranks second for cosine
}

#[test]
fn memorykv_vector_search_respects_l2_metric() {
    let store = MemoryKV::new_with_limit(Some(16 * 1024));
    let manager = store.txn_manager();

    let mut txn = manager.begin(TxnMode::ReadWrite).unwrap();
    txn.put(b"near".to_vec(), encode_vec(&[0.1, 0.0])).unwrap();
    txn.put(b"far".to_vec(), encode_vec(&[5.0, 5.0])).unwrap();
    manager.commit(txn).unwrap();

    let pairs: Vec<_> = manager
        .snapshot()
        .into_iter()
        .map(|(k, v)| {
            let mut vec = Vec::with_capacity(v.len() / 4);
            for chunk in v.chunks_exact(4) {
                vec.push(f32::from_le_bytes(chunk.try_into().unwrap()));
            }
            (k, vec)
        })
        .collect();

    let candidates = pairs.iter().map(|(k, v)| Candidate {
        key: k,
        vector: v,
    });

    let res =
        search_flat(&[0.0, 0.0], Metric::L2, 2, candidates, None::<fn(&_) -> bool>).unwrap();
    assert_eq!(res[0].key, b"near");
}
