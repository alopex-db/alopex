use crate::kv::memory::MemoryKV;
use crate::kv::KVStore;
use crate::kv::KVTransaction;
use crate::txn::TxnManager;
use crate::types::TxnMode;
use crate::vector::hnsw::storage::HNSW_FORMAT_VERSION;
use crate::vector::hnsw::types::HnswMetadata;
use crate::vector::hnsw::HnswConfig;
use crate::vector::hnsw::HnswGraph;
use crate::vector::hnsw::HnswStorage;
use crate::vector::Metric;
use crate::Error;

#[derive(serde::Serialize)]
struct FormatV1Node {
    key: Vec<u8>,
    vector: Vec<f32>,
    metadata: Vec<u8>,
    neighbors: Vec<Vec<u32>>,
    deleted: bool,
    level: usize,
}

fn storage() -> HnswStorage {
    HnswStorage::new("test_index")
}

fn base_config() -> HnswConfig {
    HnswConfig::default()
        .with_dimension(2)
        .with_metric(Metric::L2)
        .with_m(8)
        .with_ef_construction(32)
}

fn meta_key() -> Vec<u8> {
    format!("hnsw:meta:{}", storage().index_name).into_bytes()
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn save_and_load_roundtrip_preserves_graph() {
    let kv = MemoryKV::new();
    let mut graph = HnswGraph::new(base_config()).unwrap();
    graph.insert(b"a", &[0.0, 0.0], b"ma").unwrap();
    graph.insert(b"b", &[1.0, 0.0], b"mb").unwrap();

    let mut txn = kv.begin(TxnMode::ReadWrite).unwrap();
    storage().save(&mut txn, &graph).unwrap();
    kv.txn_manager().commit(txn).unwrap();

    let mut load_txn = kv.begin(TxnMode::ReadOnly).unwrap();
    let loaded = storage().load(&mut load_txn).unwrap();

    let (results, _) = loaded.search(&[0.1, 0.0], 2, 4).unwrap();
    let keys: Vec<_> = results.iter().map(|r| r.key.clone()).collect();
    assert!(keys.contains(&b"a".to_vec()));
    assert!(keys.contains(&b"b".to_vec()));
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn format_v1_without_persisted_norm_rebuilds_cached_norm() {
    let node_bytes = bincode::serialize(&FormatV1Node {
        key: b"legacy".to_vec(),
        vector: vec![3.0, 4.0],
        metadata: b"v1".to_vec(),
        neighbors: vec![Vec::new()],
        deleted: false,
        level: 0,
    })
    .unwrap();
    let mut metadata = HnswMetadata {
        version: HNSW_FORMAT_VERSION,
        config: base_config().with_metric(Metric::Cosine),
        entry_point: Some(0),
        max_level: 0,
        node_count: 1,
        deleted_count: 0,
        next_node_id: 1,
        checksum: 0,
    };
    let metadata_bytes = bincode::serialize(&metadata).unwrap();
    metadata.checksum = crc32fast::hash(&metadata_bytes).wrapping_add(crc32fast::hash(&node_bytes));

    let graph = storage()
        .build_graph_from_bytes(metadata, vec![Some(node_bytes)])
        .unwrap();

    assert_eq!(graph.nodes[0].as_ref().unwrap().norm, 5.0);
    assert_eq!(graph.search(&[3.0, 4.0], 1, 8).unwrap().0[0].distance, 0.0);
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn checksum_mismatch_is_detected() {
    let kv = MemoryKV::new();
    let mut graph = HnswGraph::new(base_config()).unwrap();
    graph.insert(b"a", &[0.0, 0.0], b"ma").unwrap();

    let mut txn = kv.begin(TxnMode::ReadWrite).unwrap();
    storage().save(&mut txn, &graph).unwrap();
    kv.txn_manager().commit(txn).unwrap();

    let mut tamper_txn = kv.begin(TxnMode::ReadWrite).unwrap();
    let mut meta: HnswMetadata =
        bincode::deserialize(&tamper_txn.get(&meta_key()).unwrap().unwrap()).unwrap();
    meta.checksum ^= 0xFFFF;
    let corrupted = bincode::serialize(&meta).unwrap();
    tamper_txn.put(meta_key(), corrupted).unwrap();
    kv.txn_manager().commit(tamper_txn).unwrap();

    let mut load_txn = kv.begin(TxnMode::ReadOnly).unwrap();
    match storage().load(&mut load_txn) {
        Err(Error::CorruptedIndex { .. }) => {}
        Err(err) => panic!("想定外のエラー: {err:?}"),
        Ok(_) => panic!("破損を検出できずにロードが成功してしまいました"),
    }
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn version_mismatch_is_reported() {
    let kv = MemoryKV::new();
    let mut graph = HnswGraph::new(base_config()).unwrap();
    graph.insert(b"a", &[0.0, 0.0], b"ma").unwrap();

    let mut txn = kv.begin(TxnMode::ReadWrite).unwrap();
    storage().save(&mut txn, &graph).unwrap();
    kv.txn_manager().commit(txn).unwrap();

    let mut tamper_txn = kv.begin(TxnMode::ReadWrite).unwrap();
    let mut meta: HnswMetadata =
        bincode::deserialize(&tamper_txn.get(&meta_key()).unwrap().unwrap()).unwrap();
    meta.version = HNSW_FORMAT_VERSION + 1;
    let corrupted = bincode::serialize(&meta).unwrap();
    tamper_txn.put(meta_key(), corrupted).unwrap();
    kv.txn_manager().commit(tamper_txn).unwrap();

    let mut load_txn = kv.begin(TxnMode::ReadOnly).unwrap();
    match storage().load(&mut load_txn) {
        Err(Error::UnsupportedIndexVersion { found, .. }) => {
            assert_eq!(found, HNSW_FORMAT_VERSION + 1);
        }
        Err(err) => panic!("想定外のエラー: {err:?}"),
        Ok(_) => panic!("バージョン不整合を検出できずにロードが成功してしまいました"),
    }
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn incremental_save_persists_changes() {
    let kv = MemoryKV::new();
    let mut graph = HnswGraph::new(base_config()).unwrap();

    let mut txn = kv.begin(TxnMode::ReadWrite).unwrap();
    storage().save(&mut txn, &graph).unwrap();
    kv.txn_manager().commit(txn).unwrap();

    // 増分保存で初回のノードを追加。
    let inserted = graph.insert(b"a", &[0.0, 0.0], b"ma").unwrap();

    let mut inc_txn = kv.begin(TxnMode::ReadWrite).unwrap();
    storage()
        .save_incremental(&mut inc_txn, &graph, &[], &[inserted], &[])
        .unwrap();
    kv.txn_manager().commit(inc_txn).unwrap();

    let mut load_txn = kv.begin(TxnMode::ReadOnly).unwrap();
    let loaded = storage().load(&mut load_txn).unwrap();
    let loaded_first = loaded.find_node_id(b"a").unwrap();
    assert_eq!(loaded_first, inserted);
    let (results, _) = loaded.search(&[0.0, 0.0], 1, 8).unwrap();
    assert_eq!(results[0].key, b"a");
    assert_eq!(results[0].metadata, b"ma");
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn free_list_and_key_map_reconstructed_after_compaction() {
    let kv = MemoryKV::new();
    let mut graph = HnswGraph::new(base_config()).unwrap();
    graph.insert(b"a", &[0.0, 0.0], b"ma").unwrap();
    let removed = graph.insert(b"b", &[1.0, 0.0], b"mb").unwrap();
    graph.insert(b"c", &[2.0, 0.0], b"mc").unwrap();

    graph.delete(b"b").unwrap();
    graph.compact().unwrap();

    let mut txn = kv.begin(TxnMode::ReadWrite).unwrap();
    storage().save(&mut txn, &graph).unwrap();
    kv.txn_manager().commit(txn).unwrap();

    let mut load_txn = kv.begin(TxnMode::ReadOnly).unwrap();
    let loaded = storage().load(&mut load_txn).unwrap();
    assert!(loaded.free_list.contains(&removed));
    assert!(loaded.key_to_node.contains_key(b"a".as_slice()));
    assert!(loaded.key_to_node.contains_key(b"c".as_slice()));
    assert!(!loaded.key_to_node.contains_key(b"b".as_slice()));
}
