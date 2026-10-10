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
use crate::vector::hnsw::{HnswIndex, HnswTransactionState, MAX_HNSW_EF_SEARCH};
use crate::vector::Metric;
use crate::Error;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

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

/// One process/case. Build time is a preliminary observation, not acceptance.
#[test]
#[ignore = "explicit fixed-order bulk quality and construction observation"]
fn bulk_order_quality_cost_worker() {
    let size = std::env::var("HNSW_ORDER_SIZE")
        .unwrap_or_else(|_| "9600".into())
        .parse::<usize>()
        .expect("HNSW_ORDER_SIZE must be an integer");
    let order = std::env::var("HNSW_ORDER_ORDER").unwrap_or_else(|_| "ascending".into());
    check_bulk_order_quality(&order, size);
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn shuffled_bulk_preserves_tie_aware_recall() {
    check_bulk_order_quality("shuffled", 9600);
}

fn check_bulk_order_quality(order: &str, size: usize) {
    use rand::rngs::StdRng;
    use rand::seq::SliceRandom;
    use rand::SeedableRng;
    use std::collections::HashSet;
    use std::io::Write;
    use std::time::Instant;

    const DIM: usize = 128;
    const K: usize = 10;
    const EPS: f64 = 1e-7;
    assert!(
        (K..=40_000).contains(&size),
        "size outside bounded diagnostic range"
    );
    assert!(matches!(order, "ascending" | "shuffled"));
    let keys = (0..size)
        .map(|id| ((id + 1) as u64).to_be_bytes())
        .collect::<Vec<_>>();
    let vectors = (0..size)
        .map(|id| {
            (0..DIM)
                .map(|d| ((id + d) % 997) as f32)
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    let mut insertion_order = (0..size).collect::<Vec<_>>();
    if order == "shuffled" {
        insertion_order.shuffle(&mut StdRng::seed_from_u64(42));
    }
    let entries = insertion_order
        .iter()
        .map(|&id| (keys[id].as_slice(), vectors[id].as_slice(), &[][..]))
        .collect::<Vec<_>>();
    let mut index = HnswIndex::create(
        "bulk_order",
        base_config()
            .with_dimension(DIM)
            .with_metric(Metric::Cosine),
    )
    .unwrap();
    let started = Instant::now();
    for batch in entries.chunks(128) {
        index.upsert_batch(batch).unwrap();
    }
    let build_ns = u64::try_from(started.elapsed().as_nanos()).unwrap();

    // Preparation, oracle, query, resource inspection and output are untimed.
    let scores = vectors
        .iter()
        .map(|vector| {
            let norm = vector
                .iter()
                .map(|&v| f64::from(v).powi(2))
                .sum::<f64>()
                .sqrt();
            f64::from(vector[0]) / norm
        })
        .collect::<Vec<_>>();
    let mut ranked = scores.clone();
    ranked.sort_by(|a, b| b.total_cmp(a));
    let cutoff = ranked[K - 1];
    let strict_total = scores.iter().filter(|&&score| score > cutoff + EPS).count();
    assert!(strict_total <= K);
    let mut query = vec![0.0; DIM];
    query[0] = 1.0;
    let (hits, query_stats, effective_ef) =
        index.search_with_effective_ef(&query, K, Some(64)).unwrap();
    let ids = hits
        .iter()
        .map(|hit| u64::from_be_bytes(hit.key.as_slice().try_into().unwrap()) as usize - 1)
        .collect::<Vec<_>>();
    let valid_ids = ids.len() == K
        && ids.iter().all(|&id| id < size)
        && ids.iter().copied().collect::<HashSet<_>>().len() == K;
    let strict_hits = ids
        .iter()
        .filter(|&&id| scores.get(id).is_some_and(|&score| score > cutoff + EPS))
        .count();
    let boundary_hits = ids
        .iter()
        .filter(|&&id| {
            scores
                .get(id)
                .is_some_and(|&score| score >= cutoff - EPS && score <= cutoff + EPS)
        })
        .count();
    let recall = (strict_hits + boundary_hits.min(K - strict_total)) as f64 / K as f64;
    let stats = index.stats();
    let graph = index.graph.read().unwrap();
    let mut edges = 0_usize;
    let mut max_layer0_degree = 0_usize;
    let mut max_upper_degree = 0_usize;
    for node in graph.nodes.iter().flatten() {
        for (level, neighbors) in node.neighbors.iter().enumerate() {
            edges += neighbors.len();
            if level == 0 {
                max_layer0_degree = max_layer0_degree.max(neighbors.len());
            } else {
                max_upper_degree = max_upper_degree.max(neighbors.len());
            }
        }
    }
    let row = serde_json::json!({
        "kind": "bulk_order_preliminary", "size": size, "order": order, "seed": 42,
        "dimension": DIM, "metric": "cosine", "m": 8, "ef_construction": 32,
        "batch_size": 128, "k": K, "requested_ef": 64, "effective_ef": effective_ef,
        "ids": ids, "valid_ids": valid_ids, "strict_total": strict_total,
        "strict_hits": strict_hits, "boundary_hits": boundary_hits, "recall": recall,
        "query_nodes_visited": query_stats.nodes_visited,
        "query_distance_computations": query_stats.distance_computations,
        "build_ns": build_ns, "estimated_graph_heap_bytes": stats.memory_bytes,
        "node_count": stats.node_count, "avg_edges_per_node": stats.avg_edges_per_node,
        "edges_all_layers": edges, "max_layer0_degree": max_layer0_degree,
        "max_upper_degree": max_upper_degree
    });
    let mut output = std::io::stdout().lock();
    writeln!(output, "{row}").unwrap();
    output.flush().unwrap();
    assert!(valid_ids, "invalid result IDs; see saved observation");
    assert_eq!(stats.node_count, size as u64);
    assert_eq!(effective_ef, 64.min(size));
    assert!(max_layer0_degree <= 16 && max_upper_degree <= 8);
    assert!(
        recall >= 0.95,
        "recall below fixed threshold; see saved observation"
    );
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
fn staged_batch_publishes_stats_only_after_commit() {
    let kv = MemoryKV::new();
    let mut index = HnswIndex::create("test_index", base_config()).unwrap();
    let mut state = HnswTransactionState::default();
    index
        .upsert_staged_batch(
            &[(b"a", &[0.0, 0.0], b""), (b"b", &[1.0, 0.0], b"")],
            &mut state,
        )
        .unwrap();

    assert_eq!(index.stats().node_count, 0);

    let mut txn = kv.begin(TxnMode::ReadWrite).unwrap();
    index.commit_staged(&mut txn, &mut state).unwrap();
    kv.txn_manager().commit(txn).unwrap();
    assert_eq!(index.stats().node_count, 2);

    let mut load_txn = kv.begin(TxnMode::ReadOnly).unwrap();
    let loaded = HnswIndex::load("test_index", &mut load_txn).unwrap();
    assert_eq!(loaded.search(&[0.0, 0.0], 2, None).unwrap().0.len(), 2);

    index
        .upsert_staged_batch(
            &[(b"c", &[2.0, 0.0], b""), (b"d", &[3.0, 0.0], b"")],
            &mut state,
        )
        .unwrap();
    assert_eq!(index.search(&[0.0, 0.0], 4, None).unwrap().0.len(), 4);
    index.rollback(&mut state).unwrap();
    let restored = index.search(&[0.0, 0.0], 4, None).unwrap().0;
    assert_eq!(
        restored.into_iter().map(|hit| hit.key).collect::<Vec<_>>(),
        vec![b"a".to_vec(), b"b".to_vec()]
    );
    assert_eq!(index.stats().node_count, 2);
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn public_search_rejects_ef_search_above_the_safety_limit() {
    let index = HnswIndex::create("test_index", base_config()).unwrap();

    let error = index
        .search(&[0.0, 0.0], 1, Some(MAX_HNSW_EF_SEARCH + 1))
        .expect_err("public HNSW search must reject an unsafe ef_search");
    assert!(matches!(
        error,
        Error::InvalidParameter { ref param, .. } if param == "ef_search"
    ));
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn batch_rejects_an_invalid_later_vector_without_mutation() {
    let mut index = HnswIndex::create("test_index", base_config()).unwrap();
    let mut state = HnswTransactionState::default();

    let error = index
        .upsert_staged_batch(
            &[(b"valid", &[0.0, 0.0], b""), (b"invalid", &[1.0], b"")],
            &mut state,
        )
        .unwrap_err();

    assert!(matches!(error, Error::DimensionMismatch { .. }));
    assert!(index.search(&[0.0, 0.0], 1, None).unwrap().0.is_empty());
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn batch_avoids_per_item_insert_callbacks() {
    let calls = Arc::new(AtomicUsize::new(0));
    let mut index = HnswIndex::create("test_index", base_config()).unwrap();
    let callback_calls = Arc::clone(&calls);
    index.on_insert(move |_| {
        callback_calls.fetch_add(1, Ordering::Relaxed);
    });

    index
        .upsert_batch(&[(b"a", &[0.0, 0.0], b""), (b"b", &[1.0, 0.0], b"")])
        .unwrap();

    assert_eq!(calls.load(Ordering::Relaxed), 0);
    assert_eq!(index.stats().node_count, 2);
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn bulk_batches_find_periodic_cosine_nearest_neighbor() {
    let config = base_config()
        .with_dimension(128)
        .with_metric(Metric::Cosine);
    let keys: Vec<_> = (0_u64..997).map(|id| (id + 1).to_be_bytes()).collect();
    let vectors: Vec<Vec<f32>> = (0..997)
        .map(|id| {
            (0..128)
                .map(|dimension| ((id + dimension) % 997) as f32)
                .collect()
        })
        .collect();
    let entries: Vec<_> = keys
        .iter()
        .zip(&vectors)
        .map(|(key, vector)| (key.as_slice(), vector.as_slice(), &[][..]))
        .collect();
    let mut query = vec![0.0; 128];
    query[0] = 1.0;
    let mut outcomes = Vec::new();
    for staged in [false, true] {
        let mut index = HnswIndex::create("periodic_cosine", config.clone()).unwrap();
        for batch in entries.chunks(128) {
            if staged {
                let mut state = HnswTransactionState::default();
                index.upsert_staged_batch(batch, &mut state).unwrap();
            } else {
                index.upsert_batch(batch).unwrap();
            }
        }
        let (results, _) = index.search(&query, 1, Some(64)).unwrap();
        outcomes.push(
            results
                .into_iter()
                .map(|result| result.key)
                .collect::<Vec<_>>(),
        );
    }
    // Collect both modes before asserting so a wrong first result cannot hide
    // the staged path. External row 996 is stored under internal key 997.
    eprintln!("periodic cosine batch outcomes (ordinary, staged): {outcomes:?}");
    assert_eq!(outcomes, vec![vec![997_u64.to_be_bytes().to_vec()]; 2]);
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn bulk_batch_restores_hnsw_degree_limits() {
    let mut index = HnswIndex::create("test_index", base_config()).unwrap();
    let keys: Vec<Vec<u8>> = (0_u32..300).map(|id| id.to_be_bytes().to_vec()).collect();
    let vectors: Vec<[f32; 2]> = (0_u32..300)
        .map(|id| [id as f32 + 1.0, (id % 17) as f32 + 1.0])
        .collect();
    let entries: Vec<_> = keys
        .iter()
        .zip(&vectors)
        .map(|(key, vector)| (key.as_slice(), vector.as_slice(), &[][..]))
        .collect();

    index.upsert_batch(&entries).unwrap();

    let graph = index.graph.read().unwrap();
    for node in graph.nodes.iter().flatten() {
        for (level, neighbors) in node.neighbors.iter().enumerate() {
            let limit = if level == 0 {
                graph.config.m * 2
            } else {
                graph.config.m
            };
            assert!(neighbors.len() <= limit);
        }
    }
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
