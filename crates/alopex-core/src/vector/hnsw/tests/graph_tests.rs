use crate::vector::hnsw::HnswGraph;
use crate::vector::Metric;

fn base_config() -> crate::vector::hnsw::HnswConfig {
    crate::vector::hnsw::HnswConfig::default()
        .with_dimension(2)
        .with_metric(Metric::L2)
        .with_m(8)
        .with_ef_construction(32)
}

fn make_graph() -> HnswGraph {
    HnswGraph::new(base_config()).expect("設定が正しいので初期化に失敗しない")
}

#[test]
fn insert_and_search_basic_flow() {
    let mut graph = make_graph();
    graph.insert(b"a", &[0.0, 0.0], b"ma").unwrap();
    graph.insert(b"b", &[1.0, 0.0], b"mb").unwrap();
    graph.insert(b"c", &[0.0, 2.0], b"mc").unwrap();

    let (results, stats) = graph.search(&[1.0, 0.1], 2, 4).unwrap();
    assert_eq!(results.len(), 2);
    // もっとも近いのは b、次が a（L2 距離は負の値で大きいほど近い）
    assert_eq!(results[0].key, b"b");
    assert_eq!(results[1].key, b"a");
    assert!(stats.nodes_visited > 0);
}

#[test]
fn ef_search_is_auto_corrected() {
    let mut graph = make_graph();
    for i in 0..5u8 {
        let key = [b'k', i];
        graph
            .insert(&key, &[i as f32, 0.0], &[i])
            .expect("挿入に失敗しない");
    }

    let (results, _stats) = graph.search(&[0.0, 0.0], 3, 1).unwrap();
    // ef_search=1 でも k=3 に補正されるので 3 件返る
    assert_eq!(results.len(), 3);
}

#[test]
fn deleted_nodes_are_skipped_in_results() {
    let mut graph = make_graph();
    graph.insert(b"a", &[0.0, 0.0], b"ma").unwrap();
    graph.insert(b"b", &[0.1, 0.0], b"mb").unwrap();

    graph.delete(b"a").unwrap();
    let (results, _) = graph.search(&[0.0, 0.0], 2, 8).unwrap();
    assert!(results.iter().all(|r| r.key != b"a"));
    assert_eq!(graph.deleted_count, 1);
}

#[test]
fn tie_breaks_by_key_order() {
    let mut graph = make_graph();
    graph.insert(b"alpha", &[1.0, 1.0], b"ma").unwrap();
    graph.insert(b"bravo", &[1.0, 1.0], b"mb").unwrap();

    let (results, _) = graph.search(&[1.0, 1.0], 2, 10).unwrap();
    assert_eq!(results.len(), 2);
    // 距離が同一なのでキーの辞書順で alpha, bravo になる
    assert_eq!(results[0].key, b"alpha");
    assert_eq!(results[1].key, b"bravo");
}

#[test]
fn returns_less_than_k_when_insufficient() {
    let mut graph = make_graph();
    graph.insert(b"solo", &[0.0, 0.0], b"m").unwrap();

    let (results, _) = graph.search(&[0.0, 0.0], 3, 5).unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].key, b"solo");
}

#[test]
fn delete_marks_node_and_compact_removes_it() {
    let mut graph = make_graph();
    graph.insert(b"a", &[0.0, 0.0], b"ma").unwrap();
    let removed_id = graph.insert(b"b", &[1.0, 0.0], b"mb").unwrap();
    graph.insert(b"c", &[2.0, 0.0], b"mc").unwrap();

    assert!(graph.delete(b"b").unwrap());
    assert_eq!(graph.deleted_count, 1);

    let compaction = graph.compact().unwrap();
    assert_eq!(compaction.vectors_removed, 1);
    assert_eq!(graph.deleted_count, 0);
    assert!(!graph.key_to_node.contains_key(b"b".as_slice()));
    assert!(graph.nodes.get(removed_id as usize).is_some());
    assert!(graph.nodes[removed_id as usize].is_none());

    let stats = graph.stats();
    assert_eq!(stats.node_count, 2);
    assert_eq!(stats.deleted_count, 0);
}
