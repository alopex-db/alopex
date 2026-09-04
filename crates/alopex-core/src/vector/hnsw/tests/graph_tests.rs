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

#[cfg_attr(not(feature = "lane_ci"), ignore)]
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

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn l2_search_exposes_non_negative_euclidean_distance() {
    let mut graph = make_graph();
    graph.insert(b"same", &[1.0, 0.0], b"").unwrap();
    graph.insert(b"quarter", &[0.75, 0.0], b"").unwrap();
    graph.insert(b"far", &[0.0, 0.0], b"").unwrap();

    let (results, _) = graph.search(&[1.0, 0.0], 3, 8).unwrap();

    assert_eq!(results[0].key, b"same");
    assert_eq!(results[0].distance, 0.0);
    assert!((results[1].distance - 0.25).abs() < f32::EPSILON);
    assert!((results[2].distance - 1.0).abs() < f32::EPSILON);
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn cosine_and_inner_product_search_expose_lower_is_closer_distance() {
    for (metric, expected) in [
        (Metric::Cosine, [0.0, 1.0, 2.0]),
        (Metric::InnerProduct, [-1.0, -0.0, 1.0]),
    ] {
        let config = base_config().with_metric(metric);
        let mut graph = HnswGraph::new(config).unwrap();
        graph.insert(b"same", &[1.0, 0.0], b"").unwrap();
        graph.insert(b"orthogonal", &[0.0, 1.0], b"").unwrap();
        graph.insert(b"opposite", &[-1.0, 0.0], b"").unwrap();

        let (results, _) = graph.search(&[1.0, 0.0], 3, 8).unwrap();

        assert_eq!(results[0].key, b"same");
        assert_eq!(results[1].key, b"orthogonal");
        assert_eq!(results[2].key, b"opposite");
        for (result, expected_distance) in results.iter().zip(expected) {
            assert!((result.distance - expected_distance).abs() < f32::EPSILON);
        }
    }
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn hnsw_rejects_nonfinite_and_cosine_zero_vectors_at_insert_and_search() {
    let mut cosine = HnswGraph::new(base_config().with_metric(Metric::Cosine)).unwrap();
    assert!(cosine.insert(b"zero", &[0.0, 0.0], b"").is_err());
    assert!(cosine.insert(b"nan", &[f32::NAN, 1.0], b"").is_err());
    assert!(cosine.insert(b"inf", &[f32::INFINITY, 1.0], b"").is_err());
    cosine.insert(b"unit", &[1.0, 0.0], b"").unwrap();
    assert!(cosine.search(&[0.0, 0.0], 1, 8).is_err());
    assert!(cosine.search(&[1.0], 1, 8).is_err());

    for metric in [Metric::L2, Metric::InnerProduct] {
        let mut graph = HnswGraph::new(base_config().with_metric(metric)).unwrap();
        graph.insert(b"zero", &[0.0, 0.0], b"").unwrap();
        let (result, _) = graph.search(&[0.0, 0.0], 1, 8).unwrap();
        assert_eq!(result[0].key, b"zero");
    }
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn cosine_search_prepares_query_norm_once() {
    let mut graph = HnswGraph::new(base_config().with_metric(Metric::Cosine)).unwrap();
    for (key, vector) in [
        (&b"a"[..], &[1.0, 0.0][..]),
        (&b"b"[..], &[0.8, 0.6][..]),
        (&b"c"[..], &[0.0, 1.0][..]),
    ] {
        graph.insert(key, vector, b"").unwrap();
    }

    let (_, stats) = graph.search(&[1.0, 0.0], 3, 8).unwrap();

    assert_eq!(stats.query_norm_computations, 1);
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn reverse_link_pruning_reuses_diverse_neighbor_selection() {
    let mut graph = HnswGraph::new(base_config().with_metric(Metric::Cosine)).unwrap();
    let root = graph.insert(b"root", &[1.0, 0.0], b"").unwrap();
    let near = graph.insert(b"near", &[0.9, 0.435_889_9], b"").unwrap();
    let redundant = graph.insert(b"redundant", &[0.8, 0.6], b"").unwrap();
    let diverse = graph
        .insert(b"diverse", &[0.7, -0.714_142_86], b"")
        .unwrap();
    graph.nodes[root as usize].as_mut().unwrap().neighbors[0] = vec![near, redundant, diverse];

    graph.prune_neighbors(root, 0, 2);

    assert_eq!(
        graph.nodes[root as usize].as_ref().unwrap().neighbors[0],
        vec![near, diverse]
    );
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn level_assignment_is_reproducible_for_fixed_keys() {
    let mut first = make_graph();
    let mut second = make_graph();
    for index in 0_u32..64 {
        let key = index.to_le_bytes();
        let vector = [index as f32, 1.0];
        first.insert(&key, &vector, b"").unwrap();
        second.insert(&key, &vector, b"").unwrap();
    }

    let levels = |graph: &HnswGraph| {
        graph
            .nodes
            .iter()
            .flatten()
            .map(|node| node.neighbors.len())
            .collect::<Vec<_>>()
    };
    assert_eq!(levels(&first), levels(&second));
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
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

#[cfg_attr(not(feature = "lane_ci"), ignore)]
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

#[cfg_attr(not(feature = "lane_ci"), ignore)]
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

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn returns_less_than_k_when_insufficient() {
    let mut graph = make_graph();
    graph.insert(b"solo", &[0.0, 0.0], b"m").unwrap();

    let (results, _) = graph.search(&[0.0, 0.0], 3, 5).unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].key, b"solo");
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn full_ef_self_search_reaches_every_active_node() {
    const COUNT: usize = 64;
    let mut graph = make_graph();
    let vectors: Vec<[f32; 2]> = (0..COUNT)
        .map(|index| [index as f32, (index * index) as f32])
        .collect();
    for (index, vector) in vectors.iter().enumerate() {
        graph
            .insert(&(index as u64).to_be_bytes(), vector, b"")
            .unwrap();
    }

    for (index, vector) in vectors.iter().enumerate() {
        let (results, _) = graph.search(vector, 1, COUNT).unwrap();
        assert_eq!(results[0].key, (index as u64).to_be_bytes());
    }
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
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
