use crate::vector::hnsw::HnswGraph;
use crate::vector::hnsw::MAX_HNSW_EF_SEARCH;
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
fn reverse_link_pruning_preserves_near_parallel_diverse_direction() {
    let vector = |base: usize| (0..128).map(|d| (base + d) as f32).collect::<Vec<_>>();
    let center_vector = vector(500);
    let a_vector = vector(501);
    let b_vector = vector(502);
    let c_vector = vector(499);
    let cosine64 = |left: &[f32], right: &[f32]| {
        let dot = left
            .iter()
            .zip(right)
            .map(|(&l, &r)| f64::from(l) * f64::from(r))
            .sum::<f64>();
        let left_squared = left
            .iter()
            .map(|&x| f64::from(x) * f64::from(x))
            .sum::<f64>();
        let right_squared = right
            .iter()
            .map(|&x| f64::from(x) * f64::from(x))
            .sum::<f64>();
        dot / (left_squared.sqrt() * right_squared.sqrt())
    };
    let ca = cosine64(&center_vector, &a_vector);
    let cb = cosine64(&center_vector, &b_vector);
    let cc = cosine64(&center_vector, &c_vector);
    let ba = cosine64(&b_vector, &a_vector);
    let ac = cosine64(&a_vector, &c_vector);
    assert!(
        ca > cc && cc > cb,
        "independent oracle: a closest, then c, then b"
    );
    assert!(ba > cb, "independent oracle: a excludes redundant b");
    assert!(ac < cc, "independent oracle: a retains opposite-side c");

    let mut graph = HnswGraph::new(
        base_config()
            .with_dimension(128)
            .with_metric(Metric::Cosine),
    )
    .unwrap();
    let center = graph.insert(b"center", &center_vector, b"").unwrap();
    let a = graph.insert(b"a", &a_vector, b"").unwrap();
    let b = graph.insert(b"b", &b_vector, b"").unwrap();
    let c = graph.insert(b"c", &c_vector, b"").unwrap();
    // Three candidates exceed max=2: the under-capacity shortcut cannot apply.
    graph.nodes[center as usize].as_mut().unwrap().neighbors[0] = vec![a, b, c];
    graph.prune_neighbors(center, 0, 2);
    assert_eq!(
        graph.nodes[center as usize].as_ref().unwrap().neighbors[0],
        vec![a, c],
        "near-parallel cosine pruning must retain a and opposite-side c"
    );
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn reverse_link_pruning_preserves_direction_among_duplicate_vectors() {
    for metric in [Metric::Cosine, Metric::L2, Metric::InnerProduct] {
        let mut graph = HnswGraph::new(base_config().with_metric(metric)).unwrap();
        let center = graph.insert(b"center", &[1.0, 0.0], b"").unwrap();
        let a = graph.insert(b"a", &[1.0, 0.0], b"").unwrap();
        let b = graph.insert(b"b", &[1.0, 0.0], b"").unwrap();
        let c = graph.insert(b"c", &[1.0, 0.0], b"").unwrap();
        let bridge = graph.insert(b"bridge", &[0.0, 1.0], b"").unwrap();
        graph.nodes[center as usize].as_mut().unwrap().neighbors[0] = vec![a, b, bridge];
        graph.prune_neighbors(center, 0, 2);
        assert_eq!(
            graph.nodes[center as usize].as_ref().unwrap().neighbors[0],
            vec![a, bridge],
            "identical vectors must not consume the last edge to another direction"
        );

        graph.nodes[center as usize].as_mut().unwrap().neighbors[0] = vec![a, b, c];
        graph.prune_neighbors(center, 0, 2);
        assert_eq!(
            graph.nodes[center as usize].as_ref().unwrap().neighbors[0],
            vec![a, b],
            "distinct keys with duplicate vectors still fill unused neighbor capacity"
        );
    }
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn bulk_entry_hint_ignores_invalid_and_duplicate_entries() {
    let mut graph = make_graph();
    graph.insert(b"a", &[0.0, 0.0], b"").unwrap();
    let deleted = graph.insert(b"b", &[1.0, 0.0], b"").unwrap();
    graph.insert(b"c", &[2.0, 0.0], b"").unwrap();
    graph.delete(b"b").unwrap();
    let mut baseline = graph.clone();
    let inserted = baseline
        .insert_unpruned(b"d", &[3.0, 0.0], b"", None)
        .unwrap();
    for hint in [u32::MAX, deleted, inserted] {
        let mut actual = graph.clone();
        actual
            .insert_unpruned(b"d", &[3.0, 0.0], b"", Some(hint))
            .unwrap();
        assert_eq!(actual.entry_point, baseline.entry_point);
        for (actual, expected) in actual.nodes.iter().zip(&baseline.nodes) {
            assert_eq!(
                actual.as_ref().map(|node| &node.neighbors),
                expected.as_ref().map(|node| &node.neighbors),
                "invalid hint {hint} must not change graph edges"
            );
        }
    }
    let mut graph = make_graph();
    let entry = graph.insert(b"a", &[0.0, 0.0], b"").unwrap();
    let mut baseline = graph.clone();
    let inserted = baseline
        .insert_unpruned(b"b", &[1.0, 0.0], b"", None)
        .unwrap();
    graph
        .insert_unpruned(b"b", &[1.0, 0.0], b"", Some(entry))
        .unwrap();
    assert_eq!(
        graph.nodes[inserted as usize].as_ref().unwrap().neighbors,
        baseline.nodes[inserted as usize]
            .as_ref()
            .unwrap()
            .neighbors,
        "duplicate entry hint must not duplicate graph edges"
    );
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn public_insert_keeps_candidates_below_neighbor_limit() {
    let mut graph = make_graph();
    let a = graph.insert(b"a", &[0.0, 0.0], b"").unwrap();
    let b = graph.insert(b"b", &[1.0, 0.0], b"").unwrap();
    let c = graph.insert(b"c", &[2.0, 0.0], b"").unwrap();
    let mut neighbors = graph.nodes[c as usize].as_ref().unwrap().neighbors[0].clone();
    neighbors.sort_unstable();
    assert_eq!(
        neighbors,
        vec![a, b],
        "fewer candidates than M must retain both candidates without diversity pruning"
    );
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
fn neighbor_selection_reuses_cached_node_norms() {
    for (metric, left_vector, right_vector, expected_score) in [
        (Metric::Cosine, [1.0, 0.0], [0.0, 1.0], 0.0),
        (Metric::L2, [0.0, 0.0], [3.0, 4.0], -5.0),
        (Metric::InnerProduct, [1.0, 2.0], [3.0, 4.0], 11.0),
    ] {
        let mut graph = HnswGraph::new(base_config().with_metric(metric)).unwrap();
        let left = graph.insert(b"left", &left_vector, b"").unwrap();
        let right = graph.insert(b"right", &right_vector, b"").unwrap();

        assert!(
            (graph.node_similarity(left, right) - expected_score).abs() < f64::from(f32::EPSILON)
        );
    }
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
fn reverse_link_pruning_preserves_equidistant_directions() {
    let mut graph = HnswGraph::new(base_config().with_metric(Metric::Cosine)).unwrap();
    let root = graph.insert(b"root", &[1.0, 0.0], b"").unwrap();
    let same = graph.insert(b"same", &[1.0, 0.0], b"").unwrap();
    let left = graph.insert(b"left", &[0.0, 1.0], b"").unwrap();
    let right = graph.insert(b"right", &[0.0, -1.0], b"").unwrap();
    graph.nodes[root as usize].as_mut().unwrap().neighbors[0] = vec![same, left, right];

    graph.prune_neighbors(root, 0, 2);

    assert_eq!(
        graph.nodes[root as usize].as_ref().unwrap().neighbors[0],
        vec![same, left]
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
fn maximum_ef_search_is_clamped_to_active_node_count() {
    let mut graph = make_graph();
    for (key, vector) in [
        (&b"a"[..], &[0.0, 0.0][..]),
        (&b"b"[..], &[1.0, 0.0][..]),
        (&b"c"[..], &[2.0, 0.0][..]),
    ] {
        graph.insert(key, vector, b"").unwrap();
    }

    let active_count = usize::try_from(graph.active_count).unwrap();
    let (results, stats) = graph.search(&[0.0, 0.0], 1, MAX_HNSW_EF_SEARCH).unwrap();

    assert_eq!(active_count, 3);
    assert_eq!(stats.effective_ef_search, active_count);
    assert!(stats.effective_ef_search <= active_count);
    assert_eq!(results.len(), 1);
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
fn deleted_bridges_do_not_consume_live_ef_slots() {
    let mut graph = make_graph();
    graph.insert(b"a", &[0.0, 0.0], b"ma").unwrap();
    graph.insert(b"b", &[1.0, 0.0], b"mb").unwrap();
    graph.insert(b"c", &[2.0, 0.0], b"mc").unwrap();
    graph.delete(b"b").unwrap();
    // A deterministic layer requires traversal through the deleted middle node.
    graph.nodes[0].as_mut().unwrap().neighbors = vec![vec![1]];
    graph.nodes[1].as_mut().unwrap().neighbors = vec![vec![0, 2]];
    graph.nodes[2].as_mut().unwrap().neighbors = vec![vec![1]];
    graph.entry_point = Some(0);
    graph.max_level = 0;

    let (results, stats) = graph.search(&[0.0, 0.0], 3, MAX_HNSW_EF_SEARCH).unwrap();
    assert_eq!(stats.effective_ef_search, 2);
    assert_eq!(
        results
            .iter()
            .map(|hit| hit.key.as_slice())
            .collect::<Vec<_>>(),
        vec![b"a", b"c"]
    );
    assert_eq!(results[1].metadata, b"mc");
    assert_eq!(results[1].distance, 2.0);

    // ef=1 must expand the entry before comparing its neighbors; equality with
    // the current worst result is not a reason to terminate the traversal.
    let (results, stats) = graph.search(&[2.0, 0.0], 1, 1).unwrap();
    assert_eq!(stats.effective_ef_search, 1);
    assert_eq!(results[0].key, b"c");
    assert_eq!(results[0].distance, 0.0);
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
fn search_retains_original_entry_base_layer_route() {
    let mut graph = make_graph();
    let root = graph.insert(b"root", &[10.0, 0.0], b"").unwrap();
    let local = graph.insert(b"local", &[1.0, 0.0], b"").unwrap();
    let bridge = graph.insert(b"bridge", &[5.0, 0.0], b"").unwrap();
    let nearest = graph.insert(b"nearest", &[0.0, 0.0], b"").unwrap();
    graph.entry_point = Some(root);
    graph.max_level = 1;
    graph.nodes[root as usize].as_mut().unwrap().neighbors = vec![vec![bridge], vec![local]];
    graph.nodes[local as usize].as_mut().unwrap().neighbors = vec![vec![root], vec![root]];
    graph.nodes[bridge as usize].as_mut().unwrap().neighbors = vec![vec![nearest]];
    graph.nodes[nearest as usize].as_mut().unwrap().neighbors = vec![vec![]];

    let (hits, stats, ef) = graph.search_with_effective_ef(&[0.0, 0.0], 1, 1).unwrap();
    assert_eq!(hits[0].key, b"nearest");
    assert_eq!(ef, 1, "the additional route must not enlarge ef");
    assert!(
        stats.distance_computations >= 6,
        "both routes count as search work"
    );
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn additional_base_route_does_not_discard_original_search_results() {
    let mut graph = make_graph();
    let root = graph.insert(b"root", &[10.0, 0.0], b"").unwrap();
    let local = graph.insert(b"local", &[1.0, 0.0], b"").unwrap();
    let auxiliary = graph.insert(b"auxiliary", &[0.5, 0.0], b"").unwrap();
    let nearest = graph.insert(b"nearest", &[0.0, 0.0], b"").unwrap();
    graph.entry_point = Some(root);
    graph.max_level = 1;
    graph.nodes[root as usize].as_mut().unwrap().neighbors = vec![vec![auxiliary], vec![local]];
    graph.nodes[local as usize].as_mut().unwrap().neighbors = vec![vec![nearest], vec![root]];
    graph.nodes[auxiliary as usize].as_mut().unwrap().neighbors = vec![vec![]];
    graph.nodes[nearest as usize].as_mut().unwrap().neighbors = vec![vec![]];

    let (hits, _, ef) = graph.search_with_effective_ef(&[0.0, 0.0], 1, 1).unwrap();
    assert_eq!(hits[0].key, b"nearest");
    assert_eq!(ef, 1);

    // With k=2 the auxiliary route improves the cutoff, so the second search
    // must run while retaining the nearest result from the original route.
    let (hits, stats, ef) = graph.search_with_effective_ef(&[0.0, 0.0], 2, 2).unwrap();
    assert_eq!(
        hits.iter()
            .map(|hit| hit.key.as_slice())
            .collect::<Vec<_>>(),
        vec![b"nearest".as_slice(), b"auxiliary".as_slice()]
    );
    assert_eq!(ef, 2);
    assert_eq!(stats.distance_computations, 10);
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn root_probe_abandons_slow_ladder_that_never_beats_cutoff() {
    const CHAIN: usize = 40;
    let mut graph = make_graph();
    let root = graph.insert(b"root", &[100.0, 0.0], b"").unwrap();
    let local = graph.insert(b"local", &[1.0, 0.0], b"").unwrap();
    let chain: Vec<u32> = (1..=CHAIN)
        .map(|i| {
            let key = format!("c{i:03}");
            graph
                .insert(key.as_bytes(), &[100.0 - i as f32, 0.0], b"")
                .unwrap()
        })
        .collect();
    graph.entry_point = Some(root);
    graph.max_level = 1;
    graph.nodes[root as usize].as_mut().unwrap().neighbors = vec![vec![chain[0]], vec![local]];
    graph.nodes[local as usize].as_mut().unwrap().neighbors = vec![vec![root], vec![root]];
    for (i, &id) in chain.iter().enumerate() {
        let next = chain.get(i + 1).map_or(vec![], |&n| vec![n]);
        graph.nodes[id as usize].as_mut().unwrap().neighbors = vec![next];
    }

    // ef=1, max_level=1: the probe may take 2 hops. The ladder stays far below
    // the first route's result (`local`), so it is abandoned: descent (3) +
    // first search (2) + root walk (3). HEAD walks all 40 hops (~47).
    let (hits, stats, _) = graph.search_with_effective_ef(&[0.0, 0.0], 1, 1).unwrap();
    assert_eq!(hits[0].key, b"local");
    assert_eq!(stats.distance_computations, 8);
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn root_probe_finds_distinct_basin_that_crosses_cutoff_within_bound() {
    // The first route is stuck at an isolated `local`; `nearest` is two
    // base-layer hops from the root, within the probe bound (ef/2 + max_level).
    let mut graph = make_graph();
    let root = graph.insert(b"root", &[100.0, 0.0], b"").unwrap();
    let local = graph.insert(b"local", &[10.0, 0.0], b"").unwrap();
    let a = graph.insert(b"a", &[50.0, 0.0], b"").unwrap();
    let nearest = graph.insert(b"nearest", &[0.0, 0.0], b"").unwrap();
    graph.entry_point = Some(root);
    graph.max_level = 1;
    graph.nodes[root as usize].as_mut().unwrap().neighbors = vec![vec![a], vec![local]];
    graph.nodes[local as usize].as_mut().unwrap().neighbors = vec![vec![], vec![root]];
    graph.nodes[a as usize].as_mut().unwrap().neighbors = vec![vec![nearest]];
    graph.nodes[nearest as usize].as_mut().unwrap().neighbors = vec![vec![]];

    let (hits, _, _) = graph.search_with_effective_ef(&[0.0, 0.0], 1, 3).unwrap();
    assert_eq!(hits[0].key, b"nearest");
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
