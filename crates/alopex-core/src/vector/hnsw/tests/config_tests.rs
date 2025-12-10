use crate::vector::hnsw::HnswConfig;
use crate::vector::Metric;
use crate::Error;

#[test]
fn validate_accepts_valid_parameters() {
    let config = HnswConfig::default()
        .with_dimension(8)
        .with_m(32)
        .with_ef_construction(64);
    assert!(config.validate().is_ok());
}

#[test]
fn validate_rejects_invalid_m_range() {
    let low = HnswConfig {
        dimension: 8,
        metric: Metric::Cosine,
        m: 1,
        ef_construction: 10,
    };
    let high = HnswConfig {
        m: 101,
        ..low.clone()
    };

    for cfg in [low, high] {
        match cfg.validate() {
            Err(Error::InvalidParameter { param, .. }) => assert_eq!(param, "m"),
            other => panic!("m の検証に失敗: {:?}", other),
        }
    }
}

#[test]
fn validate_rejects_ef_construction_lt_m() {
    let cfg = HnswConfig {
        dimension: 16,
        metric: Metric::Cosine,
        m: 20,
        ef_construction: 10,
    };
    match cfg.validate() {
        Err(Error::InvalidParameter { param, .. }) => assert_eq!(param, "ef_construction"),
        other => panic!("ef_construction の検証に失敗: {:?}", other),
    }
}

#[test]
fn validate_rejects_invalid_dimension() {
    let zero = HnswConfig::default().with_dimension(0);
    let too_large = HnswConfig::default().with_dimension(70_000);
    for cfg in [zero, too_large] {
        match cfg.validate() {
            Err(Error::InvalidParameter { param, .. }) => assert_eq!(param, "dimension"),
            other => panic!("dimension の検証に失敗: {:?}", other),
        }
    }
}

#[test]
fn builder_methods_apply_each_field() {
    let cfg = HnswConfig::default()
        .with_dimension(42)
        .with_metric(Metric::L2)
        .with_m(24)
        .with_ef_construction(240);
    assert_eq!(cfg.dimension, 42);
    assert!(matches!(cfg.metric, Metric::L2));
    assert_eq!(cfg.m, 24);
    assert_eq!(cfg.ef_construction, 240);
}
