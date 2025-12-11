use std::str::FromStr;

use thiserror::Error;

/// ベクトル演算で発生するエラー。
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum VectorError {
    /// 引数数が想定と異なる。
    #[error("argument count mismatch: expected 3, got {actual}")]
    ArgumentCountMismatch { actual: usize },

    /// 第一引数がベクトル列ではない。
    #[error("type mismatch: first argument must be VECTOR column")]
    TypeMismatch,

    /// ベクトルリテラルが不正。
    #[error("invalid vector literal: {reason}")]
    InvalidVectorLiteral { reason: String },

    /// メトリクス指定が不正。
    #[error("invalid metric '{metric}': {reason}")]
    InvalidMetric { metric: String, reason: String },

    /// ベクトル次元が一致しない。
    #[error("dimension mismatch: column has {expected} dimensions, query has {actual}")]
    DimensionMismatch { expected: usize, actual: usize },

    /// コサイン類似度でゼロノルムベクトルが渡された。
    #[error("zero-norm vector cannot be used for cosine similarity")]
    ZeroNormVector,
}

/// ベクトル類似度/距離のメトリクス。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VectorMetric {
    Cosine,
    L2,
    Inner,
}

impl VectorMetric {
    /// 文字列をメトリクスに変換する（前後空白除去・小文字化）。
    pub fn from_str(s: &str) -> Result<Self, VectorError> {
        let normalized = s.trim().to_lowercase();
        match normalized.as_str() {
            "cosine" => Ok(Self::Cosine),
            "l2" => Ok(Self::L2),
            "inner" => Ok(Self::Inner),
            "" => Err(VectorError::InvalidMetric {
                metric: s.to_string(),
                reason: "empty metric string".into(),
            }),
            _ => Err(VectorError::InvalidMetric {
                metric: s.to_string(),
                reason: format!("expected 'cosine', 'l2', or 'inner', got '{}'", normalized),
            }),
        }
    }
}

impl FromStr for VectorMetric {
    type Err = VectorError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        VectorMetric::from_str(s)
    }
}

/// ベクトル類似度/距離を計算する。
///
/// - 内部計算: f32（メモリ効率）
/// - 返却値: f64（SQL DOUBLE として返す想定）
pub fn vector_similarity(
    column_value: &[f32],
    query_vector: &[f32],
    metric: VectorMetric,
) -> Result<f64, VectorError> {
    validate_dimensions(column_value, query_vector)?;

    match metric {
        VectorMetric::Cosine => compute_cosine_similarity(column_value, query_vector),
        VectorMetric::L2 => compute_l2_distance(column_value, query_vector),
        VectorMetric::Inner => compute_inner_product(column_value, query_vector),
    }
}

/// vector_similarity のエイリアス。距離メトリクスでも同一の実装を利用する。
pub fn vector_distance(
    column_value: &[f32],
    query_vector: &[f32],
    metric: VectorMetric,
) -> Result<f64, VectorError> {
    vector_similarity(column_value, query_vector, metric)
}

fn validate_dimensions(a: &[f32], b: &[f32]) -> Result<(), VectorError> {
    if a.len() != b.len() {
        return Err(VectorError::DimensionMismatch {
            expected: a.len(),
            actual: b.len(),
        });
    }
    Ok(())
}

fn compute_cosine_similarity(a: &[f32], b: &[f32]) -> Result<f64, VectorError> {
    let dot: f32 = a.iter().zip(b.iter()).map(|(x, y)| x * y).sum();
    let norm_a: f32 = a.iter().map(|x| x * x).sum::<f32>().sqrt();
    let norm_b: f32 = b.iter().map(|x| x * x).sum::<f32>().sqrt();

    if norm_a == 0.0 || norm_b == 0.0 {
        return Err(VectorError::ZeroNormVector);
    }

    Ok((dot / (norm_a * norm_b)) as f64)
}

fn compute_l2_distance(a: &[f32], b: &[f32]) -> Result<f64, VectorError> {
    let sum_sq: f32 = a.iter().zip(b.iter()).map(|(x, y)| (x - y).powi(2)).sum();
    Ok(sum_sq.sqrt() as f64)
}

fn compute_inner_product(a: &[f32], b: &[f32]) -> Result<f64, VectorError> {
    let sum: f32 = a.iter().zip(b.iter()).map(|(x, y)| x * y).sum();
    Ok(sum as f64)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn vector_metric_from_str_trims_and_lowercases() {
        assert_eq!(
            VectorMetric::from_str(" COSINE ").unwrap(),
            VectorMetric::Cosine
        );
        assert_eq!(VectorMetric::from_str("l2").unwrap(), VectorMetric::L2);
        assert_eq!(
            VectorMetric::from_str("Inner").unwrap(),
            VectorMetric::Inner
        );
    }

    #[test]
    fn vector_metric_from_str_empty_rejected() {
        let err = VectorMetric::from_str("").unwrap_err();
        assert!(matches!(
            err,
            VectorError::InvalidMetric { reason, .. } if reason.contains("empty")
        ));
    }

    #[test]
    fn vector_metric_from_str_unknown_rejected() {
        let err = VectorMetric::from_str("minkowski").unwrap_err();
        assert!(matches!(
            err,
            VectorError::InvalidMetric { reason, .. } if reason.contains("expected 'cosine', 'l2', or 'inner'")
        ));
    }

    #[test]
    fn vector_metric_from_str_trait_parse() {
        let m: VectorMetric = "cosine".parse().unwrap();
        assert_eq!(m, VectorMetric::Cosine);
    }

    #[test]
    fn cosine_similarity_basic() {
        let a = [1.0_f32, 0.0];
        let b = [0.0_f32, 1.0];
        let v = vector_similarity(&a, &b, VectorMetric::Cosine).unwrap();
        assert!((v - 0.0).abs() < 1e-6);
    }

    #[test]
    fn cosine_similarity_parallel() {
        let a = [1.0_f32, 1.0];
        let b = [2.0_f32, 2.0];
        let v = vector_similarity(&a, &b, VectorMetric::Cosine).unwrap();
        assert!((v - 1.0).abs() < 1e-6);
    }

    #[test]
    fn cosine_similarity_zero_norm_error() {
        let a = [0.0_f32, 0.0];
        let b = [1.0_f32, 1.0];
        let err = vector_similarity(&a, &b, VectorMetric::Cosine).unwrap_err();
        assert!(matches!(err, VectorError::ZeroNormVector));
    }

    #[test]
    fn l2_distance_basic() {
        let a = [0.0_f32, 0.0];
        let b = [3.0_f32, 4.0];
        let v = vector_similarity(&a, &b, VectorMetric::L2).unwrap();
        assert!((v - 5.0).abs() < 1e-6);
    }

    #[test]
    fn inner_product_basic() {
        let a = [1.0_f32, 2.0, 3.0];
        let b = [4.0_f32, 5.0, 6.0];
        let v = vector_similarity(&a, &b, VectorMetric::Inner).unwrap();
        assert!((v - 32.0).abs() < 1e-6);
    }

    #[test]
    fn dimension_mismatch_rejected() {
        let a = [1.0_f32, 2.0];
        let b = [1.0_f32, 2.0, 3.0];
        let err = vector_similarity(&a, &b, VectorMetric::L2).unwrap_err();
        assert!(matches!(
            err,
            VectorError::DimensionMismatch {
                expected: 2,
                actual: 3
            }
        ));
    }

    #[test]
    fn vector_distance_alias() {
        let a = [1.0_f32, 2.0];
        let b = [3.0_f32, 4.0];
        let sim = vector_similarity(&a, &b, VectorMetric::Inner).unwrap();
        let dist = vector_distance(&a, &b, VectorMetric::Inner).unwrap();
        assert!((sim - dist).abs() < 1e-6);
    }
}
