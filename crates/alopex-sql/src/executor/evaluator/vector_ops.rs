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
}
