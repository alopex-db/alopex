//! Vector schema, metrics, and validation helpers.

use serde::{Deserialize, Serialize};
use std::str::FromStr;

use crate::{Error, Result};
pub mod columnar;
pub mod flat;
pub mod simd;

/// Supported similarity/distance metrics.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Metric {
    /// Cosine similarity.
    Cosine,
    /// Negative L2 distance (larger is closer).
    L2,
    /// Inner product (dot product).
    InnerProduct,
}

impl Metric {
    /// Returns a display name for the metric.
    pub fn as_str(&self) -> &'static str {
        match self {
            Metric::Cosine => "cosine",
            Metric::L2 => "l2",
            Metric::InnerProduct => "inner",
        }
    }
}

impl FromStr for Metric {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        match s.to_ascii_lowercase().as_str() {
            "cosine" => Ok(Metric::Cosine),
            "l2" => Ok(Metric::L2),
            "inner" | "inner_product" | "innerproduct" => Ok(Metric::InnerProduct),
            other => Err(Error::UnsupportedMetric {
                metric: other.to_string(),
            }),
        }
    }
}

/// Schema for a vector column (dimension + metric).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct VectorType {
    dim: usize,
    metric: Metric,
}

impl VectorType {
    /// Creates a new vector type with a dimension and metric.
    pub fn new(dim: usize, metric: Metric) -> Self {
        Self { dim, metric }
    }

    /// Declared dimension of the vector.
    pub fn dim(&self) -> usize {
        self.dim
    }

    /// Declared metric for the vector column.
    pub fn metric(&self) -> Metric {
        self.metric
    }

    /// Validates a vector against the declared dimension.
    pub fn validate(&self, vector: &[f32]) -> Result<()> {
        validate_dimensions(self.dim, vector.len())
    }

    /// Validates both vectors and returns a similarity score.
    pub fn score(&self, query: &[f32], item: &[f32]) -> Result<f32> {
        self.validate(query)?;
        self.validate(item)?;
        score(self.metric, query, item)
    }
}

/// Validates that the provided length matches the expected dimension.
pub fn validate_dimensions(expected: usize, actual: usize) -> Result<()> {
    if expected != actual {
        return Err(Error::DimensionMismatch { expected, actual });
    }
    Ok(())
}

/// Calculates a similarity score using the given metric.
///
/// - Cosine: dot(q, v) / (||q|| * ||v||); returns 0.0 if either norm is zero.
/// - L2: returns the negative Euclidean distance so that larger is closer.
/// - InnerProduct: dot(q, v).
pub fn score(metric: Metric, query: &[f32], item: &[f32]) -> Result<f32> {
    validate_dimensions(query.len(), item.len())?;

    match metric {
        Metric::Cosine => {
            let dot = query
                .iter()
                .zip(item.iter())
                .map(|(a, b)| a * b)
                .sum::<f32>();
            let q_norm = query.iter().map(|v| v * v).sum::<f32>().sqrt();
            let i_norm = item.iter().map(|v| v * v).sum::<f32>().sqrt();

            if q_norm == 0.0 || i_norm == 0.0 {
                return Ok(0.0);
            }

            Ok(dot / (q_norm * i_norm))
        }
        Metric::L2 => {
            let dist = query
                .iter()
                .zip(item.iter())
                .map(|(a, b)| {
                    let d = a - b;
                    d * d
                })
                .sum::<f32>()
                .sqrt();
            Ok(-dist)
        }
        Metric::InnerProduct => Ok(query
            .iter()
            .zip(item.iter())
            .map(|(a, b)| a * b)
            .sum::<f32>()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_dimension_mismatch() {
        let vt = VectorType::new(3, Metric::Cosine);
        let err = vt.validate(&[1.0, 2.0]).unwrap_err();
        assert!(matches!(
            err,
            Error::DimensionMismatch {
                expected: 3,
                actual: 2
            }
        ));

        let err = score(Metric::L2, &[1.0, 2.0], &[1.0]).unwrap_err();
        assert!(matches!(
            err,
            Error::DimensionMismatch {
                expected: 2,
                actual: 1
            }
        ));
    }

    #[test]
    fn computes_cosine() {
        let vt = VectorType::new(3, Metric::Cosine);
        let s = vt.score(&[1.0, 0.0, 0.0], &[0.0, 1.0, 0.0]).unwrap();
        assert_eq!(s, 0.0);

        let s = vt.score(&[1.0, 1.0, 0.0], &[1.0, 1.0, 0.0]).unwrap();
        assert!((s - 1.0).abs() < 1e-6);
    }

    #[test]
    fn computes_l2_as_negative_distance() {
        let s = score(Metric::L2, &[0.0, 0.0], &[3.0, 4.0]).unwrap();
        assert!((s + 5.0).abs() < 1e-6);
    }

    #[test]
    fn computes_inner_product() {
        let s = score(Metric::InnerProduct, &[1.0, 2.0, 3.0], &[4.0, 5.0, 6.0]).unwrap();
        assert_eq!(s, 32.0);
    }

    #[test]
    fn parses_metric_from_str() {
        assert_eq!(Metric::from_str("cosine").unwrap(), Metric::Cosine);
        assert_eq!(Metric::from_str("L2").unwrap(), Metric::L2);
        assert_eq!(
            Metric::from_str("inner_product").unwrap(),
            Metric::InnerProduct
        );

        let err = Metric::from_str("chebyshev").unwrap_err();
        assert!(matches!(err, Error::UnsupportedMetric { .. }));
    }
}
