//! Type definitions for the Alopex SQL planner.
//!
//! This module defines [`ResolvedType`], the normalized type representation
//! used during type checking and planning phases.

use crate::ast::ddl::{DataType, VectorMetric};

/// Normalized type information for type checking.
///
/// This enum represents the resolved type after normalization from AST types.
/// For example, `INTEGER` and `INT` both resolve to [`ResolvedType::Integer`].
///
/// # Examples
///
/// ```
/// use alopex_sql::planner::types::ResolvedType;
/// use alopex_sql::ast::ddl::{DataType, VectorMetric};
///
/// // Convert from AST DataType
/// let int_type = ResolvedType::from_ast(&DataType::Integer);
/// assert_eq!(int_type, ResolvedType::Integer);
///
/// // VECTOR with omitted metric defaults to Cosine
/// let vec_type = ResolvedType::from_ast(&DataType::Vector { dimension: 128, metric: None });
/// assert_eq!(vec_type, ResolvedType::Vector { dimension: 128, metric: VectorMetric::Cosine });
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResolvedType {
    /// Integer type (INTEGER, INT → Integer)
    Integer,
    /// Big integer type
    BigInt,
    /// Single-precision floating point
    Float,
    /// Double-precision floating point
    Double,
    /// Text/string type
    Text,
    /// Binary data type
    Blob,
    /// Boolean type (BOOLEAN, BOOL → Boolean)
    Boolean,
    /// Timestamp type
    Timestamp,
    /// Vector type with dimension and metric
    /// Metric is always populated (defaults to Cosine if omitted in AST)
    Vector {
        dimension: u32,
        metric: VectorMetric,
    },
    /// NULL type (for NULL literals)
    Null,
}

impl ResolvedType {
    /// Convert from AST [`DataType`] to [`ResolvedType`].
    ///
    /// For `VECTOR` types, if metric is omitted in the AST, it defaults to `Cosine`.
    ///
    /// # Examples
    ///
    /// ```
    /// use alopex_sql::planner::types::ResolvedType;
    /// use alopex_sql::ast::ddl::{DataType, VectorMetric};
    ///
    /// // INTEGER and INT both resolve to Integer
    /// assert_eq!(ResolvedType::from_ast(&DataType::Integer), ResolvedType::Integer);
    /// assert_eq!(ResolvedType::from_ast(&DataType::Int), ResolvedType::Integer);
    ///
    /// // BOOLEAN and BOOL both resolve to Boolean
    /// assert_eq!(ResolvedType::from_ast(&DataType::Boolean), ResolvedType::Boolean);
    /// assert_eq!(ResolvedType::from_ast(&DataType::Bool), ResolvedType::Boolean);
    ///
    /// // VECTOR with metric
    /// let vec_with_metric = ResolvedType::from_ast(&DataType::Vector {
    ///     dimension: 128,
    ///     metric: Some(VectorMetric::L2),
    /// });
    /// assert_eq!(vec_with_metric, ResolvedType::Vector {
    ///     dimension: 128,
    ///     metric: VectorMetric::L2,
    /// });
    ///
    /// // VECTOR without metric defaults to Cosine
    /// let vec_default = ResolvedType::from_ast(&DataType::Vector {
    ///     dimension: 256,
    ///     metric: None,
    /// });
    /// assert_eq!(vec_default, ResolvedType::Vector {
    ///     dimension: 256,
    ///     metric: VectorMetric::Cosine,
    /// });
    /// ```
    pub fn from_ast(dt: &DataType) -> Self {
        match dt {
            DataType::Integer | DataType::Int => Self::Integer,
            DataType::BigInt => Self::BigInt,
            DataType::Float => Self::Float,
            DataType::Double => Self::Double,
            DataType::Text => Self::Text,
            DataType::Blob => Self::Blob,
            DataType::Boolean | DataType::Bool => Self::Boolean,
            DataType::Timestamp => Self::Timestamp,
            DataType::Vector { dimension, metric } => Self::Vector {
                dimension: *dimension,
                metric: metric.unwrap_or(VectorMetric::Cosine),
            },
        }
    }

    /// Check if this type can be implicitly cast to the target type.
    ///
    /// Implicit conversion rules:
    /// - Same types are always compatible
    /// - `Null` can be cast to any type
    /// - Numeric widening: `Integer` → `BigInt`, `Float`, `Double`
    /// - Numeric widening: `BigInt` → `Double`
    /// - Numeric widening: `Float` → `Double`
    /// - `Vector` types require dimension check (done separately)
    ///
    /// # Examples
    ///
    /// ```
    /// use alopex_sql::planner::types::ResolvedType;
    ///
    /// // Same type
    /// assert!(ResolvedType::Integer.can_cast_to(&ResolvedType::Integer));
    ///
    /// // Null can cast to any type
    /// assert!(ResolvedType::Null.can_cast_to(&ResolvedType::Integer));
    /// assert!(ResolvedType::Null.can_cast_to(&ResolvedType::Text));
    ///
    /// // Numeric widening
    /// assert!(ResolvedType::Integer.can_cast_to(&ResolvedType::BigInt));
    /// assert!(ResolvedType::Integer.can_cast_to(&ResolvedType::Float));
    /// assert!(ResolvedType::Integer.can_cast_to(&ResolvedType::Double));
    /// assert!(ResolvedType::BigInt.can_cast_to(&ResolvedType::Double));
    /// assert!(ResolvedType::Float.can_cast_to(&ResolvedType::Double));
    ///
    /// // Incompatible types
    /// assert!(!ResolvedType::Text.can_cast_to(&ResolvedType::Integer));
    /// assert!(!ResolvedType::BigInt.can_cast_to(&ResolvedType::Integer));
    /// ```
    pub fn can_cast_to(&self, target: &ResolvedType) -> bool {
        use ResolvedType::*;

        match (self, target) {
            // Same type is always compatible
            (a, b) if a == b => true,

            // Null can be cast to any type
            (Null, _) => true,

            // Numeric widening conversions
            (Integer, BigInt | Float | Double) => true,
            (BigInt, Double) => true,
            (Float, Double) => true,

            // Vector types require dimension check (done separately in TypeChecker)
            (Vector { .. }, Vector { .. }) => false,

            // All other conversions are not allowed
            _ => false,
        }
    }

    /// Get a human-readable name for this type.
    ///
    /// Used for error messages.
    pub fn type_name(&self) -> &'static str {
        match self {
            Self::Integer => "Integer",
            Self::BigInt => "BigInt",
            Self::Float => "Float",
            Self::Double => "Double",
            Self::Text => "Text",
            Self::Blob => "Blob",
            Self::Boolean => "Boolean",
            Self::Timestamp => "Timestamp",
            Self::Vector { .. } => "Vector",
            Self::Null => "Null",
        }
    }
}

impl std::fmt::Display for ResolvedType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Integer => write!(f, "INTEGER"),
            Self::BigInt => write!(f, "BIGINT"),
            Self::Float => write!(f, "FLOAT"),
            Self::Double => write!(f, "DOUBLE"),
            Self::Text => write!(f, "TEXT"),
            Self::Blob => write!(f, "BLOB"),
            Self::Boolean => write!(f, "BOOLEAN"),
            Self::Timestamp => write!(f, "TIMESTAMP"),
            Self::Vector { dimension, metric } => {
                write!(f, "VECTOR({}, {:?})", dimension, metric)
            }
            Self::Null => write!(f, "NULL"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_from_ast_integer() {
        assert_eq!(
            ResolvedType::from_ast(&DataType::Integer),
            ResolvedType::Integer
        );
        assert_eq!(
            ResolvedType::from_ast(&DataType::Int),
            ResolvedType::Integer
        );
    }

    #[test]
    fn test_from_ast_boolean() {
        assert_eq!(
            ResolvedType::from_ast(&DataType::Boolean),
            ResolvedType::Boolean
        );
        assert_eq!(
            ResolvedType::from_ast(&DataType::Bool),
            ResolvedType::Boolean
        );
    }

    #[test]
    fn test_from_ast_vector_with_metric() {
        let dt = DataType::Vector {
            dimension: 128,
            metric: Some(VectorMetric::L2),
        };
        assert_eq!(
            ResolvedType::from_ast(&dt),
            ResolvedType::Vector {
                dimension: 128,
                metric: VectorMetric::L2,
            }
        );
    }

    #[test]
    fn test_from_ast_vector_default_metric() {
        let dt = DataType::Vector {
            dimension: 256,
            metric: None,
        };
        assert_eq!(
            ResolvedType::from_ast(&dt),
            ResolvedType::Vector {
                dimension: 256,
                metric: VectorMetric::Cosine,
            }
        );
    }

    #[test]
    fn test_can_cast_same_type() {
        assert!(ResolvedType::Integer.can_cast_to(&ResolvedType::Integer));
        assert!(ResolvedType::Text.can_cast_to(&ResolvedType::Text));
    }

    #[test]
    fn test_can_cast_null() {
        assert!(ResolvedType::Null.can_cast_to(&ResolvedType::Integer));
        assert!(ResolvedType::Null.can_cast_to(&ResolvedType::Text));
        assert!(ResolvedType::Null.can_cast_to(&ResolvedType::Boolean));
    }

    #[test]
    fn test_can_cast_numeric_widening() {
        // Integer → BigInt/Float/Double
        assert!(ResolvedType::Integer.can_cast_to(&ResolvedType::BigInt));
        assert!(ResolvedType::Integer.can_cast_to(&ResolvedType::Float));
        assert!(ResolvedType::Integer.can_cast_to(&ResolvedType::Double));

        // BigInt → Double
        assert!(ResolvedType::BigInt.can_cast_to(&ResolvedType::Double));

        // Float → Double
        assert!(ResolvedType::Float.can_cast_to(&ResolvedType::Double));
    }

    #[test]
    fn test_can_cast_incompatible() {
        // Text cannot cast to numeric
        assert!(!ResolvedType::Text.can_cast_to(&ResolvedType::Integer));

        // Numeric narrowing not allowed
        assert!(!ResolvedType::BigInt.can_cast_to(&ResolvedType::Integer));
        assert!(!ResolvedType::Double.can_cast_to(&ResolvedType::Float));
    }

    #[test]
    fn test_can_cast_vector() {
        let vec1 = ResolvedType::Vector {
            dimension: 128,
            metric: VectorMetric::Cosine,
        };
        let vec2 = ResolvedType::Vector {
            dimension: 128,
            metric: VectorMetric::L2,
        };
        // Vector dimension check is done separately
        assert!(!vec1.can_cast_to(&vec2));
    }

    #[test]
    fn test_display() {
        assert_eq!(format!("{}", ResolvedType::Integer), "INTEGER");
        assert_eq!(format!("{}", ResolvedType::Text), "TEXT");
        assert_eq!(
            format!(
                "{}",
                ResolvedType::Vector {
                    dimension: 128,
                    metric: VectorMetric::Cosine
                }
            ),
            "VECTOR(128, Cosine)"
        );
    }
}
