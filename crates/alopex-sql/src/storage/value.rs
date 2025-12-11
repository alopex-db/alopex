use std::cmp::Ordering;
use std::convert::TryFrom;

use crate::ast::ddl::VectorMetric;
use crate::planner::ResolvedType;
use serde::{Deserialize, Serialize};

use super::error::{Result, StorageError};

/// Runtime value representation for the SQL storage layer.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum SqlValue {
    Null,
    Integer(i32),
    BigInt(i64),
    Float(f32),
    Double(f64),
    Text(String),
    Blob(Vec<u8>),
    Boolean(bool),
    Timestamp(i64), // microseconds since epoch
    Vector(Vec<f32>),
}

impl SqlValue {
    /// Returns the type tag for serialization (see design type tags).
    pub fn type_tag(&self) -> u8 {
        match self {
            SqlValue::Null => 0x00,
            SqlValue::Integer(_) => 0x01,
            SqlValue::BigInt(_) => 0x02,
            SqlValue::Float(_) => 0x03,
            SqlValue::Double(_) => 0x04,
            SqlValue::Text(_) => 0x05,
            SqlValue::Blob(_) => 0x06,
            SqlValue::Boolean(_) => 0x07,
            SqlValue::Timestamp(_) => 0x08,
            SqlValue::Vector(_) => 0x09,
        }
    }

    /// Returns true if the value is Null.
    pub fn is_null(&self) -> bool {
        matches!(self, SqlValue::Null)
    }

    /// Human-readable type name for error reporting.
    pub fn type_name(&self) -> &'static str {
        match self {
            SqlValue::Null => "Null",
            SqlValue::Integer(_) => "Integer",
            SqlValue::BigInt(_) => "BigInt",
            SqlValue::Float(_) => "Float",
            SqlValue::Double(_) => "Double",
            SqlValue::Text(_) => "Text",
            SqlValue::Blob(_) => "Blob",
            SqlValue::Boolean(_) => "Boolean",
            SqlValue::Timestamp(_) => "Timestamp",
            SqlValue::Vector(_) => "Vector",
        }
    }

    /// Returns the corresponding ResolvedType for this value.
    pub fn resolved_type(&self) -> ResolvedType {
        match self {
            SqlValue::Null => ResolvedType::Null,
            SqlValue::Integer(_) => ResolvedType::Integer,
            SqlValue::BigInt(_) => ResolvedType::BigInt,
            SqlValue::Float(_) => ResolvedType::Float,
            SqlValue::Double(_) => ResolvedType::Double,
            SqlValue::Text(_) => ResolvedType::Text,
            SqlValue::Blob(_) => ResolvedType::Blob,
            SqlValue::Boolean(_) => ResolvedType::Boolean,
            SqlValue::Timestamp(_) => ResolvedType::Timestamp,
            SqlValue::Vector(v) => ResolvedType::Vector {
                dimension: v.len() as u32,
                metric: VectorMetric::Cosine,
            },
        }
    }
}

impl PartialOrd for SqlValue {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        use SqlValue::*;
        match (self, other) {
            (Null, _) | (_, Null) => None,
            (Integer(a), Integer(b)) => Some(a.cmp(b)),
            (BigInt(a), BigInt(b)) => Some(a.cmp(b)),
            (Float(a), Float(b)) => a.partial_cmp(b),
            (Double(a), Double(b)) => a.partial_cmp(b),
            (Text(a), Text(b)) => Some(a.cmp(b)),
            (Blob(a), Blob(b)) => Some(a.cmp(b)),
            (Boolean(a), Boolean(b)) => Some(a.cmp(b)),
            (Timestamp(a), Timestamp(b)) => Some(a.cmp(b)),
            // Vector ordering is undefined for now.
            (Vector(_), Vector(_)) => None,
            _ => None,
        }
    }
}

macro_rules! impl_from_sqlvalue_try {
    ($target:ty, $variant:ident) => {
        impl TryFrom<SqlValue> for $target {
            type Error = StorageError;
            fn try_from(value: SqlValue) -> Result<Self> {
                if let SqlValue::$variant(v) = value {
                    Ok(v.into())
                } else {
                    Err(StorageError::TypeMismatch {
                        expected: stringify!($variant).to_string(),
                        actual: value.type_name().to_string(),
                    })
                }
            }
        }
    };
}

macro_rules! impl_from_primitive {
    ($source:ty, $variant:ident) => {
        impl From<$source> for SqlValue {
            fn from(value: $source) -> Self {
                SqlValue::$variant(value.into())
            }
        }
    };
}

impl_from_primitive!(i32, Integer);
impl_from_primitive!(i64, BigInt);
impl_from_primitive!(f32, Float);
impl_from_primitive!(f64, Double);
impl_from_primitive!(bool, Boolean);
impl_from_primitive!(String, Text);
impl_from_primitive!(&str, Text);
impl_from_primitive!(Vec<u8>, Blob);
impl From<&[u8]> for SqlValue {
    fn from(value: &[u8]) -> Self {
        SqlValue::Blob(value.to_vec())
    }
}
impl From<Vec<f32>> for SqlValue {
    fn from(value: Vec<f32>) -> Self {
        SqlValue::Vector(value)
    }
}

impl_from_sqlvalue_try!(i32, Integer);
impl_from_sqlvalue_try!(f32, Float);
impl_from_sqlvalue_try!(f64, Double);
impl_from_sqlvalue_try!(bool, Boolean);
impl_from_sqlvalue_try!(String, Text);
impl_from_sqlvalue_try!(Vec<u8>, Blob);

impl TryFrom<SqlValue> for i64 {
    type Error = StorageError;
    fn try_from(value: SqlValue) -> Result<Self> {
        match value {
            SqlValue::BigInt(v) | SqlValue::Timestamp(v) => Ok(v),
            other => Err(StorageError::TypeMismatch {
                expected: "BigInt/Timestamp".to_string(),
                actual: other.type_name().to_string(),
            }),
        }
    }
}

impl TryFrom<SqlValue> for Vec<f32> {
    type Error = StorageError;
    fn try_from(value: SqlValue) -> Result<Self> {
        if let SqlValue::Vector(v) = value {
            Ok(v)
        } else {
            Err(StorageError::TypeMismatch {
                expected: "Vector".to_string(),
                actual: value.type_name().to_string(),
            })
        }
    }
}

impl TryFrom<&SqlValue> for ResolvedType {
    type Error = StorageError;
    fn try_from(value: &SqlValue) -> Result<Self> {
        Ok(value.resolved_type())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    #[test]
    fn null_comparisons_return_none() {
        assert!(SqlValue::Null.partial_cmp(&SqlValue::Null).is_none());
        assert!(SqlValue::Null.partial_cmp(&SqlValue::Integer(1)).is_none());
    }

    #[test]
    fn heterogeneous_comparisons_return_none() {
        assert!(
            SqlValue::Integer(1)
                .partial_cmp(&SqlValue::Double(1.0))
                .is_none()
        );
        assert!(
            SqlValue::Text("a".into())
                .partial_cmp(&SqlValue::Blob(vec![0x61]))
                .is_none()
        );
    }

    #[test]
    fn same_type_comparisons_work() {
        assert_eq!(
            SqlValue::Integer(1).partial_cmp(&SqlValue::Integer(2)),
            Some(Ordering::Less)
        );
        assert_eq!(
            SqlValue::Text("b".into()).partial_cmp(&SqlValue::Text("a".into())),
            Some(Ordering::Greater)
        );
        assert_eq!(
            SqlValue::Boolean(false).partial_cmp(&SqlValue::Boolean(true)),
            Some(Ordering::Less)
        );
    }

    proptest! {
        #[test]
        fn integer_roundtrip(v in any::<i32>()) {
            let sql: SqlValue = v.into();
            let back = i32::try_from(sql.clone()).unwrap();
            prop_assert_eq!(back, v);
            prop_assert_eq!(sql.partial_cmp(&SqlValue::Integer(v)), Some(Ordering::Equal));
        }

        #[test]
        fn bigint_roundtrip(v in any::<i64>()) {
            let sql: SqlValue = SqlValue::BigInt(v);
            let back = i64::try_from(sql.clone()).unwrap();
            prop_assert_eq!(back, v);
        }

        #[test]
        fn float_roundtrip_non_nan(v in any::<f32>().prop_filter("no NaN", |f| f.is_finite())) {
            let sql: SqlValue = SqlValue::Float(v);
            let back = f32::try_from(sql.clone()).unwrap();
            prop_assert_eq!(back, v);
            prop_assert_eq!(sql.partial_cmp(&SqlValue::Float(v)), Some(Ordering::Equal));
        }

        #[test]
        fn text_roundtrip(s in ".*") {
            let sql: SqlValue = SqlValue::Text(s.clone());
            let back = String::try_from(sql.clone()).unwrap();
            prop_assert_eq!(back, s.clone());
            prop_assert_eq!(sql.partial_cmp(&SqlValue::Text(s)), Some(Ordering::Equal));
        }

        #[test]
        fn blob_roundtrip(data in proptest::collection::vec(any::<u8>(), 0..64)) {
            let sql: SqlValue = SqlValue::Blob(data.clone());
            let back = Vec::<u8>::try_from(sql.clone()).unwrap();
            prop_assert_eq!(back, data.clone());
            prop_assert_eq!(sql.partial_cmp(&SqlValue::Blob(data)), Some(Ordering::Equal));
        }
    }
}
