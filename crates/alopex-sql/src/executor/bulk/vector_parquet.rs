//! Standard Parquet LIST/FLOAT interchange for explicitly declared SQL VECTOR columns.
use std::sync::Arc;

use arrow_array::builder::{Float32Builder, ListBuilder};
use arrow_array::{
    Array, ArrayRef, FixedSizeListArray, Float32Array, Float64Array, LargeListArray, ListArray,
};
use arrow_schema::{DataType, Field};

use crate::ast::ddl::VectorMetric;
use crate::executor::{ExecutorError, Result};
use crate::storage::SqlValue;

pub(super) fn field(name: &str, dimension: u32, metric: VectorMetric) -> Field {
    Field::new(
        name,
        DataType::List(Arc::new(Field::new("item", DataType::Float32, true))),
        true,
    )
    .with_metadata(
        [
            ("alopex.vector.dimension".to_owned(), dimension.to_string()),
            (
                "alopex.vector.metric".to_owned(),
                match metric {
                    VectorMetric::Cosine => "COSINE",
                    VectorMetric::L2 => "L2",
                    VectorMetric::Inner => "INNER",
                }
                .to_owned(),
            ),
        ]
        .into_iter()
        .collect::<std::collections::HashMap<_, _>>(),
    )
}

pub(super) fn array(rows: &[Vec<SqlValue>], column: usize, dimension: u32) -> Result<ArrayRef> {
    let mut builder = ListBuilder::new(Float32Builder::new());
    for (row, values) in rows.iter().enumerate() {
        match values.get(column) {
            Some(SqlValue::Null) | None => builder.append(false),
            Some(SqlValue::Vector(values)) => {
                validate(values, dimension, row)?;
                builder.values().append_slice(values);
                builder.append(true);
            }
            _ => return Err(error(row, "expected VECTOR value")),
        }
    }
    Ok(Arc::new(builder.finish()))
}

pub(super) fn read(
    array: &dyn Array,
    dimension: u32,
    row: usize,
    source_row: usize,
) -> Result<SqlValue> {
    let values = match array.data_type() {
        DataType::List(_) => array
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap()
            .value(row),
        DataType::LargeList(_) => array
            .as_any()
            .downcast_ref::<LargeListArray>()
            .unwrap()
            .value(row),
        DataType::FixedSizeList(_, _) => array
            .as_any()
            .downcast_ref::<FixedSizeListArray>()
            .unwrap()
            .value(row),
        _ => {
            return Err(error(
                source_row,
                "expected a floating-point list for VECTOR",
            ));
        }
    };
    if values.len() != dimension as usize {
        return Err(error(
            source_row,
            &format!(
                "vector dimension mismatch: expected {dimension}, got {}",
                values.len()
            ),
        ));
    }
    if values.null_count() != 0 {
        return Err(error(source_row, "VECTOR elements cannot be NULL"));
    }
    let values: Vec<f32> = match values.data_type() {
        DataType::Float32 => values
            .as_any()
            .downcast_ref::<Float32Array>()
            .unwrap()
            .values()
            .to_vec(),
        DataType::Float64 => values
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .values()
            .iter()
            .map(|value| *value as f32)
            .collect(),
        _ => {
            return Err(error(
                source_row,
                "VECTOR elements must be FLOAT32 or FLOAT64",
            ));
        }
    };
    validate(&values, dimension, source_row)?;
    Ok(SqlValue::Vector(values))
}

fn validate(values: &[f32], dimension: u32, row: usize) -> Result<()> {
    if values.len() != dimension as usize {
        return Err(error(
            row,
            &format!(
                "vector dimension mismatch: expected {dimension}, got {}",
                values.len()
            ),
        ));
    }
    if values.iter().any(|value| !value.is_finite()) {
        return Err(error(
            row,
            "VECTOR elements must be finite and representable as FLOAT32",
        ));
    }
    Ok(())
}

fn error(row: usize, message: &str) -> ExecutorError {
    ExecutorError::BulkLoad(format!("row {}: {message}", row + 1))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::builder::{FixedSizeListBuilder, Float64Builder, LargeListBuilder};

    #[test]
    fn export_preserves_values_outer_null_and_advisory_metadata() {
        let rows = vec![
            vec![SqlValue::Vector(vec![0.25, -1.0, 2.0])],
            vec![SqlValue::Null],
        ];
        let values = array(&rows, 0, 3).unwrap();
        assert_eq!(
            values.data_type(),
            field("v", 3, VectorMetric::L2).data_type()
        );
        assert_eq!(read(values.as_ref(), 3, 0, 0).unwrap(), rows[0][0]);
        assert!(values.is_null(1));
        let metadata = field("v", 3, VectorMetric::L2);
        assert_eq!(metadata.metadata()["alopex.vector.dimension"], "3");
        assert_eq!(metadata.metadata()["alopex.vector.metric"], "L2");
    }

    #[test]
    fn float64_list_rounds_to_float32() {
        let mut builder = ListBuilder::new(Float64Builder::new());
        builder
            .values()
            .append_slice(&[0.1, -0.0, f64::from(f32::MAX)]);
        builder.append(true);
        assert_eq!(
            read(&builder.finish(), 3, 0, 0).unwrap(),
            SqlValue::Vector(vec![0.1_f64 as f32, -0.0, f32::MAX])
        );
    }

    #[test]
    fn large_list_float32_is_supported() {
        let mut builder = LargeListBuilder::new(Float32Builder::new());
        builder.values().append_slice(&[1.0, 2.0, 3.0]);
        builder.append(true);
        assert_eq!(
            read(&builder.finish(), 3, 0, 0).unwrap(),
            SqlValue::Vector(vec![1.0, 2.0, 3.0])
        );
    }

    #[test]
    fn fixed_list_float64_is_supported() {
        let mut builder = FixedSizeListBuilder::new(Float64Builder::new(), 3);
        builder.values().append_slice(&[1.0, 2.0, 3.0]);
        builder.append(true);
        assert_eq!(
            read(&builder.finish(), 3, 0, 0).unwrap(),
            SqlValue::Vector(vec![1.0, 2.0, 3.0])
        );
    }

    #[test]
    fn malformed_dimensions_are_rejected_with_source_row() {
        for values in [vec![], vec![1.0, 2.0], vec![1.0, 2.0, 3.0, 4.0]] {
            let mut builder = ListBuilder::new(Float32Builder::new());
            builder.values().append_slice(&values);
            builder.append(true);
            let error = read(&builder.finish(), 3, 0, 7).unwrap_err().to_string();
            assert!(
                error.contains("row 8: vector dimension mismatch"),
                "{error}"
            );
        }
    }

    #[test]
    fn null_elements_are_rejected() {
        let mut builder = ListBuilder::new(Float32Builder::new());
        builder.values().append_value(1.0);
        builder.values().append_null();
        builder.values().append_value(3.0);
        builder.append(true);
        assert!(
            read(&builder.finish(), 3, 0, 0)
                .unwrap_err()
                .to_string()
                .contains("elements cannot be NULL")
        );
    }

    #[test]
    fn nonfinite_and_float64_overflow_are_rejected() {
        for value in [
            f64::NAN,
            f64::INFINITY,
            f64::NEG_INFINITY,
            f64::MAX,
            -f64::MAX,
        ] {
            let mut builder = ListBuilder::new(Float64Builder::new());
            builder.values().append_slice(&[1.0, value, 3.0]);
            builder.append(true);
            assert!(
                read(&builder.finish(), 3, 0, 0)
                    .unwrap_err()
                    .to_string()
                    .contains("finite and representable")
            );
        }
    }

    #[test]
    fn invalid_output_is_rejected() {
        for values in [
            vec![1.0],
            vec![1.0, f32::NAN, 3.0],
            vec![1.0, f32::INFINITY, 3.0],
        ] {
            assert!(array(&[vec![SqlValue::Vector(values)]], 0, 3).is_err());
        }
        assert!(array(&[vec![SqlValue::Text("not a vector".into())]], 0, 3).is_err());
    }
}
