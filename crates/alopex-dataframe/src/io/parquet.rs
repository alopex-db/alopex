use std::collections::HashSet;
use std::fs::File;
use std::path::Path;

use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::{ArrowWriter, ProjectionMask};

use crate::io::options::ParquetReadOptions;
use crate::{col, DataFrame, DataFrameError, Expr, Result};

/// Read a Parquet file eagerly into a `DataFrame` using default `ParquetReadOptions`.
pub fn read_parquet(_path: impl AsRef<Path>) -> Result<DataFrame> {
    read_parquet_with_options(_path, &ParquetReadOptions::default())
}

/// Write a `DataFrame` to a Parquet file.
pub fn write_parquet(path: impl AsRef<Path>, df: &DataFrame) -> Result<()> {
    let path = path.as_ref();
    let file = File::create(path).map_err(|source| DataFrameError::io_with_path(source, path))?;

    let mut writer = ArrowWriter::try_new(file, df.schema(), None)
        .map_err(|source| DataFrameError::Parquet { source })?;

    for batch in df.to_arrow() {
        writer
            .write(&batch)
            .map_err(|source| DataFrameError::Parquet { source })?;
    }

    writer
        .close()
        .map_err(|source| DataFrameError::Parquet { source })?;

    Ok(())
}

/// Read a Parquet file eagerly into a `DataFrame` using the provided options.
pub fn read_parquet_with_options(
    path: impl AsRef<Path>,
    options: &ParquetReadOptions,
) -> Result<DataFrame> {
    let path = path.as_ref();
    let file = File::open(path).map_err(|source| DataFrameError::io_with_path(source, path))?;

    let mut builder = ParquetRecordBatchReaderBuilder::try_new(file)
        .map_err(|source| DataFrameError::Parquet { source })?
        .with_batch_size(options.batch_size);

    if let Some(row_groups) = options.row_groups.as_deref() {
        builder = builder.with_row_groups(row_groups.to_vec());
    }

    let mut projection_columns = options.columns.clone();
    if let Some(predicate) = &options.predicate {
        let cols = referenced_columns(predicate);
        projection_columns = Some(match projection_columns {
            Some(mut existing) => {
                for c in cols {
                    if !existing.iter().any(|v| v == &c) {
                        existing.push(c);
                    }
                }
                existing
            }
            None => cols.into_iter().collect(),
        });
    }

    if let Some(columns) = projection_columns.as_deref() {
        let schema = builder.schema();
        let mut indices = Vec::with_capacity(columns.len());
        for name in columns {
            let idx = schema
                .fields()
                .iter()
                .position(|f| f.name() == name)
                .ok_or_else(|| DataFrameError::column_not_found(name.clone()))?;
            indices.push(idx);
        }
        let mask = ProjectionMask::roots(builder.parquet_schema(), indices);
        builder = builder.with_projection(mask);
    }

    let reader = builder
        .build()
        .map_err(|source| DataFrameError::Parquet { source })?;

    let batches = reader
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|source| DataFrameError::Arrow { source })?;

    let mut df = DataFrame::from_batches(batches)?;

    if let Some(predicate) = options.predicate.clone() {
        df = df.filter(predicate)?;
        if let Some(columns) = options.columns.as_deref() {
            df = df.select(columns.iter().map(|name| col(name)).collect())?;
        }
    }

    Ok(df)
}

fn referenced_columns(expr: &Expr) -> HashSet<String> {
    let mut out = HashSet::new();
    collect_referenced_columns(expr, &mut out);
    out
}

fn collect_referenced_columns(expr: &Expr, out: &mut HashSet<String>) {
    use crate::expr::Expr as E;
    match expr {
        E::Column(name) => {
            out.insert(name.clone());
        }
        E::Alias { expr, .. } => collect_referenced_columns(expr, out),
        E::UnaryOp { expr, .. } => collect_referenced_columns(expr, out),
        E::BinaryOp { left, right, .. } => {
            collect_referenced_columns(left, out);
            collect_referenced_columns(right, out);
        }
        E::Agg { expr, .. } => collect_referenced_columns(expr, out),
        E::Literal(_) | E::Wildcard => {}
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{ArrayRef, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;

    use super::{read_parquet_with_options, write_parquet};
    use crate::io::ParquetReadOptions;
    use crate::{DataFrame, DataFrameError};

    #[test]
    fn parquet_roundtrip_basic() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, true),
            Field::new("b", DataType::Utf8, true),
        ]));

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![Some(1), None, Some(3)])) as ArrayRef,
                Arc::new(StringArray::from(vec![Some("x"), None, Some("z")])) as ArrayRef,
            ],
        )
        .unwrap();

        let df = DataFrame::from_batches(vec![batch]).unwrap();
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("sample.parquet");

        write_parquet(&path, &df).unwrap();
        let df2 = read_parquet_with_options(&path, &ParquetReadOptions::default()).unwrap();

        assert_eq!(df2.schema().as_ref(), df.schema().as_ref());
        assert_eq!(df2.height(), df.height());
    }

    #[test]
    fn parquet_projection_unknown_column_is_error() {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, true)]));

        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(Int64Array::from(vec![Some(1)])) as ArrayRef],
        )
        .unwrap();

        let df = DataFrame::from_batches(vec![batch]).unwrap();
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("sample.parquet");
        write_parquet(&path, &df).unwrap();

        let options = ParquetReadOptions::default().with_columns(["x"]);
        let err = read_parquet_with_options(&path, &options).unwrap_err();
        assert!(matches!(err, DataFrameError::ColumnNotFound { .. }));
    }
}
