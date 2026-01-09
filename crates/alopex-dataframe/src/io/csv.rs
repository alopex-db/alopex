use std::fs::File;
use std::io::{BufReader, BufWriter, Seek};
use std::path::Path;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use arrow_csv::reader::{Format, ReaderBuilder};
use arrow_csv::WriterBuilder;
use regex::Regex;

use crate::io::options::CsvReadOptions;
use crate::{col, DataFrame, DataFrameError, Result};

/// Read a CSV file eagerly into a `DataFrame` using default `CsvReadOptions`.
pub fn read_csv(_path: impl AsRef<Path>) -> Result<DataFrame> {
    read_csv_with_options(_path, &CsvReadOptions::default())
}

/// Write a `DataFrame` to a CSV file (currently always includes a header row).
pub fn write_csv(path: impl AsRef<Path>, df: &DataFrame) -> Result<()> {
    let path = path.as_ref();
    let file = File::create(path).map_err(|source| DataFrameError::io_with_path(source, path))?;
    let mut writer = WriterBuilder::new()
        .with_header(true)
        .build(BufWriter::new(file));

    for batch in df.to_arrow() {
        writer
            .write(&batch)
            .map_err(|source| DataFrameError::Arrow { source })?;
    }

    Ok(())
}

/// Read a CSV file eagerly into a `DataFrame` using the provided options.
pub fn read_csv_with_options(
    path: impl AsRef<Path>,
    options: &CsvReadOptions,
) -> Result<DataFrame> {
    validate_csv_read_options(options)?;

    let path = path.as_ref();
    let file = File::open(path).map_err(|source| DataFrameError::io_with_path(source, path))?;
    let mut reader = BufReader::new(file);

    let mut format = Format::default()
        .with_header(options.has_header)
        .with_delimiter(options.delimiter);

    if let Some(quote_char) = options.quote_char {
        format = format.with_quote(quote_char);
    }

    if !options.null_values.is_empty() {
        let pattern = options
            .null_values
            .iter()
            .map(|s| regex::escape(s))
            .collect::<Vec<_>>()
            .join("|");
        let regex = Regex::new(&format!("^(?:{pattern})$")).map_err(|e| {
            DataFrameError::configuration("null_values", format!("invalid regex: {e}"))
        })?;
        format = format.with_null_regex(regex);
    }

    let (schema, _) = format
        .infer_schema(&mut reader, Some(options.infer_schema_length))
        .map_err(|source| DataFrameError::Arrow { source })?;
    let schema: SchemaRef = Arc::new(schema);

    reader
        .rewind()
        .map_err(|source| DataFrameError::io_with_path(source, path))?;

    let projection_indices =
        projection_indices_from_schema(&schema, options.projection.as_deref())?;

    let csv_reader = ReaderBuilder::new(schema.clone())
        .with_format(format)
        .build(reader)
        .map_err(|source| DataFrameError::Arrow { source })?;

    let mut batches = Vec::new();
    for maybe_batch in csv_reader {
        let batch = maybe_batch.map_err(|source| DataFrameError::Arrow { source })?;
        let batch = if options.predicate.is_some() {
            batch
        } else {
            project_batch(batch, projection_indices.as_deref())?
        };
        batches.push(batch);
    }

    let mut df = DataFrame::from_batches(batches)?;

    if let Some(predicate) = options.predicate.clone() {
        df = df.filter(predicate)?;
        if let Some(projection) = options.projection.as_deref() {
            df = df.select(projection.iter().map(|name| col(name)).collect())?;
        }
    }

    Ok(df)
}

fn validate_csv_read_options(options: &CsvReadOptions) -> Result<()> {
    if options.delimiter == b'\0' {
        return Err(DataFrameError::configuration(
            "delimiter",
            "delimiter must not be NUL (0x00)",
        ));
    }
    if options.quote_char == Some(b'\0') {
        return Err(DataFrameError::configuration(
            "quote_char",
            "quote_char must not be NUL (0x00)",
        ));
    }
    Ok(())
}

fn projection_indices_from_schema(
    schema: &SchemaRef,
    projection: Option<&[String]>,
) -> Result<Option<Vec<usize>>> {
    let Some(projection) = projection else {
        return Ok(None);
    };

    let mut indices = Vec::with_capacity(projection.len());
    for name in projection {
        let idx = schema
            .fields()
            .iter()
            .position(|f| f.name() == name)
            .ok_or_else(|| DataFrameError::column_not_found(name.clone()))?;
        indices.push(idx);
    }

    Ok(Some(indices))
}

fn project_batch(batch: RecordBatch, projection: Option<&[usize]>) -> Result<RecordBatch> {
    let Some(projection) = projection else {
        return Ok(batch);
    };

    batch.project(projection).map_err(|e| {
        DataFrameError::schema_mismatch(format!("failed to project record batch: {e}"))
    })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{ArrayRef, Float64Array, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;

    use super::{read_csv_with_options, write_csv};
    use crate::io::CsvReadOptions;
    use crate::{col, lit, DataFrame, DataFrameError};

    #[test]
    fn csv_roundtrip_basic() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, true),
            Field::new("b", DataType::Float64, true),
            Field::new("c", DataType::Utf8, true),
        ]));

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![Some(1), None, Some(3)])) as ArrayRef,
                Arc::new(Float64Array::from(vec![Some(1.5), Some(2.0), None])) as ArrayRef,
                Arc::new(StringArray::from(vec![Some("x"), None, Some("z")])) as ArrayRef,
            ],
        )
        .unwrap();

        let df = DataFrame::from_batches(vec![batch]).unwrap();
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("sample.csv");

        write_csv(&path, &df).unwrap();
        let df2 = read_csv_with_options(&path, &CsvReadOptions::default()).unwrap();

        assert_eq!(df2.schema().as_ref(), df.schema().as_ref());
        assert_eq!(df2.height(), df.height());
    }

    #[test]
    fn csv_projection_unknown_column_is_error() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("sample.csv");

        std::fs::write(&path, "a,b\n1,2\n").unwrap();

        let options = CsvReadOptions::default().with_projection(["a", "x"]);
        let err = read_csv_with_options(&path, &options).unwrap_err();
        assert!(matches!(err, DataFrameError::ColumnNotFound { .. }));
    }

    #[test]
    fn csv_predicate_is_applied() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("sample.csv");

        std::fs::write(&path, "a,b\n1,x\n2,y\n3,z\n").unwrap();

        let options = CsvReadOptions::default().with_predicate(col("a").gt(lit(1i64)));
        let df = read_csv_with_options(&path, &options).unwrap();
        assert_eq!(df.height(), 2);
    }

    #[test]
    fn csv_invalid_delimiter_is_configuration_error() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("sample.csv");

        std::fs::write(&path, "a,b\n1,2\n").unwrap();

        let options = CsvReadOptions::default().with_delimiter(b'\0');
        let err = read_csv_with_options(&path, &options).unwrap_err();
        assert!(matches!(err, DataFrameError::Configuration { .. }));
    }
}
