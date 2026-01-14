use crate::Expr;

/// Options for reading CSV files.
#[derive(Debug, Clone)]
pub struct CsvReadOptions {
    /// Whether the CSV file has a header row.
    pub has_header: bool,
    /// Field delimiter byte (e.g. `b','`).
    pub delimiter: u8,
    /// Quote character byte (defaults to `Some(b'\"')`).
    pub quote_char: Option<u8>,
    /// Values that should be interpreted as null.
    pub null_values: Vec<String>,
    /// Maximum number of rows used for schema inference.
    pub infer_schema_length: usize,
    /// Optional column projection (names).
    pub projection: Option<Vec<String>>,
    /// Optional predicate expression to apply (may be pushed down in the future).
    pub predicate: Option<Expr>,
}

impl Default for CsvReadOptions {
    fn default() -> Self {
        Self {
            has_header: true,
            delimiter: b',',
            quote_char: Some(b'"'),
            null_values: Vec::new(),
            infer_schema_length: 100,
            projection: None,
            predicate: None,
        }
    }
}

impl CsvReadOptions {
    /// Set `has_header`.
    pub fn with_has_header(mut self, has_header: bool) -> Self {
        self.has_header = has_header;
        self
    }

    /// Set `delimiter`.
    pub fn with_delimiter(mut self, delimiter: u8) -> Self {
        self.delimiter = delimiter;
        self
    }

    /// Set `quote_char`.
    pub fn with_quote_char(mut self, quote_char: Option<u8>) -> Self {
        self.quote_char = quote_char;
        self
    }

    /// Set `null_values`.
    pub fn with_null_values<I, S>(mut self, values: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.null_values = values.into_iter().map(Into::into).collect();
        self
    }

    /// Set `infer_schema_length`.
    pub fn with_infer_schema_length(mut self, infer_schema_length: usize) -> Self {
        self.infer_schema_length = infer_schema_length;
        self
    }

    /// Set a column projection by name.
    pub fn with_projection<I, S>(mut self, columns: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.projection = Some(columns.into_iter().map(Into::into).collect());
        self
    }

    /// Set a predicate to apply.
    pub fn with_predicate(mut self, predicate: Expr) -> Self {
        self.predicate = Some(predicate);
        self
    }
}

/// Options for reading Parquet files.
#[derive(Debug, Clone)]
pub struct ParquetReadOptions {
    /// Optional column projection (names).
    pub columns: Option<Vec<String>>,
    /// Optional row group selection.
    pub row_groups: Option<Vec<usize>>,
    /// Record batch size for the Parquet reader.
    pub batch_size: usize,
    /// Optional predicate expression to apply (may be pushed down in the future).
    pub predicate: Option<Expr>,
}

impl Default for ParquetReadOptions {
    fn default() -> Self {
        Self {
            columns: None,
            row_groups: None,
            batch_size: 65_536,
            predicate: None,
        }
    }
}

impl ParquetReadOptions {
    /// Set a column projection by name.
    pub fn with_columns<I, S>(mut self, columns: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.columns = Some(columns.into_iter().map(Into::into).collect());
        self
    }

    /// Set row group indices to read.
    pub fn with_row_groups<I>(mut self, row_groups: I) -> Self
    where
        I: IntoIterator<Item = usize>,
    {
        self.row_groups = Some(row_groups.into_iter().collect());
        self
    }

    /// Set record batch size.
    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }

    /// Set a predicate to apply.
    pub fn with_predicate(mut self, predicate: Expr) -> Self {
        self.predicate = Some(predicate);
        self
    }
}
