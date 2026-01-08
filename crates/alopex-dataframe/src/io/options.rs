use crate::Expr;

#[derive(Debug, Clone)]
pub struct CsvReadOptions {
    pub has_header: bool,
    pub delimiter: u8,
    pub quote_char: Option<u8>,
    pub null_values: Vec<String>,
    pub infer_schema_length: usize,
    pub projection: Option<Vec<String>>,
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
    pub fn with_has_header(mut self, has_header: bool) -> Self {
        self.has_header = has_header;
        self
    }

    pub fn with_delimiter(mut self, delimiter: u8) -> Self {
        self.delimiter = delimiter;
        self
    }

    pub fn with_quote_char(mut self, quote_char: Option<u8>) -> Self {
        self.quote_char = quote_char;
        self
    }

    pub fn with_null_values<I, S>(mut self, values: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.null_values = values.into_iter().map(Into::into).collect();
        self
    }

    pub fn with_infer_schema_length(mut self, infer_schema_length: usize) -> Self {
        self.infer_schema_length = infer_schema_length;
        self
    }

    pub fn with_projection<I, S>(mut self, columns: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.projection = Some(columns.into_iter().map(Into::into).collect());
        self
    }

    pub fn with_predicate(mut self, predicate: Expr) -> Self {
        self.predicate = Some(predicate);
        self
    }
}

#[derive(Debug, Clone)]
pub struct ParquetReadOptions {
    pub columns: Option<Vec<String>>,
    pub row_groups: Option<Vec<usize>>,
    pub batch_size: usize,
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
    pub fn with_columns<I, S>(mut self, columns: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.columns = Some(columns.into_iter().map(Into::into).collect());
        self
    }

    pub fn with_row_groups<I>(mut self, row_groups: I) -> Self
    where
        I: IntoIterator<Item = usize>,
    {
        self.row_groups = Some(row_groups.into_iter().collect());
        self
    }

    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }

    pub fn with_predicate(mut self, predicate: Expr) -> Self {
        self.predicate = Some(predicate);
        self
    }
}
