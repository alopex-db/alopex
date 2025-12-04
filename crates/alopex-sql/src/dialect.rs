/// Placeholder dialect implementation for the Alopex SQL parser.
#[derive(Debug, Default)]
pub struct AlopexDialect;

/// Dialect trait will define SQL dialect customization points.
pub trait Dialect: std::fmt::Debug {}

impl Dialect for AlopexDialect {}
