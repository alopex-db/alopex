/// Execution configuration for query execution.
#[derive(Debug, Clone)]
pub struct ExecutionConfig {
    /// Maximum number of groups allowed during aggregation.
    pub max_groups: usize,
}

impl Default for ExecutionConfig {
    fn default() -> Self {
        Self {
            max_groups: 1_000_000,
        }
    }
}
