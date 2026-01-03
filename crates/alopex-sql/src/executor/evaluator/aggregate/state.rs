use crate::storage::SqlValue;

/// Aggregate computation state.
pub trait AggregateState: Send + Sync {
    /// Update state with a new value.
    ///
    /// - COUNT(*) uses update(None) and always increments.
    /// - COUNT(column) uses update(Some(value)) and skips NULL.
    fn update(&mut self, value: Option<&SqlValue>);

    /// Finalize and return the aggregate result.
    fn finalize(&self) -> SqlValue;

    /// Create a new instance of this aggregate state.
    fn new_instance(&self) -> Box<dyn AggregateState>;
}
