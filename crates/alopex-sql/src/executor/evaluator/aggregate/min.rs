use std::cmp::Ordering;

use crate::storage::SqlValue;

use super::AggregateState;

/// MIN state - tracks the smallest non-NULL value.
pub(super) struct MinState {
    current: Option<SqlValue>,
}

impl MinState {
    pub(super) fn new() -> Self {
        Self { current: None }
    }
}

impl Default for MinState {
    fn default() -> Self {
        Self::new()
    }
}

impl AggregateState for MinState {
    fn update(&mut self, value: Option<&SqlValue>) {
        let Some(value) = value else {
            return;
        };
        if value.is_null() {
            return;
        }

        match &self.current {
            None => self.current = Some(value.clone()),
            Some(current) => {
                if let Some(Ordering::Less) = value.partial_cmp(current) {
                    self.current = Some(value.clone());
                }
            }
        }
    }

    fn finalize(&self) -> SqlValue {
        self.current.clone().unwrap_or(SqlValue::Null)
    }

    fn new_instance(&self) -> Box<dyn AggregateState> {
        Box::new(Self::new())
    }
}
