use std::collections::HashSet;

use crate::storage::SqlValue;

use super::{AggregateState, SqlValueKey};

/// COUNT(*) state - counts all rows unconditionally.
pub(super) struct CountStarState {
    count: i64,
}

impl CountStarState {
    pub(super) fn new() -> Self {
        Self { count: 0 }
    }
}

impl Default for CountStarState {
    fn default() -> Self {
        Self::new()
    }
}

impl AggregateState for CountStarState {
    fn update(&mut self, _value: Option<&SqlValue>) {
        self.count += 1;
    }

    fn finalize(&self) -> SqlValue {
        SqlValue::BigInt(self.count)
    }

    fn new_instance(&self) -> Box<dyn AggregateState> {
        Box::new(Self::new())
    }
}

/// COUNT(column) state - counts non-NULL values only.
pub(super) struct CountColumnState {
    count: i64,
}

impl CountColumnState {
    pub(super) fn new() -> Self {
        Self { count: 0 }
    }
}

impl Default for CountColumnState {
    fn default() -> Self {
        Self::new()
    }
}

impl AggregateState for CountColumnState {
    fn update(&mut self, value: Option<&SqlValue>) {
        if value.filter(|value| !value.is_null()).is_some() {
            self.count += 1;
        }
    }

    fn finalize(&self) -> SqlValue {
        SqlValue::BigInt(self.count)
    }

    fn new_instance(&self) -> Box<dyn AggregateState> {
        Box::new(Self::new())
    }
}

/// COUNT(DISTINCT column) state - counts distinct non-NULL values only.
pub(super) struct CountDistinctState {
    seen: HashSet<SqlValueKey>,
}

impl CountDistinctState {
    pub(super) fn new() -> Self {
        Self {
            seen: HashSet::new(),
        }
    }
}

impl Default for CountDistinctState {
    fn default() -> Self {
        Self::new()
    }
}

impl AggregateState for CountDistinctState {
    fn update(&mut self, value: Option<&SqlValue>) {
        if let Some(value) = value.filter(|value| !value.is_null()) {
            self.seen.insert(SqlValueKey::from_value(value));
        }
    }

    fn finalize(&self) -> SqlValue {
        SqlValue::BigInt(self.seen.len() as i64)
    }

    fn new_instance(&self) -> Box<dyn AggregateState> {
        Box::new(Self::new())
    }
}
