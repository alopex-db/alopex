use crate::storage::SqlValue;

use super::AggregateState;

/// AVG state - accumulates sum and count using f64.
pub(super) struct AvgState {
    sum: f64,
    count: i64,
}

impl AvgState {
    pub(super) fn new() -> Self {
        Self { sum: 0.0, count: 0 }
    }
}

impl Default for AvgState {
    fn default() -> Self {
        Self::new()
    }
}

impl AggregateState for AvgState {
    fn update(&mut self, value: Option<&SqlValue>) {
        let Some(value) = value else {
            return;
        };
        if value.is_null() {
            return;
        }

        match value {
            SqlValue::Integer(v) => {
                self.sum += *v as f64;
                self.count += 1;
            }
            SqlValue::Float(v) => {
                self.sum += *v as f64;
                self.count += 1;
            }
            SqlValue::Double(v) => {
                self.sum += *v;
                self.count += 1;
            }
            _ => {}
        }
    }

    fn finalize(&self) -> SqlValue {
        if self.count == 0 {
            SqlValue::Null
        } else {
            SqlValue::Double(self.sum / self.count as f64)
        }
    }

    fn new_instance(&self) -> Box<dyn AggregateState> {
        Box::new(Self::new())
    }
}
