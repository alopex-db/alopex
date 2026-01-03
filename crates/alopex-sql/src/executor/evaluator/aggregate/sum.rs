use crate::storage::SqlValue;

use super::AggregateState;

#[derive(Debug, Clone)]
enum SumAccumulator {
    Integer(i64),
    Float(f64),
    Double(f64),
}

/// SUM state - accumulates numeric values while preserving input type.
pub(super) struct SumState {
    sum: Option<SumAccumulator>,
}

impl SumState {
    pub(super) fn new() -> Self {
        Self { sum: None }
    }

    fn add_integer(&mut self, value: i64) {
        match &mut self.sum {
            None => self.sum = Some(SumAccumulator::Integer(value)),
            Some(SumAccumulator::Integer(sum)) => *sum += value,
            Some(SumAccumulator::Float(sum)) => *sum += value as f64,
            Some(SumAccumulator::Double(sum)) => *sum += value as f64,
        }
    }

    fn add_float(&mut self, value: f64) {
        match &mut self.sum {
            None => self.sum = Some(SumAccumulator::Float(value)),
            Some(SumAccumulator::Integer(sum)) => {
                let promoted = *sum as f64 + value;
                self.sum = Some(SumAccumulator::Float(promoted));
            }
            Some(SumAccumulator::Float(sum)) => *sum += value,
            Some(SumAccumulator::Double(sum)) => *sum += value,
        }
    }

    fn add_double(&mut self, value: f64) {
        match &mut self.sum {
            None => self.sum = Some(SumAccumulator::Double(value)),
            Some(SumAccumulator::Integer(sum)) => {
                let promoted = *sum as f64 + value;
                self.sum = Some(SumAccumulator::Double(promoted));
            }
            Some(SumAccumulator::Float(sum)) => {
                let promoted = *sum + value;
                self.sum = Some(SumAccumulator::Double(promoted));
            }
            Some(SumAccumulator::Double(sum)) => *sum += value,
        }
    }
}

impl Default for SumState {
    fn default() -> Self {
        Self::new()
    }
}

impl AggregateState for SumState {
    fn update(&mut self, value: Option<&SqlValue>) {
        let Some(value) = value else {
            return;
        };
        if value.is_null() {
            return;
        }

        match value {
            SqlValue::Integer(v) => self.add_integer(*v as i64),
            SqlValue::Float(v) => self.add_float(*v as f64),
            SqlValue::Double(v) => self.add_double(*v),
            _ => {}
        }
    }

    fn finalize(&self) -> SqlValue {
        match &self.sum {
            None => SqlValue::Null,
            Some(SumAccumulator::Integer(sum)) => SqlValue::Integer(*sum as i32),
            Some(SumAccumulator::Float(sum)) => SqlValue::Float(*sum as f32),
            Some(SumAccumulator::Double(sum)) => SqlValue::Double(*sum),
        }
    }

    fn new_instance(&self) -> Box<dyn AggregateState> {
        Box::new(Self::new())
    }
}
