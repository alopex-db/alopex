mod avg;
mod count;
mod group_key;
mod max;
mod min;
mod sql_value_key;
mod state;
mod sum;

pub use group_key::GroupKey;
pub use sql_value_key::SqlValueKey;
pub use state::AggregateState;

use crate::planner::{AggregateExpr, AggregateFunction};

/// Create a new aggregate state instance for the given aggregate expression.
pub fn create_aggregate_state(expr: &AggregateExpr) -> Box<dyn AggregateState> {
    match expr.function {
        AggregateFunction::Count => {
            if expr.arg.is_none() {
                Box::new(count::CountStarState::new())
            } else if expr.distinct {
                Box::new(count::CountDistinctState::new())
            } else {
                Box::new(count::CountColumnState::new())
            }
        }
        AggregateFunction::Sum => Box::new(sum::SumState::new()),
        AggregateFunction::Avg => Box::new(avg::AvgState::new()),
        AggregateFunction::Min => Box::new(min::MinState::new()),
        AggregateFunction::Max => Box::new(max::MaxState::new()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::SqlValue;

    #[test]
    fn count_star_counts_all_rows() {
        let mut state = count::CountStarState::new();
        state.update(None);
        state.update(Some(&SqlValue::Null));
        state.update(None);
        assert_eq!(state.finalize(), SqlValue::BigInt(3));
    }

    #[test]
    fn count_column_counts_non_null() {
        let mut state = count::CountColumnState::new();
        state.update(Some(&SqlValue::Integer(1)));
        state.update(Some(&SqlValue::Null));
        state.update(None);
        state.update(Some(&SqlValue::Integer(2)));
        assert_eq!(state.finalize(), SqlValue::BigInt(2));
    }

    #[test]
    fn count_distinct_dedupes_values() {
        let mut state = count::CountDistinctState::new();
        state.update(Some(&SqlValue::Integer(1)));
        state.update(Some(&SqlValue::Integer(1)));
        state.update(Some(&SqlValue::Integer(2)));
        state.update(Some(&SqlValue::Null));
        assert_eq!(state.finalize(), SqlValue::BigInt(2));
    }

    #[test]
    fn sum_state_preserves_integer_type() {
        let mut state = sum::SumState::new();
        state.update(Some(&SqlValue::Integer(2)));
        state.update(Some(&SqlValue::Null));
        state.update(Some(&SqlValue::Integer(3)));
        assert_eq!(state.finalize(), SqlValue::Integer(5));
    }

    #[test]
    fn sum_state_preserves_float_type() {
        let mut state = sum::SumState::new();
        state.update(Some(&SqlValue::Float(1.5)));
        state.update(Some(&SqlValue::Float(2.0)));
        assert_eq!(state.finalize(), SqlValue::Float(3.5));
    }

    #[test]
    fn avg_state_returns_double() {
        let mut state = avg::AvgState::new();
        state.update(Some(&SqlValue::Integer(2)));
        state.update(Some(&SqlValue::Integer(4)));
        assert_eq!(state.finalize(), SqlValue::Double(3.0));
    }

    #[test]
    fn avg_state_returns_null_for_empty() {
        let state = avg::AvgState::new();
        assert_eq!(state.finalize(), SqlValue::Null);
    }

    #[test]
    fn min_state_tracks_minimum() {
        let mut state = min::MinState::new();
        state.update(Some(&SqlValue::Integer(3)));
        state.update(Some(&SqlValue::Integer(1)));
        state.update(Some(&SqlValue::Integer(2)));
        assert_eq!(state.finalize(), SqlValue::Integer(1));
    }

    #[test]
    fn max_state_tracks_maximum() {
        let mut state = max::MaxState::new();
        state.update(Some(&SqlValue::Integer(3)));
        state.update(Some(&SqlValue::Integer(1)));
        state.update(Some(&SqlValue::Integer(4)));
        assert_eq!(state.finalize(), SqlValue::Integer(4));
    }

    #[test]
    fn min_max_return_null_for_empty() {
        let min_state = min::MinState::new();
        let max_state = max::MaxState::new();
        assert_eq!(min_state.finalize(), SqlValue::Null);
        assert_eq!(max_state.finalize(), SqlValue::Null);
    }
}
