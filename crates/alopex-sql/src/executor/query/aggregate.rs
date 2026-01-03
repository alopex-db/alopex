use std::collections::HashMap;
use std::collections::hash_map::Entry;

use crate::catalog::ColumnMetadata;
use crate::executor::evaluator::aggregate::{AggregateState, GroupKey, create_aggregate_state};
use crate::executor::evaluator::{EvalContext, evaluate};
use crate::executor::{ExecutorError, Result, Row};
use crate::planner::typed_expr::{TypedExpr, TypedExprKind};
use crate::planner::{AggregateExpr, AggregateFunction};
use crate::storage::SqlValue;

use super::iterator::RowIterator;

struct GroupState {
    key_values: Vec<SqlValue>,
    states: Vec<Box<dyn AggregateState>>,
}

/// Hash-based aggregation iterator.
pub struct AggregateIterator {
    /// Input row iterator.
    input: Box<dyn RowIterator>,
    /// Group-by key expressions.
    group_keys: Vec<TypedExpr>,
    /// Aggregate expressions.
    aggregates: Vec<AggregateExpr>,
    /// HAVING filter.
    having: Option<TypedExpr>,
    /// Max number of groups allowed (from ExecutionConfig).
    max_groups: usize,
    /// Aggregation state per group.
    groups: HashMap<GroupKey, GroupState>,
    /// Result iterator (populated after aggregation finalization).
    result_rows: Option<std::vec::IntoIter<Row>>,
    /// Output schema for aggregated rows.
    output_schema: Vec<ColumnMetadata>,
}

impl AggregateIterator {
    pub fn new(
        input: Box<dyn RowIterator>,
        group_keys: Vec<TypedExpr>,
        aggregates: Vec<AggregateExpr>,
        having: Option<TypedExpr>,
        max_groups: usize,
    ) -> Self {
        let output_schema = build_output_schema(&group_keys, &aggregates);

        Self {
            input,
            group_keys,
            aggregates,
            having,
            max_groups,
            groups: HashMap::new(),
            result_rows: None,
            output_schema,
        }
    }

    fn materialize(&mut self) -> Result<()> {
        if self.result_rows.is_some() {
            return Ok(());
        }

        let is_global = self.group_keys.is_empty();
        let global_key = GroupKey::empty();
        if is_global {
            if self.max_groups == 0 {
                return Err(ExecutorError::TooManyGroups {
                    limit: self.max_groups,
                    actual: 1,
                });
            }
            let states = self.aggregates.iter().map(create_aggregate_state).collect();
            self.groups.insert(
                global_key.clone(),
                GroupState {
                    key_values: Vec::new(),
                    states,
                },
            );
        }

        let max_groups = self.max_groups;
        let group_keys = &self.group_keys;
        let aggregates = &self.aggregates;
        let groups = &mut self.groups;

        while let Some(result) = self.input.next_row() {
            let row = result?;
            if is_global {
                let group = groups
                    .get_mut(&global_key)
                    .expect("global group entry should exist");
                update_group_states(aggregates, group, &row)?;
                continue;
            }

            let group_key = GroupKey::from_row(&row, group_keys)?;
            let current_groups = groups.len();
            let group = match groups.entry(group_key) {
                Entry::Occupied(entry) => entry.into_mut(),
                Entry::Vacant(entry) => {
                    if current_groups + 1 > max_groups {
                        return Err(ExecutorError::TooManyGroups {
                            limit: max_groups,
                            actual: current_groups + 1,
                        });
                    }
                    let key_values = Self::evaluate_group_key_values_for(&row, group_keys)?;
                    let states = aggregates.iter().map(create_aggregate_state).collect();
                    entry.insert(GroupState { key_values, states })
                }
            };
            update_group_states(aggregates, group, &row)?;
        }

        let group_count = self.groups.len();
        let groups = std::mem::take(&mut self.groups);
        let mut result_rows = Vec::with_capacity(group_count);
        let mut row_id = 0u64;
        for group in groups.into_values() {
            let mut values = group.key_values;
            values.reserve(group.states.len());
            for state in group.states {
                values.push(state.finalize());
            }

            if self.passes_having(&values)? {
                result_rows.push(Row::new(row_id, values));
                row_id += 1;
            }
        }

        self.result_rows = Some(result_rows.into_iter());
        Ok(())
    }

    fn evaluate_group_key_values_for(row: &Row, group_keys: &[TypedExpr]) -> Result<Vec<SqlValue>> {
        if group_keys.is_empty() {
            return Ok(Vec::new());
        }

        let ctx = EvalContext::new(&row.values);
        let mut values = Vec::with_capacity(group_keys.len());
        for expr in group_keys {
            values.push(evaluate(expr, &ctx)?);
        }
        Ok(values)
    }

    fn passes_having(&self, values: &[SqlValue]) -> Result<bool> {
        let Some(having) = self.having.as_ref() else {
            return Ok(true);
        };

        let ctx = EvalContext::new(values);
        match evaluate(having, &ctx)? {
            SqlValue::Boolean(true) => Ok(true),
            SqlValue::Boolean(false) | SqlValue::Null => Ok(false),
            _ => Ok(false),
        }
    }
}

impl RowIterator for AggregateIterator {
    fn next_row(&mut self) -> Option<Result<Row>> {
        if let Err(err) = self.materialize() {
            return Some(Err(err));
        }

        self.result_rows
            .as_mut()
            .and_then(|rows| rows.next().map(Ok))
    }

    fn schema(&self) -> &[ColumnMetadata] {
        &self.output_schema
    }
}

fn build_output_schema(
    group_keys: &[TypedExpr],
    aggregates: &[AggregateExpr],
) -> Vec<ColumnMetadata> {
    let mut schema = Vec::with_capacity(group_keys.len() + aggregates.len());

    for (index, expr) in group_keys.iter().enumerate() {
        let name = match &expr.kind {
            TypedExprKind::ColumnRef { column, .. } => column.clone(),
            _ => format!("group_key_{}", index + 1),
        };
        schema.push(ColumnMetadata::new(name, expr.resolved_type.clone()));
    }

    for (index, aggregate) in aggregates.iter().enumerate() {
        let name = aggregate_name(aggregate, index);
        schema.push(ColumnMetadata::new(name, aggregate.result_type.clone()));
    }

    schema
}

fn aggregate_name(aggregate: &AggregateExpr, index: usize) -> String {
    let func_name = aggregate.function.to_string().to_lowercase();
    match aggregate.arg.as_ref().map(|arg| &arg.kind) {
        None => func_name,
        Some(TypedExprKind::ColumnRef { column, .. }) => format!("{func_name}_{column}"),
        _ => format!("{func_name}_{}", index + 1),
    }
}

fn update_group_states(
    aggregates: &[AggregateExpr],
    group: &mut GroupState,
    row: &Row,
) -> Result<()> {
    let ctx = EvalContext::new(&row.values);
    for (state, aggregate) in group.states.iter_mut().zip(aggregates.iter()) {
        match aggregate.function {
            AggregateFunction::Count => {
                if aggregate.arg.is_none() {
                    state.update(None);
                } else if let Some(arg) = aggregate.arg.as_ref() {
                    match &arg.kind {
                        TypedExprKind::ColumnRef { column_index, .. } => {
                            let value = ctx.get(*column_index)?;
                            state.update(Some(value));
                        }
                        _ => {
                            let value = evaluate(arg, &ctx)?;
                            state.update(Some(&value));
                        }
                    }
                }
            }
            AggregateFunction::Sum
            | AggregateFunction::Avg
            | AggregateFunction::Min
            | AggregateFunction::Max => {
                if let Some(arg) = aggregate.arg.as_ref() {
                    match &arg.kind {
                        TypedExprKind::ColumnRef { column_index, .. } => {
                            let value = ctx.get(*column_index)?;
                            state.update(Some(value));
                        }
                        _ => {
                            let value = evaluate(arg, &ctx)?;
                            state.update(Some(&value));
                        }
                    }
                }
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Span;
    use crate::executor::query::iterator::VecIterator;
    use crate::planner::types::ResolvedType;

    #[test]
    fn global_aggregate_empty_input_returns_single_row() {
        let schema = vec![ColumnMetadata::new("value", ResolvedType::Integer)];
        let input = VecIterator::new(Vec::new(), schema);
        let aggregates = vec![
            AggregateExpr::count_star(),
            AggregateExpr::sum(TypedExpr::column_ref(
                "t".into(),
                "value".into(),
                0,
                ResolvedType::Integer,
                Span::default(),
            )),
        ];

        let mut iter = AggregateIterator::new(Box::new(input), Vec::new(), aggregates, None, 100);

        let row = iter.next_row().expect("expected first row").unwrap();
        assert_eq!(row.values.len(), 2);
        assert_eq!(row.values[0], SqlValue::BigInt(0));
        assert_eq!(row.values[1], SqlValue::Null);
        assert!(iter.next_row().is_none());
    }

    #[test]
    fn group_by_empty_input_returns_no_rows() {
        let schema = vec![ColumnMetadata::new("group_id", ResolvedType::Integer)];
        let input = VecIterator::new(Vec::new(), schema);
        let group_keys = vec![TypedExpr::column_ref(
            "t".into(),
            "group_id".into(),
            0,
            ResolvedType::Integer,
            Span::default(),
        )];
        let aggregates = vec![AggregateExpr::count_star()];

        let mut iter = AggregateIterator::new(Box::new(input), group_keys, aggregates, None, 100);

        assert!(iter.next_row().is_none());
    }
}
