use std::collections::HashSet;

use crate::expr::{Expr as E, Operator};
use crate::lazy::{LogicalPlan, ProjectionKind};
use crate::Expr;

/// Optimizer that rewrites `LogicalPlan` (e.g. predicate/projection pushdown).
pub struct Optimizer;

impl Optimizer {
    /// Optimize a `LogicalPlan` and return the rewritten plan.
    pub fn optimize(plan: &LogicalPlan) -> LogicalPlan {
        let plan = predicate_pushdown(plan.clone());
        projection_pushdown(plan)
    }
}

fn predicate_pushdown(plan: LogicalPlan) -> LogicalPlan {
    match plan {
        LogicalPlan::Filter { input, predicate } => {
            let input = predicate_pushdown(*input);

            match input {
                LogicalPlan::Filter {
                    input: inner,
                    predicate: inner_predicate,
                } => {
                    let combined = and_expr(inner_predicate, predicate);
                    predicate_pushdown(LogicalPlan::Filter {
                        input: inner,
                        predicate: combined,
                    })
                }
                LogicalPlan::Projection { input, exprs, kind } => {
                    if can_push_filter_through_projection(&predicate, &exprs, &kind) {
                        predicate_pushdown(LogicalPlan::Projection {
                            input: Box::new(LogicalPlan::Filter { input, predicate }),
                            exprs,
                            kind,
                        })
                    } else {
                        LogicalPlan::Filter {
                            input: Box::new(LogicalPlan::Projection { input, exprs, kind }),
                            predicate,
                        }
                    }
                }
                LogicalPlan::CsvScan {
                    path,
                    predicate: existing,
                    projection,
                } => LogicalPlan::CsvScan {
                    path,
                    predicate: Some(match existing {
                        Some(existing) => and_expr(existing, predicate),
                        None => predicate,
                    }),
                    projection,
                },
                LogicalPlan::ParquetScan {
                    path,
                    predicate: existing,
                    projection,
                } => LogicalPlan::ParquetScan {
                    path,
                    predicate: Some(match existing {
                        Some(existing) => and_expr(existing, predicate),
                        None => predicate,
                    }),
                    projection,
                },
                other => LogicalPlan::Filter {
                    input: Box::new(other),
                    predicate,
                },
            }
        }
        LogicalPlan::Projection { input, exprs, kind } => LogicalPlan::Projection {
            input: Box::new(predicate_pushdown(*input)),
            exprs,
            kind,
        },
        LogicalPlan::Aggregate {
            input,
            group_by,
            aggs,
        } => LogicalPlan::Aggregate {
            input: Box::new(predicate_pushdown(*input)),
            group_by,
            aggs,
        },
        other => other,
    }
}

fn and_expr(left: Expr, right: Expr) -> Expr {
    let mut conjuncts = Vec::new();
    conjuncts.extend(flatten_and(left));
    conjuncts.extend(flatten_and(right));
    build_and(conjuncts)
}

fn flatten_and(expr: Expr) -> Vec<Expr> {
    match expr {
        E::BinaryOp {
            left,
            op: Operator::And,
            right,
        } => {
            let mut out = flatten_and(*left);
            out.extend(flatten_and(*right));
            out
        }
        other => vec![other],
    }
}

fn build_and(mut conjuncts: Vec<Expr>) -> Expr {
    let first = conjuncts
        .pop()
        .expect("build_and must be called with non-empty conjuncts");
    conjuncts
        .into_iter()
        .rev()
        .fold(first, |acc, expr| E::BinaryOp {
            left: Box::new(expr),
            op: Operator::And,
            right: Box::new(acc),
        })
}

fn can_push_filter_through_projection(
    predicate: &Expr,
    exprs: &[Expr],
    kind: &ProjectionKind,
) -> bool {
    let referenced = referenced_columns(predicate);

    match kind {
        ProjectionKind::Select => match projection_select_output_columns(exprs) {
            OutputColumns::Some(cols) => referenced.is_subset(&cols),
            OutputColumns::All | OutputColumns::Unknown => false,
        },
        ProjectionKind::WithColumns => {
            let assigned = projection_assigned_columns(exprs);
            !referenced.iter().any(|c| assigned.contains(c))
        }
    }
}

fn referenced_columns(expr: &Expr) -> HashSet<String> {
    let mut out = HashSet::new();
    collect_referenced_columns(expr, &mut out);
    out
}

fn collect_referenced_columns(expr: &Expr, out: &mut HashSet<String>) {
    match expr {
        E::Column(name) => {
            out.insert(name.clone());
        }
        E::Alias { expr, .. } => collect_referenced_columns(expr, out),
        E::UnaryOp { expr, .. } => collect_referenced_columns(expr, out),
        E::BinaryOp { left, right, .. } => {
            collect_referenced_columns(left, out);
            collect_referenced_columns(right, out);
        }
        E::Agg { expr, .. } => collect_referenced_columns(expr, out),
        E::Literal(_) | E::Wildcard => {}
    }
}

enum OutputColumns {
    All,
    Some(HashSet<String>),
    Unknown,
}

fn projection_select_output_columns(exprs: &[Expr]) -> OutputColumns {
    let mut cols = HashSet::new();
    for expr in exprs {
        match expr {
            E::Wildcard => return OutputColumns::All,
            E::Alias { name, .. } => {
                cols.insert(name.clone());
            }
            E::Column(name) => {
                cols.insert(name.clone());
            }
            _ => return OutputColumns::Unknown,
        }
    }
    OutputColumns::Some(cols)
}

fn projection_assigned_columns(exprs: &[Expr]) -> HashSet<String> {
    let mut cols = HashSet::new();
    for expr in exprs {
        match expr {
            E::Alias { name, .. } => {
                cols.insert(name.clone());
            }
            E::Column(name) => {
                cols.insert(name.clone());
            }
            _ => {}
        }
    }
    cols
}

fn projection_pushdown(plan: LogicalPlan) -> LogicalPlan {
    projection_pushdown_inner(plan, RequiredColumns::All).0
}

#[derive(Debug, Clone)]
enum RequiredColumns {
    All,
    Some(HashSet<String>),
}

impl RequiredColumns {
    fn union(self, other: Self) -> Self {
        match (self, other) {
            (RequiredColumns::All, _) | (_, RequiredColumns::All) => RequiredColumns::All,
            (RequiredColumns::Some(mut a), RequiredColumns::Some(b)) => {
                a.extend(b);
                RequiredColumns::Some(a)
            }
        }
    }
}

fn projection_pushdown_inner(
    plan: LogicalPlan,
    required: RequiredColumns,
) -> (LogicalPlan, RequiredColumns) {
    match plan {
        LogicalPlan::Projection { input, exprs, kind } => match kind {
            ProjectionKind::Select => {
                let input_required = required_columns_for_select(&exprs);
                let (new_input, _) = projection_pushdown_inner(*input, input_required);
                (
                    LogicalPlan::Projection {
                        input: Box::new(new_input),
                        exprs,
                        kind: ProjectionKind::Select,
                    },
                    required,
                )
            }
            ProjectionKind::WithColumns => {
                // Conservative: keep required columns and any inputs to compute overwritten/new columns
                let mut needed = HashSet::new();
                if let RequiredColumns::Some(ref req) = required {
                    needed.extend(req.clone());
                }
                for expr in &exprs {
                    match expr {
                        E::Alias { expr, .. } => needed.extend(referenced_columns(expr)),
                        E::Column(_) => {}
                        _ => {}
                    }
                }
                let (new_input, _) =
                    projection_pushdown_inner(*input, RequiredColumns::Some(needed));
                (
                    LogicalPlan::Projection {
                        input: Box::new(new_input),
                        exprs,
                        kind: ProjectionKind::WithColumns,
                    },
                    required,
                )
            }
        },
        LogicalPlan::Filter { input, predicate } => {
            let input_required = required
                .clone()
                .union(RequiredColumns::Some(referenced_columns(&predicate)));
            let (new_input, _) = projection_pushdown_inner(*input, input_required);
            (
                LogicalPlan::Filter {
                    input: Box::new(new_input),
                    predicate,
                },
                required,
            )
        }
        LogicalPlan::Aggregate {
            input,
            group_by,
            aggs,
        } => {
            let mut needed = HashSet::new();
            for e in group_by.iter().chain(aggs.iter()) {
                needed.extend(referenced_columns(e));
            }
            let (new_input, _) = projection_pushdown_inner(*input, RequiredColumns::Some(needed));
            (
                LogicalPlan::Aggregate {
                    input: Box::new(new_input),
                    group_by,
                    aggs,
                },
                required,
            )
        }
        LogicalPlan::CsvScan {
            path,
            predicate,
            projection,
        } => {
            let mut needed = match required {
                RequiredColumns::All => None,
                RequiredColumns::Some(s) => Some(s),
            };
            if let Some(pred) = &predicate {
                let cols = referenced_columns(pred);
                needed = Some(match needed {
                    Some(mut s) => {
                        s.extend(cols);
                        s
                    }
                    None => cols,
                });
            }
            (
                LogicalPlan::CsvScan {
                    path,
                    predicate,
                    projection: merge_projection(projection, needed),
                },
                RequiredColumns::All,
            )
        }
        LogicalPlan::ParquetScan {
            path,
            predicate,
            projection,
        } => {
            let mut needed = match required {
                RequiredColumns::All => None,
                RequiredColumns::Some(s) => Some(s),
            };
            if let Some(pred) = &predicate {
                let cols = referenced_columns(pred);
                needed = Some(match needed {
                    Some(mut s) => {
                        s.extend(cols);
                        s
                    }
                    None => cols,
                });
            }
            (
                LogicalPlan::ParquetScan {
                    path,
                    predicate,
                    projection: merge_projection(projection, needed),
                },
                RequiredColumns::All,
            )
        }
        other => (other, RequiredColumns::All),
    }
}

fn required_columns_for_select(exprs: &[Expr]) -> RequiredColumns {
    let mut needed = HashSet::new();
    for expr in exprs {
        match expr {
            E::Wildcard => return RequiredColumns::All,
            other => needed.extend(referenced_columns(other)),
        }
    }
    RequiredColumns::Some(needed)
}

fn merge_projection(
    existing: Option<Vec<String>>,
    needed: Option<HashSet<String>>,
) -> Option<Vec<String>> {
    let Some(needed) = needed else {
        return existing;
    };

    let mut out = Vec::new();
    let mut seen = HashSet::new();

    if let Some(existing) = existing {
        for c in existing {
            if seen.insert(c.clone()) {
                out.push(c);
            }
        }
    }

    for c in needed {
        if seen.insert(c.clone()) {
            out.push(c);
        }
    }

    Some(out)
}

#[cfg(test)]
mod tests {
    use super::Optimizer;
    use crate::expr::{col, lit};
    use crate::lazy::LogicalPlan;
    use crate::lazy::ProjectionKind;

    #[test]
    fn predicate_pushdown_moves_filter_into_scan() {
        let plan = LogicalPlan::Filter {
            input: Box::new(LogicalPlan::CsvScan {
                path: "data.csv".into(),
                predicate: None,
                projection: None,
            }),
            predicate: col("a").gt(lit(1_i64)),
        };

        let optimized = Optimizer::optimize(&plan);
        match optimized {
            LogicalPlan::CsvScan { predicate, .. } => assert!(predicate.is_some()),
            other => panic!("expected CsvScan, got {other:?}"),
        }
    }

    #[test]
    fn predicate_pushdown_combines_multiple_filters_with_and() {
        let plan = LogicalPlan::Filter {
            input: Box::new(LogicalPlan::Filter {
                input: Box::new(LogicalPlan::CsvScan {
                    path: "data.csv".into(),
                    predicate: None,
                    projection: None,
                }),
                predicate: col("a").gt(lit(1_i64)),
            }),
            predicate: col("b").lt(lit(10_i64)),
        };

        let optimized = Optimizer::optimize(&plan);
        match optimized {
            LogicalPlan::CsvScan {
                predicate: Some(p), ..
            } => {
                let s = format!("{p:?}");
                assert!(s.contains("And"));
            }
            other => panic!("expected CsvScan with predicate, got {other:?}"),
        }
    }

    #[test]
    fn predicate_pushdown_does_not_cross_select_when_column_not_selected() {
        let plan = LogicalPlan::Filter {
            input: Box::new(LogicalPlan::Projection {
                input: Box::new(LogicalPlan::CsvScan {
                    path: "data.csv".into(),
                    predicate: None,
                    projection: None,
                }),
                exprs: vec![col("a")],
                kind: ProjectionKind::Select,
            }),
            predicate: col("b").gt(lit(1_i64)),
        };

        let optimized = Optimizer::optimize(&plan);
        assert!(matches!(optimized, LogicalPlan::Filter { .. }));
    }

    #[test]
    fn projection_pushdown_sets_scan_projection() {
        let plan = LogicalPlan::Projection {
            input: Box::new(LogicalPlan::CsvScan {
                path: "data.csv".into(),
                predicate: None,
                projection: None,
            }),
            exprs: vec![col("a"), col("b")],
            kind: ProjectionKind::Select,
        };

        let optimized = Optimizer::optimize(&plan);
        let s = optimized.display();
        assert!(s.contains("projection"));
    }
}
