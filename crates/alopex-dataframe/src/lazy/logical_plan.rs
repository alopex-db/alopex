use std::path::PathBuf;

use crate::{DataFrame, Expr};

/// How a projection node should be interpreted.
#[derive(Debug, Clone)]
pub enum ProjectionKind {
    /// Select columns/expressions, producing a new schema.
    Select,
    /// Add or overwrite columns, preserving existing columns.
    WithColumns,
}

/// Logical query plan nodes for `LazyFrame`.
#[derive(Debug, Clone)]
pub enum LogicalPlan {
    /// Scan an in-memory `DataFrame`.
    DataFrameScan { df: DataFrame },
    /// Scan a CSV file (predicate/projection may be pushed down).
    CsvScan {
        path: PathBuf,
        predicate: Option<Expr>,
        projection: Option<Vec<String>>,
    },
    /// Scan a Parquet file (predicate/projection may be pushed down).
    ParquetScan {
        path: PathBuf,
        predicate: Option<Expr>,
        projection: Option<Vec<String>>,
    },
    /// Projection node (select or with_columns).
    Projection {
        input: Box<LogicalPlan>,
        exprs: Vec<Expr>,
        kind: ProjectionKind,
    },
    /// Filter node.
    Filter {
        input: Box<LogicalPlan>,
        predicate: Expr,
    },
    /// Aggregate node (group keys and aggregations).
    Aggregate {
        input: Box<LogicalPlan>,
        group_by: Vec<Expr>,
        aggs: Vec<Expr>,
    },
}

impl LogicalPlan {
    /// Render this plan as a readable string (used by `explain()` and tests).
    pub fn display(&self) -> String {
        let mut out = String::new();
        self.fmt_into(&mut out, 0);
        out
    }

    fn fmt_into(&self, out: &mut String, indent: usize) {
        let pad = "  ".repeat(indent);
        match self {
            LogicalPlan::DataFrameScan { .. } => {
                out.push_str(&format!("{pad}scan[dataframe]\n"));
            }
            LogicalPlan::CsvScan {
                path,
                predicate,
                projection,
            } => {
                out.push_str(&format!("{pad}scan[csv path='{}']", path.display()));
                if let Some(projection) = projection {
                    out.push_str(&format!(" projection={:?}", projection));
                }
                if let Some(predicate) = predicate {
                    out.push_str(&format!(" filters=[{}]", fmt_expr(predicate)));
                }
                out.push('\n');
            }
            LogicalPlan::ParquetScan {
                path,
                predicate,
                projection,
            } => {
                out.push_str(&format!("{pad}scan[parquet path='{}']", path.display()));
                if let Some(projection) = projection {
                    out.push_str(&format!(" projection={:?}", projection));
                }
                if let Some(predicate) = predicate {
                    out.push_str(&format!(" filters=[{}]", fmt_expr(predicate)));
                }
                out.push('\n');
            }
            LogicalPlan::Projection { input, exprs, kind } => {
                let label = match kind {
                    ProjectionKind::Select => "project",
                    ProjectionKind::WithColumns => "with_columns",
                };
                out.push_str(&format!(
                    "{pad}{label} [{}]\n",
                    exprs.iter().map(fmt_expr).collect::<Vec<_>>().join(", ")
                ));
                input.fmt_into(out, indent + 1);
            }
            LogicalPlan::Filter { input, predicate } => {
                out.push_str(&format!("{pad}filter [{}]\n", fmt_expr(predicate)));
                input.fmt_into(out, indent + 1);
            }
            LogicalPlan::Aggregate {
                input,
                group_by,
                aggs,
            } => {
                out.push_str(&format!(
                    "{pad}aggregate by=[{}] aggs=[{}]\n",
                    group_by.iter().map(fmt_expr).collect::<Vec<_>>().join(", "),
                    aggs.iter().map(fmt_expr).collect::<Vec<_>>().join(", ")
                ));
                input.fmt_into(out, indent + 1);
            }
        }
    }
}

fn fmt_expr(expr: &Expr) -> String {
    use crate::expr::{AggFunc, Expr as E, Operator, Scalar, UnaryOperator};

    match expr {
        E::Column(name) => format!("col({name})"),
        E::Literal(Scalar::Null) => "lit(null)".to_string(),
        E::Literal(Scalar::Boolean(v)) => format!("lit({v})"),
        E::Literal(Scalar::Int64(v)) => format!("lit({v})"),
        E::Literal(Scalar::Float64(v)) => format!("lit({v})"),
        E::Literal(Scalar::Utf8(v)) => format!("lit({v:?})"),
        E::Wildcard => "*".to_string(),
        E::Alias { expr, name } => format!("{} as {name}", fmt_expr(expr)),
        E::UnaryOp {
            op: UnaryOperator::Not,
            expr,
        } => format!("not({})", fmt_expr(expr)),
        E::BinaryOp { left, op, right } => {
            let op_s = match op {
                Operator::Add => "+",
                Operator::Sub => "-",
                Operator::Mul => "*",
                Operator::Div => "/",
                Operator::Eq => "==",
                Operator::Neq => "!=",
                Operator::Gt => ">",
                Operator::Lt => "<",
                Operator::Ge => ">=",
                Operator::Le => "<=",
                Operator::And => "and",
                Operator::Or => "or",
            };
            format!("({} {op_s} {})", fmt_expr(left), fmt_expr(right))
        }
        E::Agg { func, expr } => {
            let f = match func {
                AggFunc::Sum => "sum",
                AggFunc::Mean => "mean",
                AggFunc::Count => "count",
                AggFunc::Min => "min",
                AggFunc::Max => "max",
            };
            format!("{f}({})", fmt_expr(expr))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{LogicalPlan, ProjectionKind};
    use crate::expr::{col, lit};

    #[test]
    fn display_is_readable_and_stable() {
        let plan = LogicalPlan::Filter {
            input: Box::new(LogicalPlan::Projection {
                input: Box::new(LogicalPlan::CsvScan {
                    path: "data.csv".into(),
                    predicate: None,
                    projection: Some(vec!["a".to_string(), "b".to_string()]),
                }),
                exprs: vec![col("a"), col("b").alias("bb")],
                kind: ProjectionKind::Select,
            }),
            predicate: col("a").gt(lit(1_i64)),
        };

        let s = plan.display();
        assert!(s.contains("scan[csv"));
        assert!(s.contains("project"));
        assert!(s.contains("filter"));
        assert!(s.contains("col(a)"));
    }
}
