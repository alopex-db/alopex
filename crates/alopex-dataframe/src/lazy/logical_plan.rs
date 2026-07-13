use std::path::PathBuf;

use crate::ops::{FillNull, JoinKeys, JoinType, SortOptions};
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
    /// Join two inputs.
    Join {
        left: Box<LogicalPlan>,
        right: Box<LogicalPlan>,
        keys: JoinKeys,
        how: JoinType,
    },
    /// Sort input rows.
    Sort {
        input: Box<LogicalPlan>,
        options: SortOptions,
    },
    /// Slice rows (used for head/tail).
    Slice {
        input: Box<LogicalPlan>,
        offset: usize,
        len: usize,
        from_end: bool,
    },
    /// Remove duplicate rows.
    Unique {
        input: Box<LogicalPlan>,
        subset: Option<Vec<String>>,
    },
    /// Fill nulls using a scalar or strategy.
    FillNull {
        input: Box<LogicalPlan>,
        fill: FillNull,
    },
    /// Drop rows containing nulls.
    DropNulls {
        input: Box<LogicalPlan>,
        subset: Option<Vec<String>>,
    },
    /// Count nulls per column.
    NullCount { input: Box<LogicalPlan> },
    /// Explode one list column.
    Explode {
        input: Box<LogicalPlan>,
        column: String,
    },
    /// Implode columns into one row of list columns.
    Implode { input: Box<LogicalPlan> },
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
            LogicalPlan::Join {
                left,
                right,
                keys,
                how,
            } => {
                out.push_str(&format!(
                    "{pad}join how={how:?} keys={}\n",
                    fmt_join_keys(keys)
                ));
                left.fmt_into(out, indent + 1);
                right.fmt_into(out, indent + 1);
            }
            LogicalPlan::Sort { input, options } => {
                out.push_str(&format!(
                    "{pad}sort by={:?} desc={:?} nulls_last={} stable={}\n",
                    options.by, options.descending, options.nulls_last, options.stable
                ));
                input.fmt_into(out, indent + 1);
            }
            LogicalPlan::Slice {
                input,
                offset,
                len,
                from_end,
            } => {
                out.push_str(&format!(
                    "{pad}slice offset={offset} len={len} from_end={from_end}\n"
                ));
                input.fmt_into(out, indent + 1);
            }
            LogicalPlan::Unique { input, subset } => {
                out.push_str(&format!("{pad}unique subset={subset:?}\n"));
                input.fmt_into(out, indent + 1);
            }
            LogicalPlan::FillNull { input, fill } => {
                out.push_str(&format!("{pad}fill_null {}\n", fmt_fill_null(fill)));
                input.fmt_into(out, indent + 1);
            }
            LogicalPlan::DropNulls { input, subset } => {
                out.push_str(&format!("{pad}drop_nulls subset={subset:?}\n"));
                input.fmt_into(out, indent + 1);
            }
            LogicalPlan::NullCount { input } => {
                out.push_str(&format!("{pad}null_count\n"));
                input.fmt_into(out, indent + 1);
            }
            LogicalPlan::Explode { input, column } => {
                out.push_str(&format!("{pad}explode column={column}\n"));
                input.fmt_into(out, indent + 1);
            }
            LogicalPlan::Implode { input } => {
                out.push_str(&format!("{pad}implode\n"));
                input.fmt_into(out, indent + 1);
            }
        }
    }
}

fn fmt_join_keys(keys: &JoinKeys) -> String {
    match keys {
        JoinKeys::On(cols) => format!("on={cols:?}"),
        JoinKeys::LeftRight { left_on, right_on } => {
            format!("left_on={left_on:?} right_on={right_on:?}")
        }
    }
}

fn fmt_fill_null(fill: &FillNull) -> String {
    match fill {
        FillNull::Value(value) => format!("value={value:?}"),
        FillNull::Strategy(strategy) => format!("strategy={strategy:?}"),
    }
}

fn fmt_expr(expr: &Expr) -> String {
    use crate::expr::{
        AggFunc, DatetimeFunction, Expr as E, ExprFunction, ListFunction, Operator, Scalar,
        StringFunction, UnaryOperator,
    };

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
        E::Function { input, function } => {
            let f = match function {
                ExprFunction::String(function) => match function {
                    StringFunction::ToLowercase => "str.to_lowercase".to_string(),
                    StringFunction::ToUppercase => "str.to_uppercase".to_string(),
                    StringFunction::Contains { pattern } => {
                        format!("str.contains({pattern:?})")
                    }
                    StringFunction::Replace {
                        pattern,
                        replacement,
                    } => format!("str.replace({pattern:?}, {replacement:?})"),
                    StringFunction::StripChars { chars } => {
                        format!("str.strip_chars({chars:?})")
                    }
                    StringFunction::Split { separator } => {
                        format!("str.split({separator:?})")
                    }
                    StringFunction::LenChars => "str.len_chars".to_string(),
                    StringFunction::Extract {
                        pattern,
                        capture_group,
                    } => format!("str.extract({pattern:?}, {capture_group})"),
                },
                ExprFunction::Datetime(function) => match function {
                    DatetimeFunction::Year => "dt.year".to_string(),
                    DatetimeFunction::Month => "dt.month".to_string(),
                    DatetimeFunction::Day => "dt.day".to_string(),
                    DatetimeFunction::Weekday => "dt.weekday".to_string(),
                    DatetimeFunction::ToString => "dt.to_string".to_string(),
                    DatetimeFunction::ConvertTimeZone {
                        from_offset,
                        to_offset,
                    } => format!("dt.convert_time_zone({from_offset:?}, {to_offset:?})"),
                },
                ExprFunction::List(function) => match function {
                    ListFunction::Join {
                        separator,
                        null_value,
                    } => format!("list.join({separator:?}, {null_value:?})"),
                    ListFunction::Len => "list.len".to_string(),
                    ListFunction::Contains { value } => {
                        format!("list.contains({value:?})")
                    }
                },
            };
            format!("{f}({})", fmt_expr(input))
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
