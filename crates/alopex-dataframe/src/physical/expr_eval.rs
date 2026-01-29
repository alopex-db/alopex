use std::sync::Arc;

use arrow::array::{ArrayRef, BooleanArray, Float64Array, Int64Array, NullArray, StringArray};
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;

use crate::expr::{Expr as E, Scalar};
use crate::{DataFrameError, Expr, Result};

/// Evaluates `Expr` values over Arrow `RecordBatch` inputs.
pub struct ExprEval;

impl ExprEval {
    /// Evaluate `expr` for every row in `batch` and return the resulting Arrow array.
    pub fn evaluate(expr: &Expr, batch: &RecordBatch) -> Result<ArrayRef> {
        eval_expr(expr, batch)
    }
}

fn eval_expr(expr: &Expr, batch: &RecordBatch) -> Result<ArrayRef> {
    match expr {
        E::Column(name) => {
            let idx = batch
                .schema()
                .fields()
                .iter()
                .position(|f| f.name() == name)
                .ok_or_else(|| DataFrameError::column_not_found(name.clone()))?;
            Ok(batch.column(idx).clone())
        }
        E::Literal(s) => scalar_to_array(s, batch.num_rows()),
        E::Alias { expr, .. } => eval_expr(expr, batch),
        E::Wildcard => Err(DataFrameError::invalid_operation(
            "wildcard cannot be evaluated as a standalone expression",
        )),
        E::UnaryOp { op, expr } => {
            let v = eval_expr(expr, batch)?;
            match op {
                crate::expr::UnaryOperator::Not => {
                    if v.data_type() != &DataType::Boolean {
                        return Err(DataFrameError::type_mismatch(
                            None::<String>,
                            DataType::Boolean.to_string(),
                            v.data_type().to_string(),
                        ));
                    }
                    let b = v.as_any().downcast_ref::<BooleanArray>().ok_or_else(|| {
                        DataFrameError::type_mismatch(
                            None::<String>,
                            "BooleanArray".to_string(),
                            format!("{:?}", v.data_type()),
                        )
                    })?;
                    Ok(Arc::new(
                        arrow::compute::not(b)
                            .map_err(|source| DataFrameError::Arrow { source })?,
                    ))
                }
            }
        }
        E::BinaryOp { left, op, right } => {
            let l = eval_expr(left, batch)?;
            let r = eval_expr(right, batch)?;
            eval_binary(op, &l, &r)
        }
        E::Agg { .. } => Err(DataFrameError::invalid_operation(
            "aggregation expressions must be evaluated by aggregate operator",
        )),
    }
}

fn scalar_to_array(s: &Scalar, len: usize) -> Result<ArrayRef> {
    match s {
        Scalar::Null => Ok(Arc::new(NullArray::new(len))),
        Scalar::Boolean(v) => Ok(Arc::new(BooleanArray::from(vec![Some(*v); len]))),
        Scalar::Int64(v) => Ok(Arc::new(Int64Array::from(vec![Some(*v); len]))),
        Scalar::Float64(v) => Ok(Arc::new(Float64Array::from(vec![Some(*v); len]))),
        Scalar::Utf8(v) => Ok(Arc::new(StringArray::from(vec![Some(v.as_str()); len]))),
    }
}

fn eval_binary(op: &crate::expr::Operator, lhs: &ArrayRef, rhs: &ArrayRef) -> Result<ArrayRef> {
    use crate::expr::Operator;

    let l = lhs.as_ref();
    let r = rhs.as_ref();

    match op {
        Operator::Add => arrow::compute::kernels::numeric::add(&l, &r)
            .map_err(|source| DataFrameError::Arrow { source }),
        Operator::Sub => arrow::compute::kernels::numeric::sub(&l, &r)
            .map_err(|source| DataFrameError::Arrow { source }),
        Operator::Mul => arrow::compute::kernels::numeric::mul(&l, &r)
            .map_err(|source| DataFrameError::Arrow { source }),
        Operator::Div => arrow::compute::kernels::numeric::div(&l, &r)
            .map_err(|source| DataFrameError::Arrow { source }),
        Operator::Eq => Ok(Arc::new(
            arrow::compute::kernels::cmp::eq(&l, &r)
                .map_err(|source| DataFrameError::Arrow { source })?,
        )),
        Operator::Neq => Ok(Arc::new(
            arrow::compute::kernels::cmp::neq(&l, &r)
                .map_err(|source| DataFrameError::Arrow { source })?,
        )),
        Operator::Gt => Ok(Arc::new(
            arrow::compute::kernels::cmp::gt(&l, &r)
                .map_err(|source| DataFrameError::Arrow { source })?,
        )),
        Operator::Lt => Ok(Arc::new(
            arrow::compute::kernels::cmp::lt(&l, &r)
                .map_err(|source| DataFrameError::Arrow { source })?,
        )),
        Operator::Ge => Ok(Arc::new(
            arrow::compute::kernels::cmp::gt_eq(&l, &r)
                .map_err(|source| DataFrameError::Arrow { source })?,
        )),
        Operator::Le => Ok(Arc::new(
            arrow::compute::kernels::cmp::lt_eq(&l, &r)
                .map_err(|source| DataFrameError::Arrow { source })?,
        )),
        Operator::And => {
            let l = lhs.as_any().downcast_ref::<BooleanArray>().ok_or_else(|| {
                DataFrameError::type_mismatch(
                    None::<String>,
                    DataType::Boolean.to_string(),
                    lhs.data_type().to_string(),
                )
            })?;
            let r = rhs.as_any().downcast_ref::<BooleanArray>().ok_or_else(|| {
                DataFrameError::type_mismatch(
                    None::<String>,
                    DataType::Boolean.to_string(),
                    rhs.data_type().to_string(),
                )
            })?;
            Ok(Arc::new(
                arrow::compute::kernels::boolean::and_kleene(l, r)
                    .map_err(|source| DataFrameError::Arrow { source })?,
            ))
        }
        Operator::Or => {
            let l = lhs.as_any().downcast_ref::<BooleanArray>().ok_or_else(|| {
                DataFrameError::type_mismatch(
                    None::<String>,
                    DataType::Boolean.to_string(),
                    lhs.data_type().to_string(),
                )
            })?;
            let r = rhs.as_any().downcast_ref::<BooleanArray>().ok_or_else(|| {
                DataFrameError::type_mismatch(
                    None::<String>,
                    DataType::Boolean.to_string(),
                    rhs.data_type().to_string(),
                )
            })?;
            Ok(Arc::new(
                arrow::compute::kernels::boolean::or_kleene(l, r)
                    .map_err(|source| DataFrameError::Arrow { source })?,
            ))
        }
    }
}
