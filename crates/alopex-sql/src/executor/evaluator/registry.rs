//! Runtime dispatch table for scalar functions.

use std::collections::HashMap;
use std::sync::OnceLock;

use crate::executor::{ExecutorError, Result};
use crate::planner::typed_expr::TypedExpr;
use crate::scalar::{self, ScalarSignature};
use crate::storage::SqlValue;

use super::{EvalContext, conditional, hash, numeric, string, type_fn};

pub type EvalFn = fn(&[SqlValue]) -> Result<SqlValue>;
pub type LazyEvalFn = fn(&[TypedExpr], &EvalContext<'_>) -> Result<SqlValue>;

pub struct ScalarFunction {
    pub signature: &'static ScalarSignature,
    pub eval: EvalFn,
    pub eval_lazy: Option<LazyEvalFn>,
}

pub struct ScalarFunctionRegistry {
    fns: HashMap<&'static str, ScalarFunction>,
}

impl ScalarFunctionRegistry {
    fn build() -> Self {
        let mut fns = HashMap::new();
        for signature in scalar::signatures() {
            let (eval, eval_lazy) = evaluator_for(signature.name);
            fns.insert(
                signature.name,
                ScalarFunction {
                    signature,
                    eval,
                    eval_lazy,
                },
            );
        }
        Self { fns }
    }

    pub fn get(&self, name: &str) -> Option<&ScalarFunction> {
        let lower = name.to_ascii_lowercase();
        self.fns.get(lower.as_str())
    }

    pub fn contains(&self, name: &str) -> bool {
        self.get(name).is_some()
    }

    pub fn names(&self) -> impl Iterator<Item = &'static str> + '_ {
        self.fns.keys().copied()
    }
}

pub fn scalar_registry() -> &'static ScalarFunctionRegistry {
    static REGISTRY: OnceLock<ScalarFunctionRegistry> = OnceLock::new();
    REGISTRY.get_or_init(ScalarFunctionRegistry::build)
}

fn unsupported(values: &[SqlValue]) -> Result<SqlValue> {
    Err(ExecutorError::Evaluation(
        crate::executor::EvaluationError::UnsupportedFunction(format!(
            "unregistered scalar with {} argument(s)",
            values.len()
        )),
    ))
}

fn system_placeholder(_values: &[SqlValue]) -> Result<SqlValue> {
    // Direct execution is handled by the executor, which owns the KV store.
    // Keeping a registry entry lets the planner enforce the function contract.
    Ok(SqlValue::Null)
}

fn evaluator_for(name: &str) -> (EvalFn, Option<LazyEvalFn>) {
    match name {
        "memory_stats" | "io_stats" | "clear_cache" => (system_placeholder, None),
        "vector_similarity" => (super::function_call::eval_vector_similarity_values, None),
        "vector_distance" => (super::function_call::eval_vector_distance_values, None),
        "vector_dims" => (super::function_call::eval_vector_dims_values, None),
        "vector_norm" => (super::function_call::eval_vector_norm_values, None),
        _ => {
            if let Some(eval) = numeric::eval_for(name) {
                (eval, None)
            } else if let Some(eval) = string::eval_for(name) {
                (eval, None)
            } else if let Some(eval) = conditional::eval_for(name) {
                (eval, conditional::lazy_eval_for(name))
            } else if let Some(eval) = type_fn::eval_for(name) {
                (eval, None)
            } else if let Some(eval) = hash::eval_for(name) {
                (eval, None)
            } else {
                (unsupported, None)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn registry_keys_match_signatures() {
        let signatures: HashSet<_> = scalar::signatures().iter().map(|s| s.name).collect();
        let registry: HashSet<_> = scalar_registry().names().collect();
        assert_eq!(signatures, registry);
    }
}
