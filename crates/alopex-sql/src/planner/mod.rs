//! Query planning module for the Alopex SQL dialect.
//!
//! This module provides:
//! - [`PlannerError`]: Error types for planning phase
//! - [`ResolvedType`]: Normalized type information for type checking
//! - [`TypedExpr`]: Type-checked expressions with resolved types
//! - [`LogicalPlan`]: Logical query plan representation

mod error;
pub mod logical_plan;
pub mod typed_expr;
pub mod types;

pub use error::PlannerError;
pub use logical_plan::LogicalPlan;
pub use typed_expr::{
    ProjectedColumn, Projection, SortExpr, TypedAssignment, TypedExpr, TypedExprKind,
};
pub use types::ResolvedType;
