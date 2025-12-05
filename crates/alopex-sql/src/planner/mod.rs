//! Query planning module for the Alopex SQL dialect.
//!
//! This module provides:
//! - [`PlannerError`]: Error types for planning phase
//! - [`ResolvedType`]: Normalized type information for type checking

mod error;
pub mod types;

pub use error::PlannerError;
pub use types::ResolvedType;
