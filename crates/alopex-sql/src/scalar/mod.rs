//! Shared scalar-function signatures.
//!
//! This module is the single source of truth for scalar names, arity, type
//! contracts, return rules, and optimizer metadata. Evaluation functions are
//! attached by `executor::evaluator::registry`.
//!
//! The v0.7.4 catalog includes the v0.5.3 scalar set, v0.5.1 hash/encoding
//! functions, and v0.5.2 system functions. `memory_stats`, `io_stats`, and
//! `clear_cache` are resolved by the executor because they require store
//! access; their signatures still participate in planner validation here.

use crate::PlannerError;
use crate::ast::expr::Literal;
use crate::ast::span::Span;
use crate::planner::typed_expr::{TypedExpr, TypedExprKind};
use crate::planner::types::ResolvedType;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Arity {
    Exact(usize),
    Range(usize, usize),
    Variadic(usize),
}

impl Arity {
    pub fn accepts(self, count: usize) -> bool {
        match self {
            Self::Exact(n) => count == n,
            Self::Range(min, max) => (min..=max).contains(&count),
            Self::Variadic(min) => count >= min,
        }
    }

    pub fn describe(self) -> String {
        match self {
            Self::Exact(n) => n.to_string(),
            Self::Range(min, max) => format!("{min}..={max}"),
            Self::Variadic(min) => format!("{min} or more"),
        }
    }

    pub fn validate(self, name: &str, count: usize, _span: Span) -> Result<(), PlannerError> {
        if self.accepts(count) {
            Ok(())
        } else {
            Err(PlannerError::invalid_expression(format!(
                "function '{name}' expects {} argument(s), got {count}",
                self.describe()
            )))
        }
    }
}

#[derive(Debug, Clone)]
pub enum ReturnRule {
    Fixed(ResolvedType),
    FromArgs(fn(&[ResolvedType]) -> Result<ResolvedType, PlannerError>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FnMeta {
    pub deterministic: bool,
    pub volatile: bool,
    pub side_effecting: bool,
    pub foldable: bool,
    pub cacheable: bool,
    pub reorderable: bool,
}

pub const PURE_META: FnMeta = FnMeta {
    deterministic: true,
    volatile: false,
    side_effecting: false,
    foldable: true,
    cacheable: true,
    reorderable: true,
};

pub const RANDOM_META: FnMeta = FnMeta {
    deterministic: false,
    volatile: true,
    side_effecting: false,
    foldable: false,
    cacheable: false,
    reorderable: true,
};

pub const SYSTEM_META: FnMeta = FnMeta {
    deterministic: false,
    volatile: true,
    side_effecting: false,
    foldable: false,
    cacheable: false,
    reorderable: false,
};

pub const SYSTEM_SIDE_EFFECT_META: FnMeta = FnMeta {
    deterministic: false,
    volatile: true,
    side_effecting: true,
    foldable: false,
    cacheable: false,
    reorderable: false,
};

#[derive(Debug, Clone)]
pub struct ScalarSignature {
    pub name: &'static str,
    pub arity: Arity,
    pub check: fn(&[TypedExpr]) -> Result<(), PlannerError>,
    pub ret: ReturnRule,
    pub meta: FnMeta,
}

/// The fixed Phase 4.7 transaction status for a scalar identity.
///
/// `Distributed` is deliberately narrower than ordinary local registration:
/// it means the identity is admitted by the closed v0.8 RemoteRead catalog.
/// `LocalOnly` preserves the local scalar contract while prohibiting an
/// implicit distributed or multi-range execution claim.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum V09ScalarExecutionStatus {
    Distributed,
    LocalOnly,
}

/// A named row from the approved v0.9 Phase 4 scalar support matrix.
///
/// The linked [`ScalarSignature`] remains the single source of truth for
/// argument type/null checking and return typing. This row fixes its public
/// matrix identity, exact arity, optimizer metadata, and transaction scope so
/// adapters cannot infer remote eligibility from local registration alone.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct V09ScalarRegisterEntry {
    pub id: &'static str,
    pub name: &'static str,
    pub arity: Arity,
    pub metadata: FnMeta,
    pub execution_status: V09ScalarExecutionStatus,
}

const fn v09_scalar_entry(
    id: &'static str,
    name: &'static str,
    arity: Arity,
    metadata: FnMeta,
    execution_status: V09ScalarExecutionStatus,
) -> V09ScalarRegisterEntry {
    V09ScalarRegisterEntry {
        id,
        name,
        arity,
        metadata,
        execution_status,
    }
}

/// Complete, non-abbreviated Phase 4.7 register for vector, numeric,
/// hash, and encoding scalar functions.
///
/// Later Phase 4 tasks add separate registers for the remaining approved
/// scalar and aggregate rows. They must not alter the closed status of any row
/// listed here.
pub const V09_NUMERIC_VECTOR_HASH_SCALAR_REGISTER: &[V09ScalarRegisterEntry] = &[
    v09_scalar_entry(
        "SQL-F-V01",
        "vector_similarity",
        Arity::Exact(3),
        PURE_META,
        V09ScalarExecutionStatus::LocalOnly,
    ),
    v09_scalar_entry(
        "SQL-F-V02",
        "vector_distance",
        Arity::Exact(3),
        PURE_META,
        V09ScalarExecutionStatus::LocalOnly,
    ),
    v09_scalar_entry(
        "SQL-F-V03",
        "vector_dims",
        Arity::Exact(1),
        PURE_META,
        V09ScalarExecutionStatus::LocalOnly,
    ),
    v09_scalar_entry(
        "SQL-F-V04",
        "vector_norm",
        Arity::Exact(1),
        PURE_META,
        V09ScalarExecutionStatus::LocalOnly,
    ),
    v09_scalar_entry(
        "SQL-F-N01",
        "abs",
        Arity::Exact(1),
        PURE_META,
        V09ScalarExecutionStatus::Distributed,
    ),
    v09_scalar_entry(
        "SQL-F-N02",
        "sign",
        Arity::Exact(1),
        PURE_META,
        V09ScalarExecutionStatus::Distributed,
    ),
    v09_scalar_entry(
        "SQL-F-N03",
        "round",
        Arity::Range(1, 2),
        PURE_META,
        V09ScalarExecutionStatus::Distributed,
    ),
    v09_scalar_entry(
        "SQL-F-N04",
        "floor",
        Arity::Exact(1),
        PURE_META,
        V09ScalarExecutionStatus::Distributed,
    ),
    v09_scalar_entry(
        "SQL-F-N05",
        "ceil",
        Arity::Exact(1),
        PURE_META,
        V09ScalarExecutionStatus::Distributed,
    ),
    v09_scalar_entry(
        "SQL-F-N06",
        "ceiling",
        Arity::Exact(1),
        PURE_META,
        V09ScalarExecutionStatus::Distributed,
    ),
    v09_scalar_entry(
        "SQL-F-N07",
        "trunc",
        Arity::Range(1, 2),
        PURE_META,
        V09ScalarExecutionStatus::Distributed,
    ),
    v09_scalar_entry(
        "SQL-F-N08",
        "mod",
        Arity::Exact(2),
        PURE_META,
        V09ScalarExecutionStatus::Distributed,
    ),
    v09_scalar_entry(
        "SQL-F-N09",
        "power",
        Arity::Exact(2),
        PURE_META,
        V09ScalarExecutionStatus::Distributed,
    ),
    v09_scalar_entry(
        "SQL-F-N10",
        "pow",
        Arity::Exact(2),
        PURE_META,
        V09ScalarExecutionStatus::Distributed,
    ),
    v09_scalar_entry(
        "SQL-F-N11",
        "sqrt",
        Arity::Exact(1),
        PURE_META,
        V09ScalarExecutionStatus::Distributed,
    ),
    v09_scalar_entry(
        "SQL-F-N12",
        "exp",
        Arity::Exact(1),
        PURE_META,
        V09ScalarExecutionStatus::Distributed,
    ),
    v09_scalar_entry(
        "SQL-F-N13",
        "ln",
        Arity::Exact(1),
        PURE_META,
        V09ScalarExecutionStatus::Distributed,
    ),
    v09_scalar_entry(
        "SQL-F-N14",
        "log",
        Arity::Range(1, 2),
        PURE_META,
        V09ScalarExecutionStatus::Distributed,
    ),
    v09_scalar_entry(
        "SQL-F-N15",
        "log10",
        Arity::Exact(1),
        PURE_META,
        V09ScalarExecutionStatus::Distributed,
    ),
    v09_scalar_entry(
        "SQL-F-N16",
        "random",
        Arity::Exact(0),
        RANDOM_META,
        V09ScalarExecutionStatus::LocalOnly,
    ),
    v09_scalar_entry(
        "SQL-F-N17",
        "sin",
        Arity::Exact(1),
        PURE_META,
        V09ScalarExecutionStatus::Distributed,
    ),
    v09_scalar_entry(
        "SQL-F-N18",
        "cos",
        Arity::Exact(1),
        PURE_META,
        V09ScalarExecutionStatus::Distributed,
    ),
    v09_scalar_entry(
        "SQL-F-N19",
        "tan",
        Arity::Exact(1),
        PURE_META,
        V09ScalarExecutionStatus::Distributed,
    ),
    v09_scalar_entry(
        "SQL-F-N20",
        "asin",
        Arity::Exact(1),
        PURE_META,
        V09ScalarExecutionStatus::Distributed,
    ),
    v09_scalar_entry(
        "SQL-F-N21",
        "acos",
        Arity::Exact(1),
        PURE_META,
        V09ScalarExecutionStatus::Distributed,
    ),
    v09_scalar_entry(
        "SQL-F-N22",
        "atan",
        Arity::Exact(1),
        PURE_META,
        V09ScalarExecutionStatus::Distributed,
    ),
    v09_scalar_entry(
        "SQL-F-N23",
        "atan2",
        Arity::Exact(2),
        PURE_META,
        V09ScalarExecutionStatus::Distributed,
    ),
    v09_scalar_entry(
        "SQL-F-N24",
        "degrees",
        Arity::Exact(1),
        PURE_META,
        V09ScalarExecutionStatus::Distributed,
    ),
    v09_scalar_entry(
        "SQL-F-N25",
        "radians",
        Arity::Exact(1),
        PURE_META,
        V09ScalarExecutionStatus::Distributed,
    ),
    v09_scalar_entry(
        "SQL-F-N26",
        "pi",
        Arity::Exact(0),
        PURE_META,
        V09ScalarExecutionStatus::Distributed,
    ),
    v09_scalar_entry(
        "SQL-F-H01",
        "sha256",
        Arity::Exact(1),
        PURE_META,
        V09ScalarExecutionStatus::Distributed,
    ),
    v09_scalar_entry(
        "SQL-F-H02",
        "md5",
        Arity::Exact(1),
        PURE_META,
        V09ScalarExecutionStatus::Distributed,
    ),
    v09_scalar_entry(
        "SQL-F-H03",
        "simhash",
        Arity::Exact(1),
        PURE_META,
        V09ScalarExecutionStatus::Distributed,
    ),
    v09_scalar_entry(
        "SQL-F-H04",
        "hamming_distance",
        Arity::Exact(2),
        PURE_META,
        V09ScalarExecutionStatus::Distributed,
    ),
    v09_scalar_entry(
        "SQL-F-H05",
        "gen_random_uuid",
        Arity::Exact(0),
        RANDOM_META,
        V09ScalarExecutionStatus::LocalOnly,
    ),
    v09_scalar_entry(
        "SQL-F-H06",
        "uuidv7",
        Arity::Exact(0),
        RANDOM_META,
        V09ScalarExecutionStatus::LocalOnly,
    ),
    v09_scalar_entry(
        "SQL-F-H07",
        "hex",
        Arity::Exact(1),
        PURE_META,
        V09ScalarExecutionStatus::Distributed,
    ),
    v09_scalar_entry(
        "SQL-F-H08",
        "unhex",
        Arity::Exact(1),
        PURE_META,
        V09ScalarExecutionStatus::Distributed,
    ),
    v09_scalar_entry(
        "SQL-F-H09",
        "encode",
        Arity::Exact(2),
        PURE_META,
        V09ScalarExecutionStatus::Distributed,
    ),
    v09_scalar_entry(
        "SQL-F-H10",
        "decode",
        Arity::Exact(2),
        PURE_META,
        V09ScalarExecutionStatus::Distributed,
    ),
];

/// Returns every scalar row owned by Phase 4.7 in approval-table order.
pub fn v09_numeric_vector_hash_scalar_register() -> &'static [V09ScalarRegisterEntry] {
    V09_NUMERIC_VECTOR_HASH_SCALAR_REGISTER
}

/// Finds a Phase 4.7 matrix row by its public scalar identity.
pub fn v09_numeric_vector_hash_scalar(name: &str) -> Option<&'static V09ScalarRegisterEntry> {
    V09_NUMERIC_VECTOR_HASH_SCALAR_REGISTER
        .iter()
        .find(|entry| entry.name.eq_ignore_ascii_case(name))
}

/// Complete, non-abbreviated Phase 4.8 register for string, conditional,
/// type, and system scalar functions.
pub const V09_STRING_CONDITIONAL_SYSTEM_SCALAR_REGISTER: &[V09ScalarRegisterEntry] = &[
    v09_scalar_entry(
        "SQL-F-S01",
        "length",
        Arity::Exact(1),
        PURE_META,
        V09ScalarExecutionStatus::Distributed,
    ),
    v09_scalar_entry(
        "SQL-F-S02",
        "char_length",
        Arity::Exact(1),
        PURE_META,
        V09ScalarExecutionStatus::Distributed,
    ),
    v09_scalar_entry(
        "SQL-F-S03",
        "octet_length",
        Arity::Exact(1),
        PURE_META,
        V09ScalarExecutionStatus::Distributed,
    ),
    v09_scalar_entry(
        "SQL-F-S04",
        "upper",
        Arity::Exact(1),
        PURE_META,
        V09ScalarExecutionStatus::Distributed,
    ),
    v09_scalar_entry(
        "SQL-F-S05",
        "lower",
        Arity::Exact(1),
        PURE_META,
        V09ScalarExecutionStatus::Distributed,
    ),
    v09_scalar_entry(
        "SQL-F-S06",
        "initcap",
        Arity::Exact(1),
        PURE_META,
        V09ScalarExecutionStatus::Distributed,
    ),
    v09_scalar_entry(
        "SQL-F-S07",
        "substr",
        Arity::Range(2, 3),
        PURE_META,
        V09ScalarExecutionStatus::Distributed,
    ),
    v09_scalar_entry(
        "SQL-F-S08",
        "left",
        Arity::Exact(2),
        PURE_META,
        V09ScalarExecutionStatus::Distributed,
    ),
    v09_scalar_entry(
        "SQL-F-S09",
        "right",
        Arity::Exact(2),
        PURE_META,
        V09ScalarExecutionStatus::Distributed,
    ),
    v09_scalar_entry(
        "SQL-F-S10",
        "trim",
        Arity::Range(1, 2),
        PURE_META,
        V09ScalarExecutionStatus::Distributed,
    ),
    v09_scalar_entry(
        "SQL-F-S11",
        "ltrim",
        Arity::Range(1, 2),
        PURE_META,
        V09ScalarExecutionStatus::Distributed,
    ),
    v09_scalar_entry(
        "SQL-F-S12",
        "rtrim",
        Arity::Range(1, 2),
        PURE_META,
        V09ScalarExecutionStatus::Distributed,
    ),
    v09_scalar_entry(
        "SQL-F-S13",
        "replace",
        Arity::Exact(3),
        PURE_META,
        V09ScalarExecutionStatus::Distributed,
    ),
    v09_scalar_entry(
        "SQL-F-S14",
        "instr",
        Arity::Exact(2),
        PURE_META,
        V09ScalarExecutionStatus::Distributed,
    ),
    v09_scalar_entry(
        "SQL-F-S15",
        "strpos",
        Arity::Exact(2),
        PURE_META,
        V09ScalarExecutionStatus::Distributed,
    ),
    v09_scalar_entry(
        "SQL-F-S16",
        "concat",
        Arity::Variadic(0),
        PURE_META,
        V09ScalarExecutionStatus::Distributed,
    ),
    v09_scalar_entry(
        "SQL-F-S17",
        "concat_ws",
        Arity::Variadic(1),
        PURE_META,
        V09ScalarExecutionStatus::Distributed,
    ),
    v09_scalar_entry(
        "SQL-F-S18",
        "repeat",
        Arity::Exact(2),
        PURE_META,
        V09ScalarExecutionStatus::Distributed,
    ),
    v09_scalar_entry(
        "SQL-F-S19",
        "reverse",
        Arity::Exact(1),
        PURE_META,
        V09ScalarExecutionStatus::Distributed,
    ),
    v09_scalar_entry(
        "SQL-F-S20",
        "lpad",
        Arity::Range(2, 3),
        PURE_META,
        V09ScalarExecutionStatus::Distributed,
    ),
    v09_scalar_entry(
        "SQL-F-S21",
        "rpad",
        Arity::Range(2, 3),
        PURE_META,
        V09ScalarExecutionStatus::Distributed,
    ),
    v09_scalar_entry(
        "SQL-F-S22",
        "split_part",
        Arity::Exact(3),
        PURE_META,
        V09ScalarExecutionStatus::Distributed,
    ),
    v09_scalar_entry(
        "SQL-F-S23",
        "regexp_replace",
        Arity::Exact(3),
        PURE_META,
        V09ScalarExecutionStatus::Distributed,
    ),
    v09_scalar_entry(
        "SQL-F-S24",
        "regexp_match",
        Arity::Exact(2),
        PURE_META,
        V09ScalarExecutionStatus::Distributed,
    ),
    v09_scalar_entry(
        "SQL-F-S25",
        "regexp_matches",
        Arity::Range(2, 3),
        PURE_META,
        V09ScalarExecutionStatus::Distributed,
    ),
    v09_scalar_entry(
        "SQL-F-C01",
        "coalesce",
        Arity::Variadic(1),
        PURE_META,
        V09ScalarExecutionStatus::Distributed,
    ),
    v09_scalar_entry(
        "SQL-F-C02",
        "nullif",
        Arity::Exact(2),
        PURE_META,
        V09ScalarExecutionStatus::Distributed,
    ),
    v09_scalar_entry(
        "SQL-F-C03",
        "ifnull",
        Arity::Exact(2),
        PURE_META,
        V09ScalarExecutionStatus::Distributed,
    ),
    v09_scalar_entry(
        "SQL-F-C04",
        "iif",
        Arity::Exact(3),
        PURE_META,
        V09ScalarExecutionStatus::Distributed,
    ),
    v09_scalar_entry(
        "SQL-F-C05",
        "greatest",
        Arity::Variadic(1),
        PURE_META,
        V09ScalarExecutionStatus::Distributed,
    ),
    v09_scalar_entry(
        "SQL-F-C06",
        "least",
        Arity::Variadic(1),
        PURE_META,
        V09ScalarExecutionStatus::Distributed,
    ),
    v09_scalar_entry(
        "SQL-F-C07",
        "typeof",
        Arity::Exact(1),
        PURE_META,
        V09ScalarExecutionStatus::Distributed,
    ),
    v09_scalar_entry(
        "SQL-F-C08",
        "pg_typeof",
        Arity::Exact(1),
        PURE_META,
        V09ScalarExecutionStatus::Distributed,
    ),
    v09_scalar_entry(
        "SQL-F-C09",
        "quote",
        Arity::Exact(1),
        PURE_META,
        V09ScalarExecutionStatus::Distributed,
    ),
    v09_scalar_entry(
        "SQL-F-SYS01",
        "memory_stats",
        Arity::Exact(0),
        SYSTEM_META,
        V09ScalarExecutionStatus::LocalOnly,
    ),
    v09_scalar_entry(
        "SQL-F-SYS02",
        "io_stats",
        Arity::Exact(0),
        SYSTEM_META,
        V09ScalarExecutionStatus::LocalOnly,
    ),
    v09_scalar_entry(
        "SQL-F-SYS03",
        "clear_cache",
        Arity::Exact(0),
        SYSTEM_SIDE_EFFECT_META,
        V09ScalarExecutionStatus::LocalOnly,
    ),
];

/// Returns every scalar row owned by Phase 4.8 in approval-table order.
pub fn v09_string_conditional_system_scalar_register() -> &'static [V09ScalarRegisterEntry] {
    V09_STRING_CONDITIONAL_SYSTEM_SCALAR_REGISTER
}

/// Finds a Phase 4.8 scalar matrix row by its public scalar identity.
pub fn v09_string_conditional_system_scalar(name: &str) -> Option<&'static V09ScalarRegisterEntry> {
    V09_STRING_CONDITIONAL_SYSTEM_SCALAR_REGISTER
        .iter()
        .find(|entry| entry.name.eq_ignore_ascii_case(name))
}

/// Public aggregate-call arity fixed by the Phase 4.8 matrix.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum V09AggregateArity {
    /// `COUNT(*)` and `COUNT(expr)` are both accepted.
    CountStarOrOne,
    /// Exactly one aggregate argument is required.
    ExactOne,
    /// A text argument and an optional literal text separator are accepted.
    OneOrTwoWithLiteralTextSeparator,
    /// A text argument and a literal text separator are both required.
    ExactlyTwoWithLiteralTextSeparator,
}

/// Input and result type contract for a Phase 4.8 aggregate row.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum V09AggregateTypeContract {
    /// Any input type is counted and the result is a `BigInt`.
    AnyInputToBigInt,
    /// A numeric input is accumulated into a `Double` result.
    NumericInputToDouble,
    /// The result preserves the single input type.
    InputTypeToSameType,
    /// A text input yields a text result.
    TextInputToText,
}

/// NULL/empty-input behavior for a Phase 4.8 aggregate row.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum V09AggregateNullContract {
    /// Count rows for `*`, or count only non-NULL values for an expression.
    CountsRowsOrNonNullValues,
    /// Ignore NULL values and return NULL when no non-NULL value remains.
    IgnoresNullAndReturnsNullWhenEmpty,
    /// Ignore NULL values and return zero when no non-NULL value remains.
    IgnoresNullAndReturnsZeroWhenEmpty,
}

/// Whether the final result is independent of input order.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum V09AggregateOrdering {
    /// Global ordering cannot change the final aggregate result.
    InputOrderIndependent,
    /// Inputs must be replayed in their global logical order.
    GlobalInputOrder,
}

/// Coordinator finalization path required by a Phase 4.8 aggregate row.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum V09AggregateFinalization {
    /// A non-DISTINCT aggregate may merge its proven exact partial state;
    /// DISTINCT still uses ordered replay.
    ExactPartialWhenProven,
    /// The coordinator must replay raw inputs in global logical order.
    OrderedInputReplay,
}

/// Optimizer-relevant metadata for a Phase 4.8 aggregate row.
///
/// This is intentionally separate from [`FnMeta`]: aggregate rows consume a
/// relation and are never scalar-foldable. `reorderable` describes whether
/// reordering aggregate inputs preserves the current SQL result.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct V09AggregateMeta {
    pub deterministic: bool,
    pub volatile: bool,
    pub side_effecting: bool,
    pub foldable: bool,
    pub reorderable: bool,
}

/// Metadata for COUNT/MIN/MAX, whose aggregate result is input-order
/// independent but still cannot be scalar-folded.
pub const V09_ORDER_INDEPENDENT_AGGREGATE_META: V09AggregateMeta = V09AggregateMeta {
    deterministic: true,
    volatile: false,
    side_effecting: false,
    foldable: false,
    reorderable: true,
};

/// Metadata for aggregates that must retain their global logical input order.
pub const V09_ORDERED_AGGREGATE_META: V09AggregateMeta = V09AggregateMeta {
    deterministic: true,
    volatile: false,
    side_effecting: false,
    foldable: false,
    reorderable: false,
};

/// A named row from the approved v0.9 Phase 4 aggregate support matrix.
///
/// The row makes the public aggregate contract explicit while leaving SQL
/// execution to the existing planner, accumulator, and distributed assembler.
/// In particular, `finalization` describes the coordinator path after all
/// planned ranges have completed and acknowledged cleanup.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct V09AggregateRegisterEntry {
    pub id: &'static str,
    pub name: &'static str,
    pub arity: V09AggregateArity,
    pub type_contract: V09AggregateTypeContract,
    pub null_contract: V09AggregateNullContract,
    pub distinct_supported: bool,
    pub ordering: V09AggregateOrdering,
    pub finalization: V09AggregateFinalization,
    pub execution_status: V09ScalarExecutionStatus,
    pub metadata: V09AggregateMeta,
}

/// Complete Phase 4.8 aggregate register in approval-table order.
pub const V09_AGGREGATE_REGISTER: &[V09AggregateRegisterEntry] = &[
    V09AggregateRegisterEntry {
        id: "SQL-A01",
        name: "count",
        arity: V09AggregateArity::CountStarOrOne,
        type_contract: V09AggregateTypeContract::AnyInputToBigInt,
        null_contract: V09AggregateNullContract::CountsRowsOrNonNullValues,
        distinct_supported: true,
        ordering: V09AggregateOrdering::InputOrderIndependent,
        finalization: V09AggregateFinalization::ExactPartialWhenProven,
        execution_status: V09ScalarExecutionStatus::Distributed,
        metadata: V09_ORDER_INDEPENDENT_AGGREGATE_META,
    },
    V09AggregateRegisterEntry {
        id: "SQL-A02",
        name: "sum",
        arity: V09AggregateArity::ExactOne,
        type_contract: V09AggregateTypeContract::NumericInputToDouble,
        null_contract: V09AggregateNullContract::IgnoresNullAndReturnsNullWhenEmpty,
        distinct_supported: true,
        ordering: V09AggregateOrdering::GlobalInputOrder,
        finalization: V09AggregateFinalization::OrderedInputReplay,
        execution_status: V09ScalarExecutionStatus::Distributed,
        metadata: V09_ORDERED_AGGREGATE_META,
    },
    V09AggregateRegisterEntry {
        id: "SQL-A03",
        name: "total",
        arity: V09AggregateArity::ExactOne,
        type_contract: V09AggregateTypeContract::NumericInputToDouble,
        null_contract: V09AggregateNullContract::IgnoresNullAndReturnsZeroWhenEmpty,
        distinct_supported: true,
        ordering: V09AggregateOrdering::GlobalInputOrder,
        finalization: V09AggregateFinalization::OrderedInputReplay,
        execution_status: V09ScalarExecutionStatus::Distributed,
        metadata: V09_ORDERED_AGGREGATE_META,
    },
    V09AggregateRegisterEntry {
        id: "SQL-A04",
        name: "avg",
        arity: V09AggregateArity::ExactOne,
        type_contract: V09AggregateTypeContract::NumericInputToDouble,
        null_contract: V09AggregateNullContract::IgnoresNullAndReturnsNullWhenEmpty,
        distinct_supported: true,
        ordering: V09AggregateOrdering::GlobalInputOrder,
        finalization: V09AggregateFinalization::OrderedInputReplay,
        execution_status: V09ScalarExecutionStatus::Distributed,
        metadata: V09_ORDERED_AGGREGATE_META,
    },
    V09AggregateRegisterEntry {
        id: "SQL-A05",
        name: "min",
        arity: V09AggregateArity::ExactOne,
        type_contract: V09AggregateTypeContract::InputTypeToSameType,
        null_contract: V09AggregateNullContract::IgnoresNullAndReturnsNullWhenEmpty,
        distinct_supported: true,
        ordering: V09AggregateOrdering::InputOrderIndependent,
        finalization: V09AggregateFinalization::ExactPartialWhenProven,
        execution_status: V09ScalarExecutionStatus::Distributed,
        metadata: V09_ORDER_INDEPENDENT_AGGREGATE_META,
    },
    V09AggregateRegisterEntry {
        id: "SQL-A06",
        name: "max",
        arity: V09AggregateArity::ExactOne,
        type_contract: V09AggregateTypeContract::InputTypeToSameType,
        null_contract: V09AggregateNullContract::IgnoresNullAndReturnsNullWhenEmpty,
        distinct_supported: true,
        ordering: V09AggregateOrdering::InputOrderIndependent,
        finalization: V09AggregateFinalization::ExactPartialWhenProven,
        execution_status: V09ScalarExecutionStatus::Distributed,
        metadata: V09_ORDER_INDEPENDENT_AGGREGATE_META,
    },
    V09AggregateRegisterEntry {
        id: "SQL-A07",
        name: "group_concat",
        arity: V09AggregateArity::OneOrTwoWithLiteralTextSeparator,
        type_contract: V09AggregateTypeContract::TextInputToText,
        null_contract: V09AggregateNullContract::IgnoresNullAndReturnsNullWhenEmpty,
        distinct_supported: true,
        ordering: V09AggregateOrdering::GlobalInputOrder,
        finalization: V09AggregateFinalization::OrderedInputReplay,
        execution_status: V09ScalarExecutionStatus::Distributed,
        metadata: V09_ORDERED_AGGREGATE_META,
    },
    V09AggregateRegisterEntry {
        id: "SQL-A08",
        name: "string_agg",
        arity: V09AggregateArity::ExactlyTwoWithLiteralTextSeparator,
        type_contract: V09AggregateTypeContract::TextInputToText,
        null_contract: V09AggregateNullContract::IgnoresNullAndReturnsNullWhenEmpty,
        distinct_supported: true,
        ordering: V09AggregateOrdering::GlobalInputOrder,
        finalization: V09AggregateFinalization::OrderedInputReplay,
        execution_status: V09ScalarExecutionStatus::Distributed,
        metadata: V09_ORDERED_AGGREGATE_META,
    },
];

/// Returns every aggregate row owned by Phase 4.8 in approval-table order.
pub fn v09_aggregate_register() -> &'static [V09AggregateRegisterEntry] {
    V09_AGGREGATE_REGISTER
}

fn is_numeric(ty: &ResolvedType) -> bool {
    matches!(
        ty,
        ResolvedType::Integer
            | ResolvedType::BigInt
            | ResolvedType::Float
            | ResolvedType::Double
            | ResolvedType::Null
    )
}

pub fn check_numeric(args: &[TypedExpr]) -> Result<(), PlannerError> {
    for arg in args {
        if !is_numeric(&arg.resolved_type) {
            return Err(PlannerError::type_mismatch(
                "Numeric",
                arg.resolved_type.type_name(),
                arg.span,
            ));
        }
    }
    Ok(())
}

pub fn check_text(args: &[TypedExpr]) -> Result<(), PlannerError> {
    for arg in args {
        if !matches!(arg.resolved_type, ResolvedType::Text | ResolvedType::Null) {
            return Err(PlannerError::type_mismatch(
                "Text",
                arg.resolved_type.type_name(),
                arg.span,
            ));
        }
    }
    Ok(())
}

pub fn check_text_or_blob(args: &[TypedExpr]) -> Result<(), PlannerError> {
    for arg in args {
        if !matches!(
            arg.resolved_type,
            ResolvedType::Text | ResolvedType::Blob | ResolvedType::Null
        ) {
            return Err(PlannerError::type_mismatch(
                "Text or Blob",
                arg.resolved_type.type_name(),
                arg.span,
            ));
        }
    }
    Ok(())
}

pub fn check_bigint(args: &[TypedExpr]) -> Result<(), PlannerError> {
    for arg in args {
        if !matches!(arg.resolved_type, ResolvedType::BigInt | ResolvedType::Null) {
            return Err(PlannerError::type_mismatch(
                "BigInt",
                arg.resolved_type.type_name(),
                arg.span,
            ));
        }
    }
    Ok(())
}

pub fn check_blob_text(args: &[TypedExpr]) -> Result<(), PlannerError> {
    if let Some(first) = args.first()
        && !matches!(first.resolved_type, ResolvedType::Blob | ResolvedType::Null)
    {
        return Err(PlannerError::type_mismatch(
            "Blob",
            first.resolved_type.type_name(),
            first.span,
        ));
    }
    check_text(&args[1..])
}

pub fn check_any(args: &[TypedExpr]) -> Result<(), PlannerError> {
    if args.is_empty() {
        return Err(PlannerError::invalid_expression(
            "at least one argument is required",
        ));
    }
    Ok(())
}

pub fn check_no_args(_args: &[TypedExpr]) -> Result<(), PlannerError> {
    Ok(())
}

fn check_vector_one(args: &[TypedExpr]) -> Result<(), PlannerError> {
    match args.first().map(|arg| &arg.resolved_type) {
        Some(ResolvedType::Vector { .. } | ResolvedType::Null) => Ok(()),
        Some(ty) => Err(PlannerError::type_mismatch(
            "Vector",
            ty.type_name(),
            args[0].span,
        )),
        None => Ok(()),
    }
}

fn check_vector_triplet(args: &[TypedExpr]) -> Result<(), PlannerError> {
    let first = match &args[0].resolved_type {
        ResolvedType::Vector { dimension, .. } => *dimension,
        ty => {
            return Err(PlannerError::type_mismatch(
                "Vector",
                ty.type_name(),
                args[0].span,
            ));
        }
    };
    let second = match &args[1].resolved_type {
        ResolvedType::Vector { dimension, .. } => *dimension,
        ty => {
            return Err(PlannerError::type_mismatch(
                "Vector",
                ty.type_name(),
                args[1].span,
            ));
        }
    };
    if first != second {
        return Err(PlannerError::vector_dimension_mismatch(
            first,
            second,
            args[1].span,
        ));
    }
    match &args[2].resolved_type {
        ResolvedType::Text => {
            if let TypedExprKind::Literal(Literal::String(value)) = &args[2].kind
                && !matches!(
                    value.to_ascii_lowercase().as_str(),
                    "cosine" | "l2" | "inner"
                )
            {
                return Err(PlannerError::invalid_metric(value, args[2].span));
            }
            Ok(())
        }
        ty => Err(PlannerError::type_mismatch(
            "Text (metric)",
            ty.type_name(),
            args[2].span,
        )),
    }
}

fn numeric_return(types: &[ResolvedType]) -> Result<ResolvedType, PlannerError> {
    let mut result = ResolvedType::Null;
    for ty in types {
        if matches!(ty, ResolvedType::Null) {
            continue;
        }
        if !is_numeric(ty) {
            return Err(PlannerError::type_mismatch(
                "Numeric",
                ty.type_name(),
                Span::default(),
            ));
        }
        result = match (&result, ty) {
            (ResolvedType::Null, _) => ty.clone(),
            (ResolvedType::Double, _) | (_, ResolvedType::Double) => ResolvedType::Double,
            (ResolvedType::Float, ResolvedType::BigInt)
            | (ResolvedType::BigInt, ResolvedType::Float) => ResolvedType::Double,
            (ResolvedType::Float, _) | (_, ResolvedType::Float) => ResolvedType::Float,
            (ResolvedType::BigInt, _) | (_, ResolvedType::BigInt) => ResolvedType::BigInt,
            _ => ResolvedType::Integer,
        };
    }
    Ok(result)
}

fn return_arg0(types: &[ResolvedType]) -> Result<ResolvedType, PlannerError> {
    Ok(types.first().cloned().unwrap_or(ResolvedType::Null))
}

fn return_first_non_null(types: &[ResolvedType]) -> Result<ResolvedType, PlannerError> {
    Ok(types
        .iter()
        .find(|ty| !matches!(ty, ResolvedType::Null))
        .cloned()
        .unwrap_or(ResolvedType::Null))
}

fn return_numeric(types: &[ResolvedType]) -> Result<ResolvedType, PlannerError> {
    numeric_return(types)
}

fn return_arg0_numeric(types: &[ResolvedType]) -> Result<ResolvedType, PlannerError> {
    return_arg0(types)
}

const fn sig(
    name: &'static str,
    arity: Arity,
    check: fn(&[TypedExpr]) -> Result<(), PlannerError>,
    ret: ReturnRule,
) -> ScalarSignature {
    ScalarSignature {
        name,
        arity,
        check,
        ret,
        meta: PURE_META,
    }
}

const fn sig_meta(
    name: &'static str,
    arity: Arity,
    check: fn(&[TypedExpr]) -> Result<(), PlannerError>,
    ret: ReturnRule,
    meta: FnMeta,
) -> ScalarSignature {
    ScalarSignature {
        name,
        arity,
        check,
        ret,
        meta,
    }
}

static SIGNATURES: &[ScalarSignature] = &[
    sig(
        "vector_similarity",
        Arity::Exact(3),
        check_vector_triplet,
        ReturnRule::Fixed(ResolvedType::Double),
    ),
    sig(
        "vector_distance",
        Arity::Exact(3),
        check_vector_triplet,
        ReturnRule::Fixed(ResolvedType::Double),
    ),
    sig(
        "vector_dims",
        Arity::Exact(1),
        check_vector_one,
        ReturnRule::Fixed(ResolvedType::Integer),
    ),
    sig(
        "vector_norm",
        Arity::Exact(1),
        check_vector_one,
        ReturnRule::Fixed(ResolvedType::Double),
    ),
    sig(
        "abs",
        Arity::Exact(1),
        check_numeric,
        ReturnRule::FromArgs(return_arg0_numeric),
    ),
    sig(
        "sign",
        Arity::Exact(1),
        check_numeric,
        ReturnRule::Fixed(ResolvedType::Integer),
    ),
    sig(
        "round",
        Arity::Range(1, 2),
        check_numeric,
        ReturnRule::FromArgs(return_arg0_numeric),
    ),
    sig(
        "floor",
        Arity::Exact(1),
        check_numeric,
        ReturnRule::FromArgs(return_arg0_numeric),
    ),
    sig(
        "ceil",
        Arity::Exact(1),
        check_numeric,
        ReturnRule::FromArgs(return_arg0_numeric),
    ),
    sig(
        "ceiling",
        Arity::Exact(1),
        check_numeric,
        ReturnRule::FromArgs(return_arg0_numeric),
    ),
    sig(
        "trunc",
        Arity::Range(1, 2),
        check_numeric,
        ReturnRule::FromArgs(return_arg0_numeric),
    ),
    sig(
        "mod",
        Arity::Exact(2),
        check_numeric,
        ReturnRule::FromArgs(return_numeric),
    ),
    sig(
        "power",
        Arity::Exact(2),
        check_numeric,
        ReturnRule::Fixed(ResolvedType::Double),
    ),
    sig(
        "pow",
        Arity::Exact(2),
        check_numeric,
        ReturnRule::Fixed(ResolvedType::Double),
    ),
    sig(
        "sqrt",
        Arity::Exact(1),
        check_numeric,
        ReturnRule::Fixed(ResolvedType::Double),
    ),
    sig(
        "exp",
        Arity::Exact(1),
        check_numeric,
        ReturnRule::Fixed(ResolvedType::Double),
    ),
    sig(
        "ln",
        Arity::Exact(1),
        check_numeric,
        ReturnRule::Fixed(ResolvedType::Double),
    ),
    sig(
        "log",
        Arity::Range(1, 2),
        check_numeric,
        ReturnRule::Fixed(ResolvedType::Double),
    ),
    sig(
        "log10",
        Arity::Exact(1),
        check_numeric,
        ReturnRule::Fixed(ResolvedType::Double),
    ),
    sig_meta(
        "random",
        Arity::Exact(0),
        check_numeric,
        ReturnRule::Fixed(ResolvedType::Double),
        RANDOM_META,
    ),
    sig(
        "sin",
        Arity::Exact(1),
        check_numeric,
        ReturnRule::Fixed(ResolvedType::Double),
    ),
    sig(
        "cos",
        Arity::Exact(1),
        check_numeric,
        ReturnRule::Fixed(ResolvedType::Double),
    ),
    sig(
        "tan",
        Arity::Exact(1),
        check_numeric,
        ReturnRule::Fixed(ResolvedType::Double),
    ),
    sig(
        "asin",
        Arity::Exact(1),
        check_numeric,
        ReturnRule::Fixed(ResolvedType::Double),
    ),
    sig(
        "acos",
        Arity::Exact(1),
        check_numeric,
        ReturnRule::Fixed(ResolvedType::Double),
    ),
    sig(
        "atan",
        Arity::Exact(1),
        check_numeric,
        ReturnRule::Fixed(ResolvedType::Double),
    ),
    sig(
        "atan2",
        Arity::Exact(2),
        check_numeric,
        ReturnRule::Fixed(ResolvedType::Double),
    ),
    sig(
        "degrees",
        Arity::Exact(1),
        check_numeric,
        ReturnRule::Fixed(ResolvedType::Double),
    ),
    sig(
        "radians",
        Arity::Exact(1),
        check_numeric,
        ReturnRule::Fixed(ResolvedType::Double),
    ),
    sig(
        "pi",
        Arity::Exact(0),
        check_numeric,
        ReturnRule::Fixed(ResolvedType::Double),
    ),
    sig(
        "sha256",
        Arity::Exact(1),
        check_text_or_blob,
        ReturnRule::Fixed(ResolvedType::Blob),
    ),
    sig(
        "md5",
        Arity::Exact(1),
        check_text_or_blob,
        ReturnRule::Fixed(ResolvedType::Text),
    ),
    sig(
        "simhash",
        Arity::Exact(1),
        check_text,
        ReturnRule::Fixed(ResolvedType::BigInt),
    ),
    sig(
        "hamming_distance",
        Arity::Exact(2),
        check_bigint,
        ReturnRule::Fixed(ResolvedType::Integer),
    ),
    sig_meta(
        "gen_random_uuid",
        Arity::Exact(0),
        check_no_args,
        ReturnRule::Fixed(ResolvedType::Text),
        RANDOM_META,
    ),
    sig_meta(
        "uuidv7",
        Arity::Exact(0),
        check_no_args,
        ReturnRule::Fixed(ResolvedType::Text),
        RANDOM_META,
    ),
    sig(
        "hex",
        Arity::Exact(1),
        check_text_or_blob,
        ReturnRule::Fixed(ResolvedType::Text),
    ),
    sig(
        "unhex",
        Arity::Exact(1),
        check_text,
        ReturnRule::Fixed(ResolvedType::Blob),
    ),
    sig(
        "encode",
        Arity::Exact(2),
        check_blob_text,
        ReturnRule::Fixed(ResolvedType::Text),
    ),
    sig(
        "decode",
        Arity::Exact(2),
        check_text,
        ReturnRule::Fixed(ResolvedType::Blob),
    ),
    sig(
        "length",
        Arity::Exact(1),
        check_text_or_blob,
        ReturnRule::Fixed(ResolvedType::Integer),
    ),
    sig(
        "char_length",
        Arity::Exact(1),
        check_text,
        ReturnRule::Fixed(ResolvedType::Integer),
    ),
    sig(
        "octet_length",
        Arity::Exact(1),
        check_text_or_blob,
        ReturnRule::Fixed(ResolvedType::Integer),
    ),
    sig(
        "upper",
        Arity::Exact(1),
        check_text,
        ReturnRule::Fixed(ResolvedType::Text),
    ),
    sig(
        "lower",
        Arity::Exact(1),
        check_text,
        ReturnRule::Fixed(ResolvedType::Text),
    ),
    sig(
        "initcap",
        Arity::Exact(1),
        check_text,
        ReturnRule::Fixed(ResolvedType::Text),
    ),
    sig(
        "substr",
        Arity::Range(2, 3),
        check_numeric_or_text_substr,
        ReturnRule::Fixed(ResolvedType::Text),
    ),
    sig(
        "left",
        Arity::Exact(2),
        check_text_numeric,
        ReturnRule::Fixed(ResolvedType::Text),
    ),
    sig(
        "right",
        Arity::Exact(2),
        check_text_numeric,
        ReturnRule::Fixed(ResolvedType::Text),
    ),
    sig(
        "trim",
        Arity::Range(1, 2),
        check_text,
        ReturnRule::Fixed(ResolvedType::Text),
    ),
    sig(
        "ltrim",
        Arity::Range(1, 2),
        check_text,
        ReturnRule::Fixed(ResolvedType::Text),
    ),
    sig(
        "rtrim",
        Arity::Range(1, 2),
        check_text,
        ReturnRule::Fixed(ResolvedType::Text),
    ),
    sig(
        "replace",
        Arity::Exact(3),
        check_text,
        ReturnRule::Fixed(ResolvedType::Text),
    ),
    sig(
        "instr",
        Arity::Exact(2),
        check_text,
        ReturnRule::Fixed(ResolvedType::Integer),
    ),
    sig(
        "strpos",
        Arity::Exact(2),
        check_text,
        ReturnRule::Fixed(ResolvedType::Integer),
    ),
    sig(
        "concat",
        Arity::Variadic(0),
        check_text,
        ReturnRule::Fixed(ResolvedType::Text),
    ),
    sig(
        "concat_ws",
        Arity::Variadic(1),
        check_text,
        ReturnRule::Fixed(ResolvedType::Text),
    ),
    sig(
        "repeat",
        Arity::Exact(2),
        check_text_numeric,
        ReturnRule::Fixed(ResolvedType::Text),
    ),
    sig(
        "reverse",
        Arity::Exact(1),
        check_text,
        ReturnRule::Fixed(ResolvedType::Text),
    ),
    sig(
        "lpad",
        Arity::Range(2, 3),
        check_pad,
        ReturnRule::Fixed(ResolvedType::Text),
    ),
    sig(
        "rpad",
        Arity::Range(2, 3),
        check_pad,
        ReturnRule::Fixed(ResolvedType::Text),
    ),
    sig(
        "split_part",
        Arity::Exact(3),
        check_text_text_numeric,
        ReturnRule::Fixed(ResolvedType::Text),
    ),
    sig(
        "regexp_replace",
        Arity::Exact(3),
        check_text,
        ReturnRule::Fixed(ResolvedType::Text),
    ),
    sig(
        "regexp_match",
        Arity::Exact(2),
        check_text,
        ReturnRule::Fixed(ResolvedType::Text),
    ),
    sig(
        "regexp_matches",
        Arity::Range(2, 3),
        check_text,
        ReturnRule::Fixed(ResolvedType::Text),
    ),
    sig(
        "coalesce",
        Arity::Variadic(1),
        check_compatible,
        ReturnRule::FromArgs(return_first_non_null),
    ),
    sig(
        "nullif",
        Arity::Exact(2),
        check_compatible,
        ReturnRule::FromArgs(return_first_non_null),
    ),
    sig(
        "ifnull",
        Arity::Exact(2),
        check_compatible,
        ReturnRule::FromArgs(return_first_non_null),
    ),
    sig(
        "iif",
        Arity::Exact(3),
        check_iif,
        ReturnRule::FromArgs(return_first_non_null),
    ),
    sig(
        "greatest",
        Arity::Variadic(1),
        check_numeric,
        ReturnRule::FromArgs(return_numeric),
    ),
    sig(
        "least",
        Arity::Variadic(1),
        check_numeric,
        ReturnRule::FromArgs(return_numeric),
    ),
    sig(
        "typeof",
        Arity::Exact(1),
        check_any,
        ReturnRule::Fixed(ResolvedType::Text),
    ),
    sig(
        "pg_typeof",
        Arity::Exact(1),
        check_any,
        ReturnRule::Fixed(ResolvedType::Text),
    ),
    sig(
        "quote",
        Arity::Exact(1),
        check_any,
        ReturnRule::Fixed(ResolvedType::Text),
    ),
    sig_meta(
        "memory_stats",
        Arity::Exact(0),
        check_no_args,
        ReturnRule::Fixed(ResolvedType::Text),
        SYSTEM_META,
    ),
    sig_meta(
        "io_stats",
        Arity::Exact(0),
        check_no_args,
        ReturnRule::Fixed(ResolvedType::Text),
        SYSTEM_META,
    ),
    sig_meta(
        "clear_cache",
        Arity::Exact(0),
        check_no_args,
        ReturnRule::Fixed(ResolvedType::BigInt),
        SYSTEM_SIDE_EFFECT_META,
    ),
];

fn check_numeric_or_text_substr(args: &[TypedExpr]) -> Result<(), PlannerError> {
    if args.is_empty() {
        return Ok(());
    }
    check_text(&args[..1])?;
    check_numeric(&args[1..])
}

fn check_text_numeric(args: &[TypedExpr]) -> Result<(), PlannerError> {
    if !args.is_empty() {
        check_text(&args[..1])?;
    }
    if args.len() >= 2 {
        check_numeric(&args[1..2])?;
    }
    Ok(())
}

fn check_pad(args: &[TypedExpr]) -> Result<(), PlannerError> {
    check_text_numeric(&args[..2])?;
    if args.len() == 3 {
        check_text(&args[2..3])?;
    }
    Ok(())
}

fn check_text_text_numeric(args: &[TypedExpr]) -> Result<(), PlannerError> {
    check_text(&args[..2])?;
    check_numeric(&args[2..3])
}

fn check_compatible(args: &[TypedExpr]) -> Result<(), PlannerError> {
    let mut expected: Option<&ResolvedType> = None;
    for arg in args {
        if matches!(arg.resolved_type, ResolvedType::Null) {
            continue;
        }
        if let Some(first) = expected {
            if !(is_numeric(first) && is_numeric(&arg.resolved_type)) && first != &arg.resolved_type
            {
                return Err(PlannerError::type_mismatch(
                    first.type_name(),
                    arg.resolved_type.type_name(),
                    arg.span,
                ));
            }
        } else {
            expected = Some(&arg.resolved_type);
        }
    }
    Ok(())
}

fn check_iif(args: &[TypedExpr]) -> Result<(), PlannerError> {
    if !matches!(
        args.first().map(|a| &a.resolved_type),
        Some(ResolvedType::Boolean | ResolvedType::Null)
    ) {
        let arg = args
            .first()
            .expect("arity is validated before type checking");
        return Err(PlannerError::type_mismatch(
            "Boolean",
            arg.resolved_type.type_name(),
            arg.span,
        ));
    }
    check_compatible(&args[1..])
}

pub fn signatures() -> &'static [ScalarSignature] {
    SIGNATURES
}

pub fn signature(name: &str) -> Option<&'static ScalarSignature> {
    let lower = name.to_ascii_lowercase();
    SIGNATURES.iter().find(|sig| sig.name == lower)
}

pub fn is_numeric_type(ty: &ResolvedType) -> bool {
    is_numeric(ty)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names_are_normalized_and_unique() {
        let mut names: Vec<_> = signatures().iter().map(|s| s.name).collect();
        names.sort_unstable();
        names.dedup();
        assert_eq!(names.len(), signatures().len());
        assert!(
            signatures()
                .iter()
                .all(|s| s.name == s.name.to_ascii_lowercase())
        );
    }

    #[test]
    fn random_metadata_is_volatile() {
        let random = signature("RANDOM").unwrap();
        assert!(!random.meta.deterministic);
        assert!(random.meta.volatile);
        assert!(!random.meta.foldable);
        assert!(!random.meta.cacheable);
    }

    #[test]
    fn scalar_names_do_not_overlap_aggregate_names() {
        let aggregates = [
            "count",
            "sum",
            "total",
            "avg",
            "min",
            "max",
            "group_concat",
            "string_agg",
        ];
        assert!(aggregates.iter().all(|name| signature(name).is_none()));
    }
}
