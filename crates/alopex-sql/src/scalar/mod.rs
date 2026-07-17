//! Shared scalar-function signatures.
//!
//! This module is the single source of truth for scalar names, arity, type
//! contracts, return rules, and optimizer metadata. Evaluation functions are
//! attached by `executor::evaluator::registry`.

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

#[derive(Debug, Clone)]
pub struct ScalarSignature {
    pub name: &'static str,
    pub arity: Arity,
    pub check: fn(&[TypedExpr]) -> Result<(), PlannerError>,
    pub ret: ReturnRule,
    pub meta: FnMeta,
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
