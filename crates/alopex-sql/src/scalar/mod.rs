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
    FromTypedArgs(fn(&[TypedExpr]) -> Result<ResolvedType, PlannerError>),
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

/// Metadata for values that vary between statements but are fixed within one.
pub const STATEMENT_STABLE_META: FnMeta = FnMeta {
    deterministic: false,
    volatile: false,
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

fn check_json_object(args: &[TypedExpr]) -> Result<(), PlannerError> {
    if !args.len().is_multiple_of(2) {
        return Err(PlannerError::invalid_expression(
            "JSON_OBJECT expects label/value pairs".to_string(),
        ));
    }
    for label in args.iter().step_by(2) {
        if !matches!(label.resolved_type, ResolvedType::Text | ResolvedType::Null) {
            return Err(PlannerError::type_mismatch(
                "Text",
                label.resolved_type.type_name(),
                label.span,
            ));
        }
    }
    Ok(())
}

fn check_json_input(args: &[TypedExpr]) -> Result<(), PlannerError> {
    for arg in args {
        if !matches!(
            arg.resolved_type,
            ResolvedType::Text | ResolvedType::Json | ResolvedType::Null
        ) {
            return Err(PlannerError::type_mismatch(
                "JSON or Text",
                arg.resolved_type.type_name(),
                arg.span,
            ));
        }
    }
    Ok(())
}

fn common_nested_type(
    types: impl IntoIterator<Item = ResolvedType>,
) -> Result<ResolvedType, PlannerError> {
    let mut common = ResolvedType::Null;
    for data_type in types {
        common = match (&common, &data_type) {
            (ResolvedType::Null, _) => data_type,
            (_, ResolvedType::Null) => common,
            (left, right) if left == right => common,
            (ResolvedType::Integer, ResolvedType::BigInt)
            | (ResolvedType::BigInt, ResolvedType::Integer) => ResolvedType::BigInt,
            (
                ResolvedType::Integer | ResolvedType::BigInt | ResolvedType::Float,
                ResolvedType::Double,
            )
            | (
                ResolvedType::Double,
                ResolvedType::Integer | ResolvedType::BigInt | ResolvedType::Float,
            ) => ResolvedType::Double,
            (ResolvedType::Integer, ResolvedType::Float)
            | (ResolvedType::Float, ResolvedType::Integer) => ResolvedType::Float,
            (ResolvedType::BigInt, ResolvedType::Float)
            | (ResolvedType::Float, ResolvedType::BigInt) => ResolvedType::Double,
            _ => {
                return Err(PlannerError::invalid_expression(
                    "nested values require a common element type",
                ));
            }
        };
    }
    Ok(common)
}

fn return_array_value(args: &[ResolvedType]) -> Result<ResolvedType, PlannerError> {
    Ok(ResolvedType::Array(Box::new(common_nested_type(
        args.to_vec(),
    )?)))
}

fn return_integer_array(_args: &[ResolvedType]) -> Result<ResolvedType, PlannerError> {
    Ok(ResolvedType::Array(Box::new(ResolvedType::Integer)))
}

fn return_text_array(_args: &[ResolvedType]) -> Result<ResolvedType, PlannerError> {
    Ok(ResolvedType::Array(Box::new(ResolvedType::Text)))
}

fn array_element(data_type: &ResolvedType) -> Result<ResolvedType, PlannerError> {
    match data_type {
        ResolvedType::Array(element) => Ok((**element).clone()),
        ResolvedType::Null => Ok(ResolvedType::Null),
        other => Err(PlannerError::invalid_expression(format!(
            "expected ARRAY, found {other}"
        ))),
    }
}

fn check_array_first(args: &[TypedExpr]) -> Result<(), PlannerError> {
    array_element(&args[0].resolved_type).map(|_| ())
}

fn check_array_append(args: &[TypedExpr]) -> Result<(), PlannerError> {
    common_nested_type([
        array_element(&args[0].resolved_type)?,
        args[1].resolved_type.clone(),
    ])
    .map(|_| ())
}

fn check_array_prepend(args: &[TypedExpr]) -> Result<(), PlannerError> {
    common_nested_type([
        args[0].resolved_type.clone(),
        array_element(&args[1].resolved_type)?,
    ])
    .map(|_| ())
}

fn check_array_replace(args: &[TypedExpr]) -> Result<(), PlannerError> {
    common_nested_type([
        array_element(&args[0].resolved_type)?,
        args[1].resolved_type.clone(),
        args[2].resolved_type.clone(),
    ])
    .map(|_| ())
}

fn check_array_slice(args: &[TypedExpr]) -> Result<(), PlannerError> {
    check_array_first(args)?;
    for arg in &args[1..] {
        if !matches!(
            arg.resolved_type,
            ResolvedType::Integer | ResolvedType::BigInt | ResolvedType::Null
        ) {
            return Err(PlannerError::invalid_expression(
                "ARRAY slice bounds must be INTEGER",
            ));
        }
    }
    Ok(())
}

fn check_array_pair(args: &[TypedExpr]) -> Result<(), PlannerError> {
    let left = array_element(&args[0].resolved_type)?;
    let right = array_element(&args[1].resolved_type)?;
    common_nested_type([left, right]).map(|_| ())
}

fn return_array_first(args: &[ResolvedType]) -> Result<ResolvedType, PlannerError> {
    Ok(ResolvedType::Array(Box::new(array_element(&args[0])?)))
}

fn return_array_append(args: &[ResolvedType]) -> Result<ResolvedType, PlannerError> {
    Ok(ResolvedType::Array(Box::new(common_nested_type([
        array_element(&args[0])?,
        args[1].clone(),
    ])?)))
}

fn return_array_prepend(args: &[ResolvedType]) -> Result<ResolvedType, PlannerError> {
    Ok(ResolvedType::Array(Box::new(common_nested_type([
        args[0].clone(),
        array_element(&args[1])?,
    ])?)))
}

fn return_array_cat(args: &[ResolvedType]) -> Result<ResolvedType, PlannerError> {
    Ok(ResolvedType::Array(Box::new(common_nested_type([
        array_element(&args[0])?,
        array_element(&args[1])?,
    ])?)))
}

fn check_map(args: &[TypedExpr]) -> Result<(), PlannerError> {
    array_element(&args[0].resolved_type)?;
    array_element(&args[1].resolved_type)?;
    Ok(())
}

fn check_array_text_tail(args: &[TypedExpr]) -> Result<(), PlannerError> {
    check_array_first(args)?;
    check_text(&args[1..])
}

fn return_map(args: &[ResolvedType]) -> Result<ResolvedType, PlannerError> {
    Ok(ResolvedType::Map {
        key: Box::new(array_element(&args[0])?),
        value: Box::new(array_element(&args[1])?),
    })
}

fn check_struct_pack(args: &[TypedExpr]) -> Result<(), PlannerError> {
    if !args.len().is_multiple_of(2) {
        return Err(PlannerError::invalid_expression(
            "STRUCT_PACK expects name/value pairs",
        ));
    }
    for name in args.iter().step_by(2) {
        if !matches!(name.kind, TypedExprKind::Literal(Literal::String(_))) {
            return Err(PlannerError::invalid_expression(
                "STRUCT_PACK field names must be string literals",
            ));
        }
    }
    Ok(())
}

fn return_struct(args: &[TypedExpr]) -> Result<ResolvedType, PlannerError> {
    Ok(ResolvedType::Struct(
        args.as_chunks::<2>()
            .0
            .iter()
            .map(|pair| {
                let TypedExprKind::Literal(Literal::String(name)) = &pair[0].kind else {
                    unreachable!("checked by check_struct_pack")
                };
                (name.clone(), pair[1].resolved_type.clone())
            })
            .collect(),
    ))
}

fn check_subscript(args: &[TypedExpr]) -> Result<(), PlannerError> {
    match &args[0].resolved_type {
        ResolvedType::Array(_) => {
            if matches!(
                args[1].resolved_type,
                ResolvedType::Integer | ResolvedType::BigInt | ResolvedType::Null
            ) {
                Ok(())
            } else {
                Err(PlannerError::invalid_expression(
                    "ARRAY subscript must be INTEGER",
                ))
            }
        }
        ResolvedType::Map { key, .. } if args[1].resolved_type.can_cast_to(key) => Ok(()),
        ResolvedType::Struct(_) if matches!(args[1].resolved_type, ResolvedType::Text) => Ok(()),
        ResolvedType::Null => Ok(()),
        other => Err(PlannerError::invalid_expression(format!(
            "cannot subscript {other}"
        ))),
    }
}

fn return_subscript(args: &[TypedExpr]) -> Result<ResolvedType, PlannerError> {
    match &args[0].resolved_type {
        ResolvedType::Array(element) => Ok((**element).clone()),
        ResolvedType::Map { value, .. } => Ok((**value).clone()),
        ResolvedType::Struct(fields) => {
            let TypedExprKind::Literal(Literal::String(name)) = &args[1].kind else {
                return Ok(ResolvedType::Null);
            };
            fields
                .iter()
                .find(|(field, _)| field == name)
                .map(|(_, data_type)| data_type.clone())
                .ok_or_else(|| {
                    PlannerError::invalid_expression(format!("unknown struct field '{name}'"))
                })
        }
        ResolvedType::Null => Ok(ResolvedType::Null),
        _ => unreachable!("checked by check_subscript"),
    }
}

fn check_json_selector(args: &[TypedExpr]) -> Result<(), PlannerError> {
    check_json_input(&args[..1])?;
    if matches!(
        args[1].resolved_type,
        ResolvedType::Text | ResolvedType::Integer | ResolvedType::BigInt | ResolvedType::Null
    ) {
        Ok(())
    } else {
        Err(PlannerError::type_mismatch(
            "Text or Integer",
            args[1].resolved_type.type_name(),
            args[1].span,
        ))
    }
}

fn check_json_path(args: &[TypedExpr]) -> Result<(), PlannerError> {
    check_json_input(&args[..1])?;
    check_text(&args[1..])
}

fn check_jsonb_update(args: &[TypedExpr]) -> Result<(), PlannerError> {
    if args.len() < 3 || args.len().is_multiple_of(2) {
        return Err(PlannerError::invalid_expression(
            "JSON update expects JSON followed by path/value pairs".to_string(),
        ));
    }
    check_json_input(&args[..1])?;
    for path in args[1..].iter().step_by(2) {
        check_text(std::slice::from_ref(path))?;
    }
    Ok(())
}

fn check_json_update(args: &[TypedExpr]) -> Result<(), PlannerError> {
    if args.len() < 3 || args.len().is_multiple_of(2) {
        return Err(PlannerError::invalid_expression(
            "JSON update expects JSON followed by path/value pairs".to_string(),
        ));
    }
    check_text(&args[..1])?;
    for path in args[1..].iter().step_by(2) {
        check_text(std::slice::from_ref(path))?;
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

fn check_timestamp(args: &[TypedExpr]) -> Result<(), PlannerError> {
    for arg in args {
        if !matches!(
            arg.resolved_type,
            ResolvedType::Timestamp | ResolvedType::Null
        ) {
            return Err(PlannerError::type_mismatch(
                "Timestamp",
                arg.resolved_type.type_name(),
                arg.span,
            ));
        }
    }
    Ok(())
}

fn check_text_timestamp(args: &[TypedExpr]) -> Result<(), PlannerError> {
    check_text(&args[..1])?;
    check_timestamp(&args[1..])
}

fn check_timestamp_text(args: &[TypedExpr]) -> Result<(), PlannerError> {
    check_timestamp(&args[..1])?;
    check_text(&args[1..])
}

fn check_to_timestamp(args: &[TypedExpr]) -> Result<(), PlannerError> {
    if args.len() == 1 {
        let arg = &args[0];
        if is_numeric(&arg.resolved_type)
            || matches!(arg.resolved_type, ResolvedType::Text | ResolvedType::Null)
        {
            return Ok(());
        }
        return Err(PlannerError::type_mismatch(
            "Numeric or Text",
            arg.resolved_type.type_name(),
            arg.span,
        ));
    }
    check_text(args)
}

fn check_temporal_input(args: &[TypedExpr]) -> Result<(), PlannerError> {
    for arg in args {
        if !matches!(
            arg.resolved_type,
            ResolvedType::Text
                | ResolvedType::Date
                | ResolvedType::Time
                | ResolvedType::Timestamp
                | ResolvedType::Null
        ) {
            return Err(PlannerError::type_mismatch(
                "Temporal or Text",
                arg.resolved_type.type_name(),
                arg.span,
            ));
        }
    }
    Ok(())
}

fn check_datetime(args: &[TypedExpr]) -> Result<(), PlannerError> {
    check_temporal_input(&args[..1])?;
    check_text(&args[1..])
}

fn check_temporal_interval(args: &[TypedExpr]) -> Result<(), PlannerError> {
    let temporal = &args[0];
    if !matches!(
        temporal.resolved_type,
        ResolvedType::Date | ResolvedType::Time | ResolvedType::Timestamp | ResolvedType::Null
    ) {
        return Err(PlannerError::type_mismatch(
            "Date, Time, or Timestamp",
            temporal.resolved_type.type_name(),
            temporal.span,
        ));
    }
    let interval = &args[1];
    if matches!(
        interval.resolved_type,
        ResolvedType::Interval | ResolvedType::Null
    ) {
        Ok(())
    } else {
        Err(PlannerError::type_mismatch(
            "Interval",
            interval.resolved_type.type_name(),
            interval.span,
        ))
    }
}

fn check_age(args: &[TypedExpr]) -> Result<(), PlannerError> {
    for arg in args {
        if !matches!(
            arg.resolved_type,
            ResolvedType::Date | ResolvedType::Timestamp | ResolvedType::Null
        ) {
            return Err(PlannerError::type_mismatch(
                "Date or Timestamp",
                arg.resolved_type.type_name(),
                arg.span,
            ));
        }
    }
    if args.len() == 2
        && args[0].resolved_type != ResolvedType::Null
        && args[1].resolved_type != ResolvedType::Null
        && args[0].resolved_type != args[1].resolved_type
    {
        return Err(PlannerError::type_mismatch(
            args[0].resolved_type.type_name(),
            args[1].resolved_type.type_name(),
            args[1].span,
        ));
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

fn check_integer(args: &[TypedExpr]) -> Result<(), PlannerError> {
    for arg in args {
        if !matches!(
            arg.resolved_type,
            ResolvedType::Integer | ResolvedType::BigInt | ResolvedType::Null
        ) {
            return Err(PlannerError::type_mismatch(
                "Integer",
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
        "to_tsvector",
        Arity::Range(1, 2),
        check_text,
        ReturnRule::Fixed(ResolvedType::Text),
    ),
    sig(
        "to_tsquery",
        Arity::Range(1, 2),
        check_text,
        ReturnRule::Fixed(ResolvedType::Text),
    ),
    sig(
        "plainto_tsquery",
        Arity::Range(1, 2),
        check_text,
        ReturnRule::Fixed(ResolvedType::Text),
    ),
    sig(
        "websearch_to_tsquery",
        Arity::Range(1, 2),
        check_text,
        ReturnRule::Fixed(ResolvedType::Text),
    ),
    sig(
        "ts_rank",
        Arity::Exact(2),
        check_text,
        ReturnRule::Fixed(ResolvedType::Double),
    ),
    sig(
        "ts_headline",
        Arity::Range(2, 3),
        check_text,
        ReturnRule::Fixed(ResolvedType::Text),
    ),
    sig(
        "array_value",
        Arity::Variadic(0),
        check_no_args,
        ReturnRule::FromArgs(return_array_value),
    ),
    sig(
        "list_value",
        Arity::Variadic(0),
        check_no_args,
        ReturnRule::FromArgs(return_array_value),
    ),
    sig(
        "array_append",
        Arity::Exact(2),
        check_array_append,
        ReturnRule::FromArgs(return_array_append),
    ),
    sig(
        "array_prepend",
        Arity::Exact(2),
        check_array_prepend,
        ReturnRule::FromArgs(return_array_prepend),
    ),
    sig(
        "array_cat",
        Arity::Exact(2),
        check_array_pair,
        ReturnRule::FromArgs(return_array_cat),
    ),
    sig(
        "array_remove",
        Arity::Exact(2),
        check_array_append,
        ReturnRule::FromArgs(return_array_first),
    ),
    sig(
        "array_replace",
        Arity::Exact(3),
        check_array_replace,
        ReturnRule::FromArgs(return_array_first),
    ),
    sig(
        "array_length",
        Arity::Exact(1),
        check_array_first,
        ReturnRule::Fixed(ResolvedType::Integer),
    ),
    sig(
        "array_position",
        Arity::Exact(2),
        check_array_append,
        ReturnRule::Fixed(ResolvedType::Integer),
    ),
    sig(
        "array_positions",
        Arity::Exact(2),
        check_array_append,
        ReturnRule::FromArgs(return_integer_array),
    ),
    sig(
        "string_to_array",
        Arity::Range(2, 3),
        check_text,
        ReturnRule::FromArgs(return_text_array),
    ),
    sig(
        "array_to_string",
        Arity::Range(2, 3),
        check_array_text_tail,
        ReturnRule::Fixed(ResolvedType::Text),
    ),
    sig(
        "map",
        Arity::Exact(2),
        check_map,
        ReturnRule::FromArgs(return_map),
    ),
    sig(
        "struct_pack",
        Arity::Variadic(0),
        check_struct_pack,
        ReturnRule::FromTypedArgs(return_struct),
    ),
    sig(
        "array_subscript",
        Arity::Exact(2),
        check_subscript,
        ReturnRule::FromTypedArgs(return_subscript),
    ),
    sig(
        "array_slice",
        Arity::Exact(3),
        check_array_slice,
        ReturnRule::FromArgs(return_array_first),
    ),
    sig(
        "jsonb_extract",
        Arity::Exact(2),
        check_json_selector,
        ReturnRule::Fixed(ResolvedType::Json),
    ),
    sig(
        "jsonb_extract_text",
        Arity::Exact(2),
        check_json_selector,
        ReturnRule::Fixed(ResolvedType::Text),
    ),
    sig(
        "jsonb_extract_path",
        Arity::Exact(2),
        check_json_path,
        ReturnRule::Fixed(ResolvedType::Json),
    ),
    sig(
        "jsonb_extract_path_text",
        Arity::Exact(2),
        check_json_path,
        ReturnRule::Fixed(ResolvedType::Text),
    ),
    sig(
        "jsonb_set",
        Arity::Variadic(3),
        check_jsonb_update,
        ReturnRule::Fixed(ResolvedType::Json),
    ),
    sig(
        "jsonb_insert",
        Arity::Variadic(3),
        check_jsonb_update,
        ReturnRule::Fixed(ResolvedType::Json),
    ),
    sig(
        "jsonb_build_object",
        Arity::Variadic(0),
        check_json_object,
        ReturnRule::Fixed(ResolvedType::Json),
    ),
    sig(
        "jsonb_build_array",
        Arity::Variadic(0),
        check_no_args,
        ReturnRule::Fixed(ResolvedType::Json),
    ),
    sig(
        "json",
        Arity::Exact(1),
        check_text,
        ReturnRule::Fixed(ResolvedType::Text),
    ),
    sig(
        "json_valid",
        Arity::Exact(1),
        check_text,
        ReturnRule::Fixed(ResolvedType::Boolean),
    ),
    sig(
        "json_type",
        Arity::Range(1, 2),
        check_text,
        ReturnRule::Fixed(ResolvedType::Text),
    ),
    sig(
        "json_extract",
        Arity::Variadic(2),
        check_text,
        ReturnRule::Fixed(ResolvedType::Text),
    ),
    sig(
        "json_object",
        Arity::Variadic(0),
        check_json_object,
        ReturnRule::Fixed(ResolvedType::Text),
    ),
    sig(
        "json_array",
        Arity::Variadic(0),
        check_no_args,
        ReturnRule::Fixed(ResolvedType::Text),
    ),
    sig(
        "json_insert",
        Arity::Variadic(3),
        check_json_update,
        ReturnRule::Fixed(ResolvedType::Text),
    ),
    sig(
        "json_replace",
        Arity::Variadic(3),
        check_json_update,
        ReturnRule::Fixed(ResolvedType::Text),
    ),
    sig(
        "json_set",
        Arity::Variadic(3),
        check_json_update,
        ReturnRule::Fixed(ResolvedType::Text),
    ),
    sig(
        "json_remove",
        Arity::Variadic(2),
        check_text,
        ReturnRule::Fixed(ResolvedType::Text),
    ),
    sig(
        "json_array_length",
        Arity::Range(1, 2),
        check_text,
        ReturnRule::Fixed(ResolvedType::Integer),
    ),
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
    sig(
        "cbrt",
        Arity::Exact(1),
        check_numeric,
        ReturnRule::Fixed(ResolvedType::Double),
    ),
    sig(
        "cot",
        Arity::Exact(1),
        check_numeric,
        ReturnRule::Fixed(ResolvedType::Double),
    ),
    sig(
        "log2",
        Arity::Exact(1),
        check_numeric,
        ReturnRule::Fixed(ResolvedType::Double),
    ),
    sig(
        "acosh",
        Arity::Exact(1),
        check_numeric,
        ReturnRule::Fixed(ResolvedType::Double),
    ),
    sig(
        "asinh",
        Arity::Exact(1),
        check_numeric,
        ReturnRule::Fixed(ResolvedType::Double),
    ),
    sig(
        "atanh",
        Arity::Exact(1),
        check_numeric,
        ReturnRule::Fixed(ResolvedType::Double),
    ),
    sig(
        "cosh",
        Arity::Exact(1),
        check_numeric,
        ReturnRule::Fixed(ResolvedType::Double),
    ),
    sig(
        "sinh",
        Arity::Exact(1),
        check_numeric,
        ReturnRule::Fixed(ResolvedType::Double),
    ),
    sig(
        "tanh",
        Arity::Exact(1),
        check_numeric,
        ReturnRule::Fixed(ResolvedType::Double),
    ),
    sig(
        "isnan",
        Arity::Exact(1),
        check_numeric,
        ReturnRule::Fixed(ResolvedType::Boolean),
    ),
    sig_meta(
        "random",
        Arity::Exact(0),
        check_numeric,
        ReturnRule::Fixed(ResolvedType::Double),
        RANDOM_META,
    ),
    sig_meta(
        "now",
        Arity::Range(0, 1),
        check_numeric,
        ReturnRule::Fixed(ResolvedType::Timestamp),
        STATEMENT_STABLE_META,
    ),
    sig_meta(
        "current_timestamp",
        Arity::Range(0, 1),
        check_numeric,
        ReturnRule::Fixed(ResolvedType::Timestamp),
        STATEMENT_STABLE_META,
    ),
    sig_meta(
        "current_date",
        Arity::Exact(0),
        check_numeric,
        ReturnRule::Fixed(ResolvedType::Date),
        STATEMENT_STABLE_META,
    ),
    sig_meta(
        "current_time",
        Arity::Exact(0),
        check_numeric,
        ReturnRule::Fixed(ResolvedType::Time),
        STATEMENT_STABLE_META,
    ),
    sig(
        "make_date",
        Arity::Exact(3),
        check_numeric,
        ReturnRule::Fixed(ResolvedType::Date),
    ),
    sig(
        "make_time",
        Arity::Exact(3),
        check_numeric,
        ReturnRule::Fixed(ResolvedType::Time),
    ),
    sig(
        "make_timestamp",
        Arity::Exact(6),
        check_numeric,
        ReturnRule::Fixed(ResolvedType::Timestamp),
    ),
    sig(
        "make_interval",
        Arity::Range(0, 7),
        check_numeric,
        ReturnRule::Fixed(ResolvedType::Interval),
    ),
    sig(
        "date",
        Arity::Exact(1),
        check_temporal_input,
        ReturnRule::Fixed(ResolvedType::Date),
    ),
    sig(
        "time",
        Arity::Exact(1),
        check_temporal_input,
        ReturnRule::Fixed(ResolvedType::Time),
    ),
    sig(
        "datetime",
        Arity::Variadic(1),
        check_datetime,
        ReturnRule::Fixed(ResolvedType::Timestamp),
    ),
    sig(
        "to_date",
        Arity::Exact(2),
        check_text,
        ReturnRule::Fixed(ResolvedType::Date),
    ),
    sig(
        "age",
        Arity::Range(1, 2),
        check_age,
        ReturnRule::Fixed(ResolvedType::Interval),
    ),
    sig(
        "date_add",
        Arity::Exact(2),
        check_temporal_interval,
        ReturnRule::FromArgs(return_arg0),
    ),
    sig(
        "date_sub",
        Arity::Exact(2),
        check_temporal_interval,
        ReturnRule::FromArgs(return_arg0),
    ),
    sig(
        "extract",
        Arity::Exact(2),
        check_text_timestamp,
        ReturnRule::Fixed(ResolvedType::Double),
    ),
    sig(
        "date_part",
        Arity::Exact(2),
        check_text_timestamp,
        ReturnRule::Fixed(ResolvedType::Double),
    ),
    sig(
        "date_trunc",
        Arity::Exact(2),
        check_text_timestamp,
        ReturnRule::Fixed(ResolvedType::Timestamp),
    ),
    sig(
        "to_char",
        Arity::Exact(2),
        check_timestamp_text,
        ReturnRule::Fixed(ResolvedType::Text),
    ),
    sig(
        "to_timestamp",
        Arity::Range(1, 2),
        check_to_timestamp,
        ReturnRule::Fixed(ResolvedType::Timestamp),
    ),
    sig(
        "strftime",
        Arity::Exact(2),
        check_text_timestamp,
        ReturnRule::Fixed(ResolvedType::Text),
    ),
    sig(
        "julianday",
        Arity::Exact(1),
        check_timestamp,
        ReturnRule::Fixed(ResolvedType::Double),
    ),
    sig(
        "unixepoch",
        Arity::Exact(1),
        check_timestamp,
        ReturnRule::Fixed(ResolvedType::BigInt),
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
        "ascii",
        Arity::Exact(1),
        check_text,
        ReturnRule::Fixed(ResolvedType::Integer),
    ),
    sig(
        "chr",
        Arity::Exact(1),
        check_integer,
        ReturnRule::Fixed(ResolvedType::Text),
    ),
    sig(
        "bit_length",
        Arity::Exact(1),
        check_text_or_blob,
        ReturnRule::Fixed(ResolvedType::Integer),
    ),
    sig(
        "starts_with",
        Arity::Exact(2),
        check_text,
        ReturnRule::Fixed(ResolvedType::Boolean),
    ),
    sig(
        "ends_with",
        Arity::Exact(2),
        check_text,
        ReturnRule::Fixed(ResolvedType::Boolean),
    ),
    sig(
        "translate",
        Arity::Exact(3),
        check_text,
        ReturnRule::Fixed(ResolvedType::Text),
    ),
    sig(
        "levenshtein",
        Arity::Exact(2),
        check_text,
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
        "regexp_like",
        Arity::Range(2, 3),
        check_text,
        ReturnRule::Fixed(ResolvedType::Boolean),
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
