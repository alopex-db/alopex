//! Query-block scoped named-window resolution.

use super::PlannerError;
use crate::ast::dml::{NamedWindow, OrderByExpr, Select, SelectItem};
use crate::ast::expr::{Expr, ExprKind, WindowSpec};
use std::collections::{HashMap, HashSet};

/// Resolve named-window inheritance without crossing a SELECT query-block
/// boundary. The parser preserves references; this module owns scope,
/// duplicate/undefined/cycle diagnostics, and inheritance constraints.
pub(super) fn resolve_named_windows(stmt: &Select) -> Result<Select, PlannerError> {
    let mut resolver = NamedWindowResolver::new(&stmt.windows)?;
    resolver.validate_all()?;
    let mut resolved = stmt.clone();
    for item in &mut resolved.projection {
        if let SelectItem::Expr { expr, .. } = item {
            *expr = resolver.resolve_expr(expr)?;
        }
    }
    if let Some(selection) = &mut resolved.selection {
        *selection = resolver.resolve_expr(selection)?;
    }
    if let Some(group_by) = &mut resolved.group_by {
        for expr in group_by {
            *expr = resolver.resolve_expr(expr)?;
        }
    }
    if let Some(having) = &mut resolved.having {
        *having = resolver.resolve_expr(having)?;
    }
    if let Some(qualify) = &mut resolved.qualify {
        *qualify = resolver.resolve_expr(qualify)?;
    }
    for order in &mut resolved.order_by {
        order.expr = resolver.resolve_expr(&order.expr)?;
    }
    resolved.windows.clear();
    Ok(resolved)
}

struct NamedWindowResolver {
    definitions: HashMap<String, NamedWindow>,
    definition_order: Vec<String>,
    resolved: HashMap<String, WindowSpec>,
    resolving: HashSet<String>,
}

impl NamedWindowResolver {
    fn new(definitions: &[NamedWindow]) -> Result<Self, PlannerError> {
        let mut by_name = HashMap::new();
        let mut definition_order = Vec::with_capacity(definitions.len());
        for definition in definitions {
            let key = definition.name.to_ascii_lowercase();
            if by_name.insert(key.clone(), definition.clone()).is_some() {
                return Err(PlannerError::invalid_expression(format!(
                    "named window '{}' is defined more than once",
                    definition.name
                )));
            }
            definition_order.push(key);
        }
        Ok(Self {
            definitions: by_name,
            definition_order,
            resolved: HashMap::new(),
            resolving: HashSet::new(),
        })
    }

    fn validate_all(&mut self) -> Result<(), PlannerError> {
        let names = self.definition_order.clone();
        for name in names {
            self.resolve_definition(&name)?;
        }
        Ok(())
    }

    fn resolve_definition(&mut self, name: &str) -> Result<WindowSpec, PlannerError> {
        let key = name.to_ascii_lowercase();
        if let Some(spec) = self.resolved.get(&key) {
            return Ok(spec.clone());
        }
        let definition = self.definitions.get(&key).cloned().ok_or_else(|| {
            PlannerError::invalid_expression(format!("named window '{name}' is not defined"))
        })?;
        if !self.resolving.insert(key.clone()) {
            return Err(PlannerError::invalid_expression(format!(
                "named window inheritance cycle involving '{}'",
                definition.name
            )));
        }

        let result = self.resolve_spec(&definition.spec);
        self.resolving.remove(&key);
        let spec = result?;
        self.resolved.insert(key, spec.clone());
        Ok(spec)
    }

    fn resolve_spec(&mut self, spec: &WindowSpec) -> Result<WindowSpec, PlannerError> {
        let mut merged = if let Some(base_name) = &spec.base {
            let inherited = self.resolve_definition(base_name)?;
            if !spec.partition_by.is_empty() {
                return Err(PlannerError::invalid_expression(format!(
                    "named window '{base_name}' cannot be overridden with PARTITION BY"
                )));
            }
            if !spec.order_by.is_empty() && !inherited.order_by.is_empty() {
                return Err(PlannerError::invalid_expression(format!(
                    "named window '{base_name}' already defines ORDER BY"
                )));
            }
            if spec.frame.is_some() && inherited.frame.is_some() {
                return Err(PlannerError::invalid_expression(format!(
                    "named window '{base_name}' already defines a frame"
                )));
            }
            WindowSpec {
                base: None,
                partition_by: inherited.partition_by,
                order_by: if spec.order_by.is_empty() {
                    inherited.order_by
                } else {
                    spec.order_by.clone()
                },
                frame: spec.frame.clone().or(inherited.frame),
            }
        } else {
            let mut local = spec.clone();
            local.base = None;
            local
        };

        merged.partition_by = merged
            .partition_by
            .iter()
            .map(|expr| self.resolve_expr(expr))
            .collect::<Result<Vec<_>, _>>()?;
        merged.order_by = merged
            .order_by
            .iter()
            .map(|order| {
                Ok(OrderByExpr {
                    expr: self.resolve_expr(&order.expr)?,
                    asc: order.asc,
                    nulls_first: order.nulls_first,
                    span: order.span,
                })
            })
            .collect::<Result<Vec<_>, PlannerError>>()?;
        Ok(merged)
    }

    fn resolve_expr(&mut self, expr: &Expr) -> Result<Expr, PlannerError> {
        let mut resolved = expr.clone();
        match &mut resolved.kind {
            ExprKind::BinaryOp { left, right, .. } => {
                **left = self.resolve_expr(left)?;
                **right = self.resolve_expr(right)?;
            }
            ExprKind::UnaryOp { operand, .. }
            | ExprKind::Cast { expr: operand, .. }
            | ExprKind::IsNull { expr: operand, .. }
            | ExprKind::TruthPredicate { expr: operand, .. } => {
                **operand = self.resolve_expr(operand)?;
            }
            ExprKind::IsDistinctFrom { left, right, .. } => {
                **left = self.resolve_expr(left)?;
                **right = self.resolve_expr(right)?;
            }
            ExprKind::Row { items } => {
                for item in items {
                    *item = self.resolve_expr(item)?;
                }
            }
            ExprKind::Case {
                operand,
                branches,
                else_expr,
            } => {
                if let Some(operand) = operand {
                    **operand = self.resolve_expr(operand)?;
                }
                for branch in branches {
                    branch.when = self.resolve_expr(&branch.when)?;
                    branch.then = self.resolve_expr(&branch.then)?;
                }
                if let Some(else_expr) = else_expr {
                    **else_expr = self.resolve_expr(else_expr)?;
                }
            }
            ExprKind::FunctionCall { args, over, .. } => {
                for arg in args {
                    *arg = self.resolve_expr(arg)?;
                }
                if let Some(spec) = over {
                    *spec = self.resolve_spec(spec)?;
                }
            }
            ExprKind::Between {
                expr, low, high, ..
            } => {
                **expr = self.resolve_expr(expr)?;
                **low = self.resolve_expr(low)?;
                **high = self.resolve_expr(high)?;
            }
            ExprKind::Like {
                expr,
                pattern,
                escape,
                ..
            } => {
                **expr = self.resolve_expr(expr)?;
                **pattern = self.resolve_expr(pattern)?;
                if let Some(escape) = escape {
                    **escape = self.resolve_expr(escape)?;
                }
            }
            ExprKind::InList { expr, list, .. } => {
                **expr = self.resolve_expr(expr)?;
                for item in list {
                    *item = self.resolve_expr(item)?;
                }
            }
            ExprKind::InSubquery { expr, .. } | ExprKind::Quantified { expr, .. } => {
                **expr = self.resolve_expr(expr)?;
            }
            ExprKind::Literal { .. }
            | ExprKind::ColumnRef { .. }
            | ExprKind::VectorLiteral { .. }
            | ExprKind::ScalarSubquery { .. }
            | ExprKind::Exists { .. } => {}
        }
        Ok(resolved)
    }
}
