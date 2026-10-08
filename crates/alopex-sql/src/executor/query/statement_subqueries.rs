//! Lazy results and identities owned by exactly one DML statement.

use std::cell::RefCell;
use std::collections::HashMap;
use std::ops::{ControlFlow, Range};
use std::rc::Rc;

use alopex_core::kv::KVStore;

use crate::catalog::Catalog;
use crate::executor::dml::statement_spool::StatementSpool;
use crate::executor::{ExecutorError, Result, Row};
use crate::planner::logical_plan::LogicalPlan;
use crate::planner::typed_expr::TypedExpr;
use crate::storage::{SqlTxn, SqlValue};

use super::{QueryExecutionContext, subquery, subquery_plan};

struct Entry {
    independent: bool,
    records: Option<Range<u64>>,
}

pub(super) struct StatementSubqueries {
    entries: Vec<Entry>,
    // One memory policy covers all retained results, not one allowance per query.
    spool: Option<StatementSpool<Vec<SqlValue>>>,
}

pub(crate) struct DmlSubqueries<'statement> {
    // Keep original boxed plans borrowed until evaluation and cleanup finish.
    _expressions: Vec<&'statement TypedExpr>,
    context: QueryExecutionContext,
}

impl<'statement> DmlSubqueries<'statement> {
    pub(crate) fn new<'txn, S: KVStore + 'txn, C: Catalog + ?Sized, T: SqlTxn<'txn, S>>(
        txn: &T,
        catalog: &C,
        expressions: impl IntoIterator<Item = &'statement TypedExpr>,
    ) -> Self {
        let expressions: Vec<_> = expressions.into_iter().collect();
        let mut context =
            QueryExecutionContext::default().with_copy_security(txn.read_security().cloned());
        let mut entries = Vec::new();
        for expression in &expressions {
            subquery_plan::visit_expr(expression, &mut |plan| {
                let identity = plan as *const LogicalPlan as usize;
                context.subquery_ids.entry(identity).or_insert_with(|| {
                    let id = entries.len();
                    entries.push(Entry {
                        independent: subquery_plan::independent(plan, catalog),
                        records: None,
                    });
                    id
                });
            });
        }
        if !entries.is_empty() {
            context.statement_subqueries = Some(Rc::new(RefCell::new(StatementSubqueries {
                entries,
                spool: Some(StatementSpool::new(txn.memory_policy())),
            })));
        }
        Self {
            _expressions: expressions,
            context,
        }
    }

    pub(crate) fn evaluate<'txn, S: KVStore + 'txn, C: Catalog + ?Sized, T: SqlTxn<'txn, S>>(
        &self,
        txn: &mut T,
        catalog: &C,
        expr: &TypedExpr,
        row: &Row,
    ) -> Result<SqlValue> {
        subquery::evaluate_expr_with_subqueries_with_context(txn, catalog, expr, row, &self.context)
    }

    pub(crate) fn finish(self) -> Result<()> {
        if let Some(cache) = self.context.statement_subqueries {
            let spool = cache.borrow_mut().spool.take().ok_or_else(closed_cache)?;
            spool.finish()?;
        }
        Ok(())
    }
}

impl QueryExecutionContext {
    /// Bind only this execution clone. Parent addresses do not survive into
    /// the child scope, and the shared result cache contains integer IDs only.
    pub(super) fn for_plan_clone(&self, original: &LogicalPlan, cloned: &LogicalPlan) -> Self {
        let mut next = self.clone();
        next.subquery_ids = HashMap::new();
        if self.statement_subqueries.is_none() {
            return next;
        }
        let mut ids = Vec::new();
        subquery_plan::visit_plan(original, &mut |plan| {
            ids.push(
                self.subquery_ids
                    .get(&(plan as *const LogicalPlan as usize))
                    .copied(),
            );
        });
        let mut addresses = Vec::new();
        subquery_plan::visit_plan(cloned, &mut |plan| {
            addresses.push(plan as *const LogicalPlan as usize);
        });
        // This method is used only immediately after Clone. A shape mismatch
        // is not a reason to guess at a span or reuse a different plan's result.
        if ids.len() == addresses.len() {
            for (address, id) in addresses.into_iter().zip(ids) {
                if let Some(id) = id {
                    next.subquery_ids.insert(address, id);
                }
            }
        }
        next
    }

    pub(super) fn independent_subquery(&self, plan: &LogicalPlan) -> bool {
        let Some(id) = self
            .subquery_ids
            .get(&(plan as *const LogicalPlan as usize))
        else {
            return false;
        };
        self.statement_subqueries
            .as_ref()
            .is_some_and(|cache| cache.borrow().entries[*id].independent)
    }

    pub(super) fn cached_subquery(&self, plan: &LogicalPlan) -> Option<SubqueryRows> {
        let id = self
            .subquery_ids
            .get(&(plan as *const LogicalPlan as usize))?;
        let cache = self.statement_subqueries.as_ref()?;
        let records = cache.borrow().entries[*id].records.clone()?;
        Some(SubqueryRows::Cached {
            cache: cache.clone(),
            records,
        })
    }

    pub(super) fn retain_subquery(
        &self,
        plan: &LogicalPlan,
        rows: Vec<Vec<SqlValue>>,
    ) -> Result<SubqueryRows> {
        if !self.independent_subquery(plan) {
            return Ok(SubqueryRows::Owned(rows));
        }
        let id = self.subquery_ids[&(plan as *const LogicalPlan as usize)];
        let owner = self
            .statement_subqueries
            .as_ref()
            .expect("registered cache");
        let records = {
            let mut cache = owner.borrow_mut();
            let spool = cache.spool.as_mut().ok_or_else(closed_cache)?;
            let start = spool.len();
            for row in rows {
                spool.push(&row)?;
            }
            let records = start..spool.len();
            cache.entries[id].records = Some(records.clone());
            records
        };
        Ok(SubqueryRows::Cached {
            cache: owner.clone(),
            records,
        })
    }
}

pub(super) enum SubqueryRows {
    Owned(Vec<Vec<SqlValue>>),
    Cached {
        cache: Rc<RefCell<StatementSubqueries>>,
        records: Range<u64>,
    },
}

impl SubqueryRows {
    pub(super) fn len(&self) -> usize {
        match self {
            Self::Owned(rows) => rows.len(),
            Self::Cached { records, .. } => (records.end - records.start) as usize,
        }
    }

    pub(super) fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub(super) fn first(&self) -> Result<Option<Vec<SqlValue>>> {
        let mut first = None;
        self.probe(|row| {
            first = Some(row.to_vec());
            Ok(true)
        })?;
        Ok(first)
    }

    pub(super) fn probe(
        &self,
        mut predicate: impl FnMut(&[SqlValue]) -> Result<bool>,
    ) -> Result<bool> {
        let mut found = false;
        let mut visit = |row: &[SqlValue]| -> Result<ControlFlow<()>> {
            if predicate(row)? {
                found = true;
                Ok(ControlFlow::Break(()))
            } else {
                Ok(ControlFlow::Continue(()))
            }
        };
        match self {
            Self::Owned(rows) => {
                for row in rows {
                    if visit(row)?.is_break() {
                        break;
                    }
                }
            }
            Self::Cached { cache, records } => {
                cache
                    .borrow_mut()
                    .spool
                    .as_mut()
                    .ok_or_else(closed_cache)?
                    .visit_range(records.clone(), |row| visit(&row))?;
            }
        }
        Ok(found)
    }
}

fn closed_cache() -> ExecutorError {
    ExecutorError::InvalidOperation {
        operation: "DML subquery cache".into(),
        reason: "statement cache is already closed".into(),
    }
}
