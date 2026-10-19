//! Planned limits and observed outcomes for adaptive B-tree access.
use super::{MAX_BTREE_INDEX_BYTES, MAX_BTREE_INDEX_CANDIDATES, Row};
use crate::executor::MemoryPolicy;
use crate::planner::typed_expr::TypedExpr;
use std::{cell::RefCell, rc::Rc};

#[derive(Clone, Copy)]
pub(super) struct BtreeLimits {
    pub max_candidates: usize,
    pub max_bytes: u64,
}

impl BtreeLimits {
    pub fn new(memory: Option<&MemoryPolicy>) -> Self {
        let max_bytes = memory
            .and_then(MemoryPolicy::limit_bytes)
            .unwrap_or(MAX_BTREE_INDEX_BYTES)
            .min(MAX_BTREE_INDEX_BYTES);
        Self {
            max_candidates: MAX_BTREE_INDEX_CANDIDATES.min(
                usize::try_from(max_bytes / std::mem::size_of::<u64>() as u64)
                    .unwrap_or(usize::MAX),
            ),
            max_bytes,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum BtreeFallback {
    CandidateCap,
    ByteBudget,
}

pub(super) enum IndexScanOutcome {
    Rows {
        rows: Vec<Row>,
        candidates: usize,
    },
    Fallback {
        candidates: usize,
        reason: BtreeFallback,
    },
}

// An EXPLAIN ANALYZE statement may dispatch through DML before it reaches
// a query. Keep its collector scoped to the synchronous executor call (or the
// async bridge's blocking worker), including nested query invocations. No guard
// crosses an await; Drop restores the previous collector on errors and unwinds.
type TraceStore = Rc<RefCell<Vec<BtreeAccessStats>>>;

thread_local! {
    static ACTIVE_TRACE: RefCell<Option<TraceStore>> = const { RefCell::new(None) };
}

pub(crate) struct BtreeTrace {
    stats: TraceStore,
    previous: Option<TraceStore>,
}

impl BtreeTrace {
    pub(crate) fn start() -> Self {
        let stats = Rc::new(RefCell::new(Vec::new()));
        let previous = ACTIVE_TRACE.with(|active| active.replace(Some(stats.clone())));
        Self { stats, previous }
    }
    pub(crate) fn finish(self) -> Vec<BtreeAccessStats> {
        self.stats.take()
    }
}

impl Drop for BtreeTrace {
    fn drop(&mut self) {
        ACTIVE_TRACE.with(|active| active.replace(self.previous.take()));
    }
}

pub(super) fn record(table: &str, index: &str, predicate: &TypedExpr, outcome: &IndexScanOutcome) {
    ACTIVE_TRACE.with(|active| {
        if let Some(stats) = active.borrow().as_ref() {
            record_into(&mut stats.borrow_mut(), table, index, predicate, outcome);
        }
    });
}

// Include the predicate so self-joins using one index with different bounds
// do not borrow each other's outcome. Identical probes aggregate their loops.
fn probe_key(table: &str, index: &str, predicate: &TypedExpr) -> String {
    format!("{table}/{index}/{predicate:?}")
}

#[derive(Clone)]
pub(crate) struct BtreeChoice {
    pub index: String,
    key: String,
    limits: BtreeLimits,
}

pub(crate) struct BtreeAccessStats {
    key: String,
    candidates: u64,
    loops: u64,
    fallbacks: u64,
    last_fallback: Option<BtreeFallback>,
}

fn record_into(
    stats: &mut Vec<BtreeAccessStats>,
    table: &str,
    index: &str,
    predicate: &TypedExpr,
    outcome: &IndexScanOutcome,
) {
    let key = probe_key(table, index, predicate);
    let position = stats
        .iter()
        .position(|stats| stats.key == key)
        .unwrap_or_else(|| {
            stats.push(BtreeAccessStats {
                key,
                candidates: 0,
                loops: 0,
                fallbacks: 0,
                last_fallback: None,
            });
            stats.len() - 1
        });
    let entry = &mut stats[position];
    let (candidates, reason) = match outcome {
        IndexScanOutcome::Rows { candidates, .. } => (*candidates, None),
        IndexScanOutcome::Fallback { candidates, reason } => (*candidates, Some(*reason)),
    };
    entry.candidates = entry.candidates.saturating_add(candidates as u64);
    entry.loops = entry.loops.saturating_add(1);
    if reason.is_some() {
        entry.fallbacks = entry.fallbacks.saturating_add(1);
    }
    entry.last_fallback = reason;
}

impl BtreeChoice {
    pub(super) fn new(
        table: &str,
        index: &str,
        predicate: &TypedExpr,
        memory: Option<&MemoryPolicy>,
    ) -> Self {
        Self {
            index: index.to_owned(),
            key: probe_key(table, index, predicate),
            limits: BtreeLimits::new(memory),
        }
    }

    fn actual<'a>(&self, stats: &'a [BtreeAccessStats]) -> Option<&'a BtreeAccessStats> {
        stats.iter().find(|stats| stats.key == self.key)
    }

    pub(crate) fn annotate_json(
        &self,
        node: &mut serde_json::Value,
        stats: Option<&[BtreeAccessStats]>,
    ) {
        node["node"] = serde_json::json!("IndexScan");
        node["index"] = serde_json::json!(self.index);
        node["adaptive"] = serde_json::json!({"fallback":"Scan",
            "max_candidates":self.limits.max_candidates,"max_bytes":self.limits.max_bytes});
        if let Some(stats) = stats {
            let actual = self.actual(stats);
            let path = actual.map_or("NotExecuted", BtreeAccessStats::path);
            node["actual"] = serde_json::json!({"path":path,
                "candidates_examined":actual.map_or(0, |s| s.candidates),
                "loops":actual.map_or(0, |s| s.loops),
                "fallbacks":actual.map_or(0, |s| s.fallbacks),
                "fallback":actual.map_or("none", BtreeAccessStats::reason)});
            if path == "Scan" {
                node["node"] = serde_json::json!("Scan");
                node["planned"] = serde_json::json!("IndexScan");
            }
        }
    }

    pub(crate) fn annotate_text(&self, line: &str, stats: Option<&[BtreeAccessStats]>) -> String {
        let mut text = line.trim_end_matches('\n').to_owned();
        if let Some(stats) = stats {
            let actual = self.actual(stats);
            let path = actual.map_or("NotExecuted", BtreeAccessStats::path);
            if path == "Scan" {
                text.push_str(&format!(" planned=IndexScan index={}", self.index));
            } else {
                text = text.replacen(
                    "Scan table=",
                    &format!("IndexScan index={} table=", self.index),
                    1,
                );
            }
            text.push_str(&format!(" actual_path={path} candidates={} max_candidates={} max_bytes={} loops={} fallback={} fallbacks={}",
                actual.map_or(0, |s| s.candidates), self.limits.max_candidates, self.limits.max_bytes,
                actual.map_or(0, |s| s.loops), actual.map_or("none", BtreeAccessStats::reason),
                actual.map_or(0, |s| s.fallbacks)));
        } else {
            text = text.replacen(
                "Scan table=",
                &format!("IndexScan index={} table=", self.index),
                1,
            );
            text.push_str(&format!(
                " adaptive=true max_candidates={} max_bytes={} fallback=Scan",
                self.limits.max_candidates, self.limits.max_bytes
            ));
        }
        if line.ends_with('\n') {
            text.push('\n');
        }
        text
    }
}

impl BtreeAccessStats {
    fn path(&self) -> &'static str {
        if self.fallbacks == 0 {
            "IndexScan"
        } else if self.fallbacks == self.loops {
            "Scan"
        } else {
            "Adaptive"
        }
    }
    fn reason(&self) -> &'static str {
        match self.last_fallback {
            Some(BtreeFallback::CandidateCap) => "CandidateCap",
            Some(BtreeFallback::ByteBudget) => "ByteBudget",
            None => "none",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::MemoryCatalog;
    use crate::executor::{
        ExecutionResult, Executor, ExplainAnalysis, SpillPolicy, explain_result,
    };
    use crate::storage::{SqlValue, TxnBridge};
    use crate::{AlopexDialect, ExplainFormat, Parser, Planner};
    use alopex_core::kv::memory::MemoryKV;
    use std::sync::{Arc, RwLock};

    fn probe(index: &str) {
        let predicate = TypedExpr::literal(
            crate::ast::expr::Literal::Number("1".into()),
            crate::planner::types::ResolvedType::Integer,
            crate::Span::default(),
        );
        record(
            "t",
            index,
            &predicate,
            &IndexScanOutcome::Rows {
                rows: Vec::new(),
                candidates: 1,
            },
        );
    }

    #[test]
    fn btree_adaptive_trace_is_scoped_nested_and_thread_isolated() {
        let outer = BtreeTrace::start();
        probe("outer");
        let inner = BtreeTrace::start();
        probe("inner");
        let inner_stats = inner.finish();
        assert_eq!(inner_stats.len(), 1);
        assert!(inner_stats[0].key.starts_with("t/inner/"));
        std::thread::spawn(|| {
            let other = BtreeTrace::start();
            probe("thread");
            assert_eq!(other.finish().len(), 1);
        })
        .join()
        .unwrap();
        probe("outer");
        let outer_stats = outer.finish();
        assert_eq!(outer_stats.len(), 1);
        assert_eq!(outer_stats[0].loops, 2);
        fn fail_after_probe() -> Result<(), ()> {
            let _failed = BtreeTrace::start();
            probe("failed");
            Err(())
        }
        let error = fail_after_probe();
        assert!(error.is_err());
        probe("outside");
        assert!(BtreeTrace::start().finish().is_empty());
    }

    #[test]
    fn btree_adaptive_reports_effective_memory_limit_and_byte_fallback() {
        let store = Arc::new(MemoryKV::new());
        let catalog = Arc::new(RwLock::new(MemoryCatalog::new()));
        let mut executor = Executor::new(store.clone(), catalog.clone());
        let plan = |sql: &str| {
            let statements = Parser::parse_sql(&AlopexDialect, sql).unwrap();
            Planner::new(&*catalog.read().unwrap())
                .plan(&statements[0])
                .unwrap()
        };
        executor
            .execute(plan("CREATE TABLE byte_test (id INT, v INT, wide TEXT)"))
            .unwrap();
        executor
            .execute(plan(&format!(
                "INSERT INTO byte_test VALUES (1, 7, '{}')",
                "x".repeat(1024)
            )))
            .unwrap();
        executor
            .execute(plan("CREATE INDEX byte_test_v ON byte_test(v)"))
            .unwrap();
        let query = plan("SELECT id FROM byte_test WHERE v = 7");
        let memory = MemoryPolicy::new(Some(256), SpillPolicy::FailFast);
        let bridge = TxnBridge::new(store);
        let mut txn = bridge.begin_read().unwrap();
        let catalog = catalog.read().unwrap();
        let choices = super::super::selected_btree_indexes(&query, &*catalog, Some(&memory));
        let trace = BtreeTrace::start();
        let result = super::super::execute_query_with_policy(
            &mut txn,
            &*catalog,
            query.clone(),
            Some(&memory),
        )
        .unwrap();
        let stats = trace.finish();
        let ExecutionResult::Query(rows) = result else {
            panic!()
        };
        assert_eq!(rows.rows, vec![vec![SqlValue::Integer(1)]]);
        let ExecutionResult::Query(explain) = explain_result(
            &query,
            None,
            &choices,
            ExplainFormat::Json,
            Some(ExplainAnalysis {
                knn_stats: None,
                btree_stats: Some(&stats),
                elapsed_ns: 0,
                rows: 1,
            }),
        ) else {
            panic!()
        };
        let SqlValue::Text(json) = &explain.rows[0][0] else {
            panic!()
        };
        let json: serde_json::Value = serde_json::from_str(json).unwrap();
        let access = &json["physical_plan"]["access_path"];
        assert_eq!(access["adaptive"]["max_bytes"], 256);
        assert_eq!(access["adaptive"]["max_candidates"], 32);
        assert_eq!(access["actual"]["path"], "Scan");
        assert_eq!(access["actual"]["fallback"], "ByteBudget");
        assert_eq!(access["actual"]["candidates_examined"], 1);
    }
}
