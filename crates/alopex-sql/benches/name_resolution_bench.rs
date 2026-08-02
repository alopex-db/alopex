//! Measures the planner's name-resolution cost as scope width and nesting grow.
//!
//! The resolver walks tables and columns linearly and clones outer scopes at
//! every boundary. These benchmarks exist to show which of those actually costs
//! anything at realistic schema sizes, so optimisation work is driven by numbers
//! rather than by reading the code.
//!
//! Parsing happens up front and sits outside the timed region: its own linear
//! cost is large enough to mask the resolver's growth curve, which is what made
//! an earlier reading of these numbers conclude the resolver was linear.

use std::hint::black_box;
use std::sync::{Arc, RwLock};

use alopex_core::kv::memory::MemoryKV;
use alopex_sql::ast::Statement;
use alopex_sql::catalog::MemoryCatalog;
use alopex_sql::dialect::AlopexDialect;
use alopex_sql::executor::Executor;
use alopex_sql::parser::Parser;
use alopex_sql::planner::Planner;
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};

/// A catalog with the given DDL already applied, ready to plan queries against.
struct Schema {
    catalog: Arc<RwLock<MemoryCatalog>>,
}

impl Schema {
    fn new(ddl: &str) -> Self {
        let store = Arc::new(MemoryKV::new());
        let catalog = Arc::new(RwLock::new(MemoryCatalog::new()));
        let mut executor = Executor::new(store, Arc::clone(&catalog));
        for stmt in Parser::parse_sql(&AlopexDialect, ddl).expect("parse ddl") {
            let plan = {
                let guard = catalog.read().unwrap();
                Planner::new(&*guard).plan(&stmt).expect("plan ddl")
            };
            executor.execute(plan).expect("execute ddl");
        }
        Self { catalog }
    }

    /// Parse up front so the timed region measures planning alone.
    fn parse(&self, sql: &str) -> Vec<Statement> {
        Parser::parse_sql(&AlopexDialect, sql).expect("parse query")
    }

    fn plan(&self, stmts: &[Statement]) {
        let guard = self.catalog.read().unwrap();
        for stmt in stmts {
            black_box(Planner::new(&*guard).plan(stmt).expect("plan query"));
        }
    }
}

/// DDL for one table of `column_count` INTEGER columns.
fn wide_table_ddl(name: &str, column_count: usize) -> String {
    let columns = (0..column_count)
        .map(|i| format!("c{i} INT"))
        .collect::<Vec<_>>()
        .join(", ");
    format!("CREATE TABLE {name} ({columns});")
}

/// One unqualified column reference against a table of growing width.
///
/// `get_column_index` scans linearly, so a projection of N columns over an
/// N-column table is quadratic in principle. This shows whether that matters.
fn bench_wide_schema(c: &mut Criterion) {
    let mut group = c.benchmark_group("resolve/wide_schema");
    for &width in &[50usize, 100, 200, 400, 800] {
        let schema = Schema::new(&wide_table_ddl("wide", width));
        let projection = (0..width)
            .map(|i| format!("c{i}"))
            .collect::<Vec<_>>()
            .join(", ");
        let sql = format!("SELECT {projection} FROM wide;");
        let parsed = schema.parse(&sql);
        group.bench_with_input(BenchmarkId::from_parameter(width), &parsed, |b, parsed| {
            b.iter(|| schema.plan(parsed));
        });
    }
    group.finish();
}

/// Correlated subqueries nested `depth` levels deep.
///
/// Every level clones the enclosing scope, so the total cloning work grows
/// quadratically with depth even though each individual clone is small.
fn bench_nesting_depth(c: &mut Criterion) {
    let mut ddl = String::new();
    for i in 0..16 {
        ddl.push_str(&format!("CREATE TABLE t{i} (id INT, v{i} INT);\n"));
    }
    let schema = Schema::new(&ddl);
    let mut group = c.benchmark_group("resolve/nesting_depth");
    for &depth in &[2usize, 4, 8, 12] {
        let mut sql = String::from("SELECT t0.v0 FROM t0 WHERE t0.id > 0");
        for level in 1..depth {
            sql.push_str(&format!(
                " AND t0.id IN (SELECT t{level}.id FROM t{level} WHERE t{level}.id = t0.id"
            ));
        }
        for _ in 1..depth {
            sql.push(')');
        }
        sql.push(';');
        let parsed = schema.parse(&sql);
        group.bench_with_input(BenchmarkId::from_parameter(depth), &parsed, |b, parsed| {
            b.iter(|| schema.plan(parsed));
        });
    }
    group.finish();
}

/// NATURAL JOIN pairs every left column against every right column.
fn bench_natural_join(c: &mut Criterion) {
    let mut group = c.benchmark_group("resolve/natural_join");
    for &width in &[25usize, 50, 100, 200] {
        let columns = (0..width)
            .map(|i| format!("c{i} INT"))
            .collect::<Vec<_>>()
            .join(", ");
        let ddl = format!("CREATE TABLE l ({columns}); CREATE TABLE r ({columns});");
        let schema = Schema::new(&ddl);
        let parsed = schema.parse("SELECT c0 FROM l NATURAL JOIN r;");
        group.bench_with_input(BenchmarkId::from_parameter(width), &parsed, |b, parsed| {
            b.iter(|| schema.plan(parsed));
        });
    }
    group.finish();
}

/// The single-table case, which has no scope machinery to amortise.
fn bench_single_table(c: &mut Criterion) {
    let schema = Schema::new(&wide_table_ddl("wide", 16));
    let parsed = schema.parse("SELECT c0, c1, c2 FROM wide WHERE c0 = 1;");
    c.bench_function("resolve/single_table", |b| {
        b.iter(|| schema.plan(&parsed));
    });
}

criterion_group!(
    benches,
    bench_wide_schema,
    bench_nesting_depth,
    bench_natural_join,
    bench_single_table
);
criterion_main!(benches);
