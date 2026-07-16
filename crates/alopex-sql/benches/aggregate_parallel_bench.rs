use std::time::Duration;

use alopex_sql::Span;
use alopex_sql::catalog::ColumnMetadata;
use alopex_sql::executor::query::RowIterator;
use alopex_sql::executor::query::aggregate::{
    AggregateIterator, build_aggregate_schema, execute_parallel_aggregate_rows,
};
use alopex_sql::executor::{Result, Row};
use alopex_sql::planner::aggregate_expr::AggregateExpr;
use alopex_sql::planner::typed_expr::{TypedExpr, TypedExprKind};
use alopex_sql::planner::types::ResolvedType;
use alopex_sql::storage::SqlValue;
use criterion::{BatchSize, Criterion, black_box, criterion_group, criterion_main};

const ROWS: usize = 1_000_000;

struct BenchRows {
    rows: std::vec::IntoIter<Row>,
    schema: Vec<ColumnMetadata>,
}

impl BenchRows {
    fn new(rows: Vec<Row>, schema: Vec<ColumnMetadata>) -> Self {
        Self {
            rows: rows.into_iter(),
            schema,
        }
    }
}

impl RowIterator for BenchRows {
    fn next_row(&mut self) -> Option<Result<Row>> {
        self.rows.next().map(Ok)
    }

    fn schema(&self) -> &[ColumnMetadata] {
        &self.schema
    }
}

fn schema() -> Vec<ColumnMetadata> {
    vec![
        ColumnMetadata::new("category", ResolvedType::Text),
        ColumnMetadata::new("amount", ResolvedType::Double),
    ]
}

fn rows() -> Vec<Row> {
    (0..ROWS)
        .map(|idx| {
            Row::new(
                idx as u64,
                vec![
                    SqlValue::Text(format!("c{}", idx % 256)),
                    SqlValue::Double((idx % 10_000) as f64),
                ],
            )
        })
        .collect()
}

fn column(index: usize, name: &str, ty: ResolvedType) -> TypedExpr {
    TypedExpr {
        kind: TypedExprKind::ColumnRef {
            table: "sales".to_string(),
            column: name.to_string(),
            column_index: index,
        },
        resolved_type: ty,
        span: Span::default(),
    }
}

fn plan_parts() -> (Vec<TypedExpr>, Vec<AggregateExpr>, Vec<ColumnMetadata>) {
    let category = column(0, "category", ResolvedType::Text);
    let amount = column(1, "amount", ResolvedType::Double);
    let group_keys = vec![category];
    let aggregates = vec![
        AggregateExpr::count_star(),
        AggregateExpr::sum(amount.clone()),
        AggregateExpr::avg(amount),
        AggregateExpr::sum(column(1, "amount", ResolvedType::Double)),
        AggregateExpr::avg(column(1, "amount", ResolvedType::Double)),
        AggregateExpr::min(column(1, "amount", ResolvedType::Double)),
        AggregateExpr::max(column(1, "amount", ResolvedType::Double)),
        AggregateExpr::total(column(1, "amount", ResolvedType::Double)),
        AggregateExpr::sum(column(1, "amount", ResolvedType::Double)),
        AggregateExpr::avg(column(1, "amount", ResolvedType::Double)),
        AggregateExpr::min(column(1, "amount", ResolvedType::Double)),
        AggregateExpr::max(column(1, "amount", ResolvedType::Double)),
    ];
    let final_schema = build_aggregate_schema(&group_keys, &aggregates);
    (group_keys, aggregates, final_schema)
}

fn collect_single(
    rows: Vec<Row>,
    schema: Vec<ColumnMetadata>,
    group_keys: Vec<TypedExpr>,
    aggregates: Vec<AggregateExpr>,
    final_schema: Vec<ColumnMetadata>,
) -> Result<Vec<Row>> {
    let mut iter = AggregateIterator::new(
        Box::new(BenchRows::new(rows, schema)),
        group_keys,
        aggregates,
        None,
        final_schema,
    );
    let mut output = Vec::new();
    while let Some(row) = iter.next_row() {
        output.push(row?);
    }
    Ok(output)
}

fn bench_parallel_aggregate(c: &mut Criterion) {
    let mut group = c.benchmark_group("aggregate_parallel");
    group.measurement_time(Duration::from_secs(4));
    group.sample_size(10);

    group.bench_function("single_1m", |b| {
        b.iter_batched(
            || (rows(), schema(), plan_parts()),
            |(rows, schema, (group_keys, aggregates, final_schema))| {
                black_box(
                    collect_single(rows, schema, group_keys, aggregates, final_schema).unwrap(),
                );
            },
            BatchSize::LargeInput,
        )
    });

    for parallelism in [1usize, 2usize, 4usize] {
        group.bench_function(format!("parallel_{parallelism}_1m"), |b| {
            b.iter_batched(
                || (rows(), schema(), plan_parts()),
                |(rows, schema, (group_keys, aggregates, final_schema))| {
                    black_box(
                        execute_parallel_aggregate_rows(
                            Box::new(BenchRows::new(rows, schema)),
                            group_keys,
                            aggregates,
                            None,
                            final_schema,
                            parallelism,
                        )
                        .unwrap(),
                    );
                },
                BatchSize::LargeInput,
            )
        });
    }

    group.finish();
}

criterion_group!(benches, bench_parallel_aggregate);
criterion_main!(benches);
