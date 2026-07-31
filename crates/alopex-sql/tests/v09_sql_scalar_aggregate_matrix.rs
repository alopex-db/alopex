use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, RwLock};

use alopex_core::lsm::LsmKVConfig;
use alopex_core::lsm::wal::{SyncMode, WalConfig};
use alopex_core::{StorageFactory, StorageMode};
use alopex_sql::{
    Span,
    ast::expr::Literal,
    catalog::MemoryCatalog,
    dialect::AlopexDialect,
    distributed_read::{
        AssemblyPlan, DistributedReadBudget, ExactAggregatePartial, ExactAggregatePlan,
        GlobalOrder, GlobalResultAssembler, OrderedAggregateInput, OrderedAggregatePlan,
        REMOTE_DETERMINISTIC_SCALAR_FUNCTIONS, REMOTE_LOCAL_ONLY_SCALAR_FUNCTIONS,
        RangeAssemblerInput, RangeAssemblerPayload, RangeTerminal, RemoteReadCatalogV0_8,
        RemoteReadCoverageStatus, ResultPresentation,
    },
    executor::query::aggregate::create_accumulator,
    executor::{ColumnInfo, ExecutionResult, Executor, QueryResult},
    parser::Parser,
    planner::{
        AggregateExpr, AggregateFunction, Planner,
        typed_expr::{TypedExpr, TypedExprKind},
        types::ResolvedType,
    },
    scalar::{
        self, Arity, FnMeta, ReturnRule, V09AggregateArity, V09AggregateFinalization,
        V09AggregateMeta, V09AggregateNullContract, V09AggregateOrdering, V09AggregateTypeContract,
        V09ScalarExecutionStatus,
    },
    storage::SqlValue,
};
use tempfile::TempDir;

type AggregateMatrixRow = (
    &'static str,
    V09AggregateArity,
    V09AggregateTypeContract,
    V09AggregateNullContract,
    bool,
    V09AggregateOrdering,
    V09AggregateFinalization,
    V09ScalarExecutionStatus,
    V09AggregateMeta,
);

fn typed(ty: ResolvedType) -> TypedExpr {
    TypedExpr::new(TypedExprKind::Literal(Literal::Null), ty, Span::default())
}

fn return_type(name: &str, args: &[TypedExpr]) -> ResolvedType {
    let signature = scalar::signature(name).expect("tested matrix row must resolve");
    let types = args
        .iter()
        .map(|arg| arg.resolved_type.clone())
        .collect::<Vec<_>>();
    match &signature.ret {
        ReturnRule::Fixed(ty) => ty.clone(),
        ReturnRule::FromArgs(rule) => rule(&types).expect("accepted argument types return"),
    }
}

#[test]
fn v09_string_conditional_system_register_is_closed_and_matches_catalog_metadata() {
    use scalar::V09ScalarExecutionStatus::{Distributed, LocalOnly};

    let expected: BTreeMap<&str, (&str, Arity, V09ScalarExecutionStatus, FnMeta)> =
        BTreeMap::from([
            (
                "SQL-F-S01",
                ("length", Arity::Exact(1), Distributed, scalar::PURE_META),
            ),
            (
                "SQL-F-S02",
                (
                    "char_length",
                    Arity::Exact(1),
                    Distributed,
                    scalar::PURE_META,
                ),
            ),
            (
                "SQL-F-S03",
                (
                    "octet_length",
                    Arity::Exact(1),
                    Distributed,
                    scalar::PURE_META,
                ),
            ),
            (
                "SQL-F-S04",
                ("upper", Arity::Exact(1), Distributed, scalar::PURE_META),
            ),
            (
                "SQL-F-S05",
                ("lower", Arity::Exact(1), Distributed, scalar::PURE_META),
            ),
            (
                "SQL-F-S06",
                ("initcap", Arity::Exact(1), Distributed, scalar::PURE_META),
            ),
            (
                "SQL-F-S07",
                ("substr", Arity::Range(2, 3), Distributed, scalar::PURE_META),
            ),
            (
                "SQL-F-S08",
                ("left", Arity::Exact(2), Distributed, scalar::PURE_META),
            ),
            (
                "SQL-F-S09",
                ("right", Arity::Exact(2), Distributed, scalar::PURE_META),
            ),
            (
                "SQL-F-S10",
                ("trim", Arity::Range(1, 2), Distributed, scalar::PURE_META),
            ),
            (
                "SQL-F-S11",
                ("ltrim", Arity::Range(1, 2), Distributed, scalar::PURE_META),
            ),
            (
                "SQL-F-S12",
                ("rtrim", Arity::Range(1, 2), Distributed, scalar::PURE_META),
            ),
            (
                "SQL-F-S13",
                ("replace", Arity::Exact(3), Distributed, scalar::PURE_META),
            ),
            (
                "SQL-F-S14",
                ("instr", Arity::Exact(2), Distributed, scalar::PURE_META),
            ),
            (
                "SQL-F-S15",
                ("strpos", Arity::Exact(2), Distributed, scalar::PURE_META),
            ),
            (
                "SQL-F-S16",
                ("concat", Arity::Variadic(0), Distributed, scalar::PURE_META),
            ),
            (
                "SQL-F-S17",
                (
                    "concat_ws",
                    Arity::Variadic(1),
                    Distributed,
                    scalar::PURE_META,
                ),
            ),
            (
                "SQL-F-S18",
                ("repeat", Arity::Exact(2), Distributed, scalar::PURE_META),
            ),
            (
                "SQL-F-S19",
                ("reverse", Arity::Exact(1), Distributed, scalar::PURE_META),
            ),
            (
                "SQL-F-S20",
                ("lpad", Arity::Range(2, 3), Distributed, scalar::PURE_META),
            ),
            (
                "SQL-F-S21",
                ("rpad", Arity::Range(2, 3), Distributed, scalar::PURE_META),
            ),
            (
                "SQL-F-S22",
                (
                    "split_part",
                    Arity::Exact(3),
                    Distributed,
                    scalar::PURE_META,
                ),
            ),
            (
                "SQL-F-S23",
                (
                    "regexp_replace",
                    Arity::Exact(3),
                    Distributed,
                    scalar::PURE_META,
                ),
            ),
            (
                "SQL-F-S24",
                (
                    "regexp_match",
                    Arity::Exact(2),
                    Distributed,
                    scalar::PURE_META,
                ),
            ),
            (
                "SQL-F-S25",
                (
                    "regexp_matches",
                    Arity::Range(2, 3),
                    Distributed,
                    scalar::PURE_META,
                ),
            ),
            (
                "SQL-F-C01",
                (
                    "coalesce",
                    Arity::Variadic(1),
                    Distributed,
                    scalar::PURE_META,
                ),
            ),
            (
                "SQL-F-C02",
                ("nullif", Arity::Exact(2), Distributed, scalar::PURE_META),
            ),
            (
                "SQL-F-C03",
                ("ifnull", Arity::Exact(2), Distributed, scalar::PURE_META),
            ),
            (
                "SQL-F-C04",
                ("iif", Arity::Exact(3), Distributed, scalar::PURE_META),
            ),
            (
                "SQL-F-C05",
                (
                    "greatest",
                    Arity::Variadic(1),
                    Distributed,
                    scalar::PURE_META,
                ),
            ),
            (
                "SQL-F-C06",
                ("least", Arity::Variadic(1), Distributed, scalar::PURE_META),
            ),
            (
                "SQL-F-C07",
                ("typeof", Arity::Exact(1), Distributed, scalar::PURE_META),
            ),
            (
                "SQL-F-C08",
                ("pg_typeof", Arity::Exact(1), Distributed, scalar::PURE_META),
            ),
            (
                "SQL-F-C09",
                ("quote", Arity::Exact(1), Distributed, scalar::PURE_META),
            ),
            (
                "SQL-F-SYS01",
                (
                    "memory_stats",
                    Arity::Exact(0),
                    LocalOnly,
                    scalar::SYSTEM_META,
                ),
            ),
            (
                "SQL-F-SYS02",
                ("io_stats", Arity::Exact(0), LocalOnly, scalar::SYSTEM_META),
            ),
            (
                "SQL-F-SYS03",
                (
                    "clear_cache",
                    Arity::Exact(0),
                    LocalOnly,
                    scalar::SYSTEM_SIDE_EFFECT_META,
                ),
            ),
        ]);

    let entries = scalar::v09_string_conditional_system_scalar_register();
    let actual = entries
        .iter()
        .map(|entry| {
            (
                entry.id,
                (
                    entry.name,
                    entry.arity,
                    entry.execution_status,
                    entry.metadata,
                ),
            )
        })
        .collect::<BTreeMap<_, _>>();
    assert_eq!(
        actual, expected,
        "every Phase 4.8 scalar row must be fixed once"
    );
    assert_eq!(
        entries.len(),
        actual.len(),
        "row IDs must not be duplicated"
    );
    assert_eq!(
        entries.len(),
        37,
        "Phase 4.8 must not abbreviate scalar rows"
    );

    for entry in entries {
        let signature = scalar::signature(entry.name).expect("matrix row must resolve locally");
        assert_eq!(signature.arity, entry.arity, "{} arity drift", entry.id);
        assert_eq!(
            signature.meta, entry.metadata,
            "{} metadata drift",
            entry.id
        );
        match entry.execution_status {
            Distributed => assert!(
                REMOTE_DETERMINISTIC_SCALAR_FUNCTIONS.contains(&entry.name),
                "{} must stay in the closed distributed catalog",
                entry.id
            ),
            LocalOnly => assert!(
                REMOTE_LOCAL_ONLY_SCALAR_FUNCTIONS.contains(&entry.name),
                "{} must remain local-only",
                entry.id
            ),
        }
    }
}

#[test]
fn v09_string_conditional_system_rows_preserve_type_and_null_contracts() {
    for entry in scalar::v09_string_conditional_system_scalar_register() {
        let (args, expected_return) = match entry.name {
            "length" | "char_length" | "octet_length" | "instr" | "strpos" => {
                let count = match entry.arity {
                    Arity::Exact(count) => count,
                    _ => unreachable!("fixed Phase 4.8 arity"),
                };
                (
                    vec![typed(ResolvedType::Null); count],
                    ResolvedType::Integer,
                )
            }
            "upper" | "lower" | "initcap" | "substr" | "left" | "right" | "trim" | "ltrim"
            | "rtrim" | "replace" | "concat" | "concat_ws" | "repeat" | "reverse" | "lpad"
            | "rpad" | "split_part" | "regexp_replace" | "regexp_match" | "regexp_matches" => {
                let count = match entry.arity {
                    Arity::Exact(count) | Arity::Range(_, count) | Arity::Variadic(count) => count,
                };
                (vec![typed(ResolvedType::Null); count], ResolvedType::Text)
            }
            "coalesce" | "nullif" | "ifnull" | "iif" | "greatest" | "least" => {
                let count = match entry.arity {
                    Arity::Exact(count) | Arity::Range(_, count) | Arity::Variadic(count) => count,
                };
                (vec![typed(ResolvedType::Null); count], ResolvedType::Null)
            }
            "typeof" | "pg_typeof" | "quote" | "memory_stats" | "io_stats" => {
                let count = match entry.arity {
                    Arity::Exact(count) => count,
                    _ => unreachable!("fixed Phase 4.8 arity"),
                };
                (vec![typed(ResolvedType::Null); count], ResolvedType::Text)
            }
            "clear_cache" => (Vec::new(), ResolvedType::BigInt),
            name => panic!("uncovered Phase 4.8 type/null row: {name}"),
        };

        let signature = scalar::signature(entry.name).expect("matrix row must resolve locally");
        signature
            .arity
            .validate(entry.name, args.len(), Span::default())
            .expect("test fixture must use the exact registered arity");
        (signature.check)(&args).unwrap_or_else(|error| {
            panic!(
                "{} must preserve its accepted type/null contract: {error}",
                entry.id
            )
        });
        assert_eq!(
            return_type(entry.name, &args),
            expected_return,
            "{} return-type/null contract drift",
            entry.id
        );
    }
}

#[test]
fn v09_aggregate_register_is_closed_and_declares_the_full_contract() {
    use scalar::V09ScalarExecutionStatus::Distributed;

    let expected: BTreeMap<&str, AggregateMatrixRow> = BTreeMap::from([
        (
            "SQL-A01",
            (
                "count",
                V09AggregateArity::CountStarOrOne,
                V09AggregateTypeContract::AnyInputToBigInt,
                V09AggregateNullContract::CountsRowsOrNonNullValues,
                true,
                V09AggregateOrdering::InputOrderIndependent,
                V09AggregateFinalization::ExactPartialWhenProven,
                Distributed,
                scalar::V09_ORDER_INDEPENDENT_AGGREGATE_META,
            ),
        ),
        (
            "SQL-A02",
            (
                "sum",
                V09AggregateArity::ExactOne,
                V09AggregateTypeContract::NumericInputToDouble,
                V09AggregateNullContract::IgnoresNullAndReturnsNullWhenEmpty,
                true,
                V09AggregateOrdering::GlobalInputOrder,
                V09AggregateFinalization::OrderedInputReplay,
                Distributed,
                scalar::V09_ORDERED_AGGREGATE_META,
            ),
        ),
        (
            "SQL-A03",
            (
                "total",
                V09AggregateArity::ExactOne,
                V09AggregateTypeContract::NumericInputToDouble,
                V09AggregateNullContract::IgnoresNullAndReturnsZeroWhenEmpty,
                true,
                V09AggregateOrdering::GlobalInputOrder,
                V09AggregateFinalization::OrderedInputReplay,
                Distributed,
                scalar::V09_ORDERED_AGGREGATE_META,
            ),
        ),
        (
            "SQL-A04",
            (
                "avg",
                V09AggregateArity::ExactOne,
                V09AggregateTypeContract::NumericInputToDouble,
                V09AggregateNullContract::IgnoresNullAndReturnsNullWhenEmpty,
                true,
                V09AggregateOrdering::GlobalInputOrder,
                V09AggregateFinalization::OrderedInputReplay,
                Distributed,
                scalar::V09_ORDERED_AGGREGATE_META,
            ),
        ),
        (
            "SQL-A05",
            (
                "min",
                V09AggregateArity::ExactOne,
                V09AggregateTypeContract::InputTypeToSameType,
                V09AggregateNullContract::IgnoresNullAndReturnsNullWhenEmpty,
                true,
                V09AggregateOrdering::InputOrderIndependent,
                V09AggregateFinalization::ExactPartialWhenProven,
                Distributed,
                scalar::V09_ORDER_INDEPENDENT_AGGREGATE_META,
            ),
        ),
        (
            "SQL-A06",
            (
                "max",
                V09AggregateArity::ExactOne,
                V09AggregateTypeContract::InputTypeToSameType,
                V09AggregateNullContract::IgnoresNullAndReturnsNullWhenEmpty,
                true,
                V09AggregateOrdering::InputOrderIndependent,
                V09AggregateFinalization::ExactPartialWhenProven,
                Distributed,
                scalar::V09_ORDER_INDEPENDENT_AGGREGATE_META,
            ),
        ),
        (
            "SQL-A07",
            (
                "group_concat",
                V09AggregateArity::OneOrTwoWithLiteralTextSeparator,
                V09AggregateTypeContract::TextInputToText,
                V09AggregateNullContract::IgnoresNullAndReturnsNullWhenEmpty,
                true,
                V09AggregateOrdering::GlobalInputOrder,
                V09AggregateFinalization::OrderedInputReplay,
                Distributed,
                scalar::V09_ORDERED_AGGREGATE_META,
            ),
        ),
        (
            "SQL-A08",
            (
                "string_agg",
                V09AggregateArity::ExactlyTwoWithLiteralTextSeparator,
                V09AggregateTypeContract::TextInputToText,
                V09AggregateNullContract::IgnoresNullAndReturnsNullWhenEmpty,
                true,
                V09AggregateOrdering::GlobalInputOrder,
                V09AggregateFinalization::OrderedInputReplay,
                Distributed,
                scalar::V09_ORDERED_AGGREGATE_META,
            ),
        ),
    ]);
    let entries = scalar::v09_aggregate_register();
    let actual = entries
        .iter()
        .map(|entry| {
            (
                entry.id,
                (
                    entry.name,
                    entry.arity,
                    entry.type_contract,
                    entry.null_contract,
                    entry.distinct_supported,
                    entry.ordering,
                    entry.finalization,
                    entry.execution_status,
                    entry.metadata,
                ),
            )
        })
        .collect::<BTreeMap<_, _>>();
    assert_eq!(
        actual, expected,
        "every Phase 4.8 aggregate row must be fixed once"
    );
    assert_eq!(
        entries.len(),
        actual.len(),
        "aggregate row IDs must not duplicate"
    );

    let remotely_supported = RemoteReadCatalogV0_8
        .coverage_entries()
        .iter()
        .filter(|entry| entry.remote_status == RemoteReadCoverageStatus::RemoteSupported)
        .flat_map(|entry| entry.identities.iter().copied())
        .collect::<BTreeSet<_>>();
    for entry in entries {
        assert!(
            remotely_supported.contains(entry.name),
            "{} must remain in the v0.8 closed aggregate catalog",
            entry.id
        );

        let expression = aggregate_expr(entry.name, false);
        match entry.type_contract {
            V09AggregateTypeContract::AnyInputToBigInt => {
                assert_eq!(expression.result_type, ResolvedType::BigInt)
            }
            V09AggregateTypeContract::NumericInputToDouble => {
                assert_eq!(expression.result_type, ResolvedType::Double)
            }
            V09AggregateTypeContract::InputTypeToSameType => {
                assert_eq!(expression.result_type, ResolvedType::Integer)
            }
            V09AggregateTypeContract::TextInputToText => {
                assert_eq!(expression.result_type, ResolvedType::Text)
            }
        }
    }
}

#[test]
fn v09_aggregate_global_finalizer_replays_or_merges_each_matrix_row_correctly() {
    for entry in scalar::v09_aggregate_register() {
        let aggregate = aggregate_expr(entry.name, false);
        let (partitions, global_order) = aggregate_inputs(entry.name);
        let expected = local_aggregate_result(&aggregate.function, false, global_order);
        let actual = match entry.finalization {
            V09AggregateFinalization::ExactPartialWhenProven => {
                let partials = partitions
                    .iter()
                    .map(|(range_id, rows)| {
                        (
                            *range_id,
                            rows.iter().map(|(_, _, value)| value.clone()).collect(),
                        )
                    })
                    .collect();
                exact_aggregate_result(aggregate, partials)
            }
            V09AggregateFinalization::OrderedInputReplay => {
                ordered_aggregate_result(aggregate, partitions)
            }
        };
        assert_eq!(
            actual, expected,
            "{} must finalize once across every completed range",
            entry.id
        );
    }

    for entry in scalar::v09_aggregate_register() {
        if !entry.distinct_supported || entry.name == "count" {
            continue;
        }
        let aggregate = aggregate_expr(entry.name, true);
        let (partitions, global_order) = aggregate_inputs(entry.name);
        let expected = local_aggregate_result(&aggregate.function, true, global_order);
        assert_eq!(
            ordered_aggregate_result(aggregate, partitions),
            expected,
            "{} DISTINCT must replay every range in global logical order",
            entry.id
        );
    }
}

#[test]
fn v09_count_handles_null_distinct_group_by_and_having_before_public_output() {
    let mut harness = SqlHarness::new();
    let result = harness.query_sql(
        "CREATE TABLE metrics (bucket TEXT, value INT); \
         INSERT INTO metrics (bucket, value) VALUES \
             ('a', 1), ('a', 1), ('a', NULL), ('b', NULL), ('b', 2), ('c', NULL); \
         SELECT bucket, COUNT(value), COUNT(DISTINCT value) \
         FROM metrics GROUP BY bucket HAVING COUNT(value) > 0 ORDER BY bucket;",
    );
    assert_eq!(
        result.rows,
        vec![
            vec![
                SqlValue::Text("a".into()),
                SqlValue::BigInt(2),
                SqlValue::BigInt(1),
            ],
            vec![
                SqlValue::Text("b".into()),
                SqlValue::BigInt(1),
                SqlValue::BigInt(1),
            ],
        ]
    );
    let all_rows = harness.query_sql(
        "SELECT COUNT(*), SUM(value), TOTAL(value), TOTAL(DISTINCT value), AVG(value), MIN(value), MAX(value), \
                GROUP_CONCAT(bucket, '|'), STRING_AGG(bucket, '~') \
         FROM metrics;",
    );
    assert_eq!(
        all_rows.rows,
        vec![vec![
            SqlValue::BigInt(6),
            SqlValue::Double(4.0),
            SqlValue::Double(4.0),
            SqlValue::Double(3.0),
            SqlValue::Double(4.0 / 3.0),
            SqlValue::Integer(1),
            SqlValue::Integer(2),
            SqlValue::Text("a|a|a|b|b|c".into()),
            SqlValue::Text("a~a~a~b~b~c".into()),
        ]],
        "the local planner and accumulator must preserve every Phase 4.8 aggregate contract"
    );
    assert!(
        harness
            .plan_sql("SELECT STRING_AGG(bucket) FROM metrics")
            .is_err(),
        "STRING_AGG must reject a missing literal separator"
    );
    assert!(
        harness
            .plan_sql("SELECT STRING_AGG(bucket, bucket) FROM metrics")
            .is_err(),
        "STRING_AGG must reject a non-literal separator"
    );
    harness
        .plan_sql("SELECT STRING_AGG(bucket, '|') FROM metrics")
        .expect("STRING_AGG must accept exactly two arguments with a literal separator");

    let aggregate = AggregateExpr::count(typed(ResolvedType::Integer), true);
    let columns = vec![
        ColumnInfo::new("bucket", ResolvedType::Text),
        ColumnInfo::new("count", ResolvedType::BigInt),
    ];
    let plan = AssemblyPlan::OrderedAggregates(OrderedAggregatePlan {
        presentation: aggregate_presentation(columns),
        group_column_count: 1,
        aggregates: vec![aggregate],
        logical_input_order: vec![ascending_order()],
    });
    let mut assembler = GlobalResultAssembler::new(
        vec!["range-a".into(), "range-b".into()],
        plan,
        DistributedReadBudget::default(),
    )
    .expect("valid planned ranges");
    assembler
        .push_range(completed_aggregate_range(
            "range-a",
            vec![
                ordered_input("a", Some(SqlValue::Integer(1)), 2, 1),
                ordered_input("b", Some(SqlValue::Null), 3, 2),
            ],
        ))
        .expect("first range accepted");
    assembler
        .push_range(completed_aggregate_range(
            "range-b",
            vec![
                ordered_input("a", Some(SqlValue::Integer(1)), 1, 3),
                ordered_input("b", Some(SqlValue::Integer(2)), 4, 4),
            ],
        ))
        .expect("second range accepted");
    assert_eq!(
        assembler
            .prepare()
            .expect("global count finalization")
            .query_result()
            .rows,
        vec![
            vec![SqlValue::Text("a".into()), SqlValue::BigInt(1)],
            vec![SqlValue::Text("b".into()), SqlValue::BigInt(1)],
        ],
        "COUNT(DISTINCT) must see values from every range before group output is released"
    );
}

fn aggregate_expr(name: &str, distinct: bool) -> AggregateExpr {
    match name {
        "count" => AggregateExpr::count_star(),
        "sum" => AggregateExpr {
            function: AggregateFunction::Sum,
            arg: Some(typed(ResolvedType::Integer)),
            distinct,
            result_type: ResolvedType::Double,
        },
        "total" => AggregateExpr {
            function: AggregateFunction::Total,
            arg: Some(typed(ResolvedType::Integer)),
            distinct,
            result_type: ResolvedType::Double,
        },
        "avg" => AggregateExpr {
            function: AggregateFunction::Avg,
            arg: Some(typed(ResolvedType::Integer)),
            distinct,
            result_type: ResolvedType::Double,
        },
        "min" => AggregateExpr {
            function: AggregateFunction::Min,
            arg: Some(typed(ResolvedType::Integer)),
            distinct,
            result_type: ResolvedType::Integer,
        },
        "max" => AggregateExpr {
            function: AggregateFunction::Max,
            arg: Some(typed(ResolvedType::Integer)),
            distinct,
            result_type: ResolvedType::Integer,
        },
        "group_concat" => AggregateExpr {
            function: AggregateFunction::GroupConcat {
                separator: Some("|".to_owned()),
            },
            arg: Some(typed(ResolvedType::Text)),
            distinct,
            result_type: ResolvedType::Text,
        },
        "string_agg" => AggregateExpr {
            function: AggregateFunction::StringAgg {
                separator: Some("~".to_owned()),
            },
            arg: Some(typed(ResolvedType::Text)),
            distinct,
            result_type: ResolvedType::Text,
        },
        name => panic!("unregistered aggregate: {name}"),
    }
}

type OrderedPartition = (&'static str, Vec<(i32, u64, Option<SqlValue>)>);

fn aggregate_inputs(name: &str) -> (Vec<OrderedPartition>, Vec<Option<SqlValue>>) {
    match name {
        "count" => (
            vec![
                ("range-a", vec![(3, 1, None), (4, 2, None)]),
                ("range-b", vec![(1, 3, None), (2, 4, None)]),
            ],
            vec![None, None, None, None],
        ),
        "group_concat" | "string_agg" => (
            vec![
                (
                    "range-a",
                    vec![
                        (2, 1, Some(SqlValue::Text("beta".to_owned()))),
                        (4, 2, Some(SqlValue::Null)),
                    ],
                ),
                (
                    "range-b",
                    vec![
                        (1, 3, Some(SqlValue::Text("alpha".to_owned()))),
                        (3, 4, Some(SqlValue::Text("beta".to_owned()))),
                    ],
                ),
            ],
            vec![
                Some(SqlValue::Text("alpha".to_owned())),
                Some(SqlValue::Text("beta".to_owned())),
                Some(SqlValue::Text("beta".to_owned())),
                Some(SqlValue::Null),
            ],
        ),
        _ => (
            vec![
                (
                    "range-a",
                    vec![
                        (3, 1, Some(SqlValue::Integer(1))),
                        (4, 2, Some(SqlValue::Null)),
                    ],
                ),
                (
                    "range-b",
                    vec![
                        (1, 3, Some(SqlValue::Integer(3))),
                        (2, 4, Some(SqlValue::Integer(1))),
                    ],
                ),
            ],
            vec![
                Some(SqlValue::Integer(3)),
                Some(SqlValue::Integer(1)),
                Some(SqlValue::Integer(1)),
                Some(SqlValue::Null),
            ],
        ),
    }
}

fn local_aggregate_result(
    function: &AggregateFunction,
    distinct: bool,
    values: Vec<Option<SqlValue>>,
) -> SqlValue {
    let mut accumulator = create_accumulator(function, distinct);
    for value in values {
        accumulator.update(value).expect("valid aggregate input");
    }
    accumulator.finalize().expect("single finalization")
}

fn exact_aggregate_result(
    aggregate: AggregateExpr,
    partitions: Vec<(&str, Vec<Option<SqlValue>>)>,
) -> SqlValue {
    let output_columns = vec![ColumnInfo::new("aggregate", aggregate.result_type.clone())];
    let plan = AssemblyPlan::ExactAggregates(ExactAggregatePlan {
        presentation: aggregate_presentation(output_columns),
        group_column_count: 0,
        aggregates: vec![aggregate.clone()],
    });
    let ranges: Vec<String> = partitions
        .iter()
        .map(|(range_id, _)| (*range_id).to_owned())
        .collect();
    let mut assembler = GlobalResultAssembler::new(ranges, plan, DistributedReadBudget::default())
        .expect("valid planned ranges");
    for (range_id, values) in partitions {
        let mut partial = create_accumulator(&aggregate.function, aggregate.distinct);
        for value in values {
            partial.update(value).expect("valid partial input");
        }
        assembler
            .push_range(completed_aggregate_range(
                range_id,
                vec![RangeAssemblerPayload::ExactAggregatePartial(
                    ExactAggregatePartial {
                        group_key: vec![],
                        states: vec![partial.state().expect("partial state")],
                    },
                )],
            ))
            .expect("exact partial accepted");
    }
    one_value(
        assembler
            .prepare()
            .expect("exact global finalization")
            .query_result()
            .rows,
    )
}

fn ordered_aggregate_result(
    aggregate: AggregateExpr,
    partitions: Vec<OrderedPartition>,
) -> SqlValue {
    let output_columns = vec![ColumnInfo::new("aggregate", aggregate.result_type.clone())];
    let plan = AssemblyPlan::OrderedAggregates(OrderedAggregatePlan {
        presentation: aggregate_presentation(output_columns),
        group_column_count: 0,
        aggregates: vec![aggregate],
        logical_input_order: vec![ascending_order()],
    });
    let ranges: Vec<String> = partitions
        .iter()
        .map(|(range_id, _)| (*range_id).to_owned())
        .collect();
    let mut assembler = GlobalResultAssembler::new(ranges, plan, DistributedReadBudget::default())
        .expect("valid planned ranges");
    for (range_id, values) in partitions {
        let payloads = values
            .into_iter()
            .map(|(order, row_key, value)| {
                RangeAssemblerPayload::OrderedAggregateInput(OrderedAggregateInput {
                    group_key: vec![],
                    aggregate_arguments: vec![value],
                    logical_order_keys: vec![SqlValue::Integer(order)],
                    row_key,
                })
            })
            .collect();
        assembler
            .push_range(completed_aggregate_range(range_id, payloads))
            .expect("ordered input accepted");
    }
    one_value(
        assembler
            .prepare()
            .expect("ordered global finalization")
            .query_result()
            .rows,
    )
}

fn aggregate_presentation(columns: Vec<ColumnInfo>) -> ResultPresentation {
    ResultPresentation {
        columns,
        distinct: false,
        order: vec![],
        final_order_key_indexes: vec![],
        offset: 0,
        limit: None,
    }
}

fn ascending_order() -> GlobalOrder {
    GlobalOrder {
        ascending: true,
        nulls_first: false,
    }
}

fn completed_aggregate_range(
    range_id: &str,
    payloads: Vec<RangeAssemblerPayload>,
) -> RangeAssemblerInput {
    RangeAssemblerInput {
        range_id: range_id.to_owned(),
        columns: vec![],
        payloads,
        terminal: RangeTerminal::Completed {
            cleanup_acknowledged: true,
        },
    }
}

fn ordered_input(
    group: &str,
    value: Option<SqlValue>,
    order: i32,
    row_key: u64,
) -> RangeAssemblerPayload {
    RangeAssemblerPayload::OrderedAggregateInput(OrderedAggregateInput {
        group_key: vec![SqlValue::Text(group.to_owned())],
        aggregate_arguments: vec![value],
        logical_order_keys: vec![SqlValue::Integer(order)],
        row_key,
    })
}

fn one_value(rows: Vec<Vec<SqlValue>>) -> SqlValue {
    let [row]: [Vec<SqlValue>; 1] = rows.try_into().expect("one global aggregate row");
    let [value]: [SqlValue; 1] = row.try_into().expect("one aggregate column");
    value
}

struct SqlHarness {
    executor: Executor<alopex_core::kv::AnyKV, MemoryCatalog>,
    catalog: Arc<RwLock<MemoryCatalog>>,
    _temporary_store: TempDir,
}

impl SqlHarness {
    fn new() -> Self {
        let temporary_store = tempfile::tempdir().expect("temporary SQL store");
        let config = LsmKVConfig {
            wal: WalConfig {
                segment_size: 4096,
                max_segments: 2,
                sync_mode: SyncMode::NoSync,
            },
            ..Default::default()
        };
        let store = Arc::new(
            StorageFactory::create(StorageMode::Disk {
                path: temporary_store.path().to_path_buf(),
                config: Some(config),
            })
            .expect("temporary disk store"),
        );
        let catalog = Arc::new(RwLock::new(MemoryCatalog::new()));
        Self {
            executor: Executor::new(store, Arc::clone(&catalog)),
            catalog,
            _temporary_store: temporary_store,
        }
    }

    fn query_sql(&mut self, sql: &str) -> QueryResult {
        let statements = Parser::parse_sql(&AlopexDialect, sql).expect("parse SQL");
        let mut result = None;
        for statement in statements {
            let plan = {
                let catalog = self.catalog.read().expect("catalog read");
                Planner::new(&*catalog).plan(&statement).expect("plan SQL")
            };
            if let ExecutionResult::Query(query) = self.executor.execute(plan).expect("execute SQL")
            {
                result = Some(query);
            }
        }
        result.expect("final statement must return rows")
    }

    fn plan_sql(&self, sql: &str) -> Result<(), alopex_sql::PlannerError> {
        let statements = Parser::parse_sql(&AlopexDialect, sql).expect("parse SQL");
        let catalog = self.catalog.read().expect("catalog read");
        let planner = Planner::new(&*catalog);
        statements
            .into_iter()
            .try_for_each(|statement| planner.plan(&statement).map(|_| ()))
    }
}
