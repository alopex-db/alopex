#![cfg(target_os = "linux")]

use alopex_sql::{
    AlopexDialect, CommonTableExpr, CreateContinuousAggregate, DataType, ExprKind, FromItem,
    InsertSource, JoinType, Literal, Parser, ParserError, QueryBody, SelectItem, Span, Statement,
    StatementKind, VectorMetric, WindowFrameBound, WindowFrameUnits, parser_contract_version,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

const MAX_SQL_INPUT_BYTES: usize = 1_048_576;
const MAX_MESSAGEPACK_PAYLOAD_BYTES: usize = 1_048_576;
const MINIMAL_CONTINUOUS_AGGREGATE_SQL: &str = "CREATE CONTINUOUS AGGREGATE cpu_hourly AS SELECT 1 FROM cpu_metrics \
     WITH (retention = '30d', refresh_interval = '1h')";

#[derive(Debug, Serialize, Deserialize)]
struct LegacyCommonTableExpr {
    name: String,
    query: Box<Statement>,
    span: Span,
}

fn wire_span(start_line: u64, start_column: u64, end_line: u64, end_column: u64) -> Value {
    json!({
        "start": {"line": start_line, "column": start_column},
        "end": {"line": end_line, "column": end_column},
    })
}

fn staged_continuous_aggregate_value() -> Value {
    let statement_span = wire_span(1, 1, 4, 1);
    json!({
        "kind": {
            "variant": "CreateContinuousAggregate",
            "name": "cpu_hourly",
            "name_span": wire_span(1, 29, 1, 38),
            "query": {
                "variant": "Select",
                "distinct": false,
                "projection": [{
                    "variant": "Expr",
                    "expr": {
                        "kind": {
                            "variant": "Literal",
                            "literal": {"variant": "Number", "value": "1"},
                        },
                        "span": wire_span(2, 8, 2, 8),
                    },
                    "alias": null,
                    "span": wire_span(2, 8, 2, 8),
                }],
                "from": [],
                "selection": null,
                "group_by": null,
                "having": null,
                "order_by": [],
                "limit": null,
                "offset": null,
                "span": wire_span(2, 1, 2, 8),
            },
            "options": [
                {
                    "key": "retention",
                    "key_span": wire_span(3, 7, 3, 15),
                    "value": "30d",
                    "value_span": wire_span(3, 19, 3, 23),
                    "span": wire_span(3, 7, 3, 23),
                },
                {
                    "key": "refresh_interval",
                    "key_span": wire_span(3, 26, 3, 41),
                    "value": "1h",
                    "value_span": wire_span(3, 45, 3, 48),
                    "span": wire_span(3, 26, 3, 48),
                },
            ],
            "span": statement_span.clone(),
        },
        "span": statement_span,
    })
}

fn encode_staged_continuous_aggregate(value: &Value) -> Vec<u8> {
    rmp_serde::to_vec_named(value).expect("synthetic staged payload should encode")
}

fn padded_sql(total_bytes: usize) -> String {
    const STATEMENT: &str = "SELECT 1";
    assert!(total_bytes >= STATEMENT.len());
    format!("{}{STATEMENT}", " ".repeat(total_bytes - STATEMENT.len()))
}

fn padded_expression(total_bytes: usize) -> String {
    const EXPRESSION: &str = "1";
    assert!(total_bytes >= EXPRESSION.len());
    format!("{}{EXPRESSION}", " ".repeat(total_bytes - EXPRESSION.len()))
}

fn assert_input_too_large(error: ParserError) {
    let ParserError::UnexpectedToken {
        line,
        column,
        expected,
        found,
    } = error
    else {
        panic!("expected bounded input error, got {error:?}");
    };
    assert_eq!((line, column), (0, 0));
    assert_eq!(expected, "SQL input at most 1048576 UTF-8 bytes");
    assert_eq!(found, "SQL input exceeds byte limit");
}

#[test]
fn public_sql_boundary_accepts_minus_and_exact_then_rejects_plus() {
    for total_bytes in [MAX_SQL_INPUT_BYTES - 1, MAX_SQL_INPUT_BYTES] {
        let sql = padded_sql(total_bytes);
        assert_eq!(sql.len(), total_bytes);
        let statements = Parser::parse_sql(&AlopexDialect, &sql)
            .unwrap_or_else(|error| panic!("{total_bytes}-byte SQL should pass guard: {error}"));
        assert_eq!(statements.len(), 1);
    }

    let plus = padded_sql(MAX_SQL_INPUT_BYTES + 1);
    assert_input_too_large(
        Parser::parse_sql(&AlopexDialect, &plus).expect_err("limit plus one must be rejected"),
    );
}

#[test]
fn public_expression_boundary_accounts_for_select_wrapper_before_allocation() {
    const WRAPPER_BYTES: usize = "SELECT ".len();
    let exact = padded_expression(MAX_SQL_INPUT_BYTES - WRAPPER_BYTES);
    assert_eq!(exact.len() + WRAPPER_BYTES, MAX_SQL_INPUT_BYTES);
    let expression = Parser::parse_expression_sql(&AlopexDialect, &exact)
        .expect("wrapped exact-limit expression should pass guard and parse");
    assert!(matches!(
        expression.kind,
        ExprKind::Literal {
            literal: Literal::Number(ref value)
        } if value == "1"
    ));

    let plus = padded_expression(MAX_SQL_INPUT_BYTES - WRAPPER_BYTES + 1);
    assert_input_too_large(
        Parser::parse_expression_sql(&AlopexDialect, &plus)
            .expect_err("wrapper-adjusted limit plus one must be rejected"),
    );
}

#[test]
fn public_nul_guard_and_normal_sql_behavior_are_stable() {
    let nul = Parser::parse_sql(&AlopexDialect, "SELECT \0 1")
        .expect_err("interior NUL must be rejected without calling FFI");
    let ParserError::UnexpectedToken {
        expected, found, ..
    } = nul
    else {
        panic!("expected bounded NUL error, got {nul:?}");
    };
    assert_eq!(expected, "valid SQL without interior NUL bytes");
    assert_eq!(found, "interior NUL byte");

    let statements =
        Parser::parse_sql(&AlopexDialect, "SELECT 1").expect("ordinary SQL must remain unchanged");
    assert_eq!(statements.len(), 1);
}

#[test]
fn query_body_shape_requires_the_contract_0_7_cutover() {
    let statements = Parser::parse_sql(
        &AlopexDialect,
        "WITH c(identifier, label) AS (SELECT 1, 'one') SELECT identifier FROM c",
    )
    .expect("CTE column names should deserialize from Nim MessagePack");
    let StatementKind::Select(select) = &statements[0].kind else {
        panic!("expected SELECT");
    };
    let cte = &select.with.as_ref().expect("expected WITH clause").ctes[0];
    assert_eq!(cte.columns, vec!["identifier", "label"]);

    let current_payload = rmp_serde::to_vec_named(cte).expect("encode contract-0.7 CTE payload");
    rmp_serde::from_slice::<LegacyCommonTableExpr>(&current_payload)
        .expect_err("a contract-0.6 consumer must reject the direct QueryBody payload");

    let QueryBody::Select(select_body) = cte.query.as_ref() else {
        panic!("expected SELECT CTE body");
    };
    let legacy = LegacyCommonTableExpr {
        name: cte.name.clone(),
        query: Box::new(Statement {
            kind: StatementKind::Select(select_body.clone()),
            span: cte.span,
        }),
        span: cte.span,
    };
    let legacy_payload = rmp_serde::to_vec_named(&legacy).expect("encode contract-0.6 CTE payload");
    rmp_serde::from_slice::<CommonTableExpr>(&legacy_payload)
        .expect_err("the contract-0.7 consumer must reject a wrapped Statement query");
}

#[test]
fn window_frame_crosses_the_nim_messagepack_boundary_compatibly() {
    let statements = Parser::parse_sql(
        &AlopexDialect,
        "SELECT SUM(value) OVER (ORDER BY value ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) \
         FROM samples",
    )
    .expect("window frame should deserialize from Nim MessagePack");
    let StatementKind::Select(select) = &statements[0].kind else {
        panic!("expected SELECT");
    };
    let SelectItem::Expr { expr, .. } = &select.projection[0] else {
        panic!("expected expression projection");
    };
    let ExprKind::FunctionCall {
        over: Some(window), ..
    } = &expr.kind
    else {
        panic!("expected window function");
    };
    let frame = window.frame.as_ref().expect("expected explicit frame");
    assert_eq!(frame.units, WindowFrameUnits::Rows);
    assert_eq!(frame.start_bound, WindowFrameBound::Preceding(2));
    assert_eq!(frame.end_bound, WindowFrameBound::CurrentRow);
}

#[test]
fn pre_frame_nim_producer_payload_defaults_frame_to_none() {
    // Captured from the real contract-0.4.0 Nim producer built from 157f214
    // before WindowSpec.frame existed (library SHA-256
    // 770ebbf668326b3184f4c6aeab364b7c4113da60d7cececb27492bd6a5e526e1).
    let payload = hex::decode(include_str!("fixtures/window_spec_v040_pre_frame.hex").trim())
        .expect("legacy Nim fixture must be valid hex");
    let statements: Vec<Statement> =
        rmp_serde::from_slice(&payload).expect("current Rust consumer must decode old Nim payload");
    let StatementKind::Select(select) = &statements[0].kind else {
        panic!("expected SELECT");
    };
    let SelectItem::Expr { expr, .. } = &select.projection[0] else {
        panic!("expected expression projection");
    };
    let ExprKind::FunctionCall {
        over: Some(window), ..
    } = &expr.kind
    else {
        panic!("expected legacy window function");
    };
    assert!(window.frame.is_none());
}

#[test]
fn v050_nim_payload_defaults_named_window_fields_without_loading_v050_code() {
    // Captured from the real contract-0.5.0 producer at SHA-256
    // 362b912093223890b2ff69328490a604a09522622c07dcbdbf837dd2cc88e182.
    // Decode compatibility is one-way migration evidence only; the runtime
    // contract gate still rejects a 0.5.0 shared library before parsing.
    let payload = hex::decode(include_str!("fixtures/select_v050_pre_named_window.hex").trim())
        .expect("v0.5.0 Nim fixture must be valid hex");
    let statements: Vec<Statement> =
        rmp_serde::from_slice(&payload).expect("current Rust AST must decode old map fields");
    let StatementKind::Select(select) = &statements[0].kind else {
        panic!("expected SELECT");
    };
    assert!(select.windows.is_empty());
    assert!(select.qualify.is_none());
    let SelectItem::Expr { expr, .. } = &select.projection[0] else {
        panic!("expected expression projection");
    };
    let ExprKind::FunctionCall {
        over: Some(window), ..
    } = &expr.kind
    else {
        panic!("expected window function");
    };
    assert!(window.base.is_none());
    assert!(window.frame.is_none());
}

#[test]
fn case_expression_crosses_the_nim_messagepack_boundary() {
    let statements = Parser::parse_sql(
        &AlopexDialect,
        "SELECT CASE status WHEN 1 THEN 'one' WHEN 2 THEN 'two' ELSE 'other' END FROM events",
    )
    .expect("CASE expression should deserialize from Nim MessagePack");
    let StatementKind::Select(select) = &statements[0].kind else {
        panic!("expected SELECT");
    };
    let SelectItem::Expr { expr, .. } = &select.projection[0] else {
        panic!("expected expression projection");
    };
    let ExprKind::Case {
        operand,
        branches,
        else_expr,
    } = &expr.kind
    else {
        panic!("expected Case, got {:?}", expr.kind);
    };

    assert!(operand.is_some());
    assert_eq!(branches.len(), 2);
    assert!(else_expr.is_some());
    assert!(matches!(
        branches[0].when.kind,
        ExprKind::Literal {
            literal: Literal::Number(ref value)
        } if value == "1"
    ));
}

#[test]
fn exposes_the_nim_wire_contract_version() {
    assert_eq!(parser_contract_version(), "0.16.0");
}

#[test]
fn distinct_on_crosses_the_nim_messagepack_boundary() {
    let statements = Parser::parse_sql(
        &AlopexDialect,
        "SELECT DISTINCT ON (region) region FROM sales ORDER BY region",
    )
    .expect("DISTINCT ON should deserialize from Nim MessagePack");
    let StatementKind::Select(select) = &statements[0].kind else {
        panic!("expected SELECT");
    };
    assert_eq!(select.distinct_on.len(), 1);
    assert!(!select.distinct);
    assert!(matches!(
        select.distinct_on[0].kind,
        ExprKind::ColumnRef { ref column, .. } if column == "region"
    ));

    let plain = Parser::parse_sql(&AlopexDialect, "SELECT DISTINCT region FROM sales")
        .expect("plain DISTINCT parses");
    let StatementKind::Select(plain_select) = &plain[0].kind else {
        panic!("expected SELECT");
    };
    assert!(plain_select.distinct);
    assert!(plain_select.distinct_on.is_empty());
}

#[test]
fn group_by_items_cross_the_nim_messagepack_boundary() {
    use alopex_sql::GroupByItem;

    let statements = Parser::parse_sql(
        &AlopexDialect,
        "SELECT region FROM sales \
         GROUP BY region, ROLLUP(product), CUBE(amount), GROUPING SETS ((region), ())",
    )
    .expect("grouping-set items should deserialize from Nim MessagePack");
    let StatementKind::Select(select) = &statements[0].kind else {
        panic!("expected SELECT");
    };
    let group_by = select.group_by.as_ref().expect("GROUP BY items");
    assert_eq!(group_by.len(), 4);
    assert!(matches!(
        &group_by[0],
        GroupByItem::Expr { expr } if matches!(
            &expr.kind,
            ExprKind::ColumnRef { column, .. } if column == "region"
        )
    ));
    assert!(matches!(&group_by[1], GroupByItem::Rollup { exprs } if exprs.len() == 1));
    assert!(matches!(&group_by[2], GroupByItem::Cube { exprs } if exprs.len() == 1));
    let GroupByItem::GroupingSets { sets } = &group_by[3] else {
        panic!("expected GROUPING SETS item, got {:?}", group_by[3]);
    };
    assert_eq!(sets.len(), 2);
    assert_eq!(sets[0].len(), 1);
    assert!(sets[1].is_empty());

    let plain = Parser::parse_sql(&AlopexDialect, "SELECT region FROM sales GROUP BY region")
        .expect("plain GROUP BY parses");
    let StatementKind::Select(plain_select) = &plain[0].kind else {
        panic!("expected SELECT");
    };
    let plain_items = plain_select.group_by.as_ref().expect("GROUP BY items");
    assert!(matches!(&plain_items[0], GroupByItem::Expr { .. }));
}

#[test]
fn pre_distinct_on_payload_without_the_key_defaults_to_empty() {
    // A contract-0.10.0 payload has no distinct_on key; serde(default) must
    // decode it as an empty key list (one-way migration evidence only).
    let value = json!([{
        "kind": {
            "variant": "Select",
            "distinct": false,
            "projection": [{
                "variant": "Expr",
                "expr": {
                    "kind": {"variant": "Literal", "literal": {"variant": "Number", "value": "1"}},
                    "span": wire_span(1, 8, 1, 8),
                },
                "alias": null,
                "span": wire_span(1, 8, 1, 8),
            }],
            "from": [],
            "selection": null,
            "group_by": null,
            "having": null,
            "order_by": [],
            "limit": null,
            "offset": null,
            "span": wire_span(1, 1, 1, 8),
        },
        "span": wire_span(1, 1, 1, 8),
    }]);
    let payload = rmp_serde::to_vec_named(&value).expect("encode legacy payload");
    let statements: Vec<Statement> =
        rmp_serde::from_slice(&payload).expect("current Rust AST must decode the legacy payload");
    let StatementKind::Select(select) = &statements[0].kind else {
        panic!("expected SELECT");
    };
    assert!(select.distinct_on.is_empty());
    assert!(!select.limit_with_ties);
}

#[test]
fn top_level_set_operation_preserves_fetch_with_ties() {
    // The set-operation batch splitter re-assembles the query tail; dropping
    // limit_with_ties here would silently downgrade WITH TIES to ONLY.
    let statements = Parser::parse_sql(
        &AlopexDialect,
        "SELECT a FROM t UNION SELECT b FROM u ORDER BY a FETCH FIRST 1 ROW WITH TIES",
    )
    .expect("set operation with FETCH tail parses");
    let [statement] = statements.as_slice() else {
        panic!("expected one statement, got {statements:?}");
    };
    let StatementKind::Select(select) = &statement.kind else {
        panic!("expected SELECT, got {statement:?}");
    };
    assert_eq!(select.set_operations.len(), 1);
    assert_eq!(select.order_by.len(), 1);
    assert!(select.limit.is_some());
    assert!(select.limit_with_ties);

    let plain = Parser::parse_sql(
        &AlopexDialect,
        "SELECT a FROM t UNION SELECT b FROM u ORDER BY a FETCH FIRST 1 ROW ONLY",
    )
    .expect("plain FETCH tail parses");
    let StatementKind::Select(plain_select) = &plain[0].kind else {
        panic!("expected SELECT, got {plain:?}");
    };
    assert!(plain_select.limit.is_some());
    assert!(!plain_select.limit_with_ties);
}

#[test]
fn public_sql_boundary_emits_continuous_aggregate_after_contract_cutover() {
    let statements = Parser::parse_sql(&AlopexDialect, MINIMAL_CONTINUOUS_AGGREGATE_SQL)
        .expect("contract 0.16.0 must publicly emit the prepared continuous aggregate payload");
    let [statement] = statements.as_slice() else {
        panic!("expected one continuous aggregate statement, got {statements:?}");
    };
    let StatementKind::CreateContinuousAggregate(definition) = &statement.kind else {
        panic!("expected typed continuous aggregate statement, got {statement:?}");
    };

    assert_eq!(parser_contract_version(), "0.16.0");
    assert_eq!(definition.name, "cpu_hourly");
    assert_eq!(definition.query.from.len(), 1);
    assert_eq!(definition.options.len(), 2);
    assert_eq!(definition.options[0].key, "retention");
    assert_eq!(definition.options[0].value, "30d");
    assert_eq!(definition.options[1].key, "refresh_interval");
    assert_eq!(definition.options[1].value, "1h");
}

#[test]
fn staged_continuous_aggregate_decoder_preserves_every_owned_span() {
    let payload = encode_staged_continuous_aggregate(&staged_continuous_aggregate_value());
    let decoded =
        CreateContinuousAggregate::decode_staged_messagepack(&parser_contract_version(), &payload)
            .expect("bounded staged decoder should accept the canonical future shape");

    assert_eq!(decoded.name, "cpu_hourly");
    assert_eq!(
        (decoded.name_span.start.line, decoded.name_span.start.column),
        (1, 29)
    );
    assert_eq!(
        (decoded.name_span.end.line, decoded.name_span.end.column),
        (1, 38)
    );
    assert_eq!(
        (
            decoded.query.span.start.line,
            decoded.query.span.start.column
        ),
        (2, 1)
    );
    assert_eq!(
        (decoded.query.span.end.line, decoded.query.span.end.column),
        (2, 8)
    );
    let SelectItem::Expr {
        expr,
        alias: None,
        span,
    } = &decoded.query.projection[0]
    else {
        panic!("expected the synthetic query's literal projection");
    };
    assert_eq!((expr.span.start.line, expr.span.start.column), (2, 8));
    assert_eq!((expr.span.end.line, expr.span.end.column), (2, 8));
    assert_eq!((span.start.line, span.start.column), (2, 8));
    assert_eq!((span.end.line, span.end.column), (2, 8));
    assert_eq!(decoded.options.len(), 2);
    assert_eq!(decoded.options[0].key, "retention");
    assert_eq!(decoded.options[0].value, "30d");
    assert_eq!(decoded.options[0].key_span.start.column, 7);
    assert_eq!(decoded.options[0].key_span.end.column, 15);
    assert_eq!(decoded.options[0].value_span.start.column, 19);
    assert_eq!(decoded.options[0].value_span.end.column, 23);
    assert_eq!(decoded.options[0].span.start.column, 7);
    assert_eq!(decoded.options[0].span.end.column, 23);
    assert_eq!(decoded.options[1].key, "refresh_interval");
    assert_eq!(decoded.options[1].value, "1h");
    assert_eq!(decoded.options[1].key_span.start.column, 26);
    assert_eq!(decoded.options[1].key_span.end.column, 41);
    assert_eq!(decoded.options[1].value_span.start.column, 45);
    assert_eq!(decoded.options[1].value_span.end.column, 48);
    assert_eq!(decoded.options[1].span.start.column, 26);
    assert_eq!(decoded.options[1].span.end.column, 48);
    assert_eq!((decoded.span.start.line, decoded.span.start.column), (1, 1));
    assert_eq!((decoded.span.end.line, decoded.span.end.column), (4, 1));

    let reencoded = serde_json::to_value(&decoded)
        .expect("the typed payload should preserve its named nested Select wire shape");
    assert_eq!(reencoded["query"]["variant"], "Select");
    assert_eq!(
        reencoded["query"]["projection"][0]["expr"]["span"],
        wire_span(2, 8, 2, 8)
    );
}

#[test]
fn staged_decoder_rejects_linked_contract_mismatch_before_payload_preflight() {
    let oversized_invalid_payload = vec![0; MAX_MESSAGEPACK_PAYLOAD_BYTES + 1];
    let error = CreateContinuousAggregate::decode_staged_messagepack(
        "test-mismatched-contract",
        &oversized_invalid_payload,
    )
    .expect_err("linked contract mismatch must win before payload handling");
    let rendered = error.to_string();
    assert!(rendered.contains("linked Nim parser contract test-mismatched-contract"));
    assert!(!rendered.contains("MessagePack payload exceeds"));
}

#[test]
fn staged_decoder_rejects_resource_and_unknown_field_bombs() {
    let linked_contract = parser_contract_version();

    let oversized = vec![0; MAX_MESSAGEPACK_PAYLOAD_BYTES + 1];
    let size_error =
        CreateContinuousAggregate::decode_staged_messagepack(&linked_contract, &oversized)
            .expect_err("payload over the byte ceiling must fail before deserialization");
    assert!(
        size_error
            .to_string()
            .contains("MessagePack payload exceeds 1048576 bytes")
    );

    let declared_huge_array = [0xdd, 0xff, 0xff, 0xff, 0xff];
    let collection_error = CreateContinuousAggregate::decode_staged_messagepack(
        &linked_contract,
        &declared_huge_array,
    )
    .expect_err("huge declared collection must fail before allocation");
    assert!(
        collection_error
            .to_string()
            .contains("MessagePack collection limit")
    );

    let mut depth_bomb = Value::Null;
    for _ in 0..140 {
        depth_bomb = json!([depth_bomb]);
    }
    let depth_payload = encode_staged_continuous_aggregate(&depth_bomb);
    let depth_error =
        CreateContinuousAggregate::decode_staged_messagepack(&linked_contract, &depth_payload)
            .expect_err("deep input must fail during bounded preflight");
    assert!(
        depth_error
            .to_string()
            .contains("MessagePack nesting exceeds 128")
    );

    let mut unknown = staged_continuous_aggregate_value();
    unknown["kind"]["unexpected"] = Value::Array(vec![json!(0); 256]);
    let unknown_payload = encode_staged_continuous_aggregate(&unknown);
    let unknown_error =
        CreateContinuousAggregate::decode_staged_messagepack(&linked_contract, &unknown_payload)
            .expect_err("unknown fields must be rejected, not recursively retained");
    assert!(
        unknown_error
            .to_string()
            .contains("unknown field `unexpected`")
    );
}

#[test]
fn staged_decoder_rejects_inconsistent_outer_and_kind_spans() {
    let mut inconsistent = staged_continuous_aggregate_value();
    inconsistent["span"] = wire_span(9, 9, 9, 9);
    let payload = encode_staged_continuous_aggregate(&inconsistent);
    let error =
        CreateContinuousAggregate::decode_staged_messagepack(&parser_contract_version(), &payload)
            .expect_err("the outer statement span must not be silently discarded");
    assert!(error.to_string().contains("matching outer and kind spans"));
}

#[test]
fn parses_sql_ts_interval_as_a_distinct_literal() {
    let statements = Parser::parse_sql(&AlopexDialect, "SELECT NOW() - INTERVAL '24 hours'")
        .expect("SQL-TS INTERVAL should cross the Nim MessagePack boundary");
    let StatementKind::Select(select) = &statements[0].kind else {
        panic!("expected Select");
    };
    assert!(matches!(
        &select.projection[0],
        SelectItem::Expr {
            expr:
                alopex_sql::Expr {
                    kind: ExprKind::BinaryOp { right, .. },
                    ..
                },
            ..
        } if matches!(
            &right.kind,
            ExprKind::Literal {
                literal: Literal::Interval(value)
            } if value == "24 hours"
        )
    ));
}

#[test]
fn parse_join_subquery_and_vector_variants_from_nim() {
    let sql = "\
        CREATE TABLE docs (id INT PRIMARY KEY, embedding VECTOR(3, COSINE));\
        SELECT d.id, [0.1, 0.2, 0.3] FROM docs d INNER JOIN tags t ON d.id = t.doc_id \
        WHERE EXISTS (SELECT 1 FROM tags) AND d.id IN (SELECT doc_id FROM tags) \
        ORDER BY d.id NULLS LAST LIMIT 10";

    let statements = Parser::parse_sql(&AlopexDialect, sql).expect("Nim parser should parse SQL");
    assert_eq!(statements.len(), 2);

    match &statements[0].kind {
        StatementKind::CreateTable(table) => {
            assert_eq!(table.name, "docs");
            assert!(matches!(
                table.columns[1].data_type,
                DataType::Vector {
                    dimension: 3,
                    metric: Some(VectorMetric::Cosine)
                }
            ));
        }
        other => panic!("expected CreateTable, got {other:?}"),
    }

    let StatementKind::Select(select) = &statements[1].kind else {
        panic!("expected Select");
    };
    assert!(matches!(
        &select.from[0],
        FromItem::Join {
            join_type: JoinType::Inner,
            condition: Some(_),
            ..
        }
    ));
    assert!(matches!(
        &select.projection[1],
        SelectItem::Expr {
            expr:
                alopex_sql::Expr {
                    kind: ExprKind::VectorLiteral { values },
                    ..
                },
            ..
        } if values == &vec![0.1, 0.2, 0.3]
    ));
    assert!(matches!(
        select.selection.as_ref().map(|expr| &expr.kind),
        Some(ExprKind::BinaryOp { .. })
    ));
    assert!(matches!(
        select.limit.as_ref().map(|expr| &expr.kind),
        Some(ExprKind::Literal {
            literal: Literal::Number(value)
        }) if value == "10"
    ));
}

#[test]
fn join_markers_are_applied_inside_recursive_set_operation_terms() {
    let sql = "WITH RECURSIVE ancestors AS (\
         SELECT id, parent_id FROM employees WHERE id = 3 \
         UNION ALL \
         SELECT employees.id, employees.parent_id \
         FROM employees JOIN ancestors ON employees.id = ancestors.parent_id\
     ) SELECT id FROM ancestors";

    let statements = Parser::parse_sql(&AlopexDialect, sql)
        .expect("join metadata in the recursive term must cross the Nim boundary");
    let StatementKind::Select(outer) = &statements[0].kind else {
        panic!("expected outer SELECT");
    };
    let cte = &outer.with.as_ref().expect("expected WITH clause").ctes[0];
    let QueryBody::Select(body) = cte.query.as_ref() else {
        panic!("expected CTE SELECT");
    };

    let QueryBody::Select(recursive_term) = body.set_operations[0].right.as_ref() else {
        panic!("expected SELECT recursive term");
    };

    assert!(matches!(
        &recursive_term.from[0],
        FromItem::Join {
            join_type: JoinType::Inner,
            natural: false,
            condition: Some(_),
            ..
        }
    ));
}

#[test]
fn parse_multi_row_insert_without_column_list_from_nim() {
    // issue #40: 「カラムリスト省略 × 多行 VALUES」で先頭行が列リストと
    // 誤判別され、FieldDefect が FFI から漏れて ALOPEX-P001 になっていた。
    let statements = Parser::parse_sql(&AlopexDialect, "INSERT INTO t1 VALUES (1, 'a'), (2, 'b')")
        .expect("multi-row INSERT without column list should parse");
    assert_eq!(statements.len(), 1);
    let StatementKind::Insert(insert) = &statements[0].kind else {
        panic!("expected Insert, got {:?}", statements[0].kind);
    };
    assert_eq!(insert.table, "t1");
    assert!(insert.columns.is_none());
    let InsertSource::Values { values } = &insert.source else {
        panic!("expected VALUES source");
    };
    assert_eq!(values.len(), 2);
    assert_eq!(values[0].len(), 2);
    assert!(matches!(
        &values[0][0].kind,
        ExprKind::Literal {
            literal: Literal::Number(value)
        } if value == "1"
    ));
    assert!(matches!(
        &values[1][1].kind,
        ExprKind::Literal {
            literal: Literal::String(value)
        } if value == "b"
    ));
}

#[test]
fn parse_multi_row_all_string_insert_without_column_list_from_nim() {
    // issue #40: 先頭行が全て文字列だと例外にならず、先頭行が列リストへ
    // 静かに誤変換される (columns = ["a","b"], values = [["c","d"]])。
    let statements = Parser::parse_sql(
        &AlopexDialect,
        "INSERT INTO t1 VALUES ('a', 'b'), ('c', 'd')",
    )
    .expect("all-string multi-row INSERT should parse");
    let StatementKind::Insert(insert) = &statements[0].kind else {
        panic!("expected Insert, got {:?}", statements[0].kind);
    };
    assert!(insert.columns.is_none());
    let InsertSource::Values { values } = &insert.source else {
        panic!("expected VALUES source");
    };
    assert_eq!(values.len(), 2);
    assert!(matches!(
        &values[0][0].kind,
        ExprKind::Literal {
            literal: Literal::String(value)
        } if value == "a"
    ));
}

#[test]
fn parse_insert_with_values_query_from_nim() {
    let statements = Parser::parse_sql(
        &AlopexDialect,
        "INSERT INTO t1 WITH seed(n) AS (VALUES (1)) VALUES (2), (3)",
    )
    .expect("INSERT query source ending in VALUES should parse");
    let StatementKind::Insert(insert) = &statements[0].kind else {
        panic!("expected Insert, got {:?}", statements[0].kind);
    };
    let InsertSource::Query { query } = &insert.source else {
        panic!("expected Query source");
    };
    let QueryBody::Values(values) = query.as_ref() else {
        panic!("expected VALUES query body");
    };
    assert_eq!(values.rows.len(), 2);
    assert_eq!(values.with.as_ref().expect("WITH clause").ctes.len(), 1);
}

#[test]
fn parse_lateral_table_function_and_alias_columns_from_nim() {
    let statements = Parser::parse_sql(
        &AlopexDialect,
        "SELECT * FROM sales AS p(a, b) CROSS JOIN LATERAL (SELECT p.a AS x) AS l",
    )
    .expect("Nim parser should parse LATERAL");
    let StatementKind::Select(select) = &statements[0].kind else {
        panic!("expected Select");
    };
    let FromItem::Join { left, right, .. } = &select.from[0] else {
        panic!("expected a join");
    };
    let FromItem::Table {
        name,
        alias,
        columns,
        ..
    } = left.as_ref()
    else {
        panic!("expected a base table on the left");
    };
    assert_eq!(name, "sales");
    assert_eq!(alias.as_deref(), Some("p"));
    assert_eq!(columns, &["a".to_string(), "b".to_string()]);
    assert!(matches!(
        right.as_ref(),
        FromItem::Derived { lateral: true, alias: Some(alias), .. } if alias == "l"
    ));

    let functions = Parser::parse_sql(
        &AlopexDialect,
        "SELECT * FROM d, LATERAL UNNEST(d.emb) AS u(component)",
    )
    .expect("Nim parser should parse a table function");
    let StatementKind::Select(select) = &functions[0].kind else {
        panic!("expected Select");
    };
    let FromItem::Join { right, .. } = &select.from[0] else {
        panic!("expected a join");
    };
    let FromItem::Function {
        name,
        args,
        alias,
        columns,
        lateral,
        ..
    } = right.as_ref()
    else {
        panic!("expected a table function");
    };
    // The bridge folds bare identifiers to lowercase before parsing, exactly
    // as PostgreSQL does; the planner resolves the name case-insensitively.
    assert_eq!(name, "unnest");
    assert_eq!(args.len(), 1);
    assert_eq!(alias.as_deref(), Some("u"));
    assert_eq!(columns, &["component".to_string()]);
    assert!(lateral);

    // A plain table and a plain derived table keep the absent-clause defaults.
    let plain = Parser::parse_sql(&AlopexDialect, "SELECT * FROM sales")
        .expect("Nim parser should parse a plain table");
    let StatementKind::Select(select) = &plain[0].kind else {
        panic!("expected Select");
    };
    assert!(matches!(
        &select.from[0],
        FromItem::Table { columns, alias: None, .. } if columns.is_empty()
    ));
}

#[test]
fn parse_derived_and_quantified_subqueries_from_nim() {
    let sql = "\
        SELECT x.id FROM (SELECT id FROM docs) x \
        WHERE x.id > ANY (SELECT doc_id FROM tags)";
    let statements = Parser::parse_sql(&AlopexDialect, sql).expect("Nim parser should parse SQL");
    let StatementKind::Select(select) = &statements[0].kind else {
        panic!("expected Select");
    };
    assert!(
        matches!(&select.from[0], FromItem::Derived { alias: Some(alias), .. } if alias == "x")
    );
    assert!(matches!(
        select.selection.as_ref().map(|expr| &expr.kind),
        Some(ExprKind::Quantified { .. })
    ));
}

#[test]
fn aggregate_filter_and_within_group_cross_the_nim_messagepack_boundary() {
    let statements = Parser::parse_sql(
        &AlopexDialect,
        "SELECT COUNT(*) FILTER (WHERE v > 10), \
         STRING_AGG(name, ',' ORDER BY v DESC), \
         PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY v) FROM t",
    )
    .expect("issue #148 clauses should deserialize from Nim MessagePack");
    let StatementKind::Select(select) = &statements[0].kind else {
        panic!("expected SELECT");
    };
    let call = |index: usize| match &select.projection[index] {
        SelectItem::Expr { expr, .. } => &expr.kind,
        other => panic!("expected expression projection, got {other:?}"),
    };

    let ExprKind::FunctionCall {
        star,
        filter,
        order_by,
        within_group,
        ..
    } = call(0)
    else {
        panic!("expected FunctionCall");
    };
    assert!(*star);
    assert!(filter.is_some());
    assert!(order_by.is_empty());
    assert!(within_group.is_empty());

    let ExprKind::FunctionCall {
        args,
        filter,
        order_by,
        within_group,
        ..
    } = call(1)
    else {
        panic!("expected FunctionCall");
    };
    assert_eq!(args.len(), 2);
    assert!(filter.is_none());
    assert_eq!(order_by.len(), 1);
    assert_eq!(order_by[0].asc, Some(false));
    assert!(within_group.is_empty());

    let ExprKind::FunctionCall {
        name,
        args,
        within_group,
        order_by,
        ..
    } = call(2)
    else {
        panic!("expected FunctionCall");
    };
    assert!(name.eq_ignore_ascii_case("percentile_disc"));
    assert_eq!(args.len(), 1);
    assert!(order_by.is_empty());
    assert_eq!(within_group.len(), 1);
}

#[test]
fn clause_free_function_calls_still_decode_without_the_new_keys() {
    // The Nim writer keeps the historical 6-key FunctionCall map when no
    // aggregate clause is present; serde defaults must fill the new fields.
    let statements = Parser::parse_sql(&AlopexDialect, "SELECT COUNT(*) FROM t")
        .expect("clause-free calls stay decodable");
    let StatementKind::Select(select) = &statements[0].kind else {
        panic!("expected SELECT");
    };
    let SelectItem::Expr { expr, .. } = &select.projection[0] else {
        panic!("expected expression projection");
    };
    let ExprKind::FunctionCall {
        filter,
        order_by,
        within_group,
        ..
    } = &expr.kind
    else {
        panic!("expected FunctionCall");
    };
    assert!(filter.is_none());
    assert!(order_by.is_empty());
    assert!(within_group.is_empty());
}
