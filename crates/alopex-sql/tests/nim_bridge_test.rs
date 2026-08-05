#![cfg(target_os = "linux")]

use alopex_sql::{
    AlopexDialect, CreateContinuousAggregate, DataType, ExprKind, FromItem, InsertSource, JoinType,
    Literal, Parser, ParserError, SelectItem, StatementKind, VectorMetric, parser_contract_version,
};
use serde_json::{Value, json};

const MAX_SQL_INPUT_BYTES: usize = 1_048_576;
const MAX_MESSAGEPACK_PAYLOAD_BYTES: usize = 1_048_576;

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
fn exposes_the_nim_wire_contract_version() {
    assert_eq!(parser_contract_version(), "0.3.0");
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
