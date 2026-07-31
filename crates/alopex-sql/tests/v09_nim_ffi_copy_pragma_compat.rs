#![cfg(target_os = "linux")]

use alopex_sql::{
    AlopexDialect, Parser, PragmaValue, StatementKind, TransactionCopyFormat, TransactionSqlRow,
    TransactionSqlStatus, classify_transaction_sql, classify_transaction_statement,
    parser_contract_version, preflight_transaction_copy, preflight_transaction_sql,
};

#[test]
fn nim_ffi_retains_the_pragma_ast_and_error_recovery_contract() {
    assert_eq!(parser_contract_version(), "0.3.0");

    for (sql, expected_name, expected_value) in [
        (
            "PRAGMA cache_size = 16",
            "cache_size",
            Some(PragmaValue::Int(16)),
        ),
        (
            "PRAGMA memory_limit = '100MiB'",
            "memory_limit",
            Some(PragmaValue::Text("100MiB".to_string())),
        ),
        (
            "PRAGMA memory_limit = 8192",
            "memory_limit",
            Some(PragmaValue::Int(8192)),
        ),
        ("PRAGMA memory_limit", "memory_limit", None),
        ("PRAGMA io_stats", "io_stats", None),
    ] {
        let statement = Parser::parse_sql(&AlopexDialect, sql)
            .unwrap_or_else(|error| panic!("{sql} must cross the Nim FFI: {error}"))
            .pop()
            .expect("one PRAGMA statement");
        assert!(matches!(
            statement.kind,
            StatementKind::Pragma { name, value }
                if name == expected_name && value == expected_value
        ));
    }

    assert!(Parser::parse_sql(&AlopexDialect, "PRAGMA cache_size = TRUE").is_err());
    Parser::parse_sql(&AlopexDialect, "PRAGMA io_stats")
        .expect("an FFI parse error must not poison the following parse");
}

#[test]
fn copy_and_pragma_never_bypass_their_pre_execution_boundaries() {
    for (sql, format, row) in [
        (
            "COPY records FROM 'records.csv' CSV",
            TransactionCopyFormat::Csv,
            TransactionSqlRow::CopyCsv,
        ),
        (
            "COPY records FROM 'records.parquet' PARQUET",
            TransactionCopyFormat::Parquet,
            TransactionSqlRow::CopyParquet,
        ),
    ] {
        let classified = classify_transaction_sql(sql);
        assert_eq!(classified.status, TransactionSqlStatus::SingleRange);
        assert_eq!(classified.primary_row.expect("COPY row").row, row);
        let unresolved = preflight_transaction_sql(sql)
            .expect_err("raw COPY must resolve a range before execution");
        assert_eq!(
            unresolved.error.code,
            "transaction_sql_single_range_required"
        );

        assert!(preflight_transaction_copy(format, 1).is_ok());
        for target_range_count in [0, 2] {
            let rejected = preflight_transaction_copy(format, target_range_count)
                .expect_err("COPY must not enlist zero or multiple ranges");
            assert_eq!(
                rejected.classification.status,
                TransactionSqlStatus::PreExecutionReject
            );
            assert_eq!(rejected.error.code, "copy_requires_single_range");
        }
    }

    for sql in [
        "COPY records FROM records.csv CSV",
        "COPY records FROM 'records.csv' CSV trailing",
        "COPY records FROM 'records.csv' CSV PARQUET",
        "COPY records FROM records.csv CSV 'decoy'",
        "COPY records FROM 'records.csv' FROM other CSV",
        "COPY records FROM 'records.json' JSON",
    ] {
        let rejected = preflight_transaction_sql(sql)
            .expect_err("malformed or unapproved COPY must reject before execution");
        assert_eq!(
            rejected.classification.status,
            TransactionSqlStatus::PreExecutionReject
        );
        assert_eq!(rejected.error.code, "copy_syntax_not_in_transaction_matrix");
    }

    for (sql, row) in [
        ("PRAGMA cache_size = 8", TransactionSqlRow::PragmaCacheSize),
        (
            "PRAGMA memory_limit = '8MiB'",
            TransactionSqlRow::PragmaMemoryLimit,
        ),
        (
            "PRAGMA memory_limit = 8192",
            TransactionSqlRow::PragmaMemoryLimit,
        ),
        ("PRAGMA memory_limit", TransactionSqlRow::PragmaMemoryLimit),
        ("PRAGMA io_stats", TransactionSqlRow::PragmaIoStats),
    ] {
        let statement = Parser::parse_sql(&AlopexDialect, sql)
            .unwrap_or_else(|error| panic!("{sql} must parse locally: {error}"))
            .pop()
            .expect("one PRAGMA statement");
        let classified = classify_transaction_statement(&statement);
        assert_eq!(classified.status, TransactionSqlStatus::LocalOnly);
        assert_eq!(classified.primary_row.expect("PRAGMA row").row, row);

        let rejected = preflight_transaction_sql(sql)
            .expect_err("a PRAGMA must never open a distributed participant");
        assert_eq!(
            rejected.classification.status,
            TransactionSqlStatus::LocalOnly
        );
        assert_eq!(rejected.error.code, "transaction_sql_local_only");
    }
}
