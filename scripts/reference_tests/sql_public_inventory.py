"""Generate/check the public SQL surface from its owning source registries."""
from __future__ import annotations

import argparse
import json
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]

EVIDENCE_BY_CLAIM = {
    "portable SELECT/null/order/coercion": "scripts/reference_tests/sql_v0811_differential.py#main",
    "parser/lexer": "crates/alopex-sql/tests/nim_bridge_test.rs#parses_v0811_relational_sql_across_the_nim_boundary",
    "transactions/savepoints": "crates/alopex-embedded/tests/sql_integration.rs#sql_integration_transaction_rollback_discards_sql_changes",
    "prepared statements": "crates/alopex-embedded/tests/prepared_statement.rs#prepared_statement_supports_null_rebind_reset_and_finalize",
    "EXPLAIN JSON": "crates/alopex-embedded/tests/explain.rs#json_contract_is_versioned_complete_and_redacts_literals_and_binds",
    "RETURNING": "scripts/reference_tests/sql_v0811_advanced_dml.py#main;scripts/reference_tests/sql_v0811_advanced_dml.json#returning",
    "ON CONFLICT": "scripts/reference_tests/sql_v0811_advanced_dml.py#main;scripts/reference_tests/sql_v0811_advanced_dml.json#on-conflict",
    "CHECK/FK constraints": "crates/alopex-embedded/tests/constraints.rs#check_and_composite_foreign_keys_follow_sql_null_semantics",
    "COPY": "crates/alopex-embedded/tests/copy_sql.rs#copy_reader_writer_streams_share_csv_quoting_and_atomicity",
    "SERIAL/IDENTITY/SEQUENCE": "crates/alopex-embedded/tests/sequence.rs#sequence_ddl_is_transactional_and_persistent",
    "VIEW/ALTER TABLE/TRUNCATE": "crates/alopex-embedded/tests/schema_evolution.rs#alter_and_truncate_rollback_atomically",
    "MERGE/UPDATE FROM/DELETE USING": "scripts/reference_tests/sql_v0811_advanced_dml.py#main;scripts/reference_tests/sql_v0811_advanced_dml.json#update-from;scripts/reference_tests/sql_v0811_advanced_dml.json#delete-using;scripts/reference_tests/sql_v0811_advanced_dml.json#merge",
    "VECTOR/HNSW": "crates/alopex-sql/tests/hnsw_sql_tests.rs#create_insert_and_search_hnsw_index",
    "PRAGMA": "crates/alopex-embedded/tests/sql_integration.rs#sql_integration_database_execute_sql_pragma_uses_store_path",
    "information_schema": "crates/alopex-embedded/tests/metadata.rs#portable_metadata_surfaces_have_exact_schemas_and_values",
    "streaming query": "crates/alopex-py/tests/test_sql.py#test_execute_sql_select_iteration_matches_guide_usage",
}

TRANSACTION_STATEMENTS = {
    "Begin", "Commit", "ReleaseSavepoint", "Rollback", "RollbackToSavepoint",
    "Savepoint", "SetTransaction",
}


def claim_for(row: dict[str, str]) -> str:
    surface, api = row["surface"], row["api"]
    name = api.rsplit(".", 1)[-1]
    if surface == "scalar":
        return "portable SELECT/null/order/coercion"
    if surface == "metadata":
        return "information_schema"
    if surface == "statement":
        if name in TRANSACTION_STATEMENTS:
            return "transactions/savepoints"
        if name == "Explain":
            return "EXPLAIN JSON"
        if name == "Copy":
            return "COPY"
        if name in {"CreateSequence", "AlterSequence", "DropSequence"}:
            return "SERIAL/IDENTITY/SEQUENCE"
        if name in {"AlterTable", "CreateView", "DropView", "Truncate"}:
            return "VIEW/ALTER TABLE/TRUNCATE"
        if name in {"Merge", "Update", "Delete"}:
            return "MERGE/UPDATE FROM/DELETE USING"
        if name == "Insert":
            return "RETURNING"
        if name == "CreateTable":
            return "CHECK/FK constraints"
        if name == "CreateIndex":
            return "VECTOR/HNSW"
        if name == "Pragma":
            return "PRAGMA"
        if name in {"Select", "Values"}:
            return "portable SELECT/null/order/coercion"
        return "parser/lexer"
    if "copy_" in api:
        return "COPY"
    if "list_sequences" in api:
        return "SERIAL/IDENTITY/SEQUENCE"
    if api.endswith(".prepare"):
        return "prepared statements"
    if surface == "embedded" and api == "Database.execute_sql":
        return "ON CONFLICT"
    if surface == "server":
        return "streaming query"
    if api == "Parser.parse_sql":
        return "parser/lexer"
    return "portable SELECT/null/order/coercion"


def materialize(payload: dict[str, object]) -> list[dict[str, object]]:
    claims = {str(entry["api"]): entry for entry in payload.get("entries", [])}
    rows = []
    for source in inventory():
        claim = claim_for(source)
        entry = claims.get(claim)
        if entry is None:
            raise RuntimeError(f"unknown SQL claim: {claim}")
        rows.append({
            **source,
            "claim": claim,
            "status": entry["status"],
            "reference": entry.get("reference", "alopex"),
            "evidence": EVIDENCE_BY_CLAIM[claim],
            "issue": entry.get("issue", 307),
            **({"performance_contract": entry["performance_contract"]} if "performance_contract" in entry else {}),
            **({"performance_evidence": entry["performance_evidence"]} if "performance_evidence" in entry else {}),
        })
    return rows


def inventory() -> list[dict[str, str]]:
    ast = (ROOT / "crates/alopex-sql/src/ast/mod.rs").read_text(encoding="utf-8")
    body = ast.split("pub enum StatementKind {", 1)[1].split("\n}", 1)[0]
    statements = sorted(
        set(re.findall(r"^    ([A-Z][A-Za-z0-9]+)(?:\s*[({,])", body, re.MULTILINE))
    )
    scalar = (ROOT / "crates/alopex-sql/src/scalar/mod.rs").read_text(encoding="utf-8")
    functions = sorted(
        set(re.findall(r"sig(?:_meta)?\(\s*\"([a-z0-9_]+)\"", scalar))
    )
    planner = (ROOT / "crates/alopex-sql/src/planner/mod.rs").read_text(encoding="utf-8")
    metadata = sorted(set(re.findall(r'"(information_schema\.[a-z_]+)"', planner)))
    executor = (ROOT / "crates/alopex-sql/src/executor/mod.rs").read_text(encoding="utf-8")
    embedded = (ROOT / "crates/alopex-embedded/src/sql_api.rs").read_text(encoding="utf-8")
    pyi = (ROOT / "crates/alopex-py/python/alopex/_alopex.pyi").read_text(encoding="utf-8")
    server = (ROOT / "crates/alopex-server/src/http/sql.rs").read_text(encoding="utf-8")
    rows = [
        {"surface": "statement", "api": f"statement.{name}"} for name in statements
    ]
    rows.extend({"surface": "scalar", "api": f"scalar.{name}"} for name in functions)
    rows.extend({"surface": "metadata", "api": name} for name in metadata)
    rows.extend(
        {"surface": surface, "api": api}
        for surface, api in (
            ("Rust", "Parser.parse_sql"),
            ("Rust", "Executor.execute"),
            ("embedded", "Database.execute_sql"),
            ("embedded", "Database.prepare"),
            ("embedded", "SqlSession.execute"),
            ("Python", "Python.Database.execute_sql"),
            ("Python", "Python.Database.prepare"),
            ("Python", "Python.Transaction.execute_sql"),
            ("CLI", "CLI.sql"),
            ("server", "HTTP.sql"),
            ("server", "gRPC.sql"),
        )
    )
    for surface, api, source, symbol in (
        ("Rust", "Executor.copy_from_csv_reader", executor, "copy_from_csv_reader"),
        ("Rust", "Executor.copy_to_csv_writer", executor, "copy_to_csv_writer"),
        ("embedded", "Database.copy_from_csv_reader", embedded, "copy_from_csv_reader"),
        ("embedded", "Database.copy_to_csv_writer", embedded, "copy_to_csv_writer"),
        ("embedded", "Database.list_sequences", embedded, "list_sequences"),
        ("Python", "Python.Database.copy_from_csv", pyi, "copy_from_csv"),
        ("Python", "Python.Database.copy_to_csv", pyi, "copy_to_csv"),
        ("Python", "Python.Database.list_sequences", pyi, "list_sequences"),
        ("server", "server.COPY_local_only_rejection", server, "uses_remote_copy"),
    ):
        if symbol not in source:
            raise RuntimeError(f"SQL public surface disappeared: {api}")
        rows.append({"surface": surface, "api": api})
    return sorted(rows, key=lambda row: (row["surface"], row["api"]))


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("ledger", type=Path)
    parser.add_argument("--write", action="store_true")
    args = parser.parse_args()
    payload = json.loads(args.ledger.read_text(encoding="utf-8"))
    expected = materialize(payload)
    if args.write:
        payload["public_api"] = expected
        args.ledger.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
        )
        return 0
    if payload.get("public_api") != expected:
        raise RuntimeError("SQL public inventory is stale; run with --write")
    print(f"validated {len(expected)} SQL public surface rows")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
