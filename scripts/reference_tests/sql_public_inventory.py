"""Generate/check the public SQL surface from its owning source registries."""
from __future__ import annotations

import argparse
import json
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


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
    expected = inventory()
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
