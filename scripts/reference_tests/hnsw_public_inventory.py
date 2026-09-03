"""Generate/check the public HNSW surface from Rust, Python, SQL, and docs."""
from __future__ import annotations

import argparse
import json
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
HNSWLIB = "nmslib/hnswlib@3f3429661187e4c24a490a0f148fc6bc89042b3d"
PGVECTOR = "pgvector/pgvector@2627c5ff775ae6d7aef0c430121ccf857842d2f2"


def rust_methods(source: str, predicate=lambda name: True) -> list[tuple[str, str]]:
    owner = "unknown"
    methods: list[tuple[str, str]] = []
    for line in source.splitlines():
        if match := re.match(r"^impl(?:<[^>]+>)?\s+(\w+)", line):
            owner = match.group(1)
        if (match := re.match(r"^    pub fn (\w+)", line)) and predicate(match.group(1)):
            methods.append((owner, match.group(1)))
    return methods


def row(surface: str, api: str, evidence: str) -> dict[str, object]:
    extension = any(word in api.lower() for word in ("compact", "stats", "callback"))
    result: dict[str, object] = {
        "surface": surface,
        "api": api,
        "status": "alopex-extension" if extension else "known-performance-divergence",
        "reference": "alopex" if extension else (PGVECTOR if surface in {"embedded", "SQL"} else HNSWLIB),
        "evidence": evidence,
        "issue": 306,
    }
    if not extension:
        result.update(
            performance_contract="hnsw-pareto-v1",
            performance_evidence="hnsw-pareto",
        )
    return result


def inventory() -> list[dict[str, object]]:
    core_mod = (ROOT / "crates/alopex-core/src/vector/hnsw/mod.rs").read_text(encoding="utf-8")
    core_types = (ROOT / "crates/alopex-core/src/vector/hnsw/types.rs").read_text(encoding="utf-8")
    embedded = (ROOT / "crates/alopex-embedded/src/lib.rs").read_text(encoding="utf-8")
    py_source = (ROOT / "crates/alopex-py/src/embedded/database.rs").read_text(encoding="utf-8")
    py_transaction = (ROOT / "crates/alopex-py/src/embedded/transaction.rs").read_text(encoding="utf-8")
    pyi = (ROOT / "crates/alopex-py/python/alopex/_alopex.pyi").read_text(encoding="utf-8")
    sql = (ROOT / "crates/alopex-sql/src/executor/hnsw_bridge.rs").read_text(encoding="utf-8")
    docs = (ROOT / "docs/parity/README.md").read_text(encoding="utf-8")

    rows = [
        row("Rust", f"Rust.{name}", "crates/alopex-core/src/vector/hnsw/types.rs")
        for name in re.findall(r"^pub (?:struct|enum) (\w+)", core_types, re.MULTILINE)
    ]
    rows.extend(
        row("Rust", f"Rust.{owner}.{name}", "crates/alopex-core/src/vector/hnsw/types.rs")
        for owner, name in rust_methods(core_types)
    )
    rows.append(row("Rust", "Rust.HnswIndex", "crates/alopex-core/src/vector/hnsw/mod.rs"))
    rows.extend(
        row("Rust", f"Rust.{owner}.{name}", "crates/alopex-core/src/vector/hnsw/mod.rs")
        for owner, name in rust_methods(core_mod)
    )
    rows.extend(
        row("embedded", f"embedded.{owner}.{name}", "crates/alopex-embedded/src/lib.rs")
        for owner, name in rust_methods(embedded, lambda name: "hnsw" in name)
    )

    stub_types = re.findall(r"^class ((?:Hnsw|Search)[A-Za-z0-9_]+)", pyi, re.MULTILINE)
    rows.extend(
        row("Python", f"Python.{name}", "crates/alopex-py/python/alopex/_alopex.pyi")
        for name in stub_types
    )
    stub_methods = set(re.findall(r"^    def (\w*hnsw\w*)", pyi, re.MULTILINE))
    runtime_methods = set(
        re.findall(r"^    fn (\w*hnsw\w*)", py_source + "\n" + py_transaction, re.MULTILINE)
    )
    if stub_methods != runtime_methods:
        raise RuntimeError(
            f"Python HNSW runtime/.pyi mismatch: stub-only={sorted(stub_methods-runtime_methods)}, "
            f"runtime-only={sorted(runtime_methods-stub_methods)}"
        )
    rows.extend(
        row("Python", f"Python.method.{name}", "crates/alopex-py/src/embedded;crates/alopex-py/python/alopex/_alopex.pyi")
        for name in sorted(runtime_methods)
    )

    sql_contracts = {
        "SQL.CREATE_INDEX_USING_HNSW": "create_index",
        "SQL.DROP_INDEX_USING_HNSW": "drop_index",
        "SQL.HNSW_INSERT_SYNC": "on_insert",
        "SQL.HNSW_UPDATE_SYNC": "on_update",
        "SQL.HNSW_DELETE_SYNC": "on_delete",
        "SQL.HNSW_KNN_SEARCH": "search_knn",
    }
    for api, symbol in sql_contracts.items():
        if not re.search(rf"fn {symbol}\b", sql):
            raise RuntimeError(f"SQL HNSW surface disappeared: {symbol}")
        rows.append(row("SQL", api, "crates/alopex-sql/src/executor/hnsw_bridge.rs"))
    if "HNSW" not in docs:
        raise RuntimeError("public HNSW documentation disappeared")
    rows.append(row("docs", "docs.HNSW_contract", "docs/parity/README.md"))

    names = [str(item["api"]) for item in rows]
    if len(names) != len(set(names)):
        raise RuntimeError("generated HNSW inventory contains duplicate APIs")
    return sorted(rows, key=lambda item: (str(item["surface"]), str(item["api"])))


def validate_runtime(expected: list[dict[str, object]]) -> None:
    import alopex

    runtime = {
        *(f"Python.{name}" for name in ("HnswConfig", "HnswStats", "SearchResult", "SearchStats") if hasattr(alopex, name)),
        *(f"Python.method.{name}" for owner in (alopex.Database, alopex.Transaction) for name in dir(owner) if "hnsw" in name),
    }
    static = {str(item["api"]) for item in expected if item["surface"] == "Python"}
    if runtime != static:
        raise RuntimeError(
            f"Python HNSW runtime inventory mismatch: static-only={sorted(static-runtime)}, "
            f"runtime-only={sorted(runtime-static)}"
        )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("ledger", type=Path)
    parser.add_argument("--write", action="store_true")
    parser.add_argument("--runtime", action="store_true")
    args = parser.parse_args()
    payload = json.loads(args.ledger.read_text(encoding="utf-8"))
    expected = inventory()
    if args.runtime:
        validate_runtime(expected)
    if args.write:
        payload["public_api"] = expected
        args.ledger.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
        )
        return 0
    if payload.get("public_api") != expected:
        raise RuntimeError("HNSW public inventory is stale; run with --write")
    print(f"validated {len(expected)} HNSW public API rows")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
