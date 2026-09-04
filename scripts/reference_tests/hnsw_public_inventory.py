"""Generate/check the public HNSW surface from Rust, Python, SQL, and docs."""
from __future__ import annotations

import argparse
import json
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
HNSWLIB = "nmslib/hnswlib@3f3429661187e4c24a490a0f148fc6bc89042b3d"
PGVECTOR = "pgvector/pgvector@2627c5ff775ae6d7aef0c430121ccf857842d2f2"

EVIDENCE_BY_CLAIM = {
    "Metric": "crates/alopex-core/src/vector/hnsw/tests/graph_tests.rs#cosine_and_inner_product_search_expose_lower_is_closer_distance",
    "HnswConfig": "crates/alopex-core/src/vector/hnsw/tests/graph_tests.rs#hnsw_rejects_nonfinite_and_cosine_zero_vectors_at_insert_and_search",
    "HnswIndex.create": "crates/alopex-core/src/vector/hnsw/tests/graph_tests.rs#insert_and_search_basic_flow",
    "HnswIndex.load/save": "crates/alopex-core/src/vector/hnsw/tests/storage_tests.rs#save_and_load_roundtrip_preserves_graph",
    "HnswIndex.upsert": "crates/alopex-embedded/tests/hnsw_integration_tests.rs#hnsw_upsert_reconnects_existing_key_without_duplicate_results",
    "HnswIndex.search": "scripts/performance/hnsw_v0811_contract.py#run_benchmark",
    "HnswIndex.delete": "crates/alopex-core/src/vector/hnsw/tests/graph_tests.rs#deleted_nodes_are_skipped_in_results",
    "HnswIndex.drop": "crates/alopex-embedded/tests/hnsw_integration_tests.rs#hnsw_lifecycle_via_embedded_api",
    "HnswIndex staged commit/rollback": "crates/alopex-embedded/tests/hnsw_integration_tests.rs#transaction_commit_and_rollback_are_respected",
    "HnswIndex.compact": "crates/alopex-core/src/vector/hnsw/tests/graph_tests.rs#delete_marks_node_and_compact_removes_it",
    "HnswIndex.stats": "crates/alopex-py/tests/test_hnsw.py#test_hnsw_create_search_delete",
    "HnswIndex callbacks": "crates/alopex-embedded/tests/hnsw_integration_tests.rs#callbacks_fire_on_core_index",
    "HnswSearchResult": "crates/alopex-core/src/vector/hnsw/tests/graph_tests.rs#tie_breaks_by_key_order",
    "HnswStats": "crates/alopex-py/tests/test_hnsw.py#test_hnsw_create_search_delete",
    "SearchStats": "crates/alopex-py/tests/test_hnsw.py#test_hnsw_create_search_delete",
    "Embedded create/search/drop": "crates/alopex-embedded/tests/hnsw_integration_tests.rs#hnsw_lifecycle_via_embedded_api",
    "Embedded transaction upsert/delete/commit/rollback/reopen": "crates/alopex-embedded/tests/hnsw_integration_tests.rs#hnsw_index_persists_across_reopen",
    "Python.HnswConfig": "crates/alopex-py/tests/test_hnsw.py#test_hnsw_create_search_delete",
    "Python.create/drop/get_hnsw_stats": "crates/alopex-py/tests/test_hnsw.py#test_hnsw_create_search_delete",
    "Python.search_hnsw": "crates/alopex-py/tests/test_hnsw.py#test_hnsw_create_search_delete",
    "Python transaction upsert_to_hnsw/delete_from_hnsw": "crates/alopex-py/tests/test_hnsw.py#test_hnsw_create_search_delete",
    "Python SearchResult/HnswStats/SearchStats": "crates/alopex-py/tests/test_hnsw.py#test_hnsw_create_search_delete",
    "SQL CREATE INDEX USING HNSW": "crates/alopex-sql/tests/hnsw_sql_tests.rs#create_insert_and_search_hnsw_index",
    "SQL DROP INDEX USING HNSW": "crates/alopex-sql/tests/hnsw_sql_tests.rs#create_insert_and_search_hnsw_index",
    "SQL KNN search": "crates/alopex-sql/tests/knn_optimization.rs#knn_optimization_without_index",
    "SQL HNSW DML/transaction synchronization": "crates/alopex-sql/tests/hnsw_sql_tests.rs#dml_changes_are_reflected_in_hnsw_index",
    "SQL HNSW NULL/type/option validation": "crates/alopex-sql/tests/hnsw_sql_tests.rs#dimension_mismatch_on_insert_returns_error_and_no_index_write",
    "cosine distance": "crates/alopex-core/src/vector/hnsw/tests/graph_tests.rs#cosine_and_inner_product_search_expose_lower_is_closer_distance",
    "upsert reconnect": "crates/alopex-embedded/tests/hnsw_integration_tests.rs#hnsw_deleted_node_reactivation_reconnects_at_new_position",
}


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
    return {"surface": surface, "api": api}


def claim_for(row: dict[str, object]) -> str:
    surface, api = str(row["surface"]), str(row["api"])
    if surface == "docs":
        return "HnswIndex.search"
    if surface == "SQL":
        return {
            "SQL.CREATE_INDEX_USING_HNSW": "SQL CREATE INDEX USING HNSW",
            "SQL.DROP_INDEX_USING_HNSW": "SQL DROP INDEX USING HNSW",
            "SQL.HNSW_KNN_SEARCH": "SQL KNN search",
            "SQL.HNSW_INSERT_SYNC": "SQL HNSW DML/transaction synchronization",
            "SQL.HNSW_UPDATE_SYNC": "SQL HNSW NULL/type/option validation",
            "SQL.HNSW_DELETE_SYNC": "SQL HNSW DML/transaction synchronization",
        }[api]
    if surface == "embedded":
        if "compact" in api:
            return "HnswIndex.compact"
        if "get_hnsw_stats" in api:
            return "HnswIndex.stats"
        if "Transaction" in api:
            return "Embedded transaction upsert/delete/commit/rollback/reopen"
        return "Embedded create/search/drop"
    if surface == "Python":
        if api == "Python.HnswConfig":
            return "Python.HnswConfig"
        if api in {"Python.HnswStats", "Python.SearchResult", "Python.SearchStats"}:
            return "Python SearchResult/HnswStats/SearchStats"
        if "search_hnsw" in api:
            return "Python.search_hnsw"
        if "upsert_to_hnsw" in api or "delete_from_hnsw" in api:
            return "Python transaction upsert_to_hnsw/delete_from_hnsw"
        return "Python.create/drop/get_hnsw_stats"
    if api.startswith("Rust.HnswConfig"):
        return "Metric" if api.endswith("with_metric") else "HnswConfig"
    if api == "Rust.HnswSearchResult":
        return "HnswSearchResult"
    if api == "Rust.HnswStats":
        return "HnswStats"
    if api in {"Rust.InsertStats", "Rust.SearchStats"}:
        return "SearchStats"
    if api in {"Rust.HnswIndex.on_insert", "Rust.HnswIndex.on_search"}:
        return "HnswIndex callbacks"
    method = api.rsplit(".", 1)[-1]
    return {
        "HnswIndex": "HnswIndex.create",
        "create": "HnswIndex.create",
        "load": "HnswIndex.load/save",
        "save": "HnswIndex.load/save",
        "upsert": "HnswIndex.upsert",
        "upsert_staged": "upsert reconnect",
        "search": "cosine distance",
        "delete": "HnswIndex.delete",
        "delete_staged": "HnswIndex staged commit/rollback",
        "drop": "HnswIndex.drop",
        "commit_staged": "HnswIndex staged commit/rollback",
        "rollback": "HnswIndex staged commit/rollback",
        "compact": "HnswIndex.compact",
        "stats": "HnswIndex.stats",
        "name": "HnswIndex.stats",
    }[method]


def materialize(payload: dict[str, object]) -> list[dict[str, object]]:
    claims = {str(entry["api"]): entry for entry in payload.get("entries", [])}
    rows = []
    for source in inventory():
        claim = claim_for(source)
        entry = claims.get(claim)
        if entry is None:
            raise RuntimeError(f"unknown HNSW claim: {claim}")
        rows.append({
            **source,
            "claim": claim,
            "status": entry["status"],
            "reference": entry.get("reference", "alopex"),
            "evidence": EVIDENCE_BY_CLAIM[claim],
            "issue": entry.get("issue", 306),
            **({"performance_contract": entry["performance_contract"]} if "performance_contract" in entry else {}),
            **({"performance_evidence": entry["performance_evidence"]} if "performance_evidence" in entry else {}),
        })
    return rows


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
    expected = materialize(payload)
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
