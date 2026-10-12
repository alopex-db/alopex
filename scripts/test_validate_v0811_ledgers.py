import copy
import json
import tempfile
import unittest
from pathlib import Path

from scripts.validate_v0811_ledgers import (
    LEDGERS,
    PERFORMANCE_CONTRACTS,
    load_performance_contracts,
    validate,
    validate_performance_contracts,
)


class LedgerContractTests(unittest.TestCase):
    def test_date_diff_is_not_assigned_unrelated_portable_query_evidence(self):
        from scripts.reference_tests.sql_public_inventory import claim_for

        self.assertEqual(claim_for({"surface": "scalar", "api": "scalar.date_diff"}), "DATE_DIFF")
        self.assertEqual(claim_for({"surface": "scalar", "api": "scalar.abs"}), "portable SELECT/null/order/coercion")

    def test_hnsw_capability_matrix_rejects_invalid_cells(self):
        hnsw = next(path for path in LEDGERS if path.name.startswith("hnsw-"))
        payload = json.loads(hnsw.read_text(encoding="utf-8"))
        # Exercise the validator's input boundary, not source-file wording.
        matrix = {
            capability: {
                surface: {"status": "not-applicable", "reason": "fixture boundary", "issue": 466}
                for surface in ("core.raw", "embedded.raw", "SQL", "Python.raw", "HTTP.sql", "gRPC.sql", "HTTP.raw", "gRPC.raw")
            }
            for capability in ("query_ef_search", "force_search_path", "search_statistics", "get_vectors", "atomic_batch_upsert")
        }
        from scripts.reference_tests.hnsw_public_inventory import validate_capabilities

        self.assertEqual(validate_capabilities({"capabilities": matrix}), [])
        cases = []
        missing = copy.deepcopy(matrix)
        del missing["query_ef_search"]["HTTP.sql"]
        cases.append((missing, "missing surfaces"))
        invalid = copy.deepcopy(matrix)
        invalid["query_ef_search"]["HTTP.raw"]["status"] = "PASS"
        cases.append((invalid, "invalid status"))
        for status in ([], {}, None):
            invalid_status_type = copy.deepcopy(matrix)
            invalid_status_type["query_ef_search"]["core.raw"]["status"] = status
            cases.append((invalid_status_type, "invalid status"))
        no_reason = copy.deepcopy(matrix)
        del no_reason["get_vectors"]["SQL"]["reason"]
        cases.append((no_reason, "reason"))
        pending = copy.deepcopy(matrix)
        pending["query_ef_search"]["Python.raw"]["status"] = "unverified"
        cases.append((pending, "unverified"))
        wrong_type = copy.deepcopy(matrix)
        wrong_type["search_statistics"]["core.raw"] = []
        cases.append((wrong_type, "must be an object"))
        no_evidence = copy.deepcopy(matrix)
        no_evidence["search_statistics"]["core.raw"] = {"status": "covered", "evidence": [], "observation": "nonzero work"}
        cases.append((no_evidence, "evidence"))
        missing_capability = copy.deepcopy(matrix)
        del missing_capability["get_vectors"]
        cases.append((missing_capability, "missing capabilities"))
        invalid_evidence = copy.deepcopy(matrix)
        invalid_evidence["query_ef_search"]["SQL"] = {
            "status": "covered", "evidence": "not-a-list", "observation": "effective ef"
        }
        cases.append((invalid_evidence, "evidence"))
        broken_reference = copy.deepcopy(matrix)
        broken_reference["query_ef_search"]["SQL"] = {
            "status": "covered", "evidence": ["missing.rs#missing_test"], "observation": "effective ef"
        }
        cases.append((broken_reference, "missing HNSW capability evidence"))
        invalid_issue = copy.deepcopy(matrix)
        invalid_issue["get_vectors"]["SQL"]["issue"] = True
        cases.append((invalid_issue, "owning issue"))
        for matrix, expected in cases:
            with self.subTest(expected=expected), tempfile.TemporaryDirectory() as directory:
                payload["capabilities"] = matrix
                ledger = Path(directory) / "hnsw.json"
                ledger.write_text(json.dumps(payload), encoding="utf-8")
                self.assertTrue(any(expected in error for error in validate(ledger)))

    def test_hnsw_capability_matrix_is_required(self):
        hnsw = next(path for path in LEDGERS if path.name.startswith("hnsw-"))
        payload = json.loads(hnsw.read_text(encoding="utf-8"))
        payload.pop("capabilities", None)
        with tempfile.TemporaryDirectory() as directory:
            ledger = Path(directory) / "hnsw.json"
            ledger.write_text(json.dumps(payload), encoding="utf-8")
            self.assertTrue(any("capabilities" in error for error in validate(ledger)))

    def test_all_ledgers_have_unique_evidenced_entries(self):
        contracts = load_performance_contracts(PERFORMANCE_CONTRACTS)
        errors = validate_performance_contracts(PERFORMANCE_CONTRACTS, contracts)
        errors.extend(error for path in LEDGERS for error in validate(path, contracts))
        self.assertEqual(errors, [])

    def test_compatible_entry_without_performance_contract_is_rejected(self):
        contracts = load_performance_contracts(PERFORMANCE_CONTRACTS)
        with tempfile.TemporaryDirectory() as directory:
            ledger = Path(directory) / "ledger.json"
            ledger.write_text(
                json.dumps(
                    {
                        "schema": "alopex.polars-parity/v1",
                        "status_values": ["implemented-compatible"],
                        "entries": [
                            {
                                "api": "DataFrame.select",
                                "status": "implemented-compatible",
                                "reference": "polars:1.43.2",
                                "evidence": "test.py",
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )

            errors = validate(ledger, contracts)

        self.assertTrue(any("missing performance_contract" in error for error in errors))

    def test_compatible_entry_cannot_use_a_relaxed_divergence_budget(self):
        contracts = load_performance_contracts(PERFORMANCE_CONTRACTS)
        with tempfile.TemporaryDirectory() as directory:
            ledger = Path(directory) / "ledger.json"
            ledger.write_text(
                json.dumps(
                    {
                        "schema": "alopex.polars-parity/v1",
                        "status_values": ["implemented-compatible"],
                        "entries": [
                            {
                                "api": "LazyFrame.collect",
                                "status": "implemented-compatible",
                                "reference": "polars:1.43.2",
                                "evidence": "test.py",
                                "performance_contract": "polars-lazy-streaming-v1",
                                "performance_evidence": "polars-lazy-streaming",
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )

            errors = validate(ledger, contracts)

        self.assertTrue(any("outside the compatibility budget" in error for error in errors))

    def test_contract_without_required_metric_or_threshold_is_rejected(self):
        contracts = load_performance_contracts(PERFORMANCE_CONTRACTS)
        broken = copy.deepcopy(contracts)
        name, contract = next(iter(broken["contracts"].items()))
        contract["metrics"].remove("peak_rss_bytes")
        contract["thresholds"].pop("max_peak_memory_ratio")

        errors = validate_performance_contracts(Path("contracts.json"), broken)

        self.assertTrue(any(name in error and "peak_rss_bytes" in error for error in errors))
        self.assertTrue(
            any(name in error and "max_peak_memory_ratio" in error for error in errors)
        )

    def test_hnsw_contract_requires_reference_recall_band(self):
        contracts = load_performance_contracts(PERFORMANCE_CONTRACTS)
        broken = copy.deepcopy(contracts)
        broken["contracts"]["hnsw-pareto-v1"]["thresholds"].pop(
            "max_recall_delta"
        )

        errors = validate_performance_contracts(Path("contracts.json"), broken)

        self.assertTrue(any("max_recall_delta" in error for error in errors))

    def test_performance_evidence_must_cover_the_ledger_claim(self):
        contracts = load_performance_contracts(PERFORMANCE_CONTRACTS)
        broken = copy.deepcopy(contracts)
        broken["contracts"]["polars-eager-v1"]["evidence_coverage"][
            "polars-eager-api"
        ].remove("DataFrame.explode")
        polars = next(path for path in LEDGERS if path.name.startswith("polars-"))

        errors = validate(polars, broken)

        self.assertTrue(any("does not cover DataFrame.explode" in error for error in errors))

    def test_contract_rejects_mutable_reference_and_incomplete_fixture(self):
        contracts = load_performance_contracts(PERFORMANCE_CONTRACTS)
        broken = copy.deepcopy(contracts)
        name, contract = next(iter(broken["contracts"].items()))
        contract["reference_revision"] = "example/project@main"
        contract["fixture"].pop("dataset_sha256")

        errors = validate_performance_contracts(Path("contracts.json"), broken)

        self.assertTrue(any(name in error and "exact reference_revision" in error for error in errors))
        self.assertTrue(any(name in error and "dataset_sha256" in error for error in errors))

    def test_contract_rejects_missing_kind_specific_fixture_fields(self):
        contracts = load_performance_contracts(PERFORMANCE_CONTRACTS)
        broken = copy.deepcopy(contracts)
        broken["contracts"]["hnsw-pareto-v1"]["fixture"].pop("m")
        broken["contracts"]["sql-sqlite-v1"]["fixture"].pop("queries")
        broken["contracts"]["polars-lazy-streaming-v1"]["fixture"].pop(
            "resource_limit_bytes"
        )

        errors = validate_performance_contracts(Path("contracts.json"), broken)

        self.assertTrue(any("hnsw-pareto-v1 fixture missing m" in error for error in errors))
        self.assertTrue(any("sql-sqlite-v1 fixture missing queries" in error for error in errors))
        self.assertTrue(
            any(
                "polars-lazy-streaming-v1 fixture missing resource_limit_bytes" in error
                for error in errors
            )
        )

    def test_sql_reference_engines_have_separate_runnable_contracts(self):
        contracts = load_performance_contracts(PERFORMANCE_CONTRACTS)["contracts"]
        self.assertEqual(
            contracts["sql-sqlite-v1"]["reference_revision"],
            "sqlite/sqlite@f3d536d37825302e31ed0eddd811c689f38f85a3",
        )
        self.assertEqual(
            contracts["sql-postgresql-v1"]["reference_revision"],
            "postgres/postgres@0d1c00c624fa7367d4a895f44381887757289682",
        )
        self.assertEqual(
            contracts["sql-datafusion-streaming-v1"]["reference_revision"],
            "apache/datafusion@d0a0c5a7d5867da949161b6065642d15293806de",
        )
        self.assertEqual(contracts["sql-datafusion-streaming-v1"]["kind"], "streaming")

    def test_full_polars_suite_has_distinct_parquet_streaming_evidence(self):
        contracts = load_performance_contracts(PERFORMANCE_CONTRACTS)["contracts"]
        evidence = contracts["polars-lazy-streaming-v1"]["evidence_ids"]
        self.assertEqual(
            evidence,
            ["polars-csv-streaming", "polars-parquet-streaming"],
        )
    def test_sql_ledger_has_generated_public_surface_and_upstream_provenance(self):
        sql = next(path for path in LEDGERS if path.name.startswith("sql-"))
        payload = json.loads(sql.read_text(encoding="utf-8"))
        self.assertGreaterEqual(len(payload["public_api"]), 200)
        self.assertGreaterEqual(len(payload["upstream_cases"]), 12)
        self.assertEqual(validate(sql), [])

    def test_hnsw_ledger_has_generated_cross_language_public_surface(self):
        hnsw = next(path for path in LEDGERS if path.name.startswith("hnsw-"))
        payload = json.loads(hnsw.read_text(encoding="utf-8"))
        self.assertGreaterEqual(len(payload["public_api"]), 30)
        self.assertEqual(
            {row["surface"] for row in payload["public_api"]},
            {"Rust", "embedded", "Python", "SQL", "docs"},
        )
        self.assertEqual(validate(hnsw), [])

    def test_sql_public_inventory_rejects_missing_and_unknown_claims(self):
        sql = next(path for path in LEDGERS if path.name.startswith("sql-"))
        payload = json.loads(sql.read_text(encoding="utf-8"))
        with tempfile.TemporaryDirectory() as directory:
            ledger = Path(directory) / "sql.json"
            missing = copy.deepcopy(payload)
            missing["public_api"].pop()
            ledger.write_text(json.dumps(missing), encoding="utf-8")
            self.assertTrue(
                any("SQL public inventory does not match source" in error for error in validate(ledger))
            )

            unknown = copy.deepcopy(payload)
            unknown["public_api"][0]["claim"] = "missing-claim"
            ledger.write_text(json.dumps(unknown), encoding="utf-8")
            self.assertTrue(
                any("unknown SQL claim" in error for error in validate(ledger))
            )

            wrong = copy.deepcopy(payload)
            wrong["public_api"][0]["claim"] = next(
                entry["api"]
                for entry in wrong["entries"]
                if entry["api"] != payload["public_api"][0]["claim"]
            )
            ledger.write_text(json.dumps(wrong), encoding="utf-8")
            self.assertTrue(
                any(
                    "SQL public inventory does not match materialized claims" in error
                    for error in validate(ledger)
                )
            )

    def test_hnsw_public_inventory_rejects_duplicate_claim_and_source_evidence(self):
        hnsw = next(path for path in LEDGERS if path.name.startswith("hnsw-"))
        payload = json.loads(hnsw.read_text(encoding="utf-8"))
        with tempfile.TemporaryDirectory() as directory:
            ledger = Path(directory) / "hnsw.json"
            duplicated = copy.deepcopy(payload)
            duplicated["public_api"].append(copy.deepcopy(duplicated["public_api"][0]))
            ledger.write_text(json.dumps(duplicated), encoding="utf-8")
            self.assertTrue(
                any("HNSW public inventory is empty or duplicated" in error for error in validate(ledger))
            )

            source_only = copy.deepcopy(payload)
            source_only["public_api"][0]["evidence"] = "crates/alopex-core/src/vector/hnsw/mod.rs"
            ledger.write_text(json.dumps(source_only), encoding="utf-8")
            self.assertTrue(
                any("source-only HNSW evidence" in error for error in validate(ledger))
            )

    def test_materialized_public_rows_require_test_selectors(self):
        for ledger_path in LEDGERS:
            if ledger_path.name.startswith("polars-"):
                continue
            payload = json.loads(ledger_path.read_text(encoding="utf-8"))
            for row in payload["public_api"]:
                self.assertTrue(row["claim"])
                self.assertTrue(row["status"])
                self.assertTrue(row["reference"])
                for evidence in row["evidence"].split(";"):
                    self.assertIn("#", evidence)
                    path, selector = evidence.split("#", 1)
                    self.assertTrue((ledger_path.parents[2] / path).exists(), evidence)
                    self.assertTrue(selector, evidence)


if __name__ == "__main__":
    unittest.main()
