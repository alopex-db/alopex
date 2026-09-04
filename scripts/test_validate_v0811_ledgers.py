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

    def test_required_performance_workflow_uses_dedicated_runner(self):
        workflow = (
            Path(__file__).resolve().parents[1]
            / ".github/workflows/parity-performance.yml"
        ).read_text(encoding="utf-8")
        self.assertIn("[self-hosted, linux, x64, alopex-performance]", workflow)
        self.assertIn("workflow_call:", workflow)
        self.assertIn("schedule:", workflow)
        self.assertIn("options: [curated, full]", workflow)
        self.assertIn('--suite "$SUITE"', workflow)
        self.assertIn("postgres:16.14", workflow)
        self.assertIn("datafusion==50.0.0", workflow)
        self.assertIn("duckdb==1.4.0", workflow)
        self.assertIn("pysqlite3-binary==0.5.4", workflow)
        self.assertIn("sql_v0811_differential", workflow)
        self.assertIn("polars_public_inventory.py", workflow)
        self.assertIn("sql_public_inventory.py", workflow)
        self.assertIn("hnsw_public_inventory.py", workflow)
        self.assertNotIn("continue-on-error: true", workflow)
        ci = (
            Path(__file__).resolve().parents[1] / ".github/workflows/ci.yml"
        ).read_text(encoding="utf-8")
        self.assertIn("parity: ${{ steps.classify.outputs.parity }}", ci)
        self.assertIn("name: Required parity", ci)
        self.assertIn("uses: ./.github/workflows/parity-performance.yml", ci)
        self.assertIn(
            "needs: [scope, fmt, clippy, test, coverage, security-audit, build, v08-release-gate, parity]",
            ci,
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
        runner = (
            Path(__file__).resolve().parents[1]
            / "scripts/performance/parity_v0811_measure.py"
        ).read_text(encoding="utf-8")
        self.assertIn('if args.suite == "full"', runner)
        self.assertIn('"polars-parquet-streaming"', runner)

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
            wrong["public_api"][0]["claim"] = wrong["entries"][1]["api"]
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
