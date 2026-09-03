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
        self.assertIn("pull_request:", workflow)
        self.assertIn("schedule:", workflow)
        self.assertIn("options: [curated, full]", workflow)
        self.assertIn('--suite "$SUITE"', workflow)
        self.assertIn("postgres:16.14", workflow)
        self.assertIn("datafusion==50.0.0", workflow)
        self.assertIn("pysqlite3-binary==0.5.4", workflow)
        self.assertNotIn("continue-on-error: true", workflow)

    def test_sql_reference_engines_have_separate_runnable_contracts(self):
        contracts = load_performance_contracts(PERFORMANCE_CONTRACTS)["contracts"]
        self.assertEqual(
            contracts["sql-sqlite-v1"]["reference_revision"],
            "sqlite/sqlite@version-3.46.1",
        )
        self.assertEqual(
            contracts["sql-postgresql-v1"]["reference_revision"],
            "postgres/postgres@REL_16_14",
        )
        self.assertEqual(
            contracts["sql-datafusion-streaming-v1"]["reference_revision"],
            "apache/datafusion@50.0.0",
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


if __name__ == "__main__":
    unittest.main()
