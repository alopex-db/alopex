from __future__ import annotations

import importlib.util
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import Mock


ROOT = Path(__file__).resolve().parents[3]
WORKFLOW = ROOT / ".github/workflows/issue-196-local-experiment.yml"
VERIFIER = ROOT / "scripts/issue196_experiment/verify_isolation.py"
RECORDER = ROOT / "scripts/issue196_experiment/record_command.py"
SUMMARIZER = ROOT / "scripts/issue196_experiment/summarize_results.py"
RUNNER = ROOT / "scripts/issue196_experiment/run_containerized_act.sh"
MANIFEST = ROOT / "scripts/issue196_experiment/responsibility_graph.json"
SEED_VERIFIER = ROOT / "scripts/issue196_experiment/verify_cargo_seed.py"
SEED_HYDRATOR = ROOT / "scripts/issue196_experiment/hydrate_cargo_seed.py"


def load_verifier():
    experiment = str(VERIFIER.parent)
    sys.path.insert(0, experiment)
    spec = importlib.util.spec_from_file_location("issue196_verify_isolation", VERIFIER)
    if spec is None or spec.loader is None:
        raise RuntimeError("unable to load isolation verifier")
    module = importlib.util.module_from_spec(spec)
    try:
        spec.loader.exec_module(module)
    finally:
        sys.path.remove(experiment)
    return module


class IsolationContractTests(unittest.TestCase):
    def test_only_experiment_files_are_allowed(self) -> None:
        verifier = load_verifier()
        self.assertTrue(
            verifier.is_allowed_path(".github/workflows/issue-196-local-experiment.yml")
        )
        self.assertTrue(
            verifier.is_allowed_path("scripts/issue196_experiment/record_command.py")
        )
        self.assertTrue(
            verifier.is_allowed_path("docs/issue-196-local-experiment.md")
        )
        for path in (
            ".github/workflows/ci.yml",
            ".github/workflows/alopex-py.yml",
            "Cargo.toml",
            "crates/alopex-py/tests/test_catalog.py",
        ):
            self.assertFalse(verifier.is_allowed_path(path), path)

    def test_experiment_is_manual_and_join_is_status_only(self) -> None:
        workflow = WORKFLOW.read_text(encoding="utf-8")
        manifest = json.loads(MANIFEST.read_text(encoding="utf-8"))
        trigger = workflow.split("\non:\n", 1)[1].split("\npermissions:\n", 1)[0]
        self.assertIn("workflow_dispatch:", trigger)
        self.assertNotIn("push:", trigger)
        self.assertNotIn("pull_request:", trigger)
        self.assertNotIn("schedule:", trigger)
        join = workflow.split("  experiment-success:", 1)[1].split(
            "\n  experiment-metrics:", 1
        )[0]
        for forbidden in ("cargo ", "pytest", "maturin", "build-nim", "test-nim"):
            self.assertNotIn(forbidden, join)
        for owner_job in manifest["experiment_workflow"]["final_gate_needs"]:
            self.assertIn(f"needs.{owner_job}.result", join)
        self.assertNotIn("\\\n", join)
        self.assertEqual(join.count(']]\n'), len(manifest["experiment_workflow"]["final_gate_needs"]))

    def test_current_tree_satisfies_the_isolation_contract(self) -> None:
        result = subprocess.run(
            [sys.executable, str(VERIFIER), "--base-ref", "origin/main"],
            cwd=ROOT,
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

    def test_act_controller_cannot_write_the_worktree(self) -> None:
        runner = RUNNER.read_text(encoding="utf-8")
        self.assertNotIn('--volume "${ROOT}:${ROOT}', runner)
        self.assertIn('--volume "${SOURCE_DIR}:${SOURCE_DIR}:ro"', runner)
        self.assertIn('git -C "${ROOT}" archive --format=tar "${BASE_REF}"', runner)
        self.assertIn('BASE_REF="origin/main"', runner)
        self.assertIn('BASE_SHA="$(git -C "${ROOT}" rev-parse "${BASE_REF}")"', runner)
        self.assertEqual(runner.count('--env ISSUE196_BASE_SHA="${BASE_SHA}"'), 2)
        self.assertIn('CARGO_SEED_DIR="${ISSUE196_CARGO_SEED_DIR:-}"', runner)
        self.assertIn('cp -a "${CARGO_SEED_DIR}/registry/cache/."', runner)
        self.assertIn('registry/index:/experiment/cargo-home/registry/index:ro', runner)
        self.assertNotIn('registry/src:/experiment', runner)
        self.assertNotIn('registry/cache:/experiment/cargo-home/registry/cache:ro', runner)
        self.assertNotIn('cp -a "${CARGO_SEED_DIR}/credentials', runner)
        self.assertNotIn('cp -a "${CARGO_SEED_DIR}/config', runner)
        self.assertIn("verify_cargo_seed.py \\", runner)
        self.assertIn("--bind", runner)
        self.assertIn("--container-daemon-socket -", runner)
        self.assertIn("--volume ${RUNTIME_DIR}:/experiment", runner)
        self.assertIn('RESOURCE_LABEL="alopex.issue196.experiment=true"', runner)
        self.assertIn('--filter "label=${RESOURCE_LABEL}"', runner)
        self.assertIn("SOCKET_START_ATTEMPTS=600", runner)
        self.assertIn('kill -0 "${service_pid}"', runner)
        self.assertEqual(
            runner.count('verify_isolation.py" \\\n    --base-ref "${BASE_REF}"'),
            2,
        )

    def test_pinned_act_binary_runs_inside_the_controller(self) -> None:
        runner = RUNNER.read_text(encoding="utf-8")
        self.assertIn('ACT_VERSION="0.2.88"', runner)
        self.assertIn('ACT_ASSET_ID="409583042"', runner)
        self.assertIn(
            'ACT_SHA256="1eb9996682dfcc053ac8f3f90f2ec50376f0cdfc229712d82da03d673c63a2b3"',
            runner,
        )
        self.assertIn('--entrypoint /opt/issue196/act', runner)
        self.assertIn('--volume "${ACT_BINARY}:/opt/issue196/act:ro"', runner)
        self.assertIn('"${RUNNER_IMAGE}" workflow_dispatch', runner)
        self.assertIn('CACHE_DIR="/tmp/alopex-issue-196-cache"', runner)
        self.assertIn('sha256sum --check --status', runner)

    def test_owner_results_and_isolated_bytes_are_measured(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            runtime = Path(temporary)
            results = runtime / "results"
            results.mkdir()
            for owner in ("historical-compatibility", "delivery-contract"):
                result = subprocess.run(
                    [
                        sys.executable,
                        str(RECORDER),
                        "--owner",
                        owner,
                        "--output",
                        str(results / f"{owner}.json"),
                        "--",
                        sys.executable,
                        "-c",
                        "raise SystemExit(0)",
                    ],
                    cwd=ROOT,
                    capture_output=True,
                    text=True,
                    check=False,
                )
                self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            target_file = runtime / "targets/current-dataframe/result.bin"
            target_file.parent.mkdir(parents=True)
            target_file.write_bytes(b"isolated")
            (runtime / "inventory.json").write_text(
                json.dumps(
                    {
                        "schema": "alopex-issue-196-build-signature-inventory-v1",
                        "signatures": [
                            {"owner": "current-quality"},
                            {"owner": "current-implementation"},
                        ],
                        "duplicate_groups": {"exact": [], "near": []},
                    }
                ),
                encoding="utf-8",
            )
            (runtime / "ab-results.json").write_text(
                json.dumps(
                    {
                        "schema": "alopex-issue-196-ab-measurement-v1",
                        "complete": True,
                        "aggregates": {
                            "nested-version-topology": {
                                "cold": {"samples": [20.0, 20.0, 20.0], "median_seconds": 20.0},
                                "warm": {"samples": [2.0, 2.0, 2.0], "median_seconds": 2.0},
                            },
                            "responsibility-topology": {
                                "cold": {"samples": [10.0, 10.0, 10.0], "median_seconds": 10.0},
                                "warm": {"samples": [1.0, 1.0, 1.0], "median_seconds": 1.0},
                            },
                        },
                        "median_reduction_percent": {"cold": 50.0, "warm": 50.0},
                    }
                ),
                encoding="utf-8",
            )

            result = subprocess.run(
                [sys.executable, str(SUMMARIZER), "--runtime", str(runtime)],
                cwd=ROOT,
                capture_output=True,
                text=True,
                check=False,
            )

            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            summary = json.loads((runtime / "summary.json").read_text(encoding="utf-8"))
            self.assertEqual(summary["schema"], "alopex-issue-196-local-experiment-v1")
            self.assertEqual(summary["isolated_target_bytes"], len(b"isolated"))
            self.assertEqual(
                {record["owner"] for record in summary["owners"]},
                {"historical-compatibility", "delivery-contract"},
            )
            self.assertEqual(summary["inventory"]["signature_count"], 2)
            self.assertEqual(summary["ab_measurement"]["median_reduction_percent"]["cold"], 50.0)

    def test_cargo_seed_requires_every_locked_registry_archive(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            lockfile = root / "Cargo.lock"
            cargo_home = root / "cargo-home"
            cache = cargo_home / "registry/cache/index"
            cache.mkdir(parents=True)
            lockfile.write_text(
                """version = 3

[[package]]
name = "cached"
version = "1.2.3"
source = "registry+https://github.com/rust-lang/crates.io-index"

[[package]]
name = "missing"
version = "4.5.6"
source = "registry+https://github.com/rust-lang/crates.io-index"
""",
                encoding="utf-8",
            )
            (cache / "cached-1.2.3.crate").write_bytes(b"archive")

            result = subprocess.run(
                [
                    sys.executable,
                    str(SEED_VERIFIER),
                    "--lockfile",
                    str(lockfile),
                    "--cargo-home",
                    str(cargo_home),
                ],
                capture_output=True,
                text=True,
                check=False,
            )

            self.assertNotEqual(result.returncode, 0)
            self.assertIn("missing-4.5.6.crate", result.stderr)

    def test_cargo_seed_hydration_verifies_checksum_before_atomic_install(self) -> None:
        experiment = str(SEED_HYDRATOR.parent)
        sys.path.insert(0, experiment)
        try:
            from hydrate_cargo_seed import hydrate_archive
        finally:
            sys.path.remove(experiment)

        payload = b"verified archive"
        checksum = hashlib.sha256(payload).hexdigest()
        response = Mock()
        response.read.return_value = payload
        response.__enter__ = Mock(return_value=response)
        response.__exit__ = Mock(return_value=False)
        opener = Mock(return_value=response)
        with tempfile.TemporaryDirectory() as temporary:
            cache = Path(temporary)
            changed = hydrate_archive(cache, ("example", "1.2.3", checksum), opener)

            self.assertTrue(changed)
            self.assertEqual((cache / "example-1.2.3.crate").read_bytes(), payload)
            self.assertFalse((cache / "example-1.2.3.crate.partial").exists())


if __name__ == "__main__":
    unittest.main()
