from __future__ import annotations

import json
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest import mock


ROOT = Path(__file__).resolve().parents[3]
EXPERIMENT = ROOT / "scripts/issue196_experiment"
MANIFEST = EXPERIMENT / "responsibility_graph.json"


class ResponsibilityGraphContractTests(unittest.TestCase):
    def setUp(self) -> None:
        import sys

        sys.path.insert(0, str(EXPERIMENT))
        self.addCleanup(sys.path.remove, str(EXPERIMENT))

    def test_target_graph_has_one_owner_per_responsibility_and_status_only_gate(self) -> None:
        from responsibility_graph import load_manifest, validate_manifest

        manifest = load_manifest(MANIFEST)
        validate_manifest(manifest)

        owner_ids = {owner["id"] for owner in manifest["owners"]}
        self.assertEqual(
            owner_ids,
            {
                "current-quality",
                "current-implementation",
                "historical-compatibility",
                "delivery-contract",
                "exhaustive-performance",
            },
        )
        self.assertEqual(set(manifest["target_graph"]["final_gate_needs"]), owner_ids)
        self.assertEqual(manifest["target_graph"]["final_gate_commands"], [])
        serialized = json.dumps(manifest["target_graph"])
        for version_gate in ("v06-gate", "v07-gate", "v08-release-gate"):
            self.assertNotIn(version_gate, serialized)

    def test_current_graph_records_the_nested_version_chain(self) -> None:
        from responsibility_graph import load_manifest

        graph = load_manifest(MANIFEST)["current_graph"]
        edges = {(edge["from"], edge["to"]) for edge in graph["edges"]}
        self.assertIn(("v08-release-gate", "v07-gate"), edges)
        self.assertIn(("v07-gate", "v06-gate"), edges)
        self.assertIn(("v08-release-gate", "v08-surface-verifier"), edges)

    def test_inventory_extracts_yaml_and_shell_commands(self) -> None:
        from inventory_build_signatures import extract_cargo_commands

        yaml_text = """steps:
  - run: >-
      cargo test -p alopex-core
      --features lane_ci --locked
  - run: cargo clippy --all-targets -- -D warnings
"""
        shell_text = """#!/bin/sh
cargo test -p alopex-server \\
  --test grpc_test --all-features --locked
echo "cargo test is documentation, not execution"
"""
        yaml_commands = extract_cargo_commands(Path("ci.yml"), yaml_text)
        shell_commands = extract_cargo_commands(Path("gate.sh"), shell_text)

        self.assertEqual(
            [command.command for command in yaml_commands],
            [
                "cargo test -p alopex-core --features lane_ci --locked",
                "cargo clippy --all-targets -- -D warnings",
            ],
        )
        self.assertEqual(
            [command.command for command in shell_commands],
            [
                "cargo test -p alopex-server --test grpc_test --all-features --locked"
            ],
        )

    def test_inventory_expands_shell_cargo_wrappers_at_call_sites(self) -> None:
        from inventory_build_signatures import extract_cargo_commands

        shell_text = """run_cargo_test() {
    cargo test --features lane_ci --locked \"$@\"
}
run_cargo_test -p alopex-server \\
    --test grpc_test
run_cargo_test -p alopex-dataframe --tests
"""
        commands = extract_cargo_commands(Path("surface.sh"), shell_text)

        self.assertEqual(
            [(command.line, command.command) for command in commands],
            [
                (
                    4,
                    "cargo test --features lane_ci --locked -p alopex-server --test grpc_test",
                ),
                (
                    6,
                    "cargo test --features lane_ci --locked -p alopex-dataframe --tests",
                ),
            ],
        )

    def test_inventory_assigns_every_real_signature_to_exactly_one_owner(self) -> None:
        from inventory_build_signatures import build_inventory
        from responsibility_graph import load_manifest

        inventory = build_inventory(ROOT, load_manifest(MANIFEST))
        self.assertGreater(len(inventory["signatures"]), 10)
        for signature in inventory["signatures"]:
            self.assertIn(signature["owner"], {owner["id"] for owner in inventory["owners"]})
            self.assertEqual(len(signature["ownership_matches"]), 1, signature)
        self.assertEqual(inventory["unowned"], [])
        self.assertEqual(inventory["ambiguous"], [])

    def test_exact_and_near_duplicate_signatures_are_separate(self) -> None:
        from inventory_build_signatures import classify_duplicate_groups, normalize_signature

        commands = [
            normalize_signature(Path("one.sh"), 1, "cargo test -p alopex-core --locked"),
            normalize_signature(Path("two.sh"), 2, "cargo test -p alopex-core --locked"),
            normalize_signature(
                Path("three.sh"),
                3,
                "cargo test -p alopex-core --features lane_ci --locked",
            ),
        ]
        duplicates = classify_duplicate_groups(commands)

        self.assertEqual(len(duplicates["exact"]), 1)
        self.assertEqual(duplicates["exact"][0]["count"], 2)
        self.assertEqual(len(duplicates["near"]), 1)
        self.assertEqual(duplicates["near"][0]["count"], 3)

    def test_inventory_rejects_overlapping_explicit_ownership_rules(self) -> None:
        from inventory_build_signatures import _matching_owners, normalize_signature

        signature = normalize_signature(
            Path("gate.sh"), 1, "cargo test -p alopex-core --locked"
        )
        owners = _matching_owners(
            signature,
            [
                {"owner": "first", "verbs": ["test"]},
                {"owner": "second", "command_regex": "alopex-core"},
                {"owner": "fallback", "default": True},
            ],
        )

        self.assertEqual(owners, ["first", "second"])


class AbMeasurementContractTests(unittest.TestCase):
    def setUp(self) -> None:
        import sys

        sys.path.insert(0, str(EXPERIMENT))
        self.addCleanup(sys.path.remove, str(EXPERIMENT))

    def test_plan_is_three_cold_and_warm_samples_for_both_graphs(self) -> None:
        from responsibility_graph import load_manifest
        from run_ab_measurement import build_plan

        with tempfile.TemporaryDirectory() as temporary:
            runtime = Path(temporary)
            plan = build_plan(load_manifest(MANIFEST), runtime)

        self.assertEqual({entry.sample for entry in plan}, {1, 2, 3})
        for sample in (1, 2, 3):
            for phase in ("cold", "warm"):
                graph_a = [
                    entry
                    for entry in plan
                    if entry.sample == sample
                    and entry.phase == phase
                    and entry.graph == "nested-version-topology"
                ]
                graph_b = [
                    entry
                    for entry in plan
                    if entry.sample == sample
                    and entry.phase == phase
                    and entry.graph == "responsibility-topology"
                ]
                self.assertEqual(len(graph_a), 2)
                self.assertEqual(len(graph_b), 1)
                self.assertEqual(
                    {entry.owner for entry in graph_a}, {"current-implementation"}
                )
                self.assertEqual(
                    {entry.owner for entry in graph_b}, {"current-implementation"}
                )
                self.assertEqual(len({entry.target_dir for entry in graph_a}), 2)
                self.assertEqual(len({entry.target_dir for entry in graph_b}), 1)

    def test_plan_never_uses_the_repository_target(self) -> None:
        from responsibility_graph import load_manifest
        from run_ab_measurement import build_plan

        runtime = Path("/tmp/alopex-issue-196-contract-test")
        for entry in build_plan(load_manifest(MANIFEST), runtime):
            self.assertTrue(entry.target_dir.is_relative_to(runtime / "targets"))
            self.assertNotEqual(entry.target_dir, ROOT / "target")

    def test_execution_is_injected_and_cleans_only_completed_sample_targets(self) -> None:
        from responsibility_graph import load_manifest
        from run_ab_measurement import execute_measurement

        calls: list[tuple[str, ...]] = []

        def fake_runner(command, *, cwd, env, check):
            calls.append(tuple(command))
            target = Path(env["CARGO_TARGET_DIR"])
            target.mkdir(parents=True, exist_ok=True)
            (target / "artifact.bin").write_bytes(b"measured")
            return subprocess.CompletedProcess(command, 0)

        def fake_metadata(_root: Path) -> dict[str, str]:
            return {
                "base_commit": "base",
                "experiment_snapshot_commit": "snapshot",
                "cargo_version": "cargo test-double",
                "rustc_version": "rustc test-double",
            }

        with tempfile.TemporaryDirectory() as temporary:
            runtime = Path(temporary)
            output = runtime / "ab-results.json"
            document = execute_measurement(
                ROOT,
                runtime,
                load_manifest(MANIFEST),
                output,
                command_runner=fake_runner,
                metadata_provider=fake_metadata,
            )

            self.assertEqual(len(calls), 18)
            self.assertEqual(len(document["observations"]), 12)
            self.assertTrue(document["complete"])
            self.assertEqual(document["remaining_target_bytes"], 0)
            self.assertEqual(list((runtime / "targets").rglob("artifact.bin")), [])
            self.assertEqual(
                json.loads(output.read_text(encoding="utf-8"))["schema"],
                "alopex-issue-196-ab-measurement-v1",
            )

    def test_aggregates_exclude_instrumentation_overhead_from_owner_work(self) -> None:
        from run_ab_measurement import calculate_aggregates

        observations = []
        for sample in (1, 2, 3):
            observations.extend(
                [
                    {
                        "sample": sample,
                        "graph": "nested-version-topology",
                        "phase": "cold",
                        "elapsed_seconds": 100.0,
                        "commands": [
                            {"elapsed_seconds": 10.0},
                            {"elapsed_seconds": 20.0},
                        ],
                    },
                    {
                        "sample": sample,
                        "graph": "responsibility-topology",
                        "phase": "cold",
                        "elapsed_seconds": 50.0,
                        "commands": [{"elapsed_seconds": 10.0}],
                    },
                ]
            )

        aggregates, reductions = calculate_aggregates(observations)

        self.assertEqual(
            aggregates["nested-version-topology"]["cold"]["median_owner_work_seconds"],
            30.0,
        )
        self.assertEqual(
            aggregates["nested-version-topology"]["cold"]["median_orchestration_seconds"],
            100.0,
        )
        self.assertEqual(reductions["cold"], 66.67)

    def test_recompute_recovers_a_missing_base_commit_from_the_environment(self) -> None:
        from run_ab_measurement import metadata_for_recompute

        existing = {
            "base_commit": None,
            "experiment_snapshot_commit": "snapshot",
            "cargo_version": "cargo test-double",
            "rustc_version": "rustc test-double",
        }
        with mock.patch.dict("os.environ", {"ISSUE196_BASE_SHA": "base"}):
            metadata = metadata_for_recompute(existing)

        self.assertEqual(metadata["base_commit"], "base")


if __name__ == "__main__":
    unittest.main()
