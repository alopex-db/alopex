from __future__ import annotations

import re
import tomllib
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
ACTION_SHA = "b66acf5e9fe20f8aba065be86778a8a4c846f902"


class DependencyDuplicatePolicyContractTests(unittest.TestCase):
    def read(self, relative: str) -> str:
        return (ROOT / relative).read_text(encoding="utf-8")

    def test_cargo_deny_blocks_new_duplicate_versions(self) -> None:
        with (ROOT / "deny.toml").open("rb") as handle:
            policy = tomllib.load(handle)

        graph = policy["graph"]
        bans = policy["bans"]

        self.assertIs(graph["all-features"], True)
        self.assertIn("x86_64-unknown-linux-gnu", graph["targets"])
        self.assertIn("x86_64-pc-windows-msvc", graph["targets"])
        self.assertIn("wasm32-unknown-unknown", graph["targets"])
        self.assertEqual(bans["multiple-versions"], "deny")
        self.assertIs(bans["multiple-versions-include-dev"], True)
        self.assertEqual(bans["wildcards"], "allow")
        self.assertNotIn("skip-tree", bans)

        exceptions = bans["skip"]
        specs = [entry["crate"] for entry in exceptions]
        self.assertGreater(len(specs), 0)
        self.assertEqual(len(specs), len(set(specs)))
        families = {spec.split("@", maxsplit=1)[0] for spec in specs}
        self.assertIn(
            f"has {len(families)} duplicate crate families and "
            f"{len(exceptions)} exact-version exceptions",
            self.read("docs/dependency-duplicate-policy.md"),
        )
        for entry in exceptions:
            self.assertRegex(entry["crate"], re.compile(r"^[a-zA-Z0-9_-]+@\d+\.\d+\.\d+"))
            self.assertIn("#196", entry["reason"])

    def test_duplicate_policy_is_a_blocking_ci_step(self) -> None:
        workflow = self.read(".github/workflows/ci.yml")
        audit_job = workflow.split("  security-audit:\n", maxsplit=1)[1].split(
            "\n  build:", maxsplit=1
        )[0]

        self.assertNotIn("continue-on-error", audit_job)
        self.assertIn(
            f"EmbarkStudios/cargo-deny-action@{ACTION_SHA}", audit_job
        )
        self.assertIn("command: check bans", audit_job)
        self.assertIn("arguments: --all-features --locked", audit_job)
        self.assertIn("--deny unmatched-skip", audit_job)
        self.assertIn("--deny unnecessary-skip", audit_job)
        self.assertLess(
            audit_job.index("name: Enforce dependency duplication policy"),
            audit_job.index("name: Run RustSec audit"),
        )

    def test_workspace_owns_one_arrow_and_parquet_version_line(self) -> None:
        for manifest in (
            "crates/alopex-cli/Cargo.toml",
            "crates/alopex-sql/Cargo.toml",
        ):
            contents = self.read(manifest)
            self.assertNotRegex(contents, re.compile(r'(arrow-[a-z]+|parquet)\s*=.*"52"'))

        lockfile = self.read("Cargo.lock")
        for crate in (
            "arrow-array",
            "arrow-buffer",
            "arrow-cast",
            "arrow-data",
            "arrow-ipc",
            "arrow-schema",
            "arrow-select",
            "parquet",
        ):
            self.assertNotRegex(
                lockfile,
                re.compile(rf'name = "{crate}"\nversion = "52\.'),
            )

    def test_policy_runbook_records_ownership_and_exception_lifecycle(self) -> None:
        runbook = self.read("docs/dependency-duplicate-policy.md")

        for required in (
            "New duplicate crate versions fail CI",
            "deny.toml",
            "Exact-version exceptions",
            "cargo deny check bans",
            "cargo tree --invert",
            "does not pin Cargo resolution",
            "Removal condition",
            "Rollback",
        ):
            self.assertIn(required, runbook)


if __name__ == "__main__":
    unittest.main()
