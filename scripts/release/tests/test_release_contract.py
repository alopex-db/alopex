from __future__ import annotations

import re
import textwrap
import tomllib
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]


class ReleaseContractTests(unittest.TestCase):
    def test_release_procedure_requires_a_checkpoint_issue_before_tagging(self) -> None:
        skill = (
            ROOT / ".codex/skills/alopex-development-release/SKILL.md"
        ).read_text(encoding="utf-8")
        checklist = (
            ROOT
            / ".codex/skills/alopex-development-release/references/checklist.md"
        ).read_text(encoding="utf-8")

        for required in (
            "Create the milestone-scoped release tracker issue before release work",
            "exact candidate SHA",
            "required CI",
            "return to implementation",
            "record checkpoint evidence immediately",
            "Close the release tracker only after public verification",
        ):
            self.assertIn(required, skill)
        for checkpoint in (
            "Checkpoint 0 — tracker and candidate",
            "Checkpoint 1 — implementation and review",
            "Checkpoint 2 — main and pre-tag verification",
            "Checkpoint 3 — publication",
            "Checkpoint 4 — public verification and close",
        ):
            self.assertIn(checkpoint, checklist)

    def test_performance_acceptance_is_owned_by_its_issue_not_release(self) -> None:
        skill = (
            ROOT / ".codex/skills/alopex-development-release/SKILL.md"
        ).read_text(encoding="utf-8")
        checklist = (
            ROOT
            / ".codex/skills/alopex-development-release/references/checklist.md"
        ).read_text(encoding="utf-8")
        ci = (ROOT / ".github/workflows/ci.yml").read_text(encoding="utf-8")
        performance = (
            ROOT / ".github/workflows/parity-performance.yml"
        ).read_text(encoding="utf-8")

        self.assertIn("Performance acceptance belongs to its owning issue", skill)
        self.assertIn("does not block release", checklist)
        self.assertNotIn("parity-performance.yml", ci)
        self.assertNotIn("Required parity", ci)
        self.assertIn("issue_number:", performance)

    def test_sql_type_capability_gate_runs_in_ci_and_release(self) -> None:
        surface_gate = (
            ROOT / "crates/alopex-tools/v08/verify-v08-surfaces.sh"
        ).read_text(encoding="utf-8")
        release = (ROOT / ".github/workflows/release.yml").read_text(encoding="utf-8")

        self.assertIn("scripts/release/type_capability_gate.py", surface_gate)
        self.assertIn(
            'type_capability_gate.py --release-version "${RELEASE_TAG_NAME#v}"',
            release,
        )

    def test_workspace_alopex_dependencies_are_exact_patch_pins(self) -> None:
        with (ROOT / "Cargo.toml").open("rb") as stream:
            workspace = tomllib.load(stream)
        version = workspace["workspace"]["package"]["version"]
        dependencies = workspace["workspace"]["dependencies"]
        owned = {
            "alopex-core",
            "alopex-dataframe",
            "alopex-sql",
            "alopex-embedded",
            "alopex-cluster",
        }
        for name in owned:
            self.assertEqual(dependencies[name]["version"], f"={version}")
        py_manifest = (ROOT / "crates/alopex-py/Cargo.toml").read_text(encoding="utf-8")
        self.assertIn("alopex-embedded.workspace = true", py_manifest)
        self.assertNotIn('alopex-embedded = { path = "../alopex-embedded" }', py_manifest)

    def test_target_version_is_consistent(self) -> None:
        workspace = (ROOT / "Cargo.toml").read_text(encoding="utf-8")
        run = (ROOT / "scripts/release/verify-release/run.sh").read_text(encoding="utf-8")
        release = (ROOT / ".github/workflows/release.yml").read_text(encoding="utf-8")
        python_release = (ROOT / ".github/workflows/alopex-py-release.yml").read_text(
            encoding="utf-8"
        )
        parser_build = (ROOT / "scripts/build-nim-parser.sh").read_text(
            encoding="utf-8"
        )
        parser_manifest = (
            ROOT / "scripts/release/parser_asset_manifest.py"
        ).read_text(encoding="utf-8")
        verifier_image = (
            ROOT / "scripts/release/verify-release/Dockerfile"
        ).read_text(encoding="utf-8")
        version = re.search(r'^version = "([0-9.]+)"$', workspace, re.MULTILINE)
        self.assertIsNotNone(version)
        self.assertIsNotNone(version.group(1))
        self.assertIn('["workspace"]["package"]["version"]', run)
        self.assertIn('envelope="artifacts/parser-assets-v${version}.json"', release)
        self.assertIn('parser-assets-v${ALOPEX_VERSION}.json', python_release)
        self.assertIn('["workspace"]["package"]["version"]', parser_build)
        self.assertIn("REQUIRED_ALOPEX_VERSION = _workspace_version()", parser_manifest)
        for operational_file in (
            run,
            release,
            python_release,
            parser_build,
            parser_manifest,
            verifier_image,
        ):
            self.assertNotIn("0.8.6", operational_file)

    def test_release_separates_fresh_parser_assets_from_crate_vendor(self) -> None:
        release = (ROOT / ".github/workflows/release.yml").read_text(
            encoding="utf-8"
        )
        ci = (ROOT / ".github/workflows/ci.yml").read_text(encoding="utf-8")
        self.assertIn("Run controlled Nim parser failure", ci)
        self.assertIn("Stage just-built parser for v0.8 surfaces", ci)
        self.assertNotIn('reviewed_dir="${NIM_SQL_PARSER_DIR}/vendor/', ci)
        self.assertNotIn("Run controlled Nim parser failure", release)
        self.assertNotIn("Run v0.7 baseline gate", release)
        self.assertNotIn("verify-v08-surfaces.sh", release)
        self.assertIn("Extract and run native smoke", release)
        self.assertIn("Assemble and verify parser manifest", release)
        self.assertNotIn("pattern: nim-vendor-*", release)
        self.assertNotIn("Place vendored libraries in clean source staging", release)
        self.assertNotIn("Upload vendored Nim shared library", release)

    def test_release_parser_contract_matches_source_contract(self) -> None:
        contract = (
            ROOT / "crates/alopex-sql/nim-sql-parser/PARSER_CONTRACT_VERSION"
        ).read_text(encoding="utf-8").strip()
        release = (ROOT / ".github/workflows/release.yml").read_text(
            encoding="utf-8"
        )
        python_ci = (ROOT / ".github/workflows/alopex-py.yml").read_text(
            encoding="utf-8"
        )
        python_release = (
            ROOT / ".github/workflows/alopex-py-release.yml"
        ).read_text(encoding="utf-8")

        self.assertIn(f'if version != "{contract}":', release)
        self.assertIn(f'contract-{contract}-*.tar.gz', release)
        self.assertIn(f'write_bytes(b"{contract}\\n")', release)
        self.assertIn(f"printf '{contract}\\n'", python_ci)
        self.assertIn(f'contract-{contract}-*.tar.gz', python_release)
        self.assertIn(f"write_bytes(b'{contract}\\n')", python_release)
        self.assertIn(
            f"--expected-contract-version {contract}", python_release
        )
        for workflow in (release, python_ci, python_release):
            self.assertNotIn("0.14.0", workflow)

    def test_release_rust_toolchain_is_pinned(self) -> None:
        release = (ROOT / ".github/workflows/release.yml").read_text(
            encoding="utf-8"
        )
        self.assertNotIn("dtolnay/rust-toolchain@stable", release)
        self.assertGreaterEqual(release.count("dtolnay/rust-toolchain@1.90.0"), 2)

    def test_release_flattens_downloaded_parser_payloads_before_assembly(self) -> None:
        release = (ROOT / ".github/workflows/release.yml").read_text(
            encoding="utf-8"
        )
        self.assertIn("Flatten parser artifact payloads", release)
        self.assertIn('test "${#parser_records[@]}" -eq 4', release)
        self.assertIn('test "${#parser_archives[@]}" -eq 4', release)
        self.assertIn('destination="artifacts/$(basename "$asset")"', release)
        self.assertIn('test ! -e "$destination"', release)

    def test_public_tool_dependencies_are_generated_from_exact_version(self) -> None:
        tools = (ROOT / "crates/alopex-tools/Cargo.toml").read_text(encoding="utf-8")
        run = (ROOT / "scripts/release/verify-release/run.sh").read_text(encoding="utf-8")
        self.assertIn('path = "../alopex-embedded"', tools)
        self.assertIn('alopex-embedded = { version = "=${ALOPEX_VERSION}" }', run)
        self.assertIn('alopex-sql = { version = "=${ALOPEX_VERSION}" }', run)
        self.assertNotIn('alopex-embedded = "=0.7.4"', tools)

    def test_release_dag_requires_python_demos_and_docs(self) -> None:
        rust = (ROOT / ".github/workflows/release.yml").read_text(encoding="utf-8")
        python = (ROOT / ".github/workflows/alopex-py-release.yml").read_text(
            encoding="utf-8"
        )
        self.assertIn("dispatch-python-release:", rust)
        self.assertIn('gh run watch "${run_id}" --exit-status', rust)
        self.assertNotIn("verify-public-release:", python)
        self.assertNotIn("publish_report:", python)
        self.assertIn("verify_python_vector_api.py", python)

    def test_release_procedure_keeps_known_functionality_before_delivery(self) -> None:
        procedure = (ROOT / "docs/release-v0.8-support.md").read_text(
            encoding="utf-8"
        )
        self.assertIn("Known functionality must finish here, before an RC tag.", procedure)
        self.assertIn("must not run its release demos", procedure)
        self.assertIn("later success cannot overwrite or conceal an", procedure)

        python_release = (ROOT / ".github/workflows/alopex-py-release.yml").read_text(
            encoding="utf-8"
        )
        self.assertNotIn("Verify public package availability and publish docs report", python_release)
        self.assertNotIn("Verify demos and publish docs report", python_release)
        self.assertNotIn("post-release-hnsw:", python_release)
        for moved_check in (
            "demo_cluster.py / demo_routing.py",
            "demo_dataframe_p3.py / demo_api_surfaces.py",
            "demo_sql_v074.sh / demo_sql_v08.py / demo_sql_mutations.py",
            "demo_vector_api.py / demo_embedded_v08.sh",
            "hnsw_v0811_contract.py",
        ):
            self.assertIn(moved_check, procedure)
        self.assertIn("Development CI (`v08-release-gate`)", procedure)
        self.assertIn("Extended Verification (`parity-performance.yml`)", procedure)

    def test_python_wheel_smoke_uses_search_stats_public_fields(self) -> None:
        smoke = (ROOT / "scripts/release/verify_python_vector_api.py").read_text(
            encoding="utf-8"
        )

        self.assertIn("stats.nodes_visited", smoke)
        self.assertNotIn("stats.node_count", smoke)

    def test_python_tag_creation_is_independent_and_preflighted(self) -> None:
        release = (ROOT / ".github/workflows/release.yml").read_text(
            encoding="utf-8"
        )
        dispatch = release.split("  dispatch-python-release:", maxsplit=1)[1]
        tagger = (ROOT / "scripts/release/prepare-python-release.sh").read_text(
            encoding="utf-8"
        )

        self.assertIn("bash scripts/release/prepare-python-release.sh", dispatch)
        self.assertIn("CORE_RUN_ID: ${{ github.run_id }}", dispatch)
        self.assertIn('python_workflow_ref="${python_tag}"', dispatch)
        self.assertIn('core_run_id=${GITHUB_RUN_ID}', dispatch)
        self.assertIn('--event workflow_dispatch', dispatch)
        self.assertIn('git merge-base --is-ancestor "${core_sha}" HEAD', tagger)
        self.assertIn('gh run list --workflow ci.yml --commit "${candidate_sha}"', tagger)
        self.assertIn('git tag -a "${python_tag}" "${candidate_sha}"', tagger)

    def test_crate_publish_verifies_the_packaged_vendor_tree(self) -> None:
        release = (ROOT / ".github/workflows/release.yml").read_text(
            encoding="utf-8"
        )
        publish = release.split("  publish-crate:", maxsplit=1)[1].split(
            "  dispatch-python-release:", maxsplit=1
        )[0]

        self.assertNotIn(
            "NIM_SQL_PARSER_LIB_DIR: ${{ github.workspace }}/crates/alopex-sql/nim-sql-parser",
            publish,
        )
        self.assertIn(
            "env -u NIM_SQL_PARSER_LIB_DIR cargo publish", publish
        )
        self.assertIn(
            "Bind crate source staging to freshly built parser assets", publish
        )
        self.assertIn(
            '--vendor-dir "${RELEASE_STAGE}/crates/alopex-sql/nim-sql-parser/vendor"',
            publish,
        )
        self.assertIn("parser library digest mismatch", publish)
        self.assertIn(
            "unknown or ambiguous parser vendor manifest layout", publish
        )
        self.assertIn(
            "python scripts/release/retarget_python_parser_source.py", publish
        )

    def test_crate_publish_parser_staging_python_is_valid(self) -> None:
        release = (ROOT / ".github/workflows/release.yml").read_text(
            encoding="utf-8"
        )
        staging = release.split(
            "- name: Bind crate source staging to freshly built parser assets",
            maxsplit=1,
        )[1].split("- name: Create publish helper", maxsplit=1)[0]
        script = staging.split("python - <<'PY'\n", maxsplit=1)[1].split(
            "\n          PY", maxsplit=1
        )[0]

        compile(textwrap.dedent(script), "release parser staging", "exec")

    def test_release_has_no_repair_forward_path(self) -> None:
        release = (ROOT / ".github/workflows/release.yml").read_text(
            encoding="utf-8"
        )
        python = (ROOT / ".github/workflows/alopex-py-release.yml").read_text(
            encoding="utf-8"
        )

        self.assertNotIn("repair_forward", release)
        self.assertNotIn("repair_forward", python)
        self.assertFalse((ROOT / "scripts/release/prepare-python-repair.sh").exists())

    def test_python_release_runs_from_its_immutable_tag(self) -> None:
        workflow = (ROOT / ".github/workflows/alopex-py-release.yml").read_text(
            encoding="utf-8"
        )
        for job in ("linux", "macos", "windows", "sdist"):
            header = workflow.split(f"  {job}:", maxsplit=1)[1].split(
                "    steps:", maxsplit=1
            )[0]
            self.assertNotIn("needs:", header)
        join = workflow.split("  final-release-join:", maxsplit=1)[1]
        self.assertIn("needs: [publish-pypi, github-release]", join)
        self.assertIn('git merge-base --is-ancestor "${core_tag_sha}" "${python_tag_sha}"', join)

    def test_v08_demos_are_mandatory(self) -> None:
        run = (ROOT / "scripts/release/verify-release/run.sh").read_text(encoding="utf-8")
        self.assertIn("scripts/demo/v08/demo_sql_v08.py", run)
        self.assertIn("JSON-on-TEXT", run)
        self.assertIn("scripts/demo/v074/demo_api_surfaces.py", run)
        self.assertIn("scripts/demo/v074/demo_vector_api.py", run)
        self.assertIn("--require-all", run)

    def test_v08_sql_demo_count_matches_its_declared_checks(self) -> None:
        demo = (ROOT / "scripts/demo/v08/demo_sql_v08.py").read_text(encoding="utf-8")

        self.assertIn("if completed != 81:", demo)
        self.assertIn("81 checks passed", demo)

    def test_embedded_demo_covers_every_v08_local_capability_group(self) -> None:
        run = (ROOT / "scripts/release/verify-release/run.sh").read_text(encoding="utf-8")
        wrapper = (ROOT / "scripts/demo/v08/demo_embedded_v08.sh").read_text(
            encoding="utf-8"
        )
        source = (
            ROOT / "crates/alopex-tools/src/bin/demo_v08_embedded.rs"
        ).read_text(encoding="utf-8")

        self.assertIn("scripts/demo/v08/demo_embedded_v08.sh", run)
        self.assertIn("embedded-dependency-smoke", run)
        self.assertNotIn("crates/alopex-tools/build.rs", run)
        self.assertIn("demo-v08-embedded", wrapper)
        self.assertFalse((ROOT / "crates/alopex-tools/build.rs").exists())
        for scenario_id in (
            "EMB-01-storage-durability",
            "EMB-02-kv-transactions",
            "EMB-03-persisted-transaction-manager",
            "EMB-04-local-sql-matrix",
            "EMB-05-catalog-cluster-diagnostics",
            "EMB-06-owned-and-sql-streams",
            "EMB-07-dataframe-columnar",
            "EMB-08-vector-hnsw",
            "EMB-09-large-values",
            "EMB-10-fail-closed-boundaries",
        ):
            self.assertIn(scenario_id, source)

        self.assertNotIn("distance が負値", source)

    def test_apalache_uses_the_runner_identity(self) -> None:
        process = (ROOT / ".github/workflows/release-process.yml").read_text(
            encoding="utf-8"
        )
        compose = (ROOT / "formal/release-report/compose.yml").read_text(
            encoding="utf-8"
        )
        self.assertIn('export APALACHE_UID="$(id -u)"', process)
        self.assertIn('export APALACHE_GID="$(id -g)"', process)
        self.assertIn('USER_ID: "${APALACHE_UID:-1000}"', compose)
        self.assertIn('GROUP_ID: "${APALACHE_GID:-1000}"', compose)


if __name__ == "__main__":
    unittest.main()
