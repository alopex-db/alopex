from __future__ import annotations

import re
import tomllib
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]


class ReleaseContractTests(unittest.TestCase):
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
        self.assertIn("verify-public-release:", python)
        self.assertIn("publish_report: true", python)
        self.assertIn("verify_python_vector_api.py", python)

    def test_immutable_tag_can_resume_full_release_after_early_gate_failure(self) -> None:
        release = (ROOT / ".github/workflows/release.yml").read_text(
            encoding="utf-8"
        )
        gate = release.split("  ci-gate:", maxsplit=1)[1].split(
            "  build-release:", maxsplit=1
        )[0]
        dispatch = release.split("  dispatch-python-release:", maxsplit=1)[1]

        self.assertIn(
            "RELEASE_TAG_NAME: ${{ inputs.release_tag || github.ref_name }}", release
        )
        self.assertIn(
            "RELEASE_TARGET_SHA: ${{ inputs.target_sha || github.sha }}", release
        )
        self.assertGreaterEqual(
            release.count("ref: ${{ inputs.release_tag || github.ref }}"), 4
        )
        self.assertIn(
            "ref: ${{ inputs.repair_forward && github.ref || inputs.release_tag || github.ref }}",
            release,
        )
        self.assertIn('git rev-parse "${RELEASE_TAG_NAME}^{commit}"', gate)
        self.assertIn('--commit "${RELEASE_TARGET_SHA}"', gate)
        self.assertIn("for attempt in $(seq 1 30)", gate)
        self.assertNotIn("env.GITHUB_SHA", gate)
        self.assertIn(
            "always() && needs.publish-crate.result == 'success'", dispatch
        )
        self.assertIn('python_workflow_ref="${python_tag}"', dispatch)
        self.assertIn('python_workflow_ref="${GITHUB_REF_NAME}"', dispatch)
        self.assertIn('python_source_ref="${python_tag}"', dispatch)
        self.assertIn('python_source_ref="${RELEASE_TARGET_SHA}"', dispatch)
        self.assertIn('expected_workflow_sha="${RELEASE_TARGET_SHA}"', dispatch)
        self.assertIn('target_sha=${RELEASE_TARGET_SHA}', dispatch)
        self.assertIn('core_run_id=${GITHUB_RUN_ID}', dispatch)
        self.assertIn('--event workflow_dispatch', dispatch)

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

    def test_core_repair_forward_is_bound_to_the_immutable_release_tag(self) -> None:
        release = (ROOT / ".github/workflows/release.yml").read_text(
            encoding="utf-8"
        )
        publish = release.split("  publish-crate:", maxsplit=1)[1].split(
            "  dispatch-python-release:", maxsplit=1
        )[0]
        dispatch = release.split("  dispatch-python-release:", maxsplit=1)[1]

        self.assertIn("repair_forward:", release)
        self.assertIn("release_tag:", release)
        self.assertIn("target_sha:", release)
        self.assertNotIn("branches:\n      - 'repair/v*-release'", release)
        self.assertNotIn("startsWith(github.ref_name, 'repair/v')", publish)
        self.assertIn(
            '[[ "${release_tag}" =~ ^v[0-9]+\\.[0-9]+\\.[0-9]+$ ]]',
            publish,
        )
        self.assertIn(
            '[[ "${release_target_sha}" =~ ^[0-9a-f]{40}$ ]]', publish
        )
        self.assertIn(
            'git rev-parse "${RELEASE_TAG_NAME}^{commit}"', publish
        )
        self.assertIn('"${RELEASE_TARGET_SHA}"', publish)
        self.assertIn('gh release view "${release_tag}"', publish)
        self.assertIn(
            "needs.build-release.result == 'success'", publish
        )
        self.assertIn('-f "repair_forward=${REPAIR_FORWARD}"', dispatch)
        self.assertIn("needs.publish-crate.result == 'success'", dispatch)

    def test_python_repair_skips_protected_publish_and_runs_public_join(self) -> None:
        workflow = (ROOT / ".github/workflows/alopex-py-release.yml").read_text(
            encoding="utf-8"
        )
        for job in ("linux", "macos", "windows", "sdist"):
            header = workflow.split(f"  {job}:", maxsplit=1)[1].split(
                "    steps:", maxsplit=1
            )[0]
            self.assertIn("if: ${{ !inputs.repair_forward }}", header)
        join = workflow.split("  final-release-join:", maxsplit=1)[1].split(
            "  verify-public-release:", maxsplit=1
        )[0]
        self.assertIn("inputs.repair_forward ||", join)
        self.assertIn("prepare-repair-release", join)
        public = workflow.split("  verify-public-release:", maxsplit=1)[1]
        self.assertIn(
            "always() && needs.final-release-join.result == 'success'", public
        )

    def test_v08_demos_are_mandatory(self) -> None:
        run = (ROOT / "scripts/release/verify-release/run.sh").read_text(encoding="utf-8")
        self.assertIn("scripts/demo/v08/demo_sql_v08.py", run)
        self.assertIn("scripts/demo/v074/demo_api_surfaces.py", run)
        self.assertIn("scripts/demo/v074/demo_vector_api.py", run)
        self.assertIn("--require-all", run)

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
