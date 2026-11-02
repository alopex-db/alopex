"""Small release API fixtures; no publishing, artifacts, or native builds."""
from copy import deepcopy
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[3]
spec = importlib.util.spec_from_file_location(
    "latest", ROOT / "scripts/release/verify_latest_release.py"
)
latest = importlib.util.module_from_spec(spec)
spec.loader.exec_module(latest)


def fixture(version="0.8.17"):
    def asset(name, number):
        return {
            "name": name, "id": number, "size": 32, "state": "uploaded",
            "digest": "sha256:" + "a" * 64,
            "browser_download_url": f"https://github.com/example/repo/releases/download/v{version}/{name}",
        }
    core = {"id": 1, "tag_name": f"v{version}", "draft": False, "prerelease": False,
            "assets": [asset(name, i) for i, name in enumerate(sorted(latest.CLI_ASSETS), 10)]}
    python = {"id": 2, "tag_name": f"alopex-py-v{version}", "draft": False, "prerelease": False,
              "assets": [asset(f"alopex-{version}-cp38-abi3-linux_x86_64.whl", 20),
                         asset(f"alopex-{version}.tar.gz", 21)]}
    return core, python, deepcopy(core)


class LatestReleaseTests(unittest.TestCase):
    def test_independent_releases_survive_python_publication(self):
        values = fixture()
        before = deepcopy(values)
        latest.validate("0.8.17", *values)
        self.assertEqual(values, before)  # Verification must not repair metadata.

    def test_python_becoming_latest_reproduces_reported_failure(self):
        core, python, _ = fixture()
        with self.assertRaisesRegex(ValueError, "not the requested Core"):
            latest.validate("0.8.17", core, python, deepcopy(python))

    def test_other_core_version_is_not_silently_accepted(self):
        core, python, observed = fixture()
        observed["tag_name"] = "v0.8.16"
        with self.assertRaisesRegex(ValueError, "not the requested Core"):
            latest.validate("0.8.17", core, python, observed)

    def test_each_required_cli_stays_mandatory_at_version_boundary(self):
        primary = {"alopex-linux-x86_64", "alopex-macos-aarch64", "alopex-windows-x86_64.exe"}
        for version, names in [("0.8.16", primary | {"alopex-macos-x86_64"}),
                               ("0.8.17", primary), ("0.9.0", primary)]:
            for name in names:
                with self.subTest(version=version, name=name):
                    core, python, observed = fixture(version)
                    observed["assets"] = [x for x in observed["assets"] if x["name"] != name]
                    with self.assertRaisesRegex(ValueError, "missing CLI"):
                        latest.validate(version, core, python, observed)

    def test_optional_intel_absence_pending_and_late_success_do_not_block_primary(self):
        core, python, observed = fixture()
        intel = next(x for x in core["assets"] if x["name"] == "alopex-macos-x86_64")
        for value in (core, observed):
            value["assets"] = [x for x in value["assets"] if x["name"] != "alopex-macos-x86_64"]
        self.assertEqual(latest.validate("0.8.17", core, python, observed), {"required": False, "status": "unavailable"})
        core["assets"].append(deepcopy(intel))
        self.assertEqual(latest.validate("0.8.17", core, python, observed)["status"], "pending")
        observed["assets"].append(deepcopy(intel))
        for value in (core, observed):
            value["assets"][-1].update(state="new", size=0)
        self.assertEqual(latest.validate("0.8.17", core, python, observed)["status"], "pending")
        for value in (core, observed):
            value["assets"][-1].update(intel)
        self.assertEqual(latest.validate("0.8.17", core, python, observed)["status"], "available")
        observed["assets"][-1]["digest"] = "sha256:" + "b" * 64
        self.assertEqual(latest.validate("0.8.17", core, python, observed)["status"], "inconsistent")

    def test_partial_optional_bundle_does_not_weaken_primary_asset_checks(self):
        optional_names = ["alopex-macos-x86_64", "parser-supplement-v0.8.17-intel.json",
                          "intel-release-v0.8.17.json", "parser-smoke.log", "cli-smoke.log",
                          "wheel-smoke.log", "crate-smoke.log",
                          "alopex-parser-v0.8.17-contract-0.28.0-x86_64-apple-darwin.json",
                          "alopex-parser-v0.8.17-contract-0.28.0-x86_64-apple-darwin.tar.gz",
                          "alopex-0.8.17-cp38-abi3-macosx_10_12_x86_64.whl"]
        for name in optional_names:
            with self.subTest(name=name):
                core, python, observed = fixture()
                core["assets"] = [a for a in core["assets"] if a["name"] != name]
                core["assets"].append({"name": name, "id": 100, "state": "new", "size": 0})
                latest.validate("0.8.17", core, python, observed)
        for name in ("alopex-linux-x86_64", "parser-vendor-manifest-v0.8.17.json", "unknown-primary"):
            core, python, observed = fixture()
            core["assets"] = [a for a in core["assets"] if a["name"] != name]
            core["assets"].append({"name": name, "state": "new", "size": 0})
            with self.subTest(primary=name), self.assertRaisesRegex(ValueError, "unfinished or empty"):
                latest.validate("0.8.17", core, python, observed)
        # A pending Intel wheel cannot stand in for the independent primary wheel.
        core, python, observed = fixture()
        python["assets"].append({"name": optional_names[-1], "id": 100, "state": "new", "size": 0})
        latest.validate("0.8.17", core, python, observed)
        python["assets"].pop()
        python["assets"][0].update(name=optional_names[-1], state="new", size=0)
        with self.assertRaisesRegex(ValueError, "wheel or sdist"):
            latest.validate("0.8.17", core, python, observed)

    def test_old_intel_upload_remains_a_required_asset(self):
        core, python, observed = fixture("0.8.16")
        next(a for a in core["assets"] if a["name"] == "alopex-macos-x86_64").update(state="new", size=0)
        with self.assertRaisesRegex(ValueError, "unfinished or empty"):
            latest.validate("0.8.16", core, python, observed)

    def test_replaced_cli_identity_is_rejected(self):
        for field, value in [("id", 999), ("size", 1), ("digest", "sha256:" + "b" * 64),
                             ("state", "new"), ("browser_download_url", "https://example.com/other")]:
            with self.subTest(field=field):
                core, python, observed = fixture()
                observed["assets"][0][field] = value
                with self.assertRaisesRegex(ValueError, "identity mismatch"):
                    latest.validate("0.8.17", core, python, observed)

    def test_python_wheel_and_sdist_remain_required(self):
        for index in [0, 1]:
            core, python, observed = fixture()
            python["assets"].pop(index)
            with self.assertRaisesRegex(ValueError, "wheel or sdist"):
                latest.validate("0.8.17", core, python, observed)

    def test_unpublished_empty_and_duplicate_assets_fail(self):
        for change in ["draft", "prerelease", "state", "size", "duplicate"]:
            for target in [0, 1]:
                with self.subTest(change=change, target=target):
                    values = fixture()
                    value = values[target]
                    if change in {"draft", "prerelease"}:
                        value[change] = True
                    elif change == "duplicate":
                        value["assets"].append(deepcopy(value["assets"][0]))
                    else:
                        value["assets"][0][change] = "new" if change == "state" else 0
                    with self.assertRaises(ValueError):
                        latest.validate("0.8.17", *values)

    def test_asset_inventory_is_paginated_and_read_only(self):
        pages = [fixture()[0], [{"id": i} for i in range(100)], [{"id": 100}]]
        with patch.object(latest, "get", side_effect=pages) as request:
            result = latest.release("example/repo", "v0.8.17")
        self.assertEqual(len(result["assets"]), 101)
        self.assertTrue(request.call_args_list[-1].args[0].endswith("page=2"))

    def test_transport_explicitly_uses_get(self):
        with patch.object(latest.subprocess, "run") as run:
            run.return_value.stdout = '{"id":1}'
            self.assertEqual(latest.observe("example/repo"), {"id": 1})
        self.assertEqual(run.call_args.args[0], ["gh", "api", "--method", "GET", "repos/example/repo/releases/latest"])

    def test_cli_records_success_and_failure_observations(self):
        for failure in [False, True]:
            core, python, observed = fixture()
            if failure:
                observed = deepcopy(python)
            with tempfile.TemporaryDirectory() as directory:
                output = Path(directory) / "identity.json"
                argv = ["verify", "--repository", "example/repo", "--version", "0.8.17", "--output", str(output)]
                with patch("sys.argv", argv), patch.object(latest, "release", side_effect=[core, python]), patch.object(latest, "observe", return_value=observed):
                    if failure:
                        with self.assertRaises(SystemExit):
                            latest.main()
                    else:
                        latest.main()
                result = json.loads(output.read_text())
                self.assertEqual(result["status"], "failure" if failure else "success")
                self.assertEqual(set(result["required_cli_assets"]), {"alopex-linux-x86_64", "alopex-macos-aarch64", "alopex-windows-x86_64.exe"})
                if not failure:
                    self.assertEqual(result["intel_cli"], {"required": False, "status": "available"})
                self.assertEqual(result["latest"]["id"], observed["id"])
                self.assertEqual(len(result["python"]["assets"]), 2)

    def test_workflow_order_and_failure_report(self):
        core = (ROOT / ".github/workflows/release.yml").read_text()
        python = (ROOT / ".github/workflows/alopex-py-release.yml").read_text()
        public = (ROOT / ".github/workflows/public-release-verification.yml").read_text()
        creation = python.split("  github-release:\n")[1].split("  final-release-join:\n")[0]
        self.assertIn('make_latest: "false"', creation)
        stable = core.split("  create-stable-release:\n")[1].split("  dispatch-python-release:\n")[0]
        self.assertIn('make_latest: "true"', stable)
        self.assertLess(core.index('gh run watch "${run_id}" --exit-status'), core.index("Verify final Core latest"))
        self.assertLess(python.index("--verify-join join-assets/release-join.json"), python.index("Verify Core remains latest"))
        for workflow in [core, python, public]:
            self.assertIn("verify_latest_release.py", workflow)
            self.assertIn("latest-release.json", workflow)
        self.assertIn('"${LATEST_IDENTITY_OUTCOME:-skipped}"', public)
        self.assertIn('--log "${identity}"', public)


if __name__ == "__main__":
    unittest.main()
