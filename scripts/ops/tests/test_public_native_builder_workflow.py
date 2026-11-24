"""Keep the public builder isolated from existing selected acceptance modes."""
from __future__ import annotations

import hashlib
from pathlib import Path
import re
import unittest


ROOT = Path(__file__).resolve().parents[3]
WORKFLOW = ROOT / ".github/workflows/alopex-performance.yml"
OLD_JOBS = ("s04_socket_inventory", "inventory", "qualification",
            "wal_cost_682", "s04_inspect")
CONTROLLER_SELECTOR_SHA256 = "fb7f9c0acfc32a279a9f100b509a840b8c86c4895308ca3e7021d0f232950397"
NEW_GUARD = " && !inputs.public_native_build_mode"


def job(text: str, name: str) -> str:
    jobs = text.split("\njobs:\n", 1)[1]
    match = re.search(r"^  " + re.escape(name) + r":\n", jobs, re.M)
    if match is None:
        raise AssertionError(f"missing job: {name}")
    next_job = re.search(r"^  [a-z][a-z0-9_]*:\n", jobs[match.end():], re.M)
    return jobs[match.start():match.end() + next_job.start() if next_job else None]


def enabled(text: str, name: str, inputs: dict) -> bool:
    selector = re.search(r"^    if: (.+)$", job(text, name), re.M).group(1)
    expression = selector.removeprefix("${{").removesuffix("}}").strip()
    expression = expression.replace("&&", " and ").replace("||", " or ")
    expression = re.sub(r"!(?!=)", " not ", expression)
    expression = re.sub(r"inputs\.([a-z0-9_]+)",
                        lambda match: repr(inputs.get(match.group(1), False)), expression)
    return bool(eval(expression.strip(), {"__builtins__": {}}, {}))


class PublicNativeBuilderWorkflow(unittest.TestCase):
    def setUp(self):
        self.text = WORKFLOW.read_text()

    def test_existing_jobs_keep_final_controller_selectors_and_read_permissions(self):
        selectors = []
        for name in OLD_JOBS:
            match = re.search(r"^    if: (.+)$", job(self.text, name), re.M)
            self.assertIsNotNone(match, name)
            self.assertEqual(match.group(1).count(NEW_GUARD), 1, name)
            selectors.append(match.group(1).replace(NEW_GUARD, ""))
        self.assertEqual(hashlib.sha256("\n".join(selectors).encode()).hexdigest(),
                         CONTROLLER_SELECTOR_SHA256)
        self.assertIn("\npermissions:\n  contents: read\n  actions: read\n", self.text)
        self.assertIn("\nconcurrency:\n  group: alopex-v0817-host-preflight\n", self.text)

    def test_public_build_is_exclusive_and_uploads_only_verified_export(self):
        entry = job(self.text, "public_native_build")
        selector = re.search(r"^    if: (.+)$", entry, re.M).group(1)
        self.assertIn("inputs.public_native_build_mode", selector)
        self.assertIn("inputs.issue_number == '712'", selector)
        for mode in ("qualification_mode", "sql_four_case_mode", "sql_final_mode", "wal_cost_mode",
                     "sql_diagnostic_mode", "s04_inspect_mode",
                     "s04_socket_inventory_mode", "s04_capture_mode"):
            self.assertIn("!inputs." + mode, selector)
        for field in ("public_native_build_mode", "public_native_runner_label",
                      "public_native_build_config"):
            self.assertIn("\n      " + field + ":\n", self.text)
        stages = ("Prepare source within the measured VM work budget",
                  "Build source-bound server and wheel serially",
                  "Verify public asset export",
                  "Preserve public software assets and fixed result")
        self.assertEqual([entry.index(stage) for stage in stages],
                         sorted(entry.index(stage) for stage in stages))
        self.assertIn("if: always() && steps.public_export.outcome == 'success'", entry)
        self.assertIn("path: native-assets-export/", entry)
        self.assertIn("EXPECTED_CONTROLLER_SHA: ${{ inputs.expected_controller_sha }}", entry)
        self.assertIn("os.environ['EXPECTED_CONTROLLER_SHA'] == os.environ['GITHUB_SHA']", entry)
        self.assertNotIn("private-evidence.cms", entry)
        self.assertNotIn("native-evidence-cert.pem", entry)

    def test_final_sql_and_public_builder_modes_are_mutually_exclusive(self):
        names = (*OLD_JOBS, "public_native_build")
        final = {"issue_number": "712", "qualification_mode": True,
                 "sql_final_mode": True}
        builder = {"issue_number": "712", "public_native_build_mode": True}
        both = dict(final, public_native_build_mode=True)
        self.assertEqual([name for name in names if enabled(self.text, name, final)],
                         ["qualification"])
        self.assertEqual([name for name in names if enabled(self.text, name, builder)],
                         ["public_native_build"])
        self.assertEqual([name for name in names if enabled(self.text, name, both)], [])


if __name__ == "__main__":
    unittest.main()
