from __future__ import annotations

from pathlib import Path
import sys
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import workflow_process_contracts as contracts


class WorkflowProcessContractTests(unittest.TestCase):
    def test_rejects_pre_preflight_services_and_unpinned_image(self) -> None:
        wf = """
name: Required parity performance
jobs:
  performance:
    runs-on: ubuntu-24.04
    timeout-minutes: 360
    services:
      postgres:
        image: postgres:16.14
    steps:
      - uses: actions/checkout@v4
"""
        findings = contracts.audit_workflow_text(".github/workflows/parity-performance.yml", wf)
        rule_ids = {f.rule_id for f in findings}
        self.assertIn("NO_PRE_PREFLIGHT_SERVICES", rule_ids)
        self.assertIn("PINNED_CONTAINER_IMAGE_DIGEST", rule_ids)
        self.assertIn("LONG_JOB_EARLY_STOP_REQUIRED", rule_ids)

    def test_rejects_cms_and_raw_stress_uploads(self) -> None:
        wf = """
name: Stress Tests
jobs:
  stress:
    runs-on: ubuntu-latest
    timeout-minutes: 30
    steps:
      - uses: actions/upload-artifact@v4
        with:
          name: stress-reports
          path: |
            target/stress-reports
            export/private-evidence.cms
"""
        findings = contracts.audit_workflow_text(".github/workflows/stress-tests.yml", wf)
        self.assertEqual(
            [f.rule_id for f in findings],
            [
                "NO_PUBLIC_CMS_OR_RAW_STRESS_UPLOAD",
                "NO_PUBLIC_CMS_OR_RAW_STRESS_UPLOAD",
            ],
        )

    def test_rejects_adhoc_privilege_relaxation_in_any_workflow(self) -> None:
        wf = """
name: Test env
jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - run: docker run --privileged --security-opt seccomp=unconfined alpine true
"""
        findings = contracts.audit_workflow_text(".github/workflows/test-env.yml", wf)
        self.assertEqual([f.rule_id for f in findings], ["NO_ADHOC_PRIVILEGE_RELAXATION"])

    def test_accepts_compliant_public_measurement_workflow(self) -> None:
        wf = """
name: Compliant parity performance
jobs:
  performance:
    runs-on: ubuntu-24.04
    timeout-minutes: 120
    steps:
      - name: Storage and security preflight with early_stop budget
        run: python3 scripts/check_stage_budget.py --early-stop
      - name: Start pinned peer
        run: echo "postgres@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
      - uses: actions/upload-artifact@v4
        with:
          name: parity-public
          path: |
            performance-public/hnsw-diagnostic.json
            performance-public/hnsw-diagnostic.md
"""
        findings = contracts.audit_workflow_text(".github/workflows/parity-performance.yml", wf)
        self.assertEqual(findings, [])


if __name__ == "__main__":
    unittest.main()
