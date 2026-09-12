from __future__ import annotations

import re
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]


class ReleaseIdentityContractTests(unittest.TestCase):
    def read(self, path: str) -> str:
        return (ROOT / path).read_text(encoding="utf-8")

    def test_development_workflows_do_not_use_named_branches_as_eligibility(self) -> None:
        for path in (
            ".github/workflows/ci.yml",
            ".github/workflows/alopex-cli.yml",
            ".github/workflows/alopex-py.yml",
            ".github/workflows/compatibility.yml",
            ".github/workflows/parity-harness.yml",
            ".github/workflows/release-process.yml",
        ):
            workflow = self.read(path)
            self.assertNotIn("branches: [main, develop]", workflow, path)
            self.assertNotIn("branches: [main]", workflow, path)

    def test_safe_tag_validates_commit_identity_not_branch_membership(self) -> None:
        script = self.read("scripts/release/safe-tag.sh")
        self.assertIn('git cat-file -t "${TARGET_SHA}"', script)
        for forbidden in (
            "CURRENT_BRANCH",
            "refs/remotes/origin/main",
            "local main",
            "origin/main",
            "reviewed-main-sha",
        ):
            self.assertNotIn(forbidden, script)

    def test_every_workflow_job_has_one_documented_responsibility(self) -> None:
        inventory = self.read("docs/release-pipeline-responsibilities.md")
        for workflow in sorted((ROOT / ".github/workflows").glob("*.yml")):
            text = workflow.read_text(encoding="utf-8")
            jobs = text.partition("\njobs:\n")[2]
            for job in re.findall(r"^  ([a-z][a-z0-9_-]*):", jobs, re.MULTILINE):
                owner = f"`{workflow.name}:{job}`"
                self.assertEqual(inventory.count(owner), 1, owner)


if __name__ == "__main__":
    unittest.main()
