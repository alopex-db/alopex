"""Exercise the actual primary publisher against a local bare Git repository."""
import os
from pathlib import Path
import shutil
import subprocess
import unittest

import test_report_pair as fixtures


ROOT = Path(__file__).resolve().parents[3]


@unittest.skipUnless(shutil.which("bash") and shutil.which("git"), "bash/git required")
class ReportPublisherTests(unittest.TestCase):
    def setUp(self):
        self.fixture = fixtures.ReportPairTests()
        self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)
        self.root = self.fixture.root
        self.repo = self.root / "repo"
        self.repo.mkdir()
        self.remote = self.root / "remote.git"
        self.env = dict(os.environ, GIT_CONFIG_COUNT="1", GIT_CONFIG_KEY_0="commit.gpgsign",
                        GIT_CONFIG_VALUE_0="false", GIT_AUTHOR_NAME="test", GIT_AUTHOR_EMAIL="test@example.invalid",
                        GIT_COMMITTER_NAME="test", GIT_COMMITTER_EMAIL="test@example.invalid")
        self.git("init", "--bare", str(self.remote))
        self.git("init", str(self.repo))
        self.git("remote", "add", "origin", str(self.remote))
        for filename in ("scripts/release/report_pair.py", "scripts/release/verify-release/report.py"):
            destination = self.repo / filename
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(ROOT / filename, destination)
        self.git("add", "scripts")
        self.git("commit", "-m", "fixture")
        self.base = self.git("rev-parse", "HEAD").stdout.strip()
        self.bin = self.root / "bin"
        self.bin.mkdir()
        curl = self.bin / "curl"
        curl.write_text('#!/usr/bin/env bash\nexit 22\n')
        curl.chmod(0o755)
        self.env.update(
            PATH=str(self.bin) + os.pathsep + os.environ["PATH"], VERSION="0.8.17",
            REPORT_ROOT=str(self.fixture.source), REPORT_RUN_ID="123", REPORT_RUN_ATTEMPT="1",
            GITHUB_RUN_ID="123", GITHUB_RUN_ATTEMPT="2", RUNNER_TEMP=str(self.root),
            GITHUB_ENV=str(self.root / "env"), GITHUB_OUTPUT=str(self.root / "output"),
        )
        workflow = (ROOT / ".github/workflows/public-release-verification.yml").read_text()
        block = workflow.split("      - name: Publish candidate branch for docs ingestion\n", 1)[1]
        script = block.split("        run: |\n", 1)[1].split("\n      - name:", 1)[0]
        self.script = "\n".join(line[10:] if line.startswith("          ") else line for line in script.splitlines())
        self.branch = "report/verify-release-v0.8.17-123-1"
        self.path = "reports/release-verification/v0.8.17/run-123-1"

    def git(self, *args, **kwargs):
        return subprocess.run(["git", *args], cwd=self.repo, env=self.env,
                              capture_output=True, text=True, check=True, **kwargs)

    def publish(self):
        return subprocess.run(["bash", "-c", self.script], cwd=self.repo, env=self.env,
                              capture_output=True, text=True, timeout=30)

    def test_retry_uses_original_attempt_and_reuses_identical_pair(self):
        first = self.publish()
        self.assertEqual(first.returncode, 0, first.stderr)
        published = self.git("rev-parse", self.branch).stdout
        for extension in ("json", "md"):
            data = self.git("show", f"{self.branch}:{self.path}.{extension}").stdout
            self.assertEqual(data, (self.fixture.source / f"v0.8.17.{extension}").read_text())
        self.git("checkout", "--detach", self.base)
        self.env["GITHUB_RUN_ATTEMPT"] = "3"
        second = self.publish()
        self.assertEqual(second.returncode, 0, second.stderr)
        self.assertEqual(self.git("rev-parse", self.branch).stdout, published)

    def test_conflicting_json_is_not_overwritten_when_markdown_is_unchanged(self):
        self.assertEqual(self.publish().returncode, 0)
        self.git("checkout", "--detach", self.base)
        source = self.fixture.results
        source.write_bytes(source.read_bytes() + b"\n")
        result = self.publish()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("refusing to overwrite immutable report", result.stderr)

    def test_partial_existing_branch_is_rejected_without_repair(self):
        self.git("checkout", "-b", self.branch)
        path = self.repo / (self.path + ".md")
        path.parent.mkdir(parents=True)
        shutil.copyfile(self.fixture.results.with_suffix(".md"), path)
        self.git("add", "reports")
        self.git("commit", "-m", "legacy markdown-only fixture")
        self.git("push", "origin", self.branch)
        partial = self.git("rev-parse", self.branch).stdout
        self.git("checkout", "--detach", self.base)
        self.assertNotEqual(self.publish().returncode, 0)
        self.assertEqual(self.git("rev-parse", self.branch).stdout, partial)

    def test_public_markdown_alone_does_not_count_as_pair_success(self):
        # Simulated public HTTP: Markdown matches, JSON is unavailable.
        (self.bin / "curl").write_text('''#!/usr/bin/env bash
case "$2" in
  *.md\?*) cp "$REPORT_ROOT/v0.8.17.md" "$4" ;;
  *) exit 22 ;;
esac
''')
        result = self.publish()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("branch=" + self.branch, (self.root / "output").read_text())


if __name__ == "__main__":
    unittest.main()
