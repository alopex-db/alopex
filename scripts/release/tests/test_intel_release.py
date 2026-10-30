import argparse
import contextlib
import copy
import io
import json
from pathlib import Path
import re
import shutil
import unittest
from unittest.mock import patch

from scripts.release import intel_release as intel
from scripts.release import primary_distributions
from scripts.release.tests import test_parser_supplement as fixtures


ROOT = Path(__file__).resolve().parents[3]


class IntelReleaseTests(unittest.TestCase):
    def setUp(self):
        self.fixture = fixtures.ParserSupplementTests(methodName="runTest")
        self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)
        patcher = patch.object(intel, "parser", fixtures.fixtures.MANIFEST)
        patcher.start()
        self.addCleanup(patcher.stop)
        self.root = self.fixture.root
        self.context_dir = self.root / "context"
        self.context_dir.mkdir()
        self.version = intel.parser.REQUIRED_ALOPEX_VERSION
        self.ctx = {"version": self.version, "tag": f"v{self.version}", "source_sha": "a" * 40,
                    "primary": self.fixture.primary.name, "primary_sha256": intel.digest(self.fixture.primary),
                    "candidate_sha256": "c" * 64}
        shutil.copyfile(self.fixture.primary, self.context_dir / self.ctx["primary"])
        intel.write(self.context_dir / "context.json", self.ctx)
        self.bundle = self.root / "bundle"
        self.bundle.mkdir()
        self.extra = f"parser-supplement-v{self.version}-intel.json"
        shutil.copyfile(self.fixture.output, self.bundle / self.extra)
        for path in (self.fixture.archive, self.fixture.record):
            shutil.copyfile(path, self.bundle / path.name)
        self.wheel = f"alopex-{self.version}-cp38-abi3-macosx_10_12_x86_64.whl"
        for name in ("alopex-macos-x86_64", self.wheel, *intel.LOGS):
            (self.bundle / name).write_bytes(b"tiny identity fixture\n")
        args = argparse.Namespace(context=self.context_dir, bundle=self.bundle, run_id=42)
        with patch.object(intel.platform, "system", return_value="Darwin"), patch.object(intel.platform, "machine", return_value="x86_64"):
            intel.seal(args)
        self.manifest = self.bundle / f"intel-release-v{self.version}.json"

    def publication_args(self, command="plan"):
        return argparse.Namespace(context=self.context_dir, bundle=self.bundle, run_id=42,
                                  command=command, upload=self.root / "upload")

    def test_bundle_replay_and_reject_changed_bytes_missing_logs_or_foreign_source(self):
        value, wheel = intel.verify_bundle(self.bundle, self.ctx)
        self.assertEqual(wheel, self.wheel)
        original = self.manifest.read_bytes()
        for key, changed in (("source", {"tag": self.ctx["tag"], "sha": "b" * 40}),
                             ("primary_manifest_sha256", "d" * 64),
                             ("candidate_sha256", "d" * 64),
                             ("target", "aarch64-apple-darwin")):
            with self.subTest(key=key):
                altered = copy.deepcopy(value)
                altered[key] = changed
                intel.write(self.manifest, altered)
                with self.assertRaisesRegex(ValueError, "identity mismatch"):
                    intel.verify_bundle(self.bundle, self.ctx)
        self.manifest.write_bytes(original)
        (self.bundle / self.wheel).write_bytes(b"changed")
        with self.assertRaisesRegex(ValueError, "bytes or inventory"):
            intel.verify_bundle(self.bundle, self.ctx)

    def test_empty_native_log_rejected_even_with_updated_digest(self):
        value = json.loads(self.manifest.read_bytes())
        (self.bundle / intel.LOGS[0]).write_bytes(b"")
        value["artifacts"] = [intel.identity(p) for p in sorted(self.bundle.iterdir()) if p != self.manifest]
        intel.write(self.manifest, value)
        with self.assertRaisesRegex(ValueError, "incomplete"):
            intel.verify_bundle(self.bundle, self.ctx)

    def test_supplement_record_must_match_sealed_parser_record(self):
        path = self.bundle / self.extra
        value = json.loads(path.read_bytes())
        value["record"]["library"]["sha256"] = "b" * 64
        intel.write(path, value)
        manifest = json.loads(self.manifest.read_bytes())
        manifest["artifacts"] = [intel.identity(p) for p in sorted(self.bundle.iterdir()) if p != self.manifest]
        intel.write(self.manifest, manifest)
        with self.assertRaisesRegex(ValueError, "supplement differs"):
            intel.verify_bundle(self.bundle, self.ctx)

    def test_publication_plans_only_missing_wheel_and_reuses_identical_bytes(self):
        args = self.publication_args()
        with patch.object(intel, "qualification_run"), patch.object(intel, "tag_source", return_value="a" * 40), \
             patch.object(intel, "missing_github", return_value=[]), patch.object(intel, "missing_pypi", return_value=True), \
             patch.object(intel, "run") as run:
            intel.publication(args)
            run.assert_not_called()
            self.assertEqual([self.wheel], [p.name for p in args.upload.iterdir()])
            self.assertEqual((self.bundle / self.wheel).read_bytes(), (args.upload / self.wheel).read_bytes())
        shutil.rmtree(args.upload)
        with patch.object(intel, "qualification_run"), patch.object(intel, "tag_source", return_value="a" * 40), \
             patch.object(intel, "missing_github", return_value=[]), patch.object(intel, "missing_pypi", return_value=False), \
             patch.object(intel, "run") as run:
            intel.publication(args)
            self.assertEqual([], list(args.upload.iterdir()))
            intel.publication(self.publication_args("publish"))
            run.assert_not_called()  # Idempotent completed publication.

    def test_conflict_run_mismatch_or_unpublished_pypi_prevents_any_upload(self):
        for problem in ("github", "github_python", "pypi", "run", "tag"):
            with self.subTest(problem=problem), patch.object(intel, "run") as run, \
                 patch.object(intel, "qualification_run"), \
                 patch.object(intel, "tag_source", return_value=("b" if problem == "tag" else "a") * 40), \
                 patch.object(intel, "missing_github", side_effect=ValueError("conflict") if problem == "github" else [[], ValueError("conflict")] if problem == "github_python" else None, return_value=[]), \
                 patch.object(intel, "missing_pypi", return_value=problem == "pypi"):
                args = self.publication_args("publish")
                if problem == "run":
                    args.run_id = 43
                with self.assertRaises(ValueError):
                    intel.publication(args)
                run.assert_not_called()

    def test_github_missing_identical_conflicting_and_legacy_digest_assets(self):
        item = intel.identity(self.bundle / self.wheel)
        asset = {"name": item["name"], "size": item["size"], "state": "uploaded", "digest": "sha256:" + item["sha256"]}
        with patch.object(intel, "release", return_value={"assets": []}):
            self.assertEqual([self.wheel], intel.missing_github(self.ctx["tag"], [item]))
        with patch.object(intel, "release", return_value={"assets": [asset]}):
            self.assertEqual([], intel.missing_github(self.ctx["tag"], [item]))
            for field, bad in (("size", item["size"] + 1), ("digest", "sha256:" + "0" * 64), ("state", "new")):
                original = asset[field]
                asset[field] = bad
                with self.assertRaisesRegex(ValueError, "differs"):
                    intel.missing_github(self.ctx["tag"], [item])
                asset[field] = original
        asset["digest"] = None
        with patch.object(intel, "release", return_value={"assets": [asset]}), \
             patch.object(intel, "download", return_value=self.bundle / self.wheel) as download:
            self.assertEqual([], intel.missing_github(self.ctx["tag"], [item]))
            download.assert_called_once()

    def test_pypi_identity_and_missing_wheel(self):
        item = intel.identity(self.bundle / self.wheel)
        payload = {"info": {"version": self.version}, "urls": []}
        def response(*args, **kwargs):
            return io.BytesIO(json.dumps(payload).encode())
        with patch.object(intel, "urlopen", side_effect=response):
            self.assertTrue(intel.missing_pypi(self.version, item))
            payload["urls"] = [{"filename": self.wheel, "size": item["size"], "digests": {"sha256": item["sha256"]}}]
            self.assertFalse(intel.missing_pypi(self.version, item))
            payload["urls"][0]["digests"]["sha256"] = "0" * 64
            with self.assertRaisesRegex(ValueError, "differs"):
                intel.missing_pypi(self.version, item)

    def test_qualification_requires_successful_native_job_on_exact_source(self):
        result = {"head_sha": "a" * 40, "path": ".github/workflows/alopex-py-release.yml",
                  "repository": {"full_name": intel.REPO}, "event": "workflow_dispatch"}
        jobs = {"jobs": [{"name": "Qualify optional Intel artifacts", "conclusion": "success"}]}
        with patch.object(intel, "api", side_effect=lambda path: jobs if "/jobs?" in path else result):
            intel.qualification_run(42, "a" * 40)
            for conclusion in (None, "failure", "cancelled", "skipped"):
                jobs["jobs"][0]["conclusion"] = conclusion
                with self.assertRaisesRegex(ValueError, "not succeeded"):
                    intel.qualification_run(42, "a" * 40)
            jobs["jobs"][0]["conclusion"] = "success"
            result["head_sha"] = "b" * 40
            with self.assertRaisesRegex(ValueError, "not bound"):
                intel.qualification_run(42, "a" * 40)

    def test_prepare_preserves_rc_envelope_and_binds_stable_to_same_commit(self):
        version = self.version
        primary_name = f"parser-vendor-manifest-v{version}.json"
        envelope_name = f"parser-assets-v{version}.json"
        candidate_name = f"candidate-manifest-v{version}.json"
        source = self.root / "published"
        source.mkdir()
        shutil.copyfile(self.fixture.primary, source / primary_name)
        envelope = self.fixture.payload(self.fixture.envelope)
        envelope["source"]["tag"] = f"v{version}-rc.1"
        intel.write(source / envelope_name, envelope)
        candidate = {"schema": "alopex-release-candidate-v1", "version": version, "rc_tag": f"v{version}-rc.1",
                     "commit_sha": "a" * 40, "artifacts": [intel.identity(source / primary_name), intel.identity(source / envelope_name)]}
        candidate["verification"] = [{"name": "Development CI", "run_id": 9, "conclusion": "success"}]
        intel.write(source / candidate_name, candidate)
        def download(tag, name, dest, assets, limit):
            shutil.copyfile(source / name, dest / name)
            return dest / name
        args = argparse.Namespace(tag=f"v{version}", source_sha="a" * 40, context=self.root / "prepared")
        with patch.object(intel, "tag_source", return_value="a" * 40), patch.object(intel, "run", return_value="a" * 40), \
             patch.object(intel, "release", return_value={"assets": []}), patch.object(intel, "download", side_effect=download):
            intel.prepare(args)
            self.assertEqual((source / envelope_name).read_bytes(), (args.context / envelope_name).read_bytes())
            self.assertEqual(json.loads((args.context / "stable-parser-envelope.json").read_bytes())["source"],
                             {"tag": f"v{version}", "tag_sha": "a" * 40})
            candidate["commit_sha"] = "b" * 40
            intel.write(source / candidate_name, candidate)
            args.context = self.root / "wrong-source"
            with self.assertRaisesRegex(ValueError, "identity differs"):
                intel.prepare(args)

    def test_stage_installs_same_verified_parser_into_consumer_and_wheel(self):
        shutil.copyfile(self.fixture.envelope, self.context_dir / "stable-parser-envelope.json")
        args = argparse.Namespace(context=self.context_dir, parser_dir=self.fixture.archive.parent,
                                  bundle=self.root / "staged", native=self.root / "native")
        with contextlib.chdir(self.root):
            intel.stage(args)
        record = self.fixture.payload(self.fixture.record)
        native = args.native / "libalopex_sql_parser.dylib"
        packaged = self.root / "crates/alopex-py/python/alopex/native/libalopex_sql_parser.dylib"
        self.assertEqual(record["library"]["sha256"], intel.digest(native))
        self.assertEqual(native.read_bytes(), packaged.read_bytes())
        self.assertEqual(self.fixture.primary.read_bytes(), (self.context_dir / self.ctx["primary"]).read_bytes())


class PrimaryDistributionTests(unittest.TestCase):
    def names(self, version="0.8.17"):
        return [f"alopex-{version}-cp38-abi3-{tag}.whl" for tag in
                ("manylinux_2_17_x86_64.manylinux2014_x86_64", "win_amd64", "macosx_11_0_arm64")] + [f"alopex-{version}.tar.gz"]

    def test_primary_requires_each_platform_and_sdist_but_not_intel(self):
        names = self.names()
        primary_distributions.validate(names, "0.8.17")
        for name in names:
            with self.subTest(missing=name), self.assertRaisesRegex(ValueError, "missing primary"):
                primary_distributions.validate([n for n in names if n != name], "0.8.17")
        primary_distributions.validate(names + ["alopex-0.8.17-cp38-abi3-macosx_10_12_x86_64.whl"], "0.8.17")

    def test_old_release_still_requires_intel_and_bad_artifacts_rejected(self):
        with self.assertRaisesRegex(ValueError, "intel"):
            primary_distributions.validate(self.names("0.8.16"), "0.8.16")
        for extra in (self.names()[0], "alopex-0.8.17-cp38-abi3-linux_aarch64.whl", "alopex-0.8.16.tar.gz"):
            with self.subTest(extra=extra), self.assertRaises(ValueError):
                primary_distributions.validate(self.names() + [extra], "0.8.17")


class IndependentIntelWorkflowTests(unittest.TestCase):
    @staticmethod
    def jobs(path):
        text = (ROOT / path).read_text().split("\njobs:\n", 1)[1]
        parts = re.split(r"^  ([a-z][a-z0-9_-]*):\n", text, flags=re.M)
        return dict(zip(parts[1::2], parts[2::2]))

    def test_primary_dependency_graph_ignores_every_intel_terminal_or_pending_state(self):
        core = self.jobs(".github/workflows/release.yml")
        python = self.jobs(".github/workflows/alopex-py-release.yml")
        targets = re.findall(r"^            target: (\S+)", core["build-release"], re.M)
        required = {"x86_64-unknown-linux-gnu", "x86_64-pc-windows-msvc", "aarch64-apple-darwin"}
        self.assertEqual(set(targets), required)
        self.assertEqual(re.findall(r"^            target_triple: (\S+)", python["macos"], re.M), ["aarch64-apple-darwin"])
        self.assertIn("needs: [ci-gate, build-release]", core["create-release"])
        self.assertIn("needs: [linux, macos, windows, sdist]", python["candidate-release"])
        for state in ("queued", "in_progress", "failure", "cancelled", "success"):
            # GitHub's matrix needs gate requires all matrix children to succeed.
            results = dict.fromkeys(required, "success") | {intel.TARGET: state}
            self.assertTrue(all(results[t] == "success" for t in targets), state)
            for failed in required:
                results[failed] = "failure"
                self.assertFalse(all(results[t] == "success" for t in targets), (state, failed))
                results[failed] = "success"
        for name, body in core.items():
            self.assertNotRegex(body, r"needs:.*intel", name)
        for name, body in python.items():
            if not name.startswith("intel-"):
                self.assertNotRegex(body, r"needs:.*intel", name)

    def test_intel_native_and_publication_are_manual_isolated_and_retry_without_rebuild(self):
        jobs = self.jobs(".github/workflows/alopex-py-release.yml")
        build, publish = jobs["intel-qualify"], jobs["intel-publish"]
        self.assertIn("inputs.mode == 'intel' && inputs.intel_artifact_run_id == ''", build)
        self.assertIn("runs-on: macos-15-intel", build)
        self.assertIn("cargo run --locked --release --target x86_64-apple-darwin --manifest-path intel-consumer/Cargo.toml", build)
        self.assertIn("verify_python_vector_api.py", build)
        self.assertNotIn("ALLOW_LOCAL_BUILD: \"1\"", build)
        self.assertIn("needs: [intel-qualify]", publish)
        self.assertIn("needs.intel-qualify.result == 'skipped' && inputs.intel_artifact_run_id != ''", publish)
        self.assertIn("run-id: ${{ inputs.intel_artifact_run_id || github.run_id }}", publish)
        self.assertIn("--run-id \"$QUALIFICATION_RUN_ID\"", publish)
        self.assertIn("skip-existing: false", publish)
        self.assertNotIn("--clobber", publish)
        self.assertNotIn("cargo build", publish)


if __name__ == "__main__":
    unittest.main()
