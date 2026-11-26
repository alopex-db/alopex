"""Two selected-host asset arms must finish together before promotion."""

from pathlib import Path
import sys
import tempfile
import unittest
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import issue681_hosted_build_pair as build


def pin(commit, tree, binary, asset, lock):
    return {"source_commit": commit, "source_tree": tree,
            "binary_sha256": binary, "example_sha256": "e" * 64,
            "parser_sha256": "f" * 64, "assets_digest": asset,
            "profile": "release", "platform": "linux/amd64",
            "parser_contract": "0.28.0", "parser_record_sha256": "a" * 64,
            "cargo_lock_sha256": lock, "rust_version": "1.90.0",
            "version": "0.8.17"}


class HostedBuildPairTest(unittest.TestCase):
    def guard(self, root, events=None, samples=None):
        device = root.stat().st_dev

        def scan():
            if events is not None:
                events.append("guard")
            used = next(samples) if samples is not None else 20_000_000_000
            return used, {device: 20_000_000_000}

        return build.capacity.HostStorageGuard(root, scanner=scan)

    def inputs(self, root):
        common = {"target": root / "target", "parser_record": root / "record",
                  "parser_root": root / "parser",
                  "parser_library": root / "parser/lib.so",
                  "example_sha256": "e" * 64,
                  "parser_record_sha256": "a" * 64,
                  "parser_sha256": "f" * 64, "rust_version": "1.90.0",
                  "additional_storage_bytes": 8_000_000_000,
                  "private_roots": (str(root),),
                  "build_lock": root / "build.lock",
                  "cargo_home": root / "cargo-home",
                  "vendor_mode_sha256": "c" * 64,
                  "git_binary": Path("/usr/bin/git"), "git_env": {}}
        return {
            "before": dict(common, source=root / "before", work=root / "work1",
                           source_commit="1" * 40,
                           source_tree="2" * 40, cargo_lock_sha256="3" * 64),
            "candidate": dict(common, source=root / "candidate",
                              work=root / "work2",
                              source_commit="4" * 40, source_tree="5" * 40,
                              cargo_lock_sha256="6" * 64)}

    def test_serial_arms_share_guard_and_reopen_both_bundles(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp).resolve()
            inputs = self.inputs(root)
            output_parent = root / "public-staging"
            output_parent.mkdir(mode=0o700)
            events = []
            guard = self.guard(root, events)
            events.clear()
            pins = [pin("1" * 40, "2" * 40, "7" * 64, "8" * 64, "3" * 64),
                    pin("4" * 40, "5" * 40, "9" * 64, "b" * 64, "6" * 64)]

            def produce(**kwargs):
                events.append(kwargs["source_commit"])
                self.assertIs(kwargs["host_guard"], guard)
                return pins.pop(0)

            def verify(path, result, *, private_roots):
                events.append("verify:" + path.name)
                self.assertEqual(private_roots, (str(root),))

            with mock.patch.object(build.producer, "produce", side_effect=produce), \
                 mock.patch.object(build.assets, "verify", side_effect=verify):
                result = build.build_pair(inputs, output_parent, guard)
            self.assertEqual(set(result["pins"]), {"before", "candidate"})
            self.assertEqual(result["staging"].parent, output_parent)
            self.assertEqual(events, ["guard", "guard", "1" * 40,
                                      "guard", "verify:before",
                                      "guard", "4" * 40, "guard", "verify:candidate",
                                      "guard"])

    def test_incompatible_pair_or_missing_arm_returns_fixed_failure(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp).resolve()
            inputs = self.inputs(root)
            output_parent = root / "public-staging"
            output_parent.mkdir(mode=0o700)
            guard = self.guard(root)
            bad = pin("4" * 40, "5" * 40, "9" * 64, "b" * 64, "6" * 64)
            bad["parser_sha256"] = "0" * 64
            with mock.patch.object(build.producer, "produce",
                                   side_effect=[pin("1" * 40, "2" * 40,
                                                    "7" * 64, "8" * 64,
                                                    "3" * 64), bad]), \
                 mock.patch.object(build.assets, "verify"):
                with self.assertRaisesRegex(ValueError, "^PUBLIC_BUILD$"):
                    build.build_pair(inputs, output_parent, guard)
            self.assertFalse(any(output_parent.iterdir()))
            with self.assertRaisesRegex(ValueError, "^PUBLIC_BUILD$"):
                build.build_pair({"before": inputs["before"]}, output_parent,
                                 guard)

    def test_second_arm_failure_removes_first_private_bundle(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp).resolve()
            inputs = self.inputs(root)
            output_parent = root / "public-staging"
            output_parent.mkdir(mode=0o700)
            guard = self.guard(root)

            def produce(**kwargs):
                if kwargs["source_commit"] == "4" * 40:
                    raise ValueError("private second-arm failure")
                kwargs["output"].mkdir()
                (kwargs["output"] / "partial.txt").write_text("private")
                return pin("1" * 40, "2" * 40, "7" * 64,
                           "8" * 64, "3" * 64)

            with mock.patch.object(build.producer, "produce", side_effect=produce), \
                 mock.patch.object(build.assets, "verify"):
                with self.assertRaisesRegex(ValueError, "^PUBLIC_BUILD$"):
                    build.build_pair(inputs, output_parent, guard)
            self.assertFalse(any(output_parent.iterdir()))

    def test_cross_arm_work_cannot_overlap_a_source(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp).resolve()
            inputs = self.inputs(root)
            output_parent = root / "public-staging"
            output_parent.mkdir(mode=0o700)
            events = []
            guard = self.guard(root, events)
            events.clear()
            inputs["candidate"]["work"] = inputs["before"]["source"] / "scratch"
            with self.assertRaisesRegex(ValueError, "^PUBLIC_BUILD$"):
                build.build_pair(inputs, output_parent, guard)
            self.assertEqual(events, [])

    def test_cargo_home_must_match_between_arms(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp).resolve()
            inputs = self.inputs(root)
            output_parent = root / "public-staging"
            output_parent.mkdir(mode=0o700)
            guard = self.guard(root)
            inputs["candidate"]["cargo_home"] = root / "other-cargo"
            with mock.patch.object(build.producer, "produce") as produce:
                with self.assertRaisesRegex(ValueError, "^PUBLIC_BUILD$"):
                    build.build_pair(inputs, output_parent, guard)
            produce.assert_not_called()

    def test_acquisition_growth_consumes_same_build_allowance(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp).resolve()
            inputs = self.inputs(root)
            output_parent = root / "public-staging"
            output_parent.mkdir(mode=0o700)
            guard = self.guard(root, samples=iter((20_000_000_000,
                                                   25_000_000_000,
                                                   28_000_000_000)))
            guard()  # Acquisition has consumed 5 GB since the original baseline.
            with mock.patch.object(build.producer, "produce") as produce:
                with self.assertRaisesRegex(ValueError, "^PUBLIC_BUILD$"):
                    build.build_pair(inputs, output_parent, guard)
            produce.assert_not_called()
            self.assertFalse(any(output_parent.iterdir()))


if __name__ == "__main__":
    unittest.main()
