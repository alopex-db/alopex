"""Exact acquired inputs must become one guarded two-arm asset build."""

from pathlib import Path
import sys
import tempfile
import unittest
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import issue681_hosted_build_join as join


class Guard:
    def __init__(self):
        self.calls = 0

    def __call__(self):
        self.calls += 1


class HostedBuildJoinTest(unittest.TestCase):
    def layout(self, root):
        root = root.resolve()
        workspace = root / "checkout"
        task = root / "acquired"
        cargo = root / "cargo"
        build_parent = root / "build-parent"
        for path in (workspace, task, cargo, build_parent,
                     task / "sources/before", task / "sources/candidate",
                     task / "parser-stage/parser-input",
                     task / "parser-build/archive", cargo / "vendor"):
            path.mkdir(mode=0o700, parents=True, exist_ok=True)
        guard = Guard()
        parser_root = task / "parser-stage/parser-input"
        acquired = {
            "guard": guard,
            "sources": {name: task / "sources" / name
                        for name in ("before", "candidate")},
            "parser": {
                "parser_record": task / "parser-build/archive/record.json",
                "parser_root": parser_root,
                "parser_library": parser_root / "lib/parser.so",
                "parser_record_sha256": "a" * 64,
                "parser_sha256": "b" * 64},
            "vendor": {"cargo_home": cargo, "vendor_root": cargo / "vendor",
                       "bundle_sha256": join.vendor_consume.ZIP_SHA256,
                       "vendor_tree_sha256": join.VENDOR_TREE_SHA256,
                       "vendor_mode_sha256": "c" * 64},
            "git_binary": join.acquire_source.GIT, "git_env": {},
            "task": task, "cargo_home": cargo}
        return workspace, build_parent, acquired

    def test_exact_acquired_guard_vendor_and_source_pins_reach_pair(self):
        with tempfile.TemporaryDirectory() as tmp:
            workspace, build_parent, acquired = self.layout(Path(tmp))
            calls = []

            def pair(arms, output_parent, guard):
                calls.append((arms, output_parent, guard))
                self.assertIs(guard, acquired["guard"])
                self.assertEqual(set(arms), {"before", "candidate"})
                for name in ("before", "candidate"):
                    pin = join.acquire_source.PINS[name]
                    self.assertEqual(arms[name]["source"],
                                     acquired["sources"][name])
                    self.assertEqual(arms[name]["source_commit"], pin["commit"])
                    self.assertEqual(arms[name]["source_tree"], pin["tree"])
                    self.assertEqual(arms[name]["cargo_lock_sha256"], pin["lock"])
                    self.assertEqual(arms[name]["cargo_home"],
                                     acquired["cargo_home"])
                    self.assertEqual(arms[name]["vendor_mode_sha256"], "c" * 64)
                    self.assertEqual(arms[name]["rust_version"], "1.90.0")
                self.assertEqual(arms["before"]["target"],
                                 arms["candidate"]["target"])
                self.assertEqual(arms["before"]["build_lock"],
                                 arms["candidate"]["build_lock"])
                self.assertEqual(output_parent.name, "bundles")
                return {"pins": {"before": {}, "candidate": {}},
                        "staging": output_parent / "pair"}

            with mock.patch.object(join.capacity, "HostStorageGuard", Guard), \
                 mock.patch.object(join.build_pair, "build_pair",
                                   side_effect=pair):
                result = join.build(acquired, workspace, build_parent)
            self.assertEqual(len(calls), 1)
            self.assertIs(result["guard"], acquired["guard"])
            self.assertEqual(result["cargo_home"], acquired["cargo_home"])
            self.assertGreaterEqual(acquired["guard"].calls, 3)
            self.assertEqual(result["build_task"].parent, build_parent)

    def test_pair_failure_removes_only_join_owned_tree(self):
        with tempfile.TemporaryDirectory() as tmp:
            workspace, build_parent, acquired = self.layout(Path(tmp))
            with mock.patch.object(join.capacity, "HostStorageGuard", Guard), \
                 mock.patch.object(join.build_pair, "build_pair",
                                   side_effect=RuntimeError("private build log")):
                with self.assertRaisesRegex(ValueError, "^PUBLIC_BUILD$"):
                    join.build(acquired, workspace, build_parent)
            self.assertEqual(list(build_parent.iterdir()), [])
            self.assertTrue(acquired["task"].is_dir())
            self.assertTrue(acquired["cargo_home"].is_dir())

    def test_changed_vendor_or_layout_stops_before_pair(self):
        for changed in ("vendor", "vendor-mode", "overlap"):
            with self.subTest(changed=changed), tempfile.TemporaryDirectory() as tmp:
                workspace, build_parent, acquired = self.layout(Path(tmp))
                if changed == "vendor":
                    acquired["vendor"]["cargo_home"] = acquired["task"]
                elif changed == "vendor-mode":
                    acquired["vendor"]["vendor_mode_sha256"] = "not-a-digest"
                else:
                    build_parent = acquired["task"]
                with mock.patch.object(join.capacity, "HostStorageGuard", Guard), \
                     mock.patch.object(join.build_pair, "build_pair") as pair:
                    with self.assertRaisesRegex(ValueError, "^PUBLIC_BUILD$"):
                        join.build(acquired, workspace, build_parent)
                pair.assert_not_called()

    def test_locked_cargo_vendor_mode_passes_vendor_tree_sha256_to_pair(self):
        with tempfile.TemporaryDirectory() as tmp:
            workspace, build_parent, acquired = self.layout(Path(tmp))
            tree_sha = "d" * 64
            acquired["vendor"] = {
                "cargo_home": acquired["cargo_home"],
                "vendor_root": acquired["cargo_home"] / "vendor",
                "vendor_tree_sha256": tree_sha,
                "vendor_mode_sha256": "c" * 64,
                "acquisition_mode": "cargo_vendor_locked",
            }
            seen = []

            def pair(arms, output_parent, guard):
                seen.append(arms)
                for name in ("before", "candidate"):
                    self.assertEqual(arms[name]["vendor_tree_sha256"], tree_sha)
                return {"pins": {"before": {}, "candidate": {}},
                        "staging": output_parent / "pair"}

            with mock.patch.object(join.capacity, "HostStorageGuard", Guard), \
                 mock.patch.object(join.build_pair, "build_pair", side_effect=pair):
                join.build(acquired, workspace, build_parent)
            self.assertEqual(len(seen), 1)


if __name__ == "__main__":
    unittest.main()
