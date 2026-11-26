"""A selected public entry never exposes private configuration or errors."""

from contextlib import redirect_stdout
import io
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import issue681_public_admission as admission
import issue681_public_selected as selected
from test_issue681_public_pair import PINS


class SelectedEntryTests(unittest.TestCase):
    def config(self, root):
        root = root.resolve(strict=True)
        paths = {}
        for name in ("before", "candidate", "work", "export", "admission",
                     "inventory"):
            path = root / name
            path.mkdir(mode=0o700)
            paths[name] = str(path)
        (root / "inventory").chmod(0o755)
        data = {"schema": selected.SCHEMA, "finish": "rollback",
                "profile": {"schema": admission.SCHEMA,
                            "runner_kind": "selected_linux", "cpu_count": 4,
                            "additional_bytes": 1_000_000_000},
                "pins": PINS,
                "assets": {arm: paths[arm] for arm in ("before", "candidate")},
                "work": paths["work"], "export": paths["export"],
                "admission": paths["admission"],
                "private_roots": [paths["inventory"]],
                "extra_roots": []}
        path = root / "selected.json"
        path.write_text(json.dumps(data))
        path.chmod(0o600)
        return path, data

    def invoke(self, path):
        output = io.StringIO()
        with redirect_stdout(output):
            code = selected.main([str(path)])
        return code, output.getvalue()

    def test_valid_config_runs_only_selected_public_mode(self):
        with tempfile.TemporaryDirectory() as tmp:
            path, _ = self.config(Path(tmp))
            with mock.patch.object(selected.runner, "run_selected_mode",
                                   return_value={"status": "measured"}) as run:
                self.assertEqual(self.invoke(path), (0, "PUBLIC_SELECTED_OK\n"))
            self.assertEqual(run.call_count, 1)
            self.assertEqual(run.call_args.kwargs["finish"], "rollback")

    def test_extra_debug_field_and_duplicate_key_are_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            path, data = self.config(Path(tmp))
            data["debug_log"] = "/private/diagnostics"
            path.write_text(json.dumps(data))
            self.assertEqual(self.invoke(path), (1, "PUBLIC_SELECTED\n"))
            del data["debug_log"]
            path.write_text(json.dumps(data)[:-1] + ',"finish":"commit"}')
            self.assertEqual(self.invoke(path), (1, "PUBLIC_SELECTED\n"))

    def test_readable_or_symlinked_config_is_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            path, _ = self.config(root)
            path.chmod(0o644)
            self.assertEqual(self.invoke(path), (1, "PUBLIC_SELECTED\n"))
            path.chmod(0o600)
            alias = root / "alias.json"
            alias.symlink_to(path)
            self.assertEqual(self.invoke(alias), (1, "PUBLIC_SELECTED\n"))

    def test_private_failure_prints_only_fixed_code(self):
        with tempfile.TemporaryDirectory() as tmp:
            path, _ = self.config(Path(tmp))
            with mock.patch.object(selected.runner, "run_selected_mode",
                                   side_effect=RuntimeError("/private/host/path")):
                self.assertEqual(self.invoke(path), (1, "PUBLIC_SELECTED\n"))


if __name__ == "__main__":
    unittest.main()
