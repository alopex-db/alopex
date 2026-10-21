"""Ensure API regressions, skipped cases and missing corpus families fail closed."""

import copy
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from scripts.compatibility import api


def parameter(name, *, required=True, kind="POSITIONAL_OR_KEYWORD", default=None):
    return {"name": name, "kind": kind, "required": required, "default": default}


def test_report(status="passed"):
    phases = [
        {"when": p, "outcome": "passed", "xfail": None, "detail": None}
        for p in ["setup", "call", "teardown"]
    ]
    phases[1]["outcome"] = status
    name = "crates/alopex-py/tests/test_example.py::test_contract"
    return {"collected": [name], "tests": {name: {"phases": phases}}}


class APIAcceptanceTests(unittest.TestCase):
    def test_denied_loopback_is_an_infrastructure_error(self):
        with patch("scripts.compatibility.api.socket.socket") as create:
            create.return_value.__enter__.return_value.bind.side_effect = (
                PermissionError("denied")
            )
            with self.assertRaises(PermissionError):
                api.probe_loopback()

    def test_capacity_registry_extension_preserves_baseline_codes(self):
        from scripts.compatibility.test_api_runtime import (
            test_known_error_codes_preserved_with_explicit_capacity_addition as verify,
        )
        baseline = {"ALOPEX-PY001", "ALOPEX-PY999"}
        with patch("pathlib.Path.read_text", return_value="EXPECTED_ERROR_CODES = " + repr(baseline)):
            for actual in [baseline, baseline | {"ALOPEX-S004"}]:
                with patch.dict("sys.modules", {"alopex": SimpleNamespace(ALOPEX_ERROR_CODES=actual)}):
                    verify()
            for actual in [baseline - {"ALOPEX-PY001"}, baseline | {"unexpected"}]:
                with patch.dict("sys.modules", {"alopex": SimpleNamespace(ALOPEX_ERROR_CODES=actual)}):
                    with self.assertRaises(AssertionError):
                        verify()

    def test_optional_keyword_parameter_is_additive(self):
        before = [parameter("path")]
        after = before + [
            parameter("cache", required=False, kind="KEYWORD_ONLY", default="None")
        ]
        self.assertTrue(api.call_shapes_compatible(before, after))

    def test_new_required_parameter_is_not_compatible(self):
        before = [parameter("path")]
        self.assertFalse(
            api.call_shapes_compatible(before, before + [parameter("required")])
        )

    def test_renamed_keyword_parameter_is_not_compatible(self):
        self.assertFalse(
            api.call_shapes_compatible([parameter("path")], [parameter("uri")])
        )

    def test_changed_default_is_not_compatible(self):
        self.assertFalse(
            api.call_shapes_compatible(
                [parameter("limit", required=False, default="None")],
                [parameter("limit", required=False, default="100")],
            )
        )

    def test_lost_varargs_or_kwargs_are_detected(self):
        self.assertFalse(
            api.call_shapes_compatible([parameter("args", kind="VAR_POSITIONAL")], [])
        )
        self.assertFalse(
            api.call_shapes_compatible([parameter("kwargs", kind="VAR_KEYWORD")], [])
        )

    def test_removed_symbol_and_error_code_are_detected(self):
        old = {
            "symbols": {
                "alopex.Database.begin": {"kind": "callable", "parameters": []}
            },
            "error_codes": ["old"],
        }
        new = {"symbols": {}, "error_codes": []}
        self.assertEqual(len(api.compare_surfaces(old, new)), 2)

    def test_added_symbol_and_code_are_allowed(self):
        old = {"symbols": {}, "error_codes": ["old"]}
        new = {
            "symbols": {"alopex.new_method": {"kind": "callable", "parameters": []}},
            "error_codes": ["old", "new"],
        }
        self.assertEqual(api.compare_surfaces(old, new), [])

    def compare(self, old, new):
        with patch.dict(
            api.CONFIG,
            {
                "python_files": {"test_example.py": "test family"},
                "test_replacements": {},
            },
        ):
            return api.compare_reports(old, new)

    def test_old_passing_case_must_not_fail(self):
        result = self.compare(test_report(), test_report("failed"))
        self.assertEqual(len(result["regressions"]), 1)

    def test_old_passing_case_must_not_be_skipped(self):
        result = self.compare(test_report(), test_report("skipped"))
        self.assertEqual(len(result["regressions"]), 1)

    def test_missing_case_is_not_success(self):
        with self.assertRaises(AssertionError):
            self.compare(test_report(), {"collected": [], "tests": {}})

    def test_an_entire_skipped_family_is_not_coverage(self):
        with self.assertRaises(AssertionError):
            self.compare(test_report("skipped"), test_report("skipped"))

    def test_explicit_replacement_requires_a_real_matching_case(self):
        old, new = test_report(), test_report()
        node = "tests/compatibility-extensions/test_api_runtime.py::test_replacement"
        replacement = {
            "legacy::test_wording": {
                "replacement": "test_api_runtime.py::test_replacement",
                "reason": "owner prose changed",
            }
        }
        for result in [old, new]:
            base = next(iter(result["tests"]))
            result["collected"].append(node)
            result["tests"][node] = copy.deepcopy(result["tests"][base])
            result["deselected"] = ["legacy::test_wording"]
        with patch.dict(
            api.CONFIG,
            {
                "python_files": {"test_example.py": "family"},
                "test_replacements": replacement,
            },
        ):
            result = api.compare_reports(old, new)
            self.assertEqual(len(result["explicit_replacements"]), 1)
            old["collected"].remove(node)
            new["collected"].remove(node)
            with self.assertRaises(AssertionError):
                api.compare_reports(old, new)

    def test_baseline_failure_is_reported_without_inflating_baseline_passes(self):
        old, new = test_report(), test_report()
        base = next(iter(old["tests"]))
        failed = base + "_failed_in_old"
        for result in [old, new]:
            result["collected"].append(failed)
        old["tests"][failed] = copy.deepcopy(test_report("failed")["tests"][base])
        new["tests"][failed] = copy.deepcopy(test_report()["tests"][base])
        result = self.compare(old, new)
        self.assertEqual(result["baseline_passed"], 1)
        self.assertEqual(result["candidate_passed"], 2)
        self.assertEqual(result["baseline_failures"], [failed])
        self.assertEqual(result["improved"], [failed])


if __name__ == "__main__":
    unittest.main()
