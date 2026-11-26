"""Source and trust-boundary checks for the private PERF-90 pilot profile."""

from copy import deepcopy
import hashlib
import json
import unittest

from scripts.ops.perf_profile import (StageContract, validate_profile,
                                      PERF_TESTS)


BINDING = {
    "source_sha": "a" * 40,
    "tree_sha": "b" * 40,
    "workflow_blob": "c" * 40,
    "controller_blob": "d" * 40,
    "repository_id": 71,
    "run_id": 72,
    "run_attempt": 1,
    "runner_profile_sha256": "e" * 64,
    "baseline_artifact_id": 73,
    "baseline_sha256": "f" * 64,
    "toolchain_lock_sha256": "1" * 64,
}
MANIFEST = (StageContract("source_acquisition", 1),
            StageContract(PERF_TESTS[0], 1),
            StageContract(PERF_TESTS[1], 1),
            StageContract("cleanup", 1))


def profile() -> dict:
    return {
        "schema": "alopex.perf-throughput-profile/v1",
        "binding": deepcopy(BINDING),
        "stages": [
            {"name": contract.name, "observed_units": 1,
             "observed_ms": (10_000 if contract.name == PERF_TESTS[0]
                             else 1_000_000 if contract.name == PERF_TESTS[1]
                             else 5_000)}
            for contract in MANIFEST
        ],
        "reports": {name: str(index + 2) * 64
                    for index, name in enumerate(PERF_TESTS)},
        "pilot_baseline_alopex_bytes": 1_000,
        "pilot_peak_alopex_bytes": 2_000,
        "pilot_min_free_bytes": 3_000,
    }


def canonical(value: dict) -> bytes:
    return (json.dumps(value, sort_keys=True, separators=(",", ":"),
                       allow_nan=False).encode() + b"\n")


def receipt(data: bytes) -> dict:
    return {
        "profile_sha256": hashlib.sha256(data).hexdigest(),
        "artifact_id": 74,
        "artifact_digest": "sha256:" + "4" * 64,
        "repository_id": BINDING["repository_id"],
        "run_id": BINDING["run_id"],
        "run_attempt": BINDING["run_attempt"],
        "source_sha": BINDING["source_sha"],
    }


def check(value: dict | None = None, *, data: bytes | None = None,
          binding: dict | None = None, trusted: dict | None = None,
          manifest: tuple[StageContract, ...] = MANIFEST):
    if data is None:
        data = canonical(profile() if value is None else value)
    return validate_profile(data, expected_binding=deepcopy(BINDING) if binding is None
                            else binding,
                            trusted_receipt=receipt(data) if trusted is None
                            else trusted,
                            manifest=manifest)


class PerfProfileTests(unittest.TestCase):
    def reject(self, *args, **kwargs):
        with self.assertRaisesRegex(ValueError, "^PROFILE$"):
            check(*args, **kwargs)

    def test_valid_pilot_returns_distinct_stage_rates_and_growth(self):
        result = check()
        self.assertEqual(tuple(stage.name for stage in result.stages),
                         tuple(contract.name for contract in MANIFEST))
        self.assertEqual(result.stages[1].observed_seconds, 10.0)
        self.assertEqual(result.stages[2].observed_seconds, 1000.0)
        self.assertEqual(result.pilot_growth_bytes, 1000)
        self.assertEqual(result.pilot_min_free_bytes, 3000)
        self.assertEqual(tuple(name for name, _digest in result.report_hashes),
                         PERF_TESTS)

    def test_json_must_be_bounded_canonical_and_unique(self):
        self.reject(data=canonical(profile())[:-1])
        self.reject(data=b"{" + b" " * 65_536 + b"}")
        self.reject(data=canonical(profile()).replace(b'"schema":',
                                                      b'"schema":"x","schema":', 1))
        self.reject(data=canonical(profile()).replace(b'"pilot_min_free_bytes":3000',
                                                      b'"pilot_min_free_bytes":NaN'))
        self.reject(data=b"[" * 16_000 + b"0" + b"]" * 16_000)

    def test_receipt_and_expected_identity_must_match(self):
        data = canonical(profile())
        altered = receipt(data)
        altered["profile_sha256"] = "0" * 64
        self.reject(data=data, trusted=altered)
        altered = receipt(data)
        altered["run_attempt"] = 2
        self.reject(data=data, trusted=altered)
        altered = receipt(data)
        altered["repository_id"] = True
        self.reject(data=data, trusted=altered)
        expected = deepcopy(BINDING)
        expected["source_sha"] = "0" * 40
        self.reject(data=data, binding=expected)

    def test_stage_manifest_order_completeness_and_group_are_fixed(self):
        value = profile()
        value["stages"].reverse()
        self.reject(value)
        value = profile()
        value["stages"].pop()
        self.reject(value)
        value = profile()
        value["stages"][1]["observed_units"] = 2
        self.reject(value)
        self.reject(manifest=(StageContract("source_acquisition", 1),
                              StageContract(PERF_TESTS[0], 1)))

    def test_reports_and_storage_are_bounded(self):
        value = profile()
        del value["reports"][PERF_TESTS[1]]
        self.reject(value)
        value = profile()
        value["pilot_peak_alopex_bytes"] = 40_000_000_000
        self.reject(value)
        value = profile()
        value["pilot_peak_alopex_bytes"] = 500
        self.reject(value)
        value = profile()
        value["pilot_min_free_bytes"] = False
        self.reject(value)

    def test_stage_observation_types_and_limits_are_fixed(self):
        value = profile()
        value["stages"][2]["observed_ms"] = 0
        self.reject(value)
        value = profile()
        value["stages"][2]["observed_ms"] = 5_400_001
        self.reject(value)
        value = profile()
        value["stages"][2]["observed_units"] = True
        self.reject(value)


if __name__ == "__main__":
    unittest.main()
