import unittest

from scripts.compatibility.source_config import classify


def diagnostic(code="E0063", path="legacy_config_literal.rs"):
    return {
        "reason": "compiler-message",
        "message": {
            "level": "error",
            "code": {"code": code},
            "message": "missing fields `hnsw_cache_bytes` and `hnsw_cache_entries`",
            "spans": [{"file_name": path, "is_primary": True}],
        },
    }


class SourceContractTests(unittest.TestCase):
    def test_only_the_exact_client_error_is_known_incompatible(self):
        self.assertEqual(classify(101, [diagnostic()]), "known_incompatible")
        for messages in (
            [diagnostic("E0432")],
            [diagnostic(path="dependency.rs")],
            [diagnostic(), diagnostic()],
            [],
        ):
            with self.subTest(messages=messages):
                self.assertEqual(classify(101, messages), "failed")

    def test_crash_timeout_and_inconsistent_success_fail(self):
        self.assertEqual(classify(-6, [diagnostic()]), "failed")
        self.assertEqual(classify(101, [diagnostic()], timed_out=True), "failed")
        self.assertEqual(classify(0, [diagnostic()]), "failed")
        self.assertEqual(classify(0, []), "supported")

    def test_an_additional_missing_field_is_not_the_known_break(self):
        message = diagnostic()
        message["message"]["message"] += ", `new_setting`"
        self.assertEqual(classify(101, [message]), "failed")
