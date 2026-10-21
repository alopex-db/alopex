import unittest

import polars_common_v0816 as subject


class ContractTests(unittest.TestCase):
    def test_values_alone_do_not_prove_dtype_contract(self):
        wanted = {"kind": "DataFrame", "values": {"x": [1, None]}, "schema": [["x", "Int64"]]}
        actual = {**wanted, "schema": None, "schema_error": "AttributeError"}
        result = subject.compare({"execution": "ok", "observation": wanted},
                                 {"execution": "ok", "observation": actual}, False)
        self.assertEqual(result["values"], "match")
        self.assertEqual(result["schema"], "unverified")
        self.assertEqual(result["status"], "failed")

    def test_value_type_null_and_order_mismatches_fail(self):
        expected = {"kind": "DataFrame", "values": {"a": [1, None], "b": [True, False]},
                    "schema": [["a", "Int64"], ["b", "Boolean"]]}
        for data in ({"a": [True, None], "b": [True, False]},
                     {"a": [1, 0], "b": [True, False]},
                     {"a": [None, 1], "b": [True, False]},
                     {"b": [True, False], "a": [1, None]}):
            result = subject.compare({"execution": "ok", "observation": expected},
                                     {"execution": "ok", "observation": {**expected, "values": data}}, False)
            self.assertEqual(result["status"], "failed")

    def test_reference_failure_does_not_accuse_subject(self):
        result = subject.compare({"execution": "error", "exception": "TypeError"},
                                 {"execution": "error", "exception": "AttributeError"}, False)
        self.assertEqual(result["status"], "invalid-reference")

    def test_expected_error_requires_matching_contract(self):
        error = {"execution": "error", "exception": "ValueError"}
        self.assertEqual(subject.compare(error, error, True)["status"], "passed")
        self.assertEqual(subject.compare(error, {**error, "exception": "TypeError"}, True)["status"], "failed")
        self.assertEqual(subject.compare({"execution": "ok"}, error, True)["status"], "invalid-reference")

    def test_batch_observer_consumes_every_row_and_keeps_column_order(self):
        DataFrame = type("DataFrame", (), {
            "schema": {"b": "Int64", "a": "String"},
            "to_dict": lambda self, **kwargs: self.values,
        })
        frames = []
        for values in ({"b": [1, 2], "a": ["x", None]}, {"b": [3], "a": ["z"]}):
            frame = DataFrame()
            frame.values = values
            frames.append(frame)
        actual = subject.observe(subject.Batches(iter(frames)))
        self.assertEqual(actual["values"], [["b", [1, 2, 3]], ["a", ["x", None, "z"]]])


if __name__ == "__main__":
    unittest.main()
