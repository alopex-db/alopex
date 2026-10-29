"""Keep the runtime-method exception narrower than a wire-schema change."""

import unittest
from unittest.mock import patch

from scripts.ci import check_parser_wire_contract as contract


BASE = """#[derive(Serialize, Deserialize)]
enum StatementKind { Select, Values }
impl StatementKind {
    pub fn requires_write(&self) -> bool {
        match self { Self::Select => false, _ => true }
    }
}
"""
RUNTIME = BASE.replace("Self::Select => false", "Self::Select => visitor(self)")


class WriteClassifierExceptionTests(unittest.TestCase):
    def test_runtime_body_only_is_accepted(self):
        self.assertTrue(contract.runtime_only_ast_change(BASE, RUNTIME))

    def test_variant_change_is_not_masked(self):
        self.assertFalse(contract.runtime_only_ast_change(BASE, RUNTIME.replace("Select, Values", "Select, Values, Insert")))

    def test_field_and_type_changes_are_not_masked(self):
        self.assertFalse(contract.runtime_only_ast_change(BASE, RUNTIME.replace("Select, Values", "Select { value: u64 }, Values")))

    def test_serde_attribute_change_is_not_masked(self):
        self.assertFalse(contract.runtime_only_ast_change(BASE, '#[serde(rename_all = "snake_case")]\n' + RUNTIME))

    def test_new_type_outside_method_is_not_masked(self):
        self.assertFalse(contract.runtime_only_ast_change(BASE, RUNTIME + "struct NewWire { value: u64 }\n"))

    def test_signature_change_is_not_masked(self):
        self.assertFalse(contract.runtime_only_ast_change(BASE, RUNTIME.replace("-> bool", "-> Option<bool>")))

    def test_ambiguous_syntax_fails_closed(self):
        for body in ['"}" == "{"', "/* } */ true", "#[cfg(test)] true", "true // }", "true /* { */"]:
            with self.subTest(body=body):
                self.assertFalse(contract.runtime_only_ast_change(BASE, BASE.replace("match self { Self::Select => false, _ => true }", body)))

    def test_duplicate_method_fails_closed(self):
        self.assertFalse(contract.runtime_only_ast_change(BASE + BASE, RUNTIME + BASE))

    def test_version_gate_rejects_wire_change_but_accepts_runtime_change(self):
        for after, accepted in [(RUNTIME, True), (RUNTIME.replace("Select, Values", "Values, Select"), False)]:
            def source(revision, path):
                if path == contract.AST_MODULE:
                    return BASE if revision == "base" else after
                if path == contract.CONTRACT_PATH:
                    return "0.28.0"
                if path == contract.CONSUMER_PATH:
                    return 'pub(crate) const REQUIRED_CONTRACT_VERSION: &str = "0.28.0";'
                if path == contract.MANIFEST_TOOL_PATH:
                    return 'REQUIRED_CONTRACT_VERSION = "0.28.0"'
                raise AssertionError(path)
            with patch.object(contract, "git", return_value=(contract.AST_MODULE + "\0").encode()), patch.object(contract, "source_at", side_effect=source):
                if accepted:
                    self.assertEqual(contract.check("base", "head"), [])
                else:
                    with self.assertRaisesRegex(ValueError, "without a contract version increase"):
                        contract.check("base", "head")


if __name__ == "__main__":
    unittest.main()
