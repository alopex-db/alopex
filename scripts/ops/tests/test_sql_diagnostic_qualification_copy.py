"""Keep the diagnostic qualifier on the same frozen security boundary."""

import ast
import json
from pathlib import Path
import unittest


OPS = Path(__file__).resolve().parents[1]
BASE = OPS / "hosted_test_env_qualification.py"
COPY = OPS / "hosted_test_env_qualification_1951.py"
LOCK = OPS / "sql-diagnostic-lock.json"


def definitions(source):
    tree = ast.parse(source)
    return {node.name: node for node in tree.body
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))}


def assignments(source):
    tree = ast.parse(source)
    return {target.id: ast.literal_eval(node.value)
            for node in tree.body if isinstance(node, ast.Assign)
            for target in node.targets if isinstance(target, ast.Name)
            if isinstance(node.value, (ast.Constant, ast.Dict))}


class DiagnosticQualificationCopyTests(unittest.TestCase):
    def test_frozen_security_execution_and_current_controller(self):
        base, copied = BASE.read_text(), COPY.read_text()
        old, new = assignments(base), assignments(copied)
        pin = json.loads(LOCK.read_text())["pin"]
        for key in ("HARNESS", "LOCK_DIGEST", "POLICY_DIGEST",
                    "EXPECTED_DOCKER_VERSION", "CONTROLLER_DIGEST"):
            self.assertEqual(new[key], old[key], key)
        self.assertEqual(new["CONTROLLER_DIGEST"], pin["controller_digest"])
        old_functions, new_functions = definitions(base), definitions(copied)
        for name in ("create_profile", "execute", "encrypt_evidence", "main",
                     "acquire_assets", "admission_facts"):
            self.assertEqual(ast.dump(old_functions[name], include_attributes=False),
                             ast.dump(new_functions[name], include_attributes=False), name)

    def test_qualified_product_matches_diagnostic_source_lock(self):
        new = assignments(COPY.read_text())
        pin = json.loads(LOCK.read_text())["pin"]
        self.assertEqual((new["PRODUCT"], new["PRODUCT_TREE"]),
                         (pin["source_commit"], pin["source_tree"]))
        self.assertEqual((new["ASSET_RUN"], new["ASSET_RUN_HEAD"],
                          new["ASSET_ARTIFACT_ID"]),
                         (pin["asset_run_id"], pin["asset_run_head"],
                          pin["artifact_id"]))
        self.assertEqual((new["ASSET_ZIP_DIGEST"], new["ASSETS_DIGEST"]),
                         (pin["zip_sha256"], pin["assets_digest"]))
        self.assertEqual((new["FILE_DIGESTS"]["server"],
                          new["FILE_DIGESTS"]["wheel"]),
                         (pin["server_sha256"], pin["wheel_sha256"]))


if __name__ == "__main__":
    unittest.main()
