from __future__ import annotations

import importlib.util
import json
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
MODULE_PATH = ROOT / "scripts/release/type_capability_gate.py"


def load_gate():
    spec = importlib.util.spec_from_file_location("type_capability_gate", MODULE_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load {MODULE_PATH}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class TypeCapabilityGateTests(unittest.TestCase):
    def test_current_release_allows_capabilities_planned_for_v0810(self) -> None:
        gate = load_gate()

        gate.verify(ROOT / "docs/sql-type-capabilities.json", ROOT, "0.8.9")

    def test_v0810_release_rejects_every_incomplete_type_family(self) -> None:
        gate = load_gate()

        with self.assertRaisesRegex(
            gate.GateError, "decimal.*json.*nested"
        ):
            gate.verify(ROOT / "docs/sql-type-capabilities.json", ROOT, "0.8.10")

    def test_complete_family_requires_evidence_for_every_surface(self) -> None:
        gate = load_gate()
        catalog = json.loads(
            (ROOT / "docs/sql-type-capabilities.json").read_text(encoding="utf-8")
        )
        catalog["type_families"][0]["status"] = "complete"

        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "catalog.json"
            path.write_text(json.dumps(catalog), encoding="utf-8")
            with self.assertRaisesRegex(gate.GateError, "decimal.*parser_ast"):
                gate.verify(path, ROOT, "0.8.9")

    def test_catalog_owner_paths_must_exist(self) -> None:
        gate = load_gate()
        catalog = json.loads(
            (ROOT / "docs/sql-type-capabilities.json").read_text(encoding="utf-8")
        )
        catalog["surfaces"][0]["owners"] = ["missing.rs"]

        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "catalog.json"
            path.write_text(json.dumps(catalog), encoding="utf-8")
            with self.assertRaisesRegex(gate.GateError, "missing.rs"):
                gate.verify(path, ROOT, "0.8.9")

    def test_catalog_cannot_omit_a_required_type_family(self) -> None:
        gate = load_gate()
        catalog = json.loads(
            (ROOT / "docs/sql-type-capabilities.json").read_text(encoding="utf-8")
        )
        catalog["type_families"].pop()

        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "catalog.json"
            path.write_text(json.dumps(catalog), encoding="utf-8")
            with self.assertRaisesRegex(gate.GateError, "missing a v0.8.10"):
                gate.verify(path, ROOT, "0.8.9")


if __name__ == "__main__":
    unittest.main()
