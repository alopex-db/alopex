"""The historical release check follows the versioned Intel requirement."""

from __future__ import annotations

from pathlib import Path
import subprocess
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[3]


class V07GateContractTests(unittest.TestCase):
    def test_intel_is_optional_from_v0817_but_required_for_legacy_source(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "scripts/release").mkdir(parents=True)
            (root / ".github/workflows").mkdir(parents=True)
            for name in (
                "scripts/release/v07_gate.sh",
                ".github/workflows/ci.yml",
                ".github/workflows/release.yml",
                ".github/workflows/alopex-py-release.yml",
            ):
                destination = root / name
                destination.write_bytes((ROOT / name).read_bytes())
            cargo = (ROOT / "Cargo.toml").read_text(encoding="utf-8")
            (root / "Cargo.toml").write_text(cargo, encoding="utf-8")

            def check() -> subprocess.CompletedProcess[str]:
                return subprocess.run(
                    ["bash", "scripts/release/v07_gate.sh", "--workflow-contract-only"],
                    cwd=root, capture_output=True, text=True, check=False,
                )

            current = check()
            self.assertEqual(current.returncode, 0, current.stdout + current.stderr)

            (root / "Cargo.toml").write_text(
                cargo.replace('version = "0.8.17"', 'version = "0.8.16"', 1),
                encoding="utf-8",
            )
            legacy = check()
            self.assertNotEqual(legacy.returncode, 0)
            self.assertIn("legacy release workflow publishes macOS x86_64", legacy.stdout)


if __name__ == "__main__":
    unittest.main()
