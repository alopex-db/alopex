"""Behavior checks for the v0.9 target-version candidate gate."""

import subprocess
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]


def test_gate_accepts_declared_chirps_v07_before_running_fixtures(tmp_path: Path) -> None:
    result = subprocess.run(
        [
            "bash",
            "scripts/release/v09_gate.sh",
            "--phase",
            "4",
            "--manifest",
            str(tmp_path / "manifest.json"),
        ],
        cwd=ROOT,
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 2
    assert "I-25:" not in result.stderr
    assert not (tmp_path / "manifest.json").exists()


def test_candidate_runner_defaults_to_chirps_v07() -> None:
    runner = (ROOT / "scripts/release/verify-release/run.sh").read_text(
        encoding="utf-8"
    )
    assert 'CHIRPS_REF="${CHIRPS_REF:-release/v0.7.0}"' in runner


def test_release_workflow_uses_immutable_rc_manifest_promotion() -> None:
    workflow = (ROOT / ".github/workflows/release.yml").read_text(encoding="utf-8")
    for required in (
        "v[0-9]+\\.[0-9]+\\.[0-9]+-rc",
        "candidate_manifest.py",
        "promote-release",
        "RELEASE_TARGET_SHA",
        "prepare-python-release.sh",
    ):
        assert required in workflow
    assert "type_capability_gate.py" not in workflow
    assert "v07_gate.sh" not in workflow


def test_ci_uses_the_v09_target_gate() -> None:
    workflow = (ROOT / ".github/workflows/ci.yml").read_text(encoding="utf-8")
    assert "v09-release-gate:" in workflow
    assert "--v09-candidate-gate" in workflow
    assert "v08-release-gate" not in workflow
