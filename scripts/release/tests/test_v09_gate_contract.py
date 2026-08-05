"""Static contract checks for the v0.9 target-version candidate gate.

These checks deliberately do not treat a static read as release evidence.  The
real gate is executed in Docker by the release workflow; this test only keeps
the no-legacy/no-write wiring from being silently removed.
"""

from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]


def read(relative: str) -> str:
    return (ROOT / relative).read_text(encoding="utf-8")


def test_v09_gate_is_targeted_exhaustive_and_offline() -> None:
    gate = read("scripts/release/v09_gate.sh")
    assert 'TARGET_VERSION="0.9.0"' in gate
    assert 'CHIRPS_VERSION="0.5.2"' in gate
    assert 'verify-v09-f4' in gate
    assert 'cargo test --offline --locked --workspace --all-features' in gate
    assert 'test_v09_transaction_sync.py' in gate
    assert 'test_v09_transaction_async.py' in gate
    assert 'changefeed_durable_preflight.rs' in gate
    assert 'snapshot.get("status") == "approved"' in gate
    assert 'hashlib.sha256(content.encode("utf-8")).hexdigest() == expected_hash' in gate
    assert "exit_if_blocked || return $?" in gate
    assert gate.index("exit_if_blocked || return $?") < gate.index(
        'command -v nimble'
    )
    assert 'v07_gate.sh' not in gate
    assert 'verify-v08-surfaces.sh' not in gate


def test_candidate_runner_uses_docker_read_only_mounts_and_no_network() -> None:
    runner = read("scripts/release/verify-release/run.sh")
    assert 'CHIRPS_REF="${CHIRPS_REF:-release/v0.5.2}"' in runner
    assert '--v09-candidate-gate' in runner
    assert '--no-report' in runner
    assert 'docker run --rm --network none' in runner
    assert ':/workspace:ro' in runner
    assert ':/spec-workflow:ro' in runner
    assert ':/chirps:ro' in runner
    assert 'git clone' not in runner[runner.index('run_v09_candidate_gate'):runner.index('ensure_chirps_dir')]


def test_candidate_runner_refuses_a_dirty_source_before_container_execution() -> None:
    runner = read("scripts/release/verify-release/run.sh")
    candidate_gate = runner[
        runner.index("run_v09_candidate_gate()"):runner.index("ensure_chirps_dir")
    ]
    assert 'git -C "${REPO_ROOT}" status --porcelain --untracked-files=all' in runner
    assert "candidate source must be clean and committed" in runner
    assert candidate_gate.index("ensure_clean_v09_candidate") < candidate_gate.index(
        "docker build"
    )


def test_release_workflow_invokes_the_v09_gate_not_a_legacy_substitute() -> None:
    workflow = read(".github/workflows/release.yml")
    assert 'v0.9 CI Gate' in workflow
    assert 'alopex-spec-workflow' in workflow
    assert 'V09_SPECS_DIR' in workflow
    assert 'run.sh 0.9.0 --v09-candidate-gate --no-report' in workflow
    assert 'v07_gate.sh' not in workflow
    assert 'verify-v08-surfaces.sh' not in workflow
