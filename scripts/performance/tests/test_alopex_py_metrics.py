import json
import sys
from pathlib import Path

import pytest


MODULE_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(MODULE_DIR))

from alopex_py_metrics import (  # noqa: E402
    HarnessError,
    build_record,
    evaluate,
    load_junit_metrics,
    render_comparison_markdown,
    render_history_markdown,
    render_missing_release_markdown,
    render_release_markdown,
    main,
    update_history,
    validate_release_tag,
)


def write_junit(path, *, scan=0.10, read=2.0, write=3.0):
    path.write_text(
        """<?xml version="1.0" encoding="utf-8"?>
<testsuites><testsuite><testcase><properties>
<property name="scan_overhead_ms" value="{scan:.6f}" />
<property name="read_overhead_pct" value="{read:.6f}" />
<property name="write_overhead_pct" value="{write:.6f}" />
</properties></testcase></testsuite></testsuites>
""".format(scan=scan, read=read, write=write),
        encoding="utf-8",
    )


def benchmark_payload(
    *,
    scan=0.10,
    read=2.0,
    write=3.0,
    cpu="Example CPU",
    profile="ignored-by-payload",
):
    return {
        "datetime": "2026-08-03T00:00:00+00:00",
        "machine_info": {
            "system": "Linux",
            "machine": "x86_64",
            "python_version": "3.11.11",
            "cpu": {"brand_raw": cpu},
        },
        "benchmarks": [
            {
                "fullname": "test_performance.py::test_scan_overhead_vs_polars",
                "extra_info": {
                    "scan_overhead_ms": scan,
                    "scan_direct_ms": 0.07,
                    "scan_wrapped_ms": 0.17,
                },
            },
            {
                "fullname": "test_performance.py::test_large_read_overhead",
                "extra_info": {"read_overhead_pct": read},
            },
            {
                "fullname": "test_performance.py::test_large_write_overhead",
                "extra_info": {"write_overhead_pct": write},
            },
        ],
    }


def make_record(value, *, commit, cpu="Example CPU", profile="gha-linux-x64"):
    payload = benchmark_payload(scan=value, read=value, write=value, cpu=cpu)
    return build_record(
        payload,
        commit=commit,
        ref="refs/heads/main",
        profile=profile,
        measured_at=f"2026-08-{int(commit[-2:], 16) % 28 + 1:02d}T00:00:00+00:00",
        dependency_versions={"pytest": "9.1.1", "polars": "1.43.2"},
        lock_hash="cargo-lock-sha",
        workload_signature="workload-sha",
    )


def test_build_record_extracts_numeric_metrics_and_environment_fingerprint():
    record = build_record(
        benchmark_payload(),
        commit="a" * 40,
        ref="refs/heads/main",
        profile="gha-linux-x64-single-core",
        measured_at="2026-08-03T00:00:00+00:00",
        dependency_versions={"pytest": "9.1.1", "polars": "1.43.2"},
        lock_hash="cargo-lock-sha",
        workload_signature="workload-sha",
    )

    assert record["schema_version"] == 1
    assert record["commit"] == "a" * 40
    assert record["profile"] == "gha-linux-x64-single-core"
    assert record["metrics"]["scan_overhead_ms"]["value"] == pytest.approx(0.10)
    assert record["metrics"]["read_overhead_pct"]["value"] == pytest.approx(2.0)
    assert record["metrics"]["write_overhead_pct"]["value"] == pytest.approx(3.0)
    assert record["environment"]["cpu"] == "Example CPU"
    assert len(record["environment"]["fingerprint"]) == 16
    assert record["dependencies"]["polars"] == "1.43.2"
    assert record["lock_hash"] == "cargo-lock-sha"
    assert record["workload_signature"] == "workload-sha"


def test_environment_identity_excludes_subject_version_and_application_lock():
    first = build_record(
        benchmark_payload(),
        commit="a" * 40,
        ref="refs/heads/main",
        profile="gha-linux-x64-single-core",
        measured_at="2026-08-03T00:00:00+00:00",
        dependency_versions={"alopex": "0.8.3", "pytest": "9.1.1"},
        lock_hash="first-lock",
        workload_signature="workload-sha",
    )
    second = build_record(
        benchmark_payload(),
        commit="b" * 40,
        ref="refs/heads/main",
        profile="gha-linux-x64-single-core",
        measured_at="2026-08-04T00:00:00+00:00",
        dependency_versions={"alopex": "0.8.4", "pytest": "9.1.1"},
        lock_hash="second-lock",
        workload_signature="workload-sha",
    )

    assert first["environment"]["fingerprint"] == second["environment"]["fingerprint"]
    assert first["lock_hash"] != second["lock_hash"]
    assert first["dependencies"]["alopex"] != second["dependencies"]["alopex"]


def test_junit_metrics_are_numeric_unique_and_match_benchmark_json(tmp_path):
    junit = tmp_path / "performance-junit.xml"
    write_junit(junit)
    metrics = load_junit_metrics(junit)

    record = build_record(
        benchmark_payload(),
        commit="a" * 40,
        ref="refs/heads/main",
        profile="gha-linux-x64-single-core",
        measured_at="2026-08-03T00:00:00+00:00",
        junit_metrics=metrics,
    )

    assert record["metrics"]["scan_overhead_ms"]["value"] == pytest.approx(0.10)


def test_junit_and_benchmark_metric_mismatch_is_rejected(tmp_path):
    junit = tmp_path / "performance-junit.xml"
    write_junit(junit, scan=0.20)

    with pytest.raises(HarnessError, match="metric mismatch for scan_overhead_ms"):
        build_record(
            benchmark_payload(),
            commit="a" * 40,
            ref="refs/heads/main",
            profile="gha-linux-x64-single-core",
            measured_at="2026-08-03T00:00:00+00:00",
            junit_metrics=load_junit_metrics(junit),
        )


def test_junit_rejects_missing_and_duplicate_monitored_properties(tmp_path):
    missing = tmp_path / "missing.xml"
    missing.write_text(
        '<testsuite><property name="scan_overhead_ms" value="0.1" /></testsuite>',
        encoding="utf-8",
    )
    with pytest.raises(HarnessError, match="missing JUnit metrics"):
        load_junit_metrics(missing)

    duplicate = tmp_path / "duplicate.xml"
    duplicate.write_text(
        """<testsuite>
<property name="scan_overhead_ms" value="0.1" />
<property name="scan_overhead_ms" value="0.1" />
<property name="read_overhead_pct" value="2.0" />
<property name="write_overhead_pct" value="3.0" />
</testsuite>""",
        encoding="utf-8",
    )
    with pytest.raises(HarnessError, match="duplicate JUnit metric"):
        load_junit_metrics(duplicate)


def test_build_record_rejects_missing_monitored_metric():
    payload = benchmark_payload()
    payload["benchmarks"][2]["extra_info"] = {}

    with pytest.raises(HarnessError, match="write_overhead_pct"):
        build_record(
            payload,
            commit="b" * 40,
            ref="refs/heads/main",
            profile="gha-linux-x64-single-core",
            measured_at="2026-08-03T00:00:00+00:00",
            dependency_versions={"pytest": "9.1.1", "polars": "1.43.2"},
            lock_hash="cargo-lock-sha",
            workload_signature="workload-sha",
        )


@pytest.mark.parametrize("invalid", [True, "not-a-number", float("nan"), float("inf")])
def test_build_record_rejects_non_finite_or_non_numeric_metrics(invalid):
    payload = benchmark_payload()
    payload["benchmarks"][0]["extra_info"]["scan_overhead_ms"] = invalid

    with pytest.raises(HarnessError, match="scan_overhead_ms"):
        build_record(
            payload,
            commit="b" * 40,
            ref="refs/heads/main",
            profile="gha-linux-x64-single-core",
            measured_at="2026-08-03T00:00:00+00:00",
        )


def test_build_record_rejects_duplicate_metric_writers():
    payload = benchmark_payload()
    payload["benchmarks"][1]["extra_info"]["scan_overhead_ms"] = 0.2

    with pytest.raises(HarnessError, match="duplicate metric scan_overhead_ms"):
        build_record(
            payload,
            commit="b" * 40,
            ref="refs/heads/main",
            profile="gha-linux-x64-single-core",
            measured_at="2026-08-03T00:00:00+00:00",
        )


def test_evaluate_flags_only_a_distribution_outlier_on_same_environment():
    records = [
        make_record(value, commit=f"{index:040x}")
        for index, value in enumerate([1.00, 1.10, 0.90, 1.05, 0.95], start=1)
    ]
    # A different CPU must never contribute to the baseline distribution.
    records.append(make_record(100.0, commit=f"{99:040x}", cpu="Other CPU"))
    incompatible = make_record(100.0, commit=f"{98:040x}")
    incompatible["workload_signature"] = "different-workload"
    records.append(incompatible)
    current = make_record(1.50, commit=f"{100:040x}")

    report = evaluate(current, {"schema_version": 1, "records": records})

    assert report["has_regressions"] is True
    assert report["notification_ready"] is False
    assert report["status"] == "regression"
    for metric in report["metrics"].values():
        assert metric["status"] == "regression"
        assert metric["sample_count"] == 5
        assert metric["robust_z"] > 3.5


def test_evaluate_keeps_normal_variation_advisory_and_green():
    records = [
        make_record(value, commit=f"{index:040x}")
        for index, value in enumerate([1.00, 1.10, 0.90, 1.05, 0.95], start=1)
    ]
    current = make_record(1.08, commit=f"{100:040x}")

    report = evaluate(current, {"schema_version": 1, "records": records})

    assert report["has_regressions"] is False
    assert report["status"] == "stable"
    assert all(item["status"] == "stable" for item in report["metrics"].values())


def test_evaluate_notifies_only_after_two_consecutive_distribution_outliers():
    baseline = [
        make_record(value, commit=f"{index:040x}")
        for index, value in enumerate([1.00, 1.10, 0.90, 1.05, 0.95], start=1)
    ]
    first_outlier = make_record(1.50, commit=f"{100:040x}")
    current = make_record(1.60, commit=f"{101:040x}")

    report = evaluate(
        current,
        {"schema_version": 1, "records": [*baseline, first_outlier]},
    )

    assert report["has_regressions"] is True
    assert report["notification_ready"] is True
    assert set(report["notification_metrics"]) == set(report["metrics"])


def test_evaluate_notifies_on_two_equal_outliers_after_zero_mad_plateau():
    baseline = [
        make_record(1.0, commit=f"{index:040x}") for index in range(1, 6)
    ]
    first_outlier = make_record(2.0, commit=f"{100:040x}")
    current = make_record(2.0, commit=f"{101:040x}")

    report = evaluate(
        current,
        {"schema_version": 1, "records": [*baseline, first_outlier]},
    )

    assert report["has_regressions"] is True
    assert report["notification_ready"] is True
    assert all(
        metric["method"] == "median_zero_mad"
        for metric in report["metrics"].values()
    )


def test_evaluate_uses_same_reference_for_two_nonzero_mad_outliers():
    baseline = [
        make_record(value, commit=f"{index:040x}")
        for index, value in enumerate([0.90, 0.95, 1.00, 1.05, 1.10], start=1)
    ]
    first_outlier = make_record(1.26, commit=f"{100:040x}")
    current = make_record(1.26, commit=f"{101:040x}")

    report = evaluate(
        current,
        {"schema_version": 1, "records": [*baseline, first_outlier]},
    )

    assert report["has_regressions"] is True
    assert report["notification_ready"] is True
    assert all(metric["sample_count"] == 5 for metric in report["metrics"].values())


def test_evaluate_reports_insufficient_history_without_guessing():
    history = {
        "schema_version": 1,
        "records": [make_record(1.0, commit=f"{index:040x}") for index in range(1, 5)],
    }
    current = make_record(10.0, commit=f"{100:040x}")

    report = evaluate(current, history)

    assert report["has_regressions"] is False
    assert report["status"] == "insufficient_history"
    assert all(
        item["status"] == "insufficient_history"
        for item in report["metrics"].values()
    )


def test_update_history_is_idempotent_and_retains_latest_records_per_profile():
    original = make_record(1.0, commit=f"{1:040x}")
    replacement = make_record(2.0, commit=f"{1:040x}")
    later = [make_record(float(index), commit=f"{index:040x}") for index in range(2, 6)]

    history = update_history(
        {"schema_version": 1, "records": [original]},
        replacement,
        max_records_per_profile=3,
    )
    for record in later:
        history = update_history(history, record, max_records_per_profile=3)

    assert len(history["records"]) == 3
    assert [record["commit"] for record in history["records"]] == [
        f"{index:040x}" for index in range(3, 6)
    ]
    assert json.loads(json.dumps(history)) == history


def test_markdown_views_are_regenerable_from_canonical_json():
    history = {
        "schema_version": 1,
        "records": [make_record(1.0, commit=f"{1:040x}")],
    }
    current = make_record(1.1, commit=f"{2:040x}")
    comparison = evaluate(current, history, min_samples=1)

    history_markdown = render_history_markdown(update_history(history, current))
    comparison_markdown = render_comparison_markdown(comparison)
    release_markdown = render_release_markdown(current, comparison, "v0.8.3")

    assert "scan_overhead_ms" in history_markdown
    assert current["commit"][:12] in history_markdown
    assert "Advisory" in comparison_markdown
    assert "v0.8.3" in release_markdown
    assert "Example CPU" in release_markdown


@pytest.mark.parametrize("tag", ["../escape", "release/v1", "v1.0.0\nbody", ""])
def test_release_tag_rejects_unsafe_paths(tag):
    with pytest.raises(HarnessError):
        validate_release_tag(tag)


def test_release_tag_accepts_version_tags():
    assert validate_release_tag("v0.8.3") == "v0.8.3"
    assert validate_release_tag("alopex-py-v0.8.3") == "alopex-py-v0.8.3"


def test_missing_tag_measurement_has_an_explicit_non_blocking_release_report():
    markdown = render_missing_release_markdown(
        tag="v0.8.3",
        commit="a" * 40,
        reason="measurement_workload_failed",
    )

    assert "not measured" in markdown
    assert "does not block the release" in markdown
    assert "a" * 40 in markdown


def test_cli_regression_evaluation_is_advisory_exit_zero(tmp_path):
    baseline = [
        make_record(value, commit=f"{index:040x}")
        for index, value in enumerate([1.00, 1.10, 0.90, 1.05, 0.95], start=1)
    ]
    current = make_record(1.50, commit=f"{100:040x}")
    current_path = tmp_path / "current.json"
    history_path = tmp_path / "history.json"
    output_json = tmp_path / "comparison.json"
    output_markdown = tmp_path / "report.md"
    current_path.write_text(json.dumps(current), encoding="utf-8")
    history_path.write_text(
        json.dumps({"schema_version": 1, "records": baseline}), encoding="utf-8"
    )

    exit_code = main(
        [
            "evaluate",
            "--current",
            str(current_path),
            "--history",
            str(history_path),
            "--output-json",
            str(output_json),
            "--output-markdown",
            str(output_markdown),
        ]
    )

    assert exit_code == 0
    assert json.loads(output_json.read_text(encoding="utf-8"))["has_regressions"]
    assert "Advisory only" in output_markdown.read_text(encoding="utf-8")


def test_successful_record_initializes_history_after_first_missing_release(tmp_path):
    history_root = tmp_path / "performance-history"
    release_dir = history_root / "releases"
    release_dir.mkdir(parents=True)
    missing_markdown = release_dir / "v0.8.3.md"
    missing_json = release_dir / "v0.8.3.json"
    assert main(
        [
            "missing-release",
            "--release-tag",
            "v0.8.3",
            "--commit",
            "a" * 40,
            "--reason",
            "measurement_pipeline_failed",
            "--output-markdown",
            str(missing_markdown),
            "--output-json",
            str(missing_json),
        ]
    ) == 0

    current = make_record(1.0, commit="b" * 40)
    current_path = tmp_path / "current.json"
    current_path.write_text(json.dumps(current), encoding="utf-8")
    comparison_path = tmp_path / "comparison.json"
    comparison_path.write_text(
        json.dumps(evaluate(current, {"schema_version": 1, "records": []})),
        encoding="utf-8",
    )

    assert main(
        [
            "record",
            "--current",
            str(current_path),
            "--history",
            str(history_root / "history.json"),
            "--index",
            str(history_root / "index.md"),
            "--comparison",
            str(comparison_path),
        ]
    ) == 0
    assert json.loads((history_root / "history.json").read_text())["records"]
    assert missing_markdown.is_file()
    assert missing_json.is_file()


def test_dedicated_workflow_is_serial_advisory_and_uses_pinned_inputs():
    repository_root = Path(__file__).resolve().parents[3]
    workflow = (
        repository_root / ".github/workflows/alopex-performance.yml"
    ).read_text(encoding="utf-8")

    assert "group: alopex-performance-history" in workflow
    assert "queue: max" in workflow
    assert "cancel-in-progress: false" in workflow
    assert "continue-on-error: true" in workflow
    assert "test_performance.py" in workflow
    assert "tests/benchmarks/ -v" not in workflow
    assert "--junitxml=performance-artifacts/performance-junit.xml" in workflow
    assert "--junit-xml performance-artifacts/performance-junit.xml" in workflow
    assert "junit_family=xunit1" in workflow
    assert "maturin==1.14.1" in workflow
    assert "pytest==9.1.1" in workflow
    assert "pytest-benchmark==5.2.3" in workflow
    assert "numpy==2.4.6" in workflow
    assert "polars==1.43.2" in workflow
    assert "maturin develop --release" in workflow
    assert "taskset -c 0" in workflow
    assert "performance-history" in workflow
    assert "git ls-remote --exit-code --heads origin performance-history" in workflow
    assert "git cat-file -e refs/remotes/origin/performance-history:history.json" in workflow
    assert "echo '{\"schema_version\": 1, \"records\": []}'" in workflow
    assert 'exit "$branch_status"' in workflow
    assert "notification_ready" in workflow
    assert "missing-release" in workflow
    assert "steps.record.outcome != 'success'" in workflow
    assert "--output-markdown performance-artifacts/release-report.md" in workflow
    assert "pull_request:" not in workflow
    assert "permissions:\n      contents: read" in workflow
    assert "permissions:\n      contents: write\n      issues: write" in workflow
    assert "startsWith(github.ref, 'refs/tags/alopex-py-v')" in workflow


def test_regular_python_ci_does_not_gate_on_benchmark_job():
    repository_root = Path(__file__).resolve().parents[3]
    workflow = (repository_root / ".github/workflows/alopex-py.yml").read_text(
        encoding="utf-8"
    )

    assert "needs: [rust-check, test, polars-test, typecheck]" in workflow
    assert "needs.benchmarks.result" not in workflow
    benchmark_job = workflow.split("  benchmarks:\n", 1)[1].split(
        "  ci-success:\n", 1
    )[0]
    assert "continue-on-error: true" in benchmark_job
    polars_job = workflow.split("  polars-test:\n", 1)[1].split(
        "  typecheck:\n", 1
    )[0]
    assert "python -m pip install pytest-benchmark" in polars_job

    source = (
        repository_root
        / "crates/alopex-py/tests/benchmarks/test_performance.py"
    ).read_text(encoding="utf-8")
    assert "min(overheads)" not in source
    assert source.count("statistics.median(overheads)") == 2
    assert "benchmark.extra_info[name] = numeric_value" in source


def test_nim_parser_dependencies_are_pinned_for_reproducible_measurements():
    repository_root = Path(__file__).resolve().parents[3]
    nimble = (
        repository_root
        / "crates/alopex-sql/nim-sql-parser/nim_sql_parser.nimble"
    ).read_text(encoding="utf-8")
    build_script = (repository_root / "scripts/build-nim-parser.sh").read_text(
        encoding="utf-8"
    )

    assert 'requires "npeg == 1.3.0"' in nimble
    assert 'requires "msgpack4nim == 0.4.4"' in nimble
    assert '"npeg@1.3.0" "msgpack4nim@0.4.4"' in build_script


def test_versioned_measurement_profile_matches_workflow_and_nim_build():
    repository_root = Path(__file__).resolve().parents[3]
    profile = json.loads(
        (repository_root / "scripts/performance/profile-v1.json").read_text(
            encoding="utf-8"
        )
    )
    workflow = (
        repository_root / ".github/workflows/alopex-performance.yml"
    ).read_text(encoding="utf-8")
    build_script = (repository_root / "scripts/build-nim-parser.sh").read_text(
        encoding="utf-8"
    )

    assert profile["schema_version"] == 1
    assert profile["name"] in workflow
    assert f'runs-on: {profile["runner"]}' in workflow
    assert f'taskset -c {profile["cpu_affinity"][0]}' in workflow
    assert "--workload-file scripts/performance/profile-v1.json" in workflow
    for name, value in profile["environment"].items():
        assert f'{name}: "{value}"' in workflow
    for name in (
        "maturin",
        "numpy",
        "pip",
        "polars",
        "pytest",
        "pytest-benchmark",
    ):
        assert f'{name}=={profile["toolchain"][name]}' in workflow
    assert f'python-version: "{profile["toolchain"]["python"]}"' in workflow
    assert f'dtolnay/rust-toolchain@{profile["toolchain"]["rust"]}' in workflow
    assert profile["toolchain"]["nim_image"] in build_script
    assert f'npeg@{profile["toolchain"]["npeg"]}' in build_script
    assert f'msgpack4nim@{profile["toolchain"]["msgpack4nim"]}' in build_script
