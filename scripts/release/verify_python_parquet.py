"""Qualify VECTOR/Parquet through the installed RC wheel with tiny public cases."""
import argparse
from collections import Counter
import hashlib
from importlib.metadata import version
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import xml.etree.ElementTree as ET
import zipfile


EXPECTED_CASES = {
    "test_vector_export_is_independently_readable_and_roundtrips": 4,
    "test_external_float_lists_preserve_rounded_elements_after_reopen": 12,
    "test_invalid_vector_import_preserves_preexisting_rows": 9,
    "test_integer_list_is_not_a_vector_input": 1,
    "test_empty_vector_export_keeps_declared_schema": 1,
    "test_destination_ddl_controls_dimension_despite_metadata": 2,
    "test_outer_null_obeys_not_null_without_partial_import": 2,
    "test_read_parquet_infers_plain_arrays": 3,
}


def digest(stream):
    value = hashlib.sha256()
    while block := stream.read(1024 * 1024):
        value.update(block)
    return value.hexdigest()


def file_digest(path):
    with path.open("rb") as stream:
        return digest(stream)


def installed_identity(wheel, package_file, extension_file, prefix):
    """Both loaded modules must be inside this venv and match the wheel bytes."""
    prefix = prefix.resolve(strict=True)
    result = {}
    with zipfile.ZipFile(wheel) as archive:
        names = archive.namelist()
        for role, origin in (("package", package_file), ("extension", extension_file)):
            origin = origin.resolve(strict=True)
            if not origin.is_relative_to(prefix) or origin.parent.name != "alopex":
                raise RuntimeError("loaded module is outside the qualification environment")
            member = "alopex/" + origin.name
            if names.count(member) != 1:
                raise RuntimeError("loaded module is missing or duplicated in the wheel")
            with archive.open(member) as stream:
                expected = digest(stream)
            if file_digest(origin) != expected:
                raise RuntimeError("loaded module differs from the qualified wheel")
            result[role] = {"member": member, "sha256": expected}
    return result


def verify_cases(path):
    cases = list(ET.parse(path).getroot().iter("testcase"))
    names = [case.get("name", "") for case in cases]
    counts = Counter(name.split("[", 1)[0] for name in names)
    if counts != EXPECTED_CASES or len(names) != len(set(names)):
        raise RuntimeError("VECTOR/Parquet qualification case inventory differs")
    for case in cases:
        if case.find("skipped") is not None or case.find("failure") is not None or case.find("error") is not None:
            raise RuntimeError("VECTOR/Parquet qualification requires every case to pass")
    return sorted(names)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--wheel", type=Path, required=True)
    parser.add_argument("--workspace", type=Path, required=True)
    parser.add_argument("--report", type=Path, required=True)
    args = parser.parse_args(argv)
    os.umask(0o077)
    report = {"schema": "alopex.python-vector-parquet-qualification/v1", "status": "failed"}
    try:
        if sys.prefix == sys.base_prefix or sys.flags.optimize:
            raise RuntimeError("qualification requires a venv and enabled assertions")
        import alopex
        import alopex._alopex as native
        import pyarrow  # Required; pytest.importorskip must not qualify a missing dependency.
        import pytest

        report["wheel_sha256"] = file_digest(args.wheel)
        report["installed_modules"] = installed_identity(
            args.wheel, Path(alopex.__file__), Path(native.__file__), Path(sys.prefix)
        )
        report["source_commit"] = subprocess.check_output(
            ["git", "rev-parse", "HEAD"], cwd=args.workspace, text=True
        ).strip()
        test_dir = args.workspace / "crates/alopex-py/tests"
        test = test_dir / "test_vector_parquet.py"
        report["sources"] = {path.name: file_digest(path) for path in (
            test, test_dir / "conftest.py", Path(__file__)
        )}
        report["versions"] = {name: version(name) for name in ("alopex", "pyarrow", "pytest", "numpy")}
        # The existing fixture closes every DB and removes its files after each case.
        # Keep pytest's own files in this owned, disposable venv as well.
        with tempfile.TemporaryDirectory(prefix="vector-parquet-", dir=sys.prefix) as directory:
            root = Path(directory)
            junit = root / "cases.xml"
            code = pytest.main([
                "-q", "-c", os.devnull, "--override-ini", "addopts=", "--import-mode=importlib",
                "-p", "no:cacheprovider", "--confcutdir", str(test_dir),
                "--basetemp", str(root / "tmp"), "--junitxml", str(junit), str(test),
            ])
            if code != 0:
                raise RuntimeError("VECTOR/Parquet qualification process failed")
            report["cases"] = verify_cases(junit)
        if installed_identity(args.wheel, Path(alopex.__file__), Path(native.__file__), Path(sys.prefix)) != report["installed_modules"]:
            raise RuntimeError("installed wheel changed during qualification")
        if file_digest(args.wheel) != report["wheel_sha256"]:
            raise RuntimeError("wheel archive changed during qualification")
        report["status"] = "passed"
    except Exception:
        report["error_code"] = "VECTOR_PARQUET_QUALIFICATION"
    finally:
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"status": report["status"], "cases": len(report.get("cases", []))}))
    return 0 if report["status"] == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
