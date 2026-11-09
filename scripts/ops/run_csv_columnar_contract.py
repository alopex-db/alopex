"""Run unchanged public Python CSV/columnar cases with per-case DB cleanup."""
from __future__ import annotations

import hashlib
import importlib.metadata
import importlib.util
import itertools
import json
import os
from pathlib import Path
import tempfile

ROOT = Path(__file__).resolve().parents[2]
WORK = Path(os.environ["ALOPEX_COPY_WORK"])


def load(path: Path):
    spec = importlib.util.spec_from_file_location(path.stem, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def parameters(function, names):
    marks = {mark.args[0]: mark.args[1] for mark in function.pytestmark
             if mark.name == "parametrize"}
    assert set(marks) == set(names)
    return [dict(zip(names, values)) for values in
            itertools.product(*(marks[name] for name in names))]


def run_cases(function, group, names, expected):
    items = parameters(function, names)
    assert len(items) == expected
    outcomes = []
    for index, arguments in enumerate(items):
        with tempfile.TemporaryDirectory(prefix=f"{group}-{index:02d}-", dir=WORK) as directory:
            function(Path(directory), **arguments)
        outcomes.append(f"{group}:{index:02d}")
    return outcomes


def main():
    assert importlib.metadata.version("alopex") == "0.8.17"
    assert importlib.metadata.version("pytest") == "9.1.1"
    tests = ROOT / "crates/alopex-py/tests"
    csv = load(tests / "test_csv_null.py")
    columnar = load(tests / "test_columnar_export.py")
    outcomes = run_cases(csv.test_csv_null_and_text_survive_reopen,
                         "csv", ("route", "text"), 30)
    outcomes += run_cases(columnar.test_export_preserves_rows_after_reopen,
                          "columnar", ("storage", "route", "empty"), 12)
    assert len(outcomes) == len(set(outcomes)) == 42
    assert not any(path.name.startswith(("csv-", "columnar-")) for path in WORK.iterdir())
    report = {"schema": "alopex.csv-columnar-contract/v1", "status": "passed",
              "csv_cases": 30, "columnar_cases": 12, "case_ids": outcomes,
              "case_ids_sha256": hashlib.sha256("\n".join(outcomes).encode()).hexdigest(),
              "owned_case_directories_removed": True}
    (WORK / "copy-contract.json").write_text(json.dumps(report, sort_keys=True) + "\n")
    print(json.dumps({"status": "passed", "cases": len(outcomes)}))


if __name__ == "__main__":
    main()
