"""Replay the exact historical Python contract corpus against both packages.

No copying of today's tests into an old environment, no pytest import skips
counted as passes, and no baseline failure hidden behind an overall green label.
"""

from __future__ import annotations

import argparse
import importlib
import inspect
import json
from pathlib import Path
import subprocess
import socket
import tarfile

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[1]
CONFIG = json.loads((HERE / "api-cases.json").read_text())


def write(path, value):
    Path(path).write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n")


def baseline_source(output):
    """Materialize only a pinned source snapshot; never create a Git worktree."""
    commit = CONFIG["baseline_commit"]
    subprocess.run(
        ["git", "-C", str(REPO), "cat-file", "-e", commit + "^{commit}"], check=True
    )
    archive = output / "baseline-source.tar"
    with archive.open("wb") as stream:
        subprocess.run(
            ["git", "-C", str(REPO), "archive", "--format=tar", commit],
            stdout=stream,
            check=True,
        )
    source = output / "baseline-source"
    source.mkdir()
    with tarfile.open(archive) as bundle:
        bundle.extractall(source, filter="data")
    archive.unlink()
    extension = source / "tests/compatibility-extensions"
    extension.mkdir(parents=True, exist_ok=True)
    for name in CONFIG.get("supplemental_files", []):
        assert Path(name).name == name
        destination = extension / name
        assert not destination.exists(), destination
        destination.write_bytes((HERE / name).read_bytes())
    return source


def signature(value):
    try:
        sig = inspect.signature(value)
    except (TypeError, ValueError):
        return None
    return [
        {
            "name": p.name,
            "kind": p.kind.name,
            "required": p.default is p.empty,
            "default": None if p.default is p.empty else repr(p.default),
        }
        for p in sig.parameters.values()
    ]


def describe(value):
    if isinstance(value, property) or inspect.isgetsetdescriptor(value):
        return {"kind": "property"}
    if callable(value):
        return {"kind": "callable", "parameters": signature(value)}
    return {"kind": "value"}


def surface(path):
    import alopex
    from importlib.metadata import version

    symbols = {}
    dunders = {
        "__enter__",
        "__exit__",
        "__iter__",
        "__next__",
        "__aiter__",
        "__anext__",
    }
    for module_name in [
        "alopex",
        "alopex.asyncio",
        "alopex.remote",
        "alopex.protocols",
    ]:
        module = importlib.import_module(module_name)
        names = set(getattr(module, "__all__", []))
        names.update(
            name
            for name, value in vars(module).items()
            if not name.startswith("_")
            and getattr(value, "__module__", "").startswith("alopex")
        )
        if module_name == "alopex":
            names.add("connect")
        for name in sorted(names):
            if name.startswith("_") or not hasattr(module, name):
                continue
            value = getattr(module, name)
            prefix = module_name + "." + name
            symbols[prefix] = describe(value)
            if inspect.isclass(value):
                for attr in dir(value):
                    if attr.startswith("_") and attr not in dunders:
                        continue
                    symbols[prefix + "." + attr] = describe(getattr(value, attr))
    write(
        path,
        {
            "version": version("alopex"),
            "symbols": symbols,
            "error_codes": sorted(alopex.ALOPEX_ERROR_CODES),
        },
    )


def call_shapes_compatible(before, after):
    """Probe old legal call shapes; added optional parameters are compatible."""

    def make(parameters):
        return inspect.Signature(
            [
                inspect.Parameter(
                    p["name"],
                    getattr(inspect.Parameter, p["kind"]),
                    default=inspect.Parameter.empty if p["required"] else p["default"],
                )
                for p in parameters
            ]
        )

    old, new = make(before), make(after)
    required_kw = {
        p.name: None
        for p in old.parameters.values()
        if p.kind == p.KEYWORD_ONLY and p.default is p.empty
    }
    positional = [
        p
        for p in old.parameters.values()
        if p.kind in (p.POSITIONAL_ONLY, p.POSITIONAL_OR_KEYWORD)
    ]
    witnesses = []
    for count in range(len(positional) + 1):
        kwargs = dict(required_kw)
        kwargs.update(
            {
                p.name: None
                for p in positional[count:]
                if p.kind == p.POSITIONAL_OR_KEYWORD and p.default is p.empty
            }
        )
        witnesses.append(([None] * count, kwargs))
    all_keywords = {
        p.name: None
        for p in old.parameters.values()
        if p.kind in (p.KEYWORD_ONLY, p.POSITIONAL_OR_KEYWORD)
    }
    witnesses.append(
        ([None for p in positional if p.kind == p.POSITIONAL_ONLY], all_keywords)
    )
    if any(p.kind == p.VAR_POSITIONAL for p in old.parameters.values()):
        witnesses.append(([None] * (len(positional) + 2), required_kw))
    if any(p.kind == p.VAR_KEYWORD for p in old.parameters.values()):
        witnesses.append(
            ([None] * len(positional), {**required_kw, "__compat_extra__": None})
        )
    for args, kwargs in witnesses:
        try:
            old.bind(*args, **kwargs)
        except TypeError:
            continue
        try:
            new.bind(*args, **kwargs)
        except TypeError:
            return False
    # A changed default changes the behavior of an old omitted argument.
    for name, param in old.parameters.items():
        if param.default is not param.empty and name in new.parameters:
            if param.default != new.parameters[name].default:
                return False
    return True


def compare_surfaces(old, new):
    errors = []
    for name, contract in old["symbols"].items():
        candidate = new["symbols"].get(name)
        if candidate is None or contract["kind"] != candidate["kind"]:
            errors.append({"symbol": name, "reason": "removed or changed kind"})
        elif contract["kind"] == "callable" and contract["parameters"] is not None:
            if candidate["parameters"] is None or not call_shapes_compatible(
                contract["parameters"], candidate["parameters"]
            ):
                errors.append(
                    {
                        "symbol": name,
                        "reason": "old call shape/default no longer accepted",
                        "old": contract,
                        "candidate": candidate,
                    }
                )
    missing_codes = set(old["error_codes"]) - set(new["error_codes"])
    if missing_codes:
        errors.append({"error_codes_removed": sorted(missing_codes)})
    return errors


class ReplayPlugin:
    def __init__(self):
        self.items = {}
        self.collected = []
        self.collection_errors = []
        self.deselected = []

    def pytest_collection_finish(self, session):
        self.collected = [item.nodeid for item in session.items]

    def pytest_deselected(self, items):
        self.deselected += [item.nodeid for item in items]

    def pytest_collectreport(self, report):
        if report.failed:
            self.collection_errors.append(str(report.longrepr))

    def pytest_runtest_logreport(self, report):
        item = self.items.setdefault(report.nodeid, {"phases": []})
        item["phases"].append(
            {
                "when": report.when,
                "outcome": report.outcome,
                "xfail": getattr(report, "wasxfail", None),
                "detail": str(report.longrepr)[:12000]
                if report.failed or report.skipped
                else None,
            }
        )


def probe_loopback():
    """A denied local socket is infrastructure failure, not an old API defect."""
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))


def replay(source, report):
    import pytest
    from importlib.metadata import version

    for package, expected in CONFIG["required_dependencies"].items():
        assert version(package) == expected, (package, version(package), expected)
    try:
        probe_loopback()
    except OSError as error:
        write(report, {"status": "infrastructure_error", "error": str(error)})
        raise RuntimeError(
            "local HTTP test stubs require loopback bind permission"
        ) from error
    plugin = ReplayPlugin()
    arguments = [
        "-q",
        "--tb=short",
        "--rootdir",
        str(source),
        "--basetemp",
        str(report.parent / (report.stem + "-temp")),
        "-o",
        "cache_dir=" + str(report.parent / (report.stem + "-cache")),
        "-m",
        CONFIG["markers"],
    ]
    selected = CONFIG["selected_cases"]
    assert selected and len(selected) == len(set(selected))
    assert not set(selected) & (
        set(CONFIG["test_replacements"]) | set(CONFIG["excluded_cases"])
    )
    arguments += [
        str(source / node.split("::", 1)[0]) + "::" + node.split("::", 1)[1]
        for node in selected
    ]
    code = int(pytest.main(arguments, plugins=[plugin]))
    result = {
        "version": version("alopex"),
        "exit_code": code,
        "collected": plugin.collected,
        "deselected": list(CONFIG["test_replacements"]),
        "excluded_cases": CONFIG["excluded_cases"],
        "collection_errors": plugin.collection_errors,
        "tests": plugin.items,
        "baseline_commit": CONFIG["baseline_commit"],
        "test_replacements": CONFIG.get("test_replacements", {}),
    }
    write(report, result)
    assert code in [0, 1] and plugin.collected and not plugin.collection_errors, result
    assert set(plugin.collected) == set(selected), (
        "case allowlist did not collect exactly"
    )
    assert set(plugin.items) == set(plugin.collected), "not every selected test ran"


def outcome(item):
    phases = item["phases"]
    if any(p["outcome"] == "failed" for p in phases):
        return "failed"
    if any(p["outcome"] == "skipped" for p in phases):
        return "skipped"
    assert {p["when"] for p in phases} == {"setup", "call", "teardown"}, phases
    return "passed"


def compare_reports(old, new):
    assert set(old["collected"]) == set(new["collected"]), (
        "selected corpus changed between packages"
    )
    regressions, baseline_failures, skipped, improved = [], [], [], []
    for name in old["collected"]:
        before, after = outcome(old["tests"][name]), outcome(new["tests"][name])
        if before == "passed" and after != "passed":
            regressions.append(name)
        elif before == "failed":
            baseline_failures.append(name)
            if after == "passed":
                improved.append(name)
        elif before == "skipped":
            skipped.append(name)
        if after == "skipped" and before != "skipped":
            if name not in regressions:
                regressions.append(name)
    replacements = []
    for old_node, spec in CONFIG.get("test_replacements", {}).items():
        assert old_node in old["deselected"] and old_node in new["deselected"], old_node
        matches = [
            name for name in old["collected"] if name.endswith(spec["replacement"])
        ]
        assert len(matches) == 1, spec
        assert (
            outcome(old["tests"][matches[0]])
            == outcome(new["tests"][matches[0]])
            == "passed"
        ), spec
        replacements.append(
            {"old": old_node, "replacement": matches[0], "reason": spec["reason"]}
        )
    for filename in CONFIG["python_files"]:
        names = [
            name
            for name in old["collected"]
            if name.split("::")[0].endswith("/" + filename)
        ]
        assert names and any(
            outcome(old["tests"][name]) == "passed" for name in names
        ), filename
    return {
        "regressions": regressions,
        "explicit_replacements": replacements,
        "baseline_failures": baseline_failures,
        "skipped": skipped,
        "improved": improved,
        "baseline_passed": sum(
            outcome(old["tests"][n]) == "passed" for n in old["collected"]
        ),
        "candidate_passed": sum(
            outcome(new["tests"][n]) == "passed" for n in new["collected"]
        ),
        "selected": len(old["collected"]),
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=["surface", "replay"])
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--source", type=Path)
    args = parser.parse_args()
    if args.mode == "surface":
        surface(args.output)
    else:
        assert args.source is not None
        replay(args.source, args.output)


if __name__ == "__main__":
    main()
