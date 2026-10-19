#!/usr/bin/env python3
"""Old writer -> candidate reader regression matrix, in separate processes."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import time
import zipfile

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[1]


def sha(path):
    with Path(path).open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def assert_complete(output, rust=True):
    """A pass requires every matrix result, not just the easiest Arrow case."""
    expected = [
        "native/result.json",
        "arrow/report.json",
        "python-from-python-result.json",
        "python15-python-direct-result.json",
    ]
    if rust:
        expected += [
            "python-from-rust-result.json",
            "rust-from-python-result.json",
            "rust-from-rust-result.json",
            "rust-from-python-direct-result.json",
            "rust-from-rust-direct-result.json",
            "rust15-python-direct-result.json",
        ]
    for name in expected:
        result = json.loads((output / name).read_text())
        if name == "arrow/report.json":
            cases = result["cases"]
            required = {"list_f32", "list_f64", "fixed_f32", "fixed_f64", "json_utf8"}
            assert {c["representation"] for c in cases} == required and len(cases) == 5
            assert all(
                c["pass"]
                and len(c["dimensions"]) == 2
                and {d["dimension"] for d in c["dimensions"]} == {3, 384}
                and all(
                    d["hnsw_query_pass"] and d["new_db_reopen_pass"]
                    for d in c["dimensions"]
                )
                for c in cases
            ), cases
        elif name == "native/result.json":
            assert (
                result["direct_same_database_open"]
                and result["second_target_reopen_pass"]
            )
            assert (
                result["sql_vectors_bits_equal"] and result["raw_get_vector_bits_equal"]
            )
            assert not result["old_existing_hnsw_indexes_recreated_by_harness"]
        elif "direct" in name:
            assert result["direct_open_pass"] and result["second_reopen_pass"], name
            assert result["index_recreated"] is False, name
        else:
            assert (
                result["logical_roundtrip_pass"] and result["new_write_update_pass"]
            ), name
    return expected


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--old-python", type=Path, required=True)
    parser.add_argument("--new-python", type=Path, required=True)
    parser.add_argument("--candidate-wheel", type=Path, required=True)
    parser.add_argument("--old-rust", type=Path)
    parser.add_argument("--new-rust", type=Path)
    parser.add_argument(
        "--python-only",
        action="store_true",
        help="explicitly exclude Rust; never the CI mode",
    )
    parser.add_argument(
        "--output", type=Path, required=True, help="new, empty evidence directory"
    )
    args = parser.parse_args()
    if not args.python_only and not (args.old_rust and args.new_rust):
        parser.error(
            "both Rust binaries are required, or explicitly select --python-only"
        )
    if args.python_only and (args.old_rust or args.new_rust):
        parser.error("--python-only must not be combined with Rust binaries")
    # Do not resolve venv interpreter symlinks: doing so would discard their environment.
    old_python, new_python = (
        str(p.absolute()) for p in [args.old_python, args.new_python]
    )
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=True)
    if any(output.iterdir()):
        parser.error("output must be empty; existing evidence is never overwritten")
    for name in ["native", "arrow"]:
        (output / name).mkdir()
    shutil.copyfile(HERE / "fixture.json", output / "fixture.json")
    env = dict(os.environ, ALOPEX_COMPAT_OUTPUT=str(output))
    env.pop("PYTHONPATH", None)
    env.pop("PYTHONHOME", None)
    phases = []
    report = {
        "status": "failed",
        "phases": phases,
        "mode": "python-only" if args.python_only else "python-rust",
        "direction": "0.8.15 writer -> candidate reader",
        "source_commit": subprocess.check_output(
            ["git", "-C", str(REPO), "rev-parse", "HEAD"], text=True
        ).strip(),
        "scripts": {p.name: sha(p) for p in HERE.glob("*.py")},
        "provenance_sha256": sha(HERE / "provenance.json"),
        "tracked_diff_sha256": hashlib.sha256(
            subprocess.check_output(
                ["git", "-C", str(REPO), "diff", "HEAD", "--binary"]
            )
        ).hexdigest(),
    }

    def run(label, command):
        started = time.time()
        with (output / (label + ".log")).open("w", encoding="utf-8") as log:
            result = subprocess.run(
                [str(x) for x in command],
                env=env,
                cwd=output,
                stdout=log,
                stderr=subprocess.STDOUT,
                timeout=300,
            )
        phases.append(
            {
                "label": label,
                "command": [str(x) for x in command],
                "returncode": result.returncode,
                "seconds": time.time() - started,
            }
        )
        print(f"{label}: exit {result.returncode}", flush=True)
        if result.returncode:
            raise RuntimeError(f"{label} failed; see {output / (label + '.log')}")

    def python(label, interpreter, script, *arguments):
        run(label, [interpreter, HERE / script, *arguments])

    def rust(label, binary, mode, source=None):
        command = [binary.absolute(), output, mode, label]
        if source:
            command.append(source)
        run(label, command)

    try:
        probe = """import importlib.metadata as m, json, sys, hashlib
import alopex, alopex._alopex as native, numpy, pyarrow
from pathlib import Path
p=Path(native.__file__)
with p.open('rb') as f: digest=hashlib.file_digest(f,'sha256').hexdigest()
print(json.dumps({'version':m.version('alopex'),'native_sha256':digest,
 'numpy':numpy.__version__,'pyarrow':pyarrow.__version__,'python':sys.version,'prefix':sys.prefix}))
"""
        identities = {
            key: json.loads(
                subprocess.check_output([interpreter, "-c", probe], env=env, text=True)
            )
            for key, interpreter in [("old", old_python), ("candidate", new_python)]
        }
        assert identities["old"]["version"] == "0.8.15", identities
        assert identities["old"]["prefix"] != identities["candidate"]["prefix"], (
            "use isolated old/new environments"
        )
        with zipfile.ZipFile(args.candidate_wheel) as archive:
            native_members = [
                name
                for name in archive.namelist()
                if name.startswith("alopex/_alopex.") and name.endswith((".so", ".pyd"))
            ]
            assert len(native_members) == 1, native_members
            with archive.open(native_members[0]) as native:
                wheel_native_sha = hashlib.file_digest(native, "sha256").hexdigest()
            assert wheel_native_sha == identities["candidate"]["native_sha256"], (
                "candidate wheel != installed native module"
            )
        report["candidate_wheel"] = {
            "name": args.candidate_wheel.name,
            "sha256": sha(args.candidate_wheel),
        }
        report["rust_sources"] = {
            str(path.relative_to(HERE)): sha(path)
            for path in [
                HERE / "rust_harness.rs",
                HERE / "rust-old/Cargo.toml",
                HERE / "rust-old/Cargo.lock",
                HERE / "rust-current/Cargo.toml",
                HERE / "rust-current/Cargo.lock",
            ]
        }
        env["ALOPEX_COMPAT_TARGET_VERSION"] = identities["candidate"]["version"]
        report["packages"] = identities
        if not args.python_only:
            report["rust_binaries"] = {
                key: sha(binary)
                for key, binary in [
                    ("old", args.old_rust),
                    ("candidate", args.new_rust),
                ]
            }
        python("arrow-old", old_python, "arrow.py", "export")
        python("native-old", old_python, "native.py", "old")
        python("logical-old", old_python, "logical.py", "export", "python15")
        if not args.python_only:
            rust("rust15", args.old_rust, "export")
        python("native-candidate", new_python, "native.py", "new")
        python("arrow-candidate", new_python, "arrow.py", "import")
        for source in ["python15"] + ([] if args.python_only else ["rust15"]):
            python(
                source + "-python-direct", new_python, "logical.py", "direct", source
            )
            python(
                "python-from-" + ("python" if source == "python15" else "rust"),
                new_python,
                "logical.py",
                "import",
                "python-from-" + ("python" if source == "python15" else "rust"),
                source,
            )
            if not args.python_only:
                label = "rust-from-" + ("python" if source == "python15" else "rust")
                shutil.copytree(
                    output / (source + "-source-db"),
                    output / (source + "-rust-direct-db"),
                )
                rust(label + "-direct", args.new_rust, "direct", source)
                rust(label, args.new_rust, "import", source)
        report["required_results"] = assert_complete(output, rust=not args.python_only)
        report["status"] = "passed"
    except BaseException as error:
        report["failure"] = {"type": type(error).__name__, "message": str(error)}
        raise
    finally:
        # Output is exclusively owned by this invocation; no user's DB is touched.
        removed = []
        for path in [
            *output.glob("*-db"),
            output / "native/old-created-db",
            *output.glob("arrow/*-db"),
        ]:
            if path.is_dir():
                shutil.rmtree(path)
                removed.append(str(path.relative_to(output)))
        report["removed_databases"] = removed
        (output / "summary.json").write_text(
            json.dumps(report, indent=2, ensure_ascii=False) + "\n"
        )
    print(f"PASS ({report['mode']}); retained evidence: {output / 'summary.json'}")


if __name__ == "__main__":
    main()
