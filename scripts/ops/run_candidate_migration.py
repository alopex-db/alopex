"""Run the published old writer against the audited candidate wheel."""
from __future__ import annotations

import base64
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tarfile
import zipfile

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts/test-env"))
from capacity import Budget
from common import atomic_json, digest, require, verify_assets


def metadata(path):
    raw = subprocess.check_output(["gh", "api", "repos/alopex-db/alopex/" + path], timeout=30)
    return json.loads(raw)


def product(directory, config):
    manifest_path = directory / "assets/assets.json"
    result = json.loads((directory / "result.json").read_bytes())
    source = {"commit": config["source_commit"], "tree": config["source_tree"], "profile": "release"}
    require(result["status"] == "passed" and result["source"] == source, "IDENTITY")
    require(digest(manifest_path) == result["assets_digest"] == config["assets_digest"], "IDENTITY")
    manifest = verify_assets(manifest_path, "linux/amd64")
    require(manifest["source_commit"] == source["commit"], "IDENTITY")
    files = {item["role"]: manifest_path.parent / item["path"] for item in manifest["files"]}
    receipt = json.loads(files["receipt"].read_bytes())
    require(receipt["source"] == source and receipt["version"] == "0.8.17" and
            digest(files["wheel"]) == config["wheel_sha256"] and
            digest(files["wheel_receipt"]) == config["wheel_receipt_sha256"], "IDENTITY")
    return files


def run():
    os.umask(0o077)
    require(os.geteuid() != 0 and os.environ.get("RUNNER_KIND") == "github-hosted" and
            os.environ.get("GITHUB_EVENT_NAME") == "workflow_dispatch", "POLICY")
    config = json.loads(base64.b64decode(os.environ.pop("E2E_CONFIG"), validate=True))
    require(config["source_commit"] == "a537c8beb5c0d3a135e329ab344942243487f55e" and
            config["controller"] == os.environ["GITHUB_SHA"] and
            config["mode"] == "candidate_migration", "IDENTITY")
    for name, expected in config["test_sources"].items():
        require(digest(ROOT / name) == expected, "IDENTITY")
    require(subprocess.run(["git", "diff", "--quiet", config["source_commit"], "HEAD", "--",
                            "scripts/compatibility/logical.py", "crates/alopex-py", "crates/alopex-sql"],
                           cwd=ROOT, timeout=10).returncode == 0, "IDENTITY")

    temporary = Path(os.environ["RUNNER_TEMP"])
    work, export = temporary / "alopex-candidate-migration", ROOT / "migration-export"
    require(not work.exists() and not export.exists(), "STATE")
    roots = [ROOT.parent, temporary]
    for item in (ROOT.parent.parent / "_actions", Path.home() / ".cargo", Path.home() / ".cache"):
        if item.exists():
            roots.append(item)
    phases = {"fetch": 500_000_000, "prepare": 1_000_000_000,
              "test": 3_000_000_000, "collect": 100_000_000}
    budget = Budget({"roots": list(map(str, roots)), "stop_bytes": 40_000_000_000,
                     "reserve_bytes": 1_000_000_000, "poll_seconds": 2, "planned_bytes": phases})
    initial = budget.used()
    require(initial + sum(phases.values()) + 1_000_000_000 < 40_000_000_000, "CAPACITY")
    work.mkdir(mode=0o700)
    export.mkdir(mode=0o700)
    import_dir, old_venv, new_venv, cases = (work / "product", work / "old-env",
                                           work / "new-env", work / "cases")
    cases.mkdir()
    status, failure = "failed", None
    try:
        record = metadata(f"actions/runs/{config['product_run']}")
        require(record["head_sha"] == config["product_controller"] and
                record["status"] == "completed" and record["conclusion"] == "success" and
                record["event"] == "workflow_dispatch", "IDENTITY")
        artifacts = metadata(f"actions/runs/{config['product_run']}/artifacts")["artifacts"]
        matches = [item for item in artifacts if item["name"] == "native-product-assets-no-mount"]
        require(len(matches) == 1 and matches[0]["id"] == config["artifact_id"] and
                matches[0]["digest"] == config["artifact_digest"] and not matches[0]["expired"] and
                0 < matches[0]["size_in_bytes"] < 100_000_000, "IDENTITY")
        budget.before("fetch")
        budget.run(["gh", "run", "download", str(config["product_run"]), "--repo", "alopex-db/alopex",
                    "--name", "native-product-assets-no-mount", "--dir", str(import_dir)],
                   timeout=180, log=work / "download.log")
        files = product(import_dir, config)
        env = dict(os.environ, PYTHONDONTWRITEBYTECODE="1", PIP_NO_CACHE_DIR="1", TMPDIR=str(work))
        for key in ("GH_TOKEN", "GITHUB_TOKEN", "PYTHONPATH", "PYTHONHOME"):
            env.pop(key, None)
        for venv in (old_venv, new_venv):
            budget.before("prepare")
            budget.run([sys.executable, "-m", "venv", str(venv)], timeout=90,
                       log=work / (venv.name + "-create.log"), env=env)
        old_python, new_python = old_venv / "bin/python", new_venv / "bin/python"
        for python, label in ((old_python, "old"), (new_python, "new")):
            budget.before("prepare")
            budget.run([str(python), "-m", "pip", "install", "--only-binary=:all:",
                        "numpy==2.2.1", "pyarrow==26.0.0"], timeout=240,
                       log=work / (label + "-dependencies.log"), env=env)
        budget.before("prepare")
        budget.run([str(old_python), "-m", "pip", "install", "--only-binary=:all:",
                    "--no-deps", "alopex==0.8.15"], timeout=180, log=work / "old-install.log", env=env)
        budget.before("prepare")
        budget.run([str(new_python), "-m", "pip", "install", "--no-index", "--no-deps",
                    str(files["wheel"])], timeout=90, log=work / "new-install.log", env=env)
        for key in ("LD_LIBRARY_PATH", "DYLD_LIBRARY_PATH", "DYLD_FALLBACK_LIBRARY_PATH",
                    "NIM_SQL_PARSER_LIB_DIR", "ALOPEX_NIM_PARSER_ALLOW_LOCAL_BUILD"):
            env.pop(key, None)
        identity_code = ("import alopex._alopex as n, importlib.metadata as m, hashlib, json; "
                         "from pathlib import Path; p=Path(n.__file__); "
                         "print(json.dumps({'version':m.version('alopex'),"
                         "'native_sha256':hashlib.sha256(p.read_bytes()).hexdigest()}))")
        identities = {}
        for python, label in ((old_python, "old"), (new_python, "new")):
            budget.before("test")
            budget.run([str(python), "-c", identity_code], timeout=30,
                       log=work / (label + "-identity.log"), env=env)
            identities[label] = json.loads((work / (label + "-identity.log")).read_text())
        require(identities["old"] == {"version": "0.8.15", "native_sha256": config["old_native_sha256"]} and
                identities["new"]["version"] == "0.8.17", "IDENTITY")
        with zipfile.ZipFile(files["wheel"]) as archive:
            native_members = [name for name in archive.namelist() if name.endswith(".so") and
                              name.split("/")[-1].startswith("_alopex.")]
            require(len(native_members) == 1, "IDENTITY")
            expected_native_sha256 = hashlib.sha256(archive.read(native_members[0])).hexdigest()
        require(identities["new"]["native_sha256"] == expected_native_sha256, "IDENTITY")
        env["ALOPEX_COMPAT_OUTPUT"] = str(cases)
        env["ALOPEX_COMPAT_TARGET_VERSION"] = "0.8.17"
        script = ROOT / "scripts/compatibility/logical.py"
        for label, python, arguments in (
            ("old-export", old_python, ["export", "python15"]),
            ("candidate-direct", new_python, ["direct", "python15"]),
            ("candidate-import", new_python, ["import", "python-from-python", "python15"]),
        ):
            budget.before("test")
            budget.run([str(python), str(script), *arguments], timeout=300,
                       log=work / (label + ".log"), env=env)
            if label == "candidate-direct":
                shutil.rmtree(cases / "python15-python-direct-db")
            if label == "candidate-import":
                shutil.rmtree(cases / "python-from-python-destination-db")
                shutil.rmtree(cases / "python15-source-db")
        direct = json.loads((cases / "python15-python-direct-result.json").read_text())
        logical = json.loads((cases / "python-from-python-result.json").read_text())
        bundle = json.loads((cases / "python15-bundle.json").read_text())
        require(direct["direct_open_pass"] and direct["second_reopen_pass"] and
                direct["index_recreated"] is False and
                logical["logical_roundtrip_pass"] and logical["new_write_update_pass"], "CASE")
        require(direct["source"] == logical["source"] == bundle["source"] == "Python 0.8.15" and
                direct["destination"] == logical["destination"] == "Python 0.8.17" and
                "HnswSearch" in json.dumps(direct["hnsw_plan"]) and
                logical["post_reopen_snapshot"] == bundle["expected"], "CASE")
        require(logical["stock_scalar_parquet_probe"]["preserved"] and
                logical["limitations"] == bundle["limitations"] and
                logical["limitations"]["columnar_parquet_export_count"] == 0 and
                "unsupported format" in logical["limitations"]["vector_parquet"] and
                logical["stock_csv_probe"]["preserved"] is False, "CASE")
        require(not any(cases.glob("*-db")), "CLEANUP")
        atomic_json(work / "case-evidence.json", {
            "schema": "alopex.candidate-migration-cases/v1",
            "source": bundle["source"],
            "expected": bundle["expected"],
            "direct": direct,
            "logical": logical,
            "old_limitations": bundle["limitations"],
            "source_script_sha256": digest(script),
            "loaded_native_sha256": identities["new"]["native_sha256"],
            "wheel_native_sha256": expected_native_sha256,
        })
        atomic_json(work / "test-summary.json", {"schema": "alopex.candidate-migration/v1",
                   "status": "passed", "identities": identities,
                   "direct_open_pass": True, "second_reopen_pass": True,
                   "logical_roundtrip_pass": True, "new_write_update_pass": True,
                   "stock_scalar_parquet_preserved": True,
                   "old_columnar_parquet_export_count": 0,
                   "old_vector_parquet": logical["limitations"]["vector_parquet"],
                   "stock_csv_probe": logical["stock_csv_probe"],
                   "source_commit": config["source_commit"],
                   "wheel_sha256": config["wheel_sha256"]})
        status = "passed"
    except Exception as error:
        failure = {"type": type(error).__name__, "code": getattr(error, "code", "INTERNAL")}
        atomic_json(work / "failure.json", failure)
    finally:
        for path in (import_dir, old_venv, new_venv, cases):
            if path.exists():
                shutil.rmtree(path)
        atomic_json(work / "cleanup.json", {"owned_inputs_removed": all(not p.exists() for p in
                    (import_dir, old_venv, new_venv, cases)), "allocated_bytes_after": budget.used()})
        budget.before("collect")
        archive = work / "evidence.tar.gz"
        with tarfile.open(archive, "w:gz") as output:
            for path in sorted(work.iterdir()):
                if path.is_file() and path != archive:
                    output.add(path, arcname=path.name)
        encrypted = export / "private-evidence.cms"
        subprocess.run(["openssl", "cms", "-encrypt", "-aes-256-cbc", "-binary", "-outform", "DER",
                        "-in", str(archive), "-out", str(encrypted),
                        str(ROOT / ".github/ops/native-evidence-cert.pem")], check=True,
                       capture_output=True, timeout=30)
        evidence_hash = digest(archive)
        shutil.rmtree(work)
        budget.check()
        atomic_json(export / "result.json", {"schema": "alopex.candidate-migration/v1", "status": status,
                    "source_commit": config["source_commit"], "assets_digest": config["assets_digest"],
                    "evidence_sha256": evidence_hash, "encrypted_sha256": digest(encrypted),
                    "owned_resources_removed": not work.exists(),
                    "release_qualification": False,
                    "error_code": None if status == "passed" else failure["code"]})
    print(json.dumps({"status": status, "error_code": None if status == "passed" else failure["code"]}))
    return 0 if status == "passed" else 1


if __name__ == "__main__":
    try:
        result = run()
    except Exception as error:
        location = error.__traceback__
        while location and location.tb_next:
            location = location.tb_next
        print(json.dumps({"status": "failed", "error_code": getattr(error, "code", "CONSTRUCTION"),
                          "exception": type(error).__name__, "line": location.tb_lineno if location else 0}))
        raise SystemExit(1)
    raise SystemExit(result)
