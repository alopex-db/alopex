"""Verify the candidate's public remote SQL contracts with audited assets."""
from __future__ import annotations

import base64
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tarfile

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts/test-env"))
from capacity import Budget
from common import atomic_json, digest, require, verify_assets


def metadata(path):
    output = subprocess.check_output(["gh", "api", "repos/alopex-db/alopex/" + path], timeout=30)
    return json.loads(output)


def verify_product(directory, config):
    manifest_path = directory / "assets/assets.json"
    result = json.loads((directory / "result.json").read_bytes())
    source = {"commit": config["source_commit"], "tree": config["source_tree"], "profile": "release"}
    require(result["status"] == "passed" and result["source"] == source, "IDENTITY")
    require(digest(manifest_path) == result["assets_digest"] == config["assets_digest"], "IDENTITY")
    manifest = verify_assets(manifest_path, "linux/amd64")
    require(manifest["source_commit"] == source["commit"] and
            {item["role"] for item in manifest["files"]} ==
            {"server", "wheel", "library", "receipt", "wheel_receipt", "harness"}, "IDENTITY")
    files = {item["role"]: manifest_path.parent / item["path"] for item in manifest["files"]}
    receipt = json.loads(files["receipt"].read_bytes())
    require(receipt["source"] == source and receipt["version"] == "0.8.17" and
            receipt["build"]["wheel"]["mode"] == "reused_development_asset" and
            digest(files["server"]) == config["server_sha256"] and
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
            config["mode"] == "sql_preflight" and
            config["expected_checksum"] == "b97a1fb499381d8690840092283a59ad92d042a30cf2e52400ecb9147d422fc2", "IDENTITY")
    for name, expected in config["test_sources"].items():
        require(digest(ROOT / name) == expected, "IDENTITY")
    require(subprocess.run(["git", "diff", "--quiet", config["source_commit"], "HEAD", "--",
                            "crates/alopex-py", "crates/alopex-server", "crates/alopex-sql"],
                           cwd=ROOT, timeout=10).returncode == 0, "IDENTITY")

    temporary = Path(os.environ["RUNNER_TEMP"])
    work, export = temporary / "alopex-sequence-e2e", ROOT / "sequence-e2e-export"
    require(not work.exists() and not export.exists(), "STATE")
    roots = [ROOT.parent, temporary]
    for item in (ROOT.parent.parent / "_actions", Path.home() / ".cargo", Path.home() / ".cache"):
        if item.exists():
            roots.append(item)
    phases = {"fetch": 500_000_000, "prepare": 1_000_000_000,
              "test": 500_000_000, "collect": 100_000_000}
    reserve = 1_000_000_000
    budget = Budget({"roots": list(map(str, roots)), "stop_bytes": 40_000_000_000,
                     "reserve_bytes": reserve, "poll_seconds": 2, "planned_bytes": phases})
    initial = budget.used()
    require(initial + sum(phases.values()) + reserve < 40_000_000_000, "CAPACITY")
    work.mkdir(mode=0o700)
    export.mkdir(mode=0o700)
    import_dir, venv = work / "product", work / "venv"
    status, failure = "failed", None
    try:
        run_record = metadata(f"actions/runs/{config['product_run']}")
        require(run_record["head_sha"] == config["product_controller"] and
                run_record["status"] == "completed" and run_record["conclusion"] == "success" and
                run_record["event"] == "workflow_dispatch", "IDENTITY")
        artifacts = metadata(f"actions/runs/{config['product_run']}/artifacts")["artifacts"]
        matches = [item for item in artifacts if item["name"] == "native-product-assets-no-mount"]
        require(len(matches) == 1 and matches[0]["id"] == config["artifact_id"] and
                matches[0]["digest"] == config["artifact_digest"] and not matches[0]["expired"] and
                0 < matches[0]["size_in_bytes"] < 100_000_000, "IDENTITY")
        budget.before("fetch")
        budget.run(["gh", "run", "download", str(config["product_run"]), "--repo", "alopex-db/alopex",
                    "--name", "native-product-assets-no-mount", "--dir", str(import_dir)],
                   timeout=180, log=work / "download.log")
        files = verify_product(import_dir, config)
        files["server"].chmod(0o700)
        env = dict(os.environ, PYTHONDONTWRITEBYTECODE="1", PIP_NO_CACHE_DIR="1",
                   PYTEST_DISABLE_PLUGIN_AUTOLOAD="1", ALOPEX_REQUIRE_SERVER_E2E="1",
                   ALOPEX_SERVER_BIN=str(files["server"]),
                   LD_LIBRARY_PATH=str(files["library"].parent), TMPDIR=str(work))
        for key in ("GH_TOKEN", "GITHUB_TOKEN", "PYTHONPATH", "PYTHONHOME"):
            env.pop(key, None)
        budget.before("prepare")
        budget.run([sys.executable, "-m", "venv", str(venv)], timeout=90,
                   log=work / "venv.log", env=env)
        python = venv / "bin/python"
        budget.before("prepare")
        budget.run([str(python), "-m", "pip", "install", "pytest==8.4.2"], timeout=180,
                   log=work / "pytest-install.log", env=env)
        budget.before("prepare")
        budget.run([str(python), "-m", "pip", "install", "--no-index", "--no-deps", str(files["wheel"])],
                   timeout=90, log=work / "wheel-install.log", env=env)
        env.update(ALOPEX_PREFLIGHT_WORK=str(work),
                   ALOPEX_PREFLIGHT_EXPECTED=config["expected_checksum"])
        budget.before("test")
        budget.run([str(python), "-B", "scripts/ops/run_sql_preflight.py"],
                   timeout=180, log=work / "preflight.log", env=env)
        preflight = json.loads((work / "preflight-result.json").read_bytes())
        require(preflight["rows"] == 20 and preflight["timing_samples"] == 0 and
                len(preflight["checks"]) == 6 and preflight["server_stopped"] and
                preflight["result_sha256"] == config["expected_checksum"], "CASE")
        atomic_json(work / "test-summary.json", {"checks": 6, "rows": 20,
                    "timing_samples": 0, "checksum": preflight["result_sha256"],
                    "source": config["source_commit"], "assets_digest": config["assets_digest"]})
        status = "passed"
    except Exception as error:
        failure = {"type": type(error).__name__, "code": getattr(error, "code", "INTERNAL")}
        atomic_json(work / "failure.json", failure)
    finally:
        for path in (import_dir, venv):
            if path.exists():
                shutil.rmtree(path)
        atomic_json(work / "cleanup.json", {"owned_inputs_removed": not import_dir.exists() and not venv.exists(),
                                               "allocated_bytes_after": budget.used()})
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
        atomic_json(export / "result.json", {"schema": "alopex.sql-preflight/v1", "status": status,
                  "source_commit": config["source_commit"], "assets_digest": config["assets_digest"],
                  "checks": 6 if status == "passed" else 0, "evidence_sha256": evidence_hash,
                  "encrypted_sha256": digest(encrypted), "owned_resources_removed": not work.exists(),
                  "performance_qualification": False,
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
