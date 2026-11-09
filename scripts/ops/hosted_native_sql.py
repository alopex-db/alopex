"""Run native SQL acceptance using products built from public source on the managed VM."""
from __future__ import annotations

import base64
import gzip
import json
import os
from pathlib import Path
import shutil
import sys

from hosted_native_image import construct


def verify_products(manifest_path, result_path, expected):
    from common import digest, require, verify_assets
    result = json.loads(result_path.read_text())
    require(result["schema"] == "alopex.native-assets/v1" and result["status"] == "passed" and result["source"]["commit"] == expected["source_commit"] and
            result["source"]["tree"] == expected["source_tree"] and result["source"]["profile"] == "release", "IDENTITY")
    require(digest(manifest_path) == result["assets_digest"], "IDENTITY")
    manifest = verify_assets(manifest_path, expected["platform"])
    require(manifest["source_commit"] == expected["source_commit"] and
            manifest["harness_commit"] == expected["harness_commit"], "IDENTITY")
    receipt_entry = next(item for item in manifest["files"] if item["role"] == "receipt")
    receipt = json.loads((manifest_path.parent / receipt_entry["path"]).read_text())
    require(receipt["source"] == result["source"] and receipt["version"] == expected["version"], "IDENTITY")
    if "assets_digest" in expected:
        require(digest(manifest_path) == expected["assets_digest"], "IDENTITY")
    wheel_build = receipt.get("build", {}).get("wheel")
    reused = isinstance(wheel_build, dict) and wheel_build.get("mode") == "reused_development_asset"
    has_original = any(item["role"] == "wheel_receipt" for item in manifest["files"])
    if reused or has_original:
        from reused_native_wheel import verify_composed_receipts
        require(reused and has_original and "assets_digest" in expected, "IDENTITY")
        verify_composed_receipts(manifest_path.parent, manifest)
    return manifest


def operation(repo, config):
    def run(profile, docker, budget, work, export, public):
        from common import atomic_json, digest, private_dir, require, verify_assets, CASES
        from lifecycle import Run
        import envctl

        budget.before("runtime")
        product = repo / "native-product-import"
        selected = dict(profile, run_id="native-sql", assets=str(product / "assets/assets.json"))
        verify_products(Path(selected["assets"]), product / "result.json", config)
        path = work / "sql-profile.json"
        atomic_json(path, selected)
        args = [sys.executable, "-B", str(repo / "scripts/test-env/envctl.py")]
        primary = None
        recovery_errors = []
        try:
            budget.before("accept")
            budget.run([*args, "accept", "--profile", str(path)], timeout=selected["limits"]["run_seconds"],
                       log=work / "native-sql-accept.log")
            first = Run(selected["private_output"], selected["run_id"]).load()
            first.verify_collection()
            require(digest(first.root / "acceptance.json") == first.record["acceptance_digest"], "IDENTITY")
            accepted = json.loads((first.root / "acceptance.json").read_text())
            require(accepted["status"] == "passed" and
                    {row["id"] for row in accepted["cases"]} == set(CASES) and
                    all(row["status"] == "passed" for row in accepted["cases"]), "CASE")
            for name in ("native-sql", "native-sql-repeat"):
                current = Run(selected["private_output"], name).load()
                current.verify_collection()
                report = json.loads((current.root / "collected/result.json").read_text())
                require(report["status"] == "passed", "CASE")
                for row in report["cases"]:
                    if row["id"] in ("S01", "S02", "S03"):
                        require({cell["engine"] for cell in row["evidence"]} == {"alopex", "postgresql"}, "CASE")
                        for cell in row["evidence"]:
                            resource = cell["resources"]
                            require(cell["status"] == "passed" and
                                    resource["qualified_harness"]["status"] == "observed" and
                                    resource["coverage"] == "stable_pid_namespace_allocation" and
                                    len(resource["client"]) == 1 and resource["server"], "MEASUREMENT")
                            for observed in resource["server"] + resource["client"]:
                                require(all(observed[field] >= 0 for field in
                                            ("rss_before_bytes", "rss_after_bytes", "read_bytes", "write_bytes", "cpu_seconds")), "MEASUREMENT")
            budget.before("collect")
            budget.run([*args, "export-public", "--profile", str(path)], timeout=30,
                       log=work / "native-sql-public.log")
            shutil.copyfile(first.root / "public.json", export / "sql-acceptance.json")
            public["native_sql_acceptance"] = True
            public["full_acceptance"] = True
            public["schema"] = "alopex.native-sql-acceptance/v1"
        except BaseException as error:
            primary = error
            raise
        finally:
            for name in ("native-sql-repeat", "native-sql"):
                current = Run(selected["private_output"], name)
                if not current.path.exists():
                    continue
                try:
                    current.load()
                    if current.record["phase"] not in ("collected", "deleted"):
                        envctl.collect(selected, docker, budget, name)
                        current.load()
                    current.clean(docker.inspect, docker.remove)
                    if (current.root / "bundle").exists():
                        shutil.rmtree(current.root / "bundle")
                except BaseException as error:
                    recovery_errors.append({"run": name, "error": repr(error)})
            atomic_json(work / "sql-cleanup.json", {"errors": recovery_errors})
            if recovery_errors and primary is None:
                raise RuntimeError("SQL cleanup incomplete")
    return run


def main():
    if os.environ.get("RUNNER_KIND") != "github-hosted" or os.geteuid() != 0:
        print('{"status":"failed","error_code":"VM_SCOPE"}')
        return 78
    try:
        os.umask(0o077)
        repo = Path(os.environ["GITHUB_WORKSPACE"]).resolve()
        temporary = Path(os.environ["RUNNER_TEMP"]).resolve()
        inputs = json.loads(gzip.decompress(base64.b64decode(os.environ.pop("NATIVE_IMAGE_INPUTS"), validate=True)))
        config = json.loads(base64.b64decode(os.environ.pop("PUBLIC_SQL_CONFIG"), validate=True))
        inputs.update(config["image"])
        inputs["bounds"].update(config["bounds"])
        inputs["limits"].update(config["limits"])
        result = construct(repo, temporary / "alopex-native-private", repo / "native-export",
                           inputs, repo / ".github/ops/native-evidence-cert.pem", operation(repo, config))
        print(json.dumps({"status": "passed" if result == 0 else "failed", "error_code": None if result == 0 else "CONSTRUCTION"}))
        return result
    except BaseException:
        print('{"status":"failed","error_code":"CONSTRUCTION"}')
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
