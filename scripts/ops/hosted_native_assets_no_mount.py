"""Build a source-bound server and reuse the audited wheel without a mount."""
from __future__ import annotations

import base64
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import platform
import re
import shutil
import signal
import subprocess
import sys
import tarfile
import time
import tomllib
import urllib.request

from reused_native_wheel import fetch_reuse, preserve_receipt, verify_reuse, verify_composed_receipts


def validate_storage_boundary(scratch, roots, planned_bytes, reserve_bytes):
    counted = [path.resolve() for path in roots]
    if (not isinstance(planned_bytes, dict) or
            set(planned_bytes) != {"prepare", "tools", "server", "collect"} or
            any(type(value) is not int or value <= 0 for value in planned_bytes.values()) or
            type(reserve_bytes) is not int or reserve_bytes <= 0 or
            sum(planned_bytes.values()) + reserve_bytes >= 40_000_000_000 or
            not any(scratch.resolve().is_relative_to(path) for path in counted) or
            scratch.is_symlink()):
        raise ValueError("invalid build storage accounting boundary")


class StableAllocatedMeasure:
    """Retry a transient disappearance; never count a missing path as zero."""

    def __init__(self, measure):
        self.measure = measure
        self.samples = 0
        self.retries = 0
        self.paused_checks = 0
        self.maximum = 0

    def __call__(self, roots):
        for attempt in range(3):
            try:
                value = self.measure(roots)
                self.samples += 1
                self.maximum = max(self.maximum, value)
                return value
            except FileNotFoundError:
                self.retries += 1
                if attempt == 2:
                    raise
                time.sleep(0.05)

    def summary(self):
        return {"method": "allocated-byte inventory while owned build group is paused",
                "samples": self.samples, "transient_retries": self.retries,
                "paused_checks": self.paused_checks,
                "maximum_allocated_bytes": self.maximum}


def run_counted_command(args, *, budget, meter, seconds, log, env, poll_seconds=2):
    """Pause only the owned command group for a stable storage snapshot."""
    from common import Failure, require

    budget.check()
    with Path(log).open("ab") as output:
        os.chmod(log, 0o600)
        child = subprocess.Popen(args, stdout=output, stderr=output,
                                 env=env, start_new_session=True)
        deadline = time.monotonic() + seconds
        try:
            while child.poll() is None:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise Failure("TIMEOUT")
                try:
                    child.wait(timeout=min(remaining, poll_seconds))
                except subprocess.TimeoutExpired:
                    if child.poll() is not None:
                        continue
                    paused = False
                    try:
                        os.killpg(child.pid, signal.SIGSTOP)
                        paused = True
                        time.sleep(0.05)
                        budget.check()
                        meter.paused_checks += 1
                    except ProcessLookupError:
                        budget.check()
                    finally:
                        if paused:
                            try:
                                os.killpg(child.pid, signal.SIGCONT)
                            except ProcessLookupError:
                                pass
            budget.check()
            require(child.returncode == 0, "COMMAND")
        except BaseException:
            if child.poll() is None:
                try:
                    os.killpg(child.pid, signal.SIGCONT)
                except ProcessLookupError:
                    pass
                os.killpg(child.pid, signal.SIGTERM)
                try:
                    child.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    os.killpg(child.pid, signal.SIGKILL)
                    child.wait(timeout=5)
            raise


def extract_regular(archive, destination, maximum):
    with tarfile.open(archive) as source:
        members = source.getmembers()
        if len(members) > 16 or sum(m.size for m in members) > maximum:
            raise ValueError("parser archive bound")
        names = set()
        for member in members:
            name = PurePosixPath(member.name)
            if name.is_absolute() or ".." in name.parts or member.name in names:
                raise ValueError("parser archive name")
            names.add(member.name)
            if not member.isfile() and not member.isdir():
                raise ValueError("parser archive type")
            target = destination.joinpath(*name.parts)
            if member.isdir():
                target.mkdir(mode=0o700, parents=True, exist_ok=True)
            else:
                target.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
                with target.open("xb") as output:
                    shutil.copyfileobj(source.extractfile(member), output)


def stage_parser_sidecars(parser_root, record, expected_contract):
    """Adapt a verified release archive to the Rust resolver's local layout."""
    if record["contract_version"] + "\n" != expected_contract:
        raise ValueError("parser source contract mismatch")
    libraries = [parser_root / record[kind]["path"] for kind in ("library", "static_library")]
    if libraries[0].parent != libraries[1].parent:
        raise ValueError("parser library directory mismatch")
    checksums = []
    for kind, path in zip(("library", "static_library"), libraries):
        item = record[kind]
        if (not path.is_file() or path.is_symlink() or path.stat().st_size != item["size"] or
                hashlib.sha256(path.read_bytes()).hexdigest() != item["sha256"]):
            raise ValueError("parser library identity mismatch")
        checksums.append(item["sha256"] + "  " + path.name + "\n")
    directory = libraries[0].parent
    (directory / "CONTRACT_VERSION").write_text(expected_contract)
    (directory / "SHA256SUMS").write_text("".join(checksums))


def validate_published_wheel(record, version, source_commit):
    name = record["filename"]
    tag = "alopex-py-v" + version
    if (record["version"] != version or record["source_commit"] != source_commit or
            record["release_tag"] != tag or Path(name).name != name or
            not name.startswith("alopex-" + version + "-") or not name.endswith(".whl") or
            record["url"] != "https://github.com/alopex-db/alopex/releases/download/" + tag + "/" + name or
            not re.fullmatch(r"[a-f0-9]{64}", record["sha256"]) or
            record["pypi_sha256"] != record["sha256"] or
            record["release_asset_digest"] != "sha256:" + record["sha256"] or
            type(record["size"]) is not int or not 0 < record["size"] < 100_000_000):
        raise ValueError("unqualified published wheel identity")


def main():
    if os.environ.get("RUNNER_KIND") != "github-hosted" or os.geteuid() == 0:
        print('{"status":"failed","error_code":"VM_SCOPE"}')
        return 78
    os.umask(0o077)
    repo = Path(os.environ["GITHUB_WORKSPACE"]).resolve()
    sys.path.insert(0, str(repo / "scripts/test-env"))
    from capacity import Budget, allocated_bytes
    from common import atomic_json, digest, private_dir, require, verify_assets
    work = private_dir(repo / "native-build-private")
    export = private_dir(repo / "native-assets-export")
    status = "failed"
    config = json.loads(base64.b64decode(os.environ["PUBLIC_BUILD_CONFIG"], validate=True))
    scratch = Path(os.environ["RUNNER_TEMP"]) / "alopex-native-server-build"
    require(not scratch.exists(), "STATE")
    # Count ordinary runner workspace and temporary storage, including compiler output.
    roots = [repo.parent, Path(os.environ["RUNNER_TEMP"])]
    for path in (repo.parent.parent / "_actions", Path.home() / ".cargo", Path.home() / ".cache"):
        if path.exists(): roots.append(path)
    planned = config["planned_bytes"]
    validate_storage_boundary(scratch, roots, planned, config["reserve_bytes"])
    meter = StableAllocatedMeasure(allocated_bytes)
    budget = Budget({"roots": list(map(str, roots)), "stop_bytes": 40_000_000_000,
                     "reserve_bytes": config["reserve_bytes"], "poll_seconds": 2,
                     "planned_bytes": planned}, measure=meter)
    source = scratch / "source"
    target = scratch / "target"
    env = dict(os.environ, CARGO_HOME=str(scratch / "cargo"), RUSTUP_HOME=str(scratch / "rustup"),
               RUSTUP_TOOLCHAIN=config["rust_version"], CARGO_TARGET_DIR=str(target),
               CARGO_INCREMENTAL="0", CARGO_PROFILE_DEV_DEBUG="0", CARGO_PROFILE_TEST_DEBUG="0",
               CARGO_PROFILE_RELEASE_DEBUG="0", CARGO_BUILD_JOBS="2", PIP_NO_CACHE_DIR="1",
               PYTHONDONTWRITEBYTECODE="1", TMPDIR=str(scratch / "tmp"))
    env.pop("GH_TOKEN", None)
    env.pop("GITHUB_TOKEN", None)
    env["RUSTFLAGS"] = ("--remap-path-prefix=" + str(repo) + "=/alopex-build/control " +
                        "--remap-path-prefix=" + str(scratch) + "=/alopex-build/work")
    env.pop("CARGO_ENCODED_RUSTFLAGS", None)
    def command(args, phase, name, seconds=120):
        budget.before(phase)
        run_counted_command(args, budget=budget, meter=meter, seconds=seconds,
                            log=work / (name + ".log"), env=env)
    def fetch(url, output, expected, limit):
        require(any(url.startswith(prefix) for prefix in (
            "https://github.com/alopex-db/alopex/releases/download/v0.8.16/",
            "https://github.com/alopex-db/alopex/releases/download/alopex-py-v0.8.16/")), "POLICY")
        budget.before("prepare")
        received = 0
        with urllib.request.urlopen(url, timeout=30) as response, output.open("xb") as stream:
            while chunk := response.read(65536):
                received += len(chunk)
                require(received <= limit, "CAPACITY")
                budget.check()
                stream.write(chunk)
        require(digest(output) == expected, "IDENTITY")
    try:
        require(platform.machine() == config["expected_machine"], "IDENTITY")
        require(re.fullmatch(r"[a-f0-9]{40}", config["source_commit"]) and
                re.fullmatch(r"[a-f0-9]{40}", config["harness_commit"]), "INPUT")
        initial = budget.used()
        required = sum(planned.values()) + config["reserve_bytes"]
        require(initial + required < 40_000_000_000 and
                shutil.disk_usage(scratch.parent).free > required, "CAPACITY")
        atomic_json(work / "storage-before.json", {"allocated_bytes": initial, "roots": list(map(str, roots))})
        for tool in ("git", "rustup", "cargo", "cc", "c++", "cmake", "pkg-config", "openssl"):
            require(shutil.which(tool) is not None, "PREREQUISITE")
        private_dir(scratch)
        require(not scratch.is_symlink() and scratch.stat().st_uid == os.getuid() and
                scratch.stat().st_mode & 0o777 == 0o700 and
                not os.path.ismount(scratch), "POLICY")
        atomic_json(work / "filesystem-verification.json",
                    {"scratch_owner_is_runner": True, "scratch_mode": "0700", "mount_changed": False})
        for path in (source, target, scratch / "tmp", scratch / "cargo", scratch / "rustup"):
            private_dir(path)
        command(["git", "-C", str(source), "init", "--quiet"], "prepare", "source-init")
        command(["git", "-C", str(source), "fetch", "--quiet", "--depth=1",
                 "https://github.com/alopex-db/alopex.git", config["source_commit"]], "prepare", "source-fetch")
        command(["git", "-C", str(source), "checkout", "--quiet", "--detach", "FETCH_HEAD"], "prepare", "source-checkout")
        def git_value(*args):
            return subprocess.run(["git", "-C", str(source), *args], capture_output=True, text=True, check=True).stdout.strip()
        require(git_value("rev-parse", "HEAD") == config["source_commit"], "IDENTITY")
        identity = {"commit": config["source_commit"], "tree": git_value("rev-parse", "HEAD^{tree}"), "profile": "release"}
        version = tomllib.loads((source / "Cargo.toml").read_text())["workspace"]["package"]["version"]
        require(identity["tree"] == config["source_tree"], "IDENTITY")
        reuse_config = config["reuse_wheel"]
        require(config.get("published_wheel") is None, "INPUT")
        published_wheel = None
        if published_wheel is not None:
            validate_published_wheel(published_wheel, version, config["source_commit"])
        record_file = work / "parser-record.json"
        parser_archive = work / "parser.tar.gz"
        fetch(config["parser_record_url"], record_file, config["parser_record_sha256"], 65536)
        record = json.loads(record_file.read_text())
        fetch(config["parser_archive_url"], parser_archive, record["archive"]["sha256"], record["archive"]["size"])
        parser_root = private_dir(scratch / "parser")
        extract_regular(parser_archive, parser_root, record["packing"]["decompressed_size_limit"])
        for kind in ("library", "static_library", "build_identity"):
            item = record[kind]; path = parser_root / item["path"]
            require(path.stat().st_size == item["size"] and digest(path) == item["sha256"], "IDENTITY")
        library = parser_root / record["library"]["path"]
        command([sys.executable, "-c", "import ctypes,sys; lib=ctypes.CDLL(sys.argv[1]); lib.alopex_parser_init(); lib.alopex_parser_version.restype=ctypes.c_char_p; assert lib.alopex_parser_version().decode()==sys.argv[2]", str(library), record["contract_version"]], "prepare", "parser-contract")
        stage_parser_sidecars(parser_root, record,
                              (source / "crates/alopex-sql/nim-sql-parser/PARSER_CONTRACT_VERSION").read_text())
        env.update(NIM_SQL_PARSER_LIB_DIR=str(library.parent), ALOPEX_NIM_PARSER_ALLOW_LOCAL_BUILD="1")
        native = private_dir(source / "crates/alopex-py/python/alopex/native")
        shutil.copyfile(library, native / library.name)
        (native / "CONTRACT_VERSION").write_text(record["contract_version"] + "\n")
        (native / "SHA256SUMS").write_text(digest(library) + "  " + library.name + "\n")
        reuse_root = work / "reused-wheel-input"
        fetch_reuse(reuse_root, reuse_config, budget)
        reused_paths, original_wheel_receipt = verify_reuse(reuse_root, reuse_config, config,
            version=version, cargo_lock_sha256=digest(source / "Cargo.lock"),
            parser_sha256=digest(library), parser_contract=record["contract_version"])
        command(["rustup", "toolchain", "install", config["rust_version"], "--profile", "minimal"], "tools", "rust-install", 600)
        command(["rustc", "-Vv"], "tools", "rust-version")
        rust_version = (work / "rust-version.log").read_text()
        require("release: " + config["rust_version"] + "\n" in rust_version and
                "host: " + record["target"] + "\n" in rust_version, "IDENTITY")
        command([sys.executable, "-m", "venv", str(scratch / "venv")], "tools", "venv")
        python = scratch / "venv/bin/python"
        os.chdir(source)
        command(["cargo", "build", "--locked", "--release", "-p", "alopex-server", "--bin", "alopex-server"],
                "server", "server-build", config["build_seconds"])
        assets = private_dir(export / "assets")
        server = private_dir(assets / "bin") / "alopex-server"
        shutil.copyfile(target / "release/alopex-server", server); server.chmod(0o700)
        wheel_directory = private_dir(assets / "wheels")
        shutil.copyfile(reused_paths["wheel"], wheel_directory / reused_paths["wheel"].name)
        original_receipt = assets / "receipts/original-wheel-build.json"
        original_receipt_hash = preserve_receipt(reused_paths["receipt"], original_receipt)
        wheels = list((assets / "wheels").glob("*.whl")); require(len(wheels) == 1, "IDENTITY")
        wheel = wheels[0]
        command([str(python), "-m", "pip", "install", "--no-index", "--no-deps", str(wheel)], "collect", "wheel-install")
        command([str(python), "-c", "import alopex, importlib.metadata; assert importlib.metadata.version('alopex') == " + repr(version)],
                "collect", "wheel-import")
        command([str(server), "--version"], "collect", "server-version")
        docs = private_dir(scratch / "docs")
        command(["git", "-C", str(docs), "init", "--quiet"], "collect", "docs-init")
        command(["git", "-C", str(docs), "fetch", "--quiet", "--depth=1", "https://github.com/alopex-db/docs.git", config["harness_commit"]], "collect", "docs-fetch")
        harness = assets / "harness.tar"
        command(["git", "-C", str(docs), "archive", "--format=tar", "--output", str(harness), "FETCH_HEAD", "scripts/performance"], "collect", "harness-export")
        lib_out = private_dir(assets / "lib") / library.name; shutil.copyfile(library, lib_out)
        receipt = {"schema": "alopex.performance-build/v1", "version": version, "source": identity,
                   "server_sha256": digest(server), "python_wheel_sha256": digest(wheel), "parser_sha256": digest(lib_out),
                   "parser_contract": record["contract_version"], "parser_record_sha256": digest(record_file),
                   "cargo_lock_sha256": digest(source / "Cargo.lock"),
                   "tools": {"rust": config["rust_version"], "maturin": config["maturin_version"] if published_wheel is None else None},
                   "build": {"incremental": False, "release_debug": False,
                             "path_remap": {"control": "/alopex-build/control", "work": "/alopex-build/work"},
                             "server": ["cargo", "build", "--locked", "--release", "-p", "alopex-server", "--bin", "alopex-server"],
                             "wheel": {"mode": "reused_development_asset",
                                       "producer_run": reuse_config["run"], "producer_controller": reuse_config["controller"],
                                       "artifact_id": reuse_config["artifact_id"], "artifact_digest": reuse_config["artifact_digest"],
                                       "receipt_path": str(original_receipt.relative_to(assets)),
                                       "receipt_sha256": original_receipt_hash}},
                   "qualification": "development comparison asset; not RC qualification"}
        receipt_file = private_dir(assets / "receipts") / "build.json"; atomic_json(receipt_file, receipt)
        files = [{"role": role, "path": str(path.relative_to(assets)), "size": path.stat().st_size, "sha256": digest(path)}
                 for role, path in (("server", server), ("wheel", wheel), ("library", lib_out), ("receipt", receipt_file), ("wheel_receipt", original_receipt), ("harness", harness))]
        manifest = {"schema": "alopex.test-env-assets/v1", "platform": config["platform"], "source_commit": identity["commit"],
                    "harness_commit": config["harness_commit"], "expanded_limit_bytes": config["asset_expanded_bytes"], "files": files}
        atomic_json(assets / "assets.json", manifest); verify_assets(assets / "assets.json", config["platform"])
        verify_composed_receipts(assets, manifest)
        public = {"schema": "alopex.native-assets/v1", "status": "passed", "source": identity,
                  "assets_digest": digest(assets / "assets.json"), "performance_qualification": False}
        status = "passed"
    except BaseException as error:
        atomic_json(work / "failure.json", {"error": repr(error), "detail": getattr(error, "detail", None)})
    finally:
        os.chdir(repo)
        cleanup_error = None
        try:
            require(not scratch.is_symlink() and not os.path.ismount(scratch), "POLICY")
            if scratch.exists(): shutil.rmtree(scratch)
            reuse_input = work / "reused-wheel-input"
            if reuse_input.exists(): shutil.rmtree(reuse_input)
        except BaseException as error:
            cleanup_error = repr(error)
            status = "failed"
        atomic_json(work / "filesystem-cleanup.json",
                    {"scratch_removed": not scratch.exists(), "mount_changed": False,
                     "error": cleanup_error})
        atomic_json(work / "storage-meter.json", meter.summary())
        atomic_json(work / "storage-after.json", {"allocated_bytes": allocated_bytes(roots)})
        archive = work / "evidence.tar.gz"
        budget.before("collect")
        with tarfile.open(archive, "w:gz") as output:
            for path in sorted(work.iterdir()):
                if path.is_file() and path.suffix in (".json", ".log"):
                    budget.check(); output.add(path, arcname=path.name)
        command(["openssl", "cms", "-encrypt", "-aes-256-cbc", "-binary", "-outform", "DER", "-in", str(archive),
                 "-out", str(export / "private-evidence.cms"), str(repo / ".github/ops/native-evidence-cert.pem")], "collect", "encrypt-evidence")
        if status != "passed":
            atomic_json(export / "result.json", {"schema": "alopex.native-assets/v1", "status": "failed"})
        else:
            atomic_json(export / "result.json", public)
    print(json.dumps({"status": status, "error_code": None if status == "passed" else "BUILD"}))
    return 0 if status == "passed" else 1


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except SystemExit:
        raise
    except BaseException:
        print('{"status":"failed","error_code":"BUILD"}')
        raise SystemExit(1)
