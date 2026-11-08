"""Build only the source-bound manual HNSW example on the existing managed VM boundary."""
from __future__ import annotations

import base64
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import platform
import re
import shutil
import subprocess
import sys
import tarfile
import tomllib
import urllib.request


def validate_storage_boundary(scratch, disk, roots, disk_bytes):
    counted = [path.resolve() for path in roots]
    if (type(disk_bytes) is not int or not 0 < disk_bytes < 40_000_000_000 or
            any(scratch.resolve().is_relative_to(path) for path in counted) or
            not any(disk.resolve().is_relative_to(path) for path in counted)):
        raise ValueError("invalid build storage accounting boundary")


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



def main():
    if os.environ.get("RUNNER_KIND") != "github-hosted" or os.geteuid() == 0:
        print('{"status":"failed","error_code":"VM_SCOPE"}')
        return 78
    os.umask(0o077)
    repo = Path(os.environ["GITHUB_WORKSPACE"]).resolve()
    sys.path.insert(0, str(repo / "scripts/test-env"))
    from capacity import Budget, allocated_bytes
    from common import atomic_json, digest, private_dir, require
    work = private_dir(repo / "native-build-private")
    export = private_dir(repo / "native-assets-export")
    status = "failed"
    config = json.loads(base64.b64decode(os.environ["PUBLIC_BUILD_CONFIG"], validate=True))
    scratch = Path(os.environ["RUNNER_TEMP"]) / "alopex-native-build-mount"
    disk = work / "build.ext4"
    require(not scratch.exists() and not disk.exists(), "STATE")
    # Mutable compiler/cache files live in the dedicated filesystem. Count its
    # backing allocation once, without racing rustc's temporary-file removal.
    roots = [repo.parent]
    for path in (repo.parent.parent / "_actions", Path.home() / ".cargo", Path.home() / ".cache"):
        if path.exists(): roots.append(path)
    validate_storage_boundary(scratch, disk, roots, config["disk_bytes"])
    planned = dict(config["planned_bytes"], bootstrap=config["disk_bytes"])
    budget = Budget({"roots": list(map(str, roots)), "stop_bytes": 40_000_000_000,
                     "reserve_bytes": config["reserve_bytes"], "poll_seconds": 2,
                     "planned_bytes": planned})
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
        budget.run(args, timeout=seconds, log=work / (name + ".log"), env=env)
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
        require(re.fullmatch(r"[a-f0-9]{40}", config["source_commit"]), "INPUT")
        initial = budget.used()
        require(initial + config["disk_bytes"] + planned["prepare"] + planned["collect"] + config["reserve_bytes"] < 40_000_000_000, "CAPACITY")
        atomic_json(work / "storage-before.json", {"allocated_bytes": initial, "roots": list(map(str, roots))})
        for tool in ("git", "rustup", "cargo", "cc", "c++", "cmake", "pkg-config", "openssl", "mkfs.ext4", "mount", "umount", "findmnt", "sudo"):
            require(shutil.which(tool) is not None, "PREREQUISITE")
        budget.before("bootstrap")
        private_dir(scratch)
        with disk.open("xb") as output:
            output.truncate(config["disk_bytes"])
        command(["mkfs.ext4", "-q", "-F", "-m", "0", "-E", "lazy_itable_init=0,lazy_journal_init=0", str(disk)], "bootstrap", "filesystem-format")
        command(["sudo", "-n", "mount", "-o", "loop,nodev,nosuid", str(disk), str(scratch)], "bootstrap", "filesystem-mount")
        require(os.path.ismount(scratch), "IDENTITY")
        command(["sudo", "-n", "chown", str(os.getuid()) + ":" + str(os.getgid()), str(scratch)], "prepare", "filesystem-owner")
        scratch.chmod(0o700)
        mounted = subprocess.run(["findmnt", "--json", "--mountpoint", str(scratch), "--output", "TARGET,FSTYPE,OPTIONS"],
                                 capture_output=True, text=True, check=True, timeout=10)
        mount_info = json.loads(mounted.stdout)["filesystems"]
        require(len(mount_info) == 1 and mount_info[0]["target"] == str(scratch) and
                mount_info[0]["fstype"] == "ext4" and
                {"nodev", "nosuid"}.issubset(mount_info[0]["options"].split(",")) and
                "noexec" not in mount_info[0]["options"].split(",") and
                scratch.stat().st_dev != disk.stat().st_dev and disk.stat().st_size == config["disk_bytes"], "POLICY")
        atomic_json(work / "filesystem-verification.json", mount_info)
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
        example_path = source / "crates/alopex-embedded/examples/issue681_hnsw_first_write.rs"
        require(digest(example_path) == config["example_sha256"] and git_value("diff", "--name-only") == "", "IDENTITY")
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
        command(["rustup", "toolchain", "install", config["rust_version"], "--profile", "minimal"], "tools", "rust-install", 600)
        command(["rustc", "-Vv"], "tools", "rust-version")
        rust_version = (work / "rust-version.log").read_text()
        require("release: " + config["rust_version"] + "\n" in rust_version and
                "host: " + record["target"] + "\n" in rust_version, "IDENTITY")
        os.chdir(source)
        build_command = ["cargo", "build", "--locked", "--release", "-p", "alopex-embedded",
                         "--example", "issue681_hnsw_first_write"]
        command(build_command, "example", "example-build", config["build_seconds"])
        require(git_value("diff", "--name-only") == "" and digest(example_path) == config["example_sha256"], "IDENTITY")
        assets = private_dir(export / "assets")
        binary = private_dir(assets / "bin") / "issue681_hnsw_first_write"
        shutil.copyfile(target / "release/examples/issue681_hnsw_first_write", binary)
        binary.chmod(0o700)
        lib_out = private_dir(assets / "lib") / library.name
        shutil.copyfile(library, lib_out)
        example_out = assets / "example.rs"
        shutil.copyfile(example_path, example_out)
        receipt = {"schema": "alopex.hnsw-example-build/v1", "source": identity,
                   "version": version, "binary_sha256": digest(binary),
                   "example_source_sha256": digest(example_out), "parser_sha256": digest(lib_out),
                   "parser_contract": record["contract_version"], "parser_record_sha256": digest(record_file),
                   "cargo_lock_sha256": digest(source / "Cargo.lock"), "rust_version": config["rust_version"],
                   "build": {"command": build_command, "incremental": False, "release_debug": False,
                             "path_remap": {"control": "/alopex-build/control", "work": "/alopex-build/work"}},
                   "smoke": None,
                   "qualification": "manual development comparison asset; not RC qualification"}
        receipt_file = private_dir(assets / "receipts") / "build.json"
        atomic_json(receipt_file, receipt)
        files = [{"role": role, "path": str(path.relative_to(assets)), "size": path.stat().st_size, "sha256": digest(path)}
                 for role, path in (("binary", binary), ("library", lib_out), ("example", example_out), ("receipt", receipt_file))]
        manifest = {"schema": "alopex.hnsw-example-assets/v1", "platform": config["platform"],
                    "source": identity, "expanded_limit_bytes": config["asset_expanded_bytes"], "files": files}
        require(sum(row["size"] for row in files) <= config["asset_expanded_bytes"], "CAPACITY")
        atomic_json(assets / "assets.json", manifest)
        public = {"schema": "alopex.hnsw-example-assets/v1", "status": "passed", "source": identity,
                  "assets_digest": digest(assets / "assets.json"), "performance_qualification": False}
        status = "passed"
    except BaseException as error:
        atomic_json(work / "failure.json", {"error": repr(error), "detail": getattr(error, "detail", None)})
    finally:
        os.chdir(repo)
        for path in (target, scratch / "cargo", scratch / "rustup", scratch / "venv", source, scratch / "docs", scratch / "tmp"):
            if path.exists(): shutil.rmtree(path)
        cleanup_error = None
        if os.path.ismount(scratch):
            with (work / "filesystem-unmount.log").open("ab") as output:
                try:
                    subprocess.run(["sudo", "-n", "umount", str(scratch)], stdout=output, stderr=output, check=True, timeout=30)
                except (OSError, subprocess.SubprocessError) as error:
                    cleanup_error = repr(error)
                    status = "failed"
        if not os.path.ismount(scratch):
            disk.unlink(missing_ok=True)
            if scratch.exists(): scratch.rmdir()
        atomic_json(work / "filesystem-cleanup.json", {"unmounted": not os.path.ismount(scratch), "error": cleanup_error})
        atomic_json(work / "storage-after.json", {"allocated_bytes": allocated_bytes(roots)})
        if status == "passed":
            try:
                smoke = work / "smoke"
                smoke_env = dict(env, LD_LIBRARY_PATH=str(lib_out.parent), TMPDIR=str(work))
                smoke_env.pop("CI", None)
                runner = repo / "scripts/performance/issue681_manual.py"
                budget.before("collect")
                budget.run([sys.executable, "-B", str(runner), str(binary), "--output", str(smoke),
                            "--rows", "2", "--dimensions", "4", "--timeout", "30",
                            "--rss-limit", "268435456", "--phase-kernel-rss"], timeout=60,
                           log=work / "example-smoke.log", env=smoke_env)
                smoke_report = json.loads((smoke / "summary.json").read_text())
                smoke_identity = json.loads((smoke / "identity.json").read_text())
                require(smoke_report["outcome"] == "passed" and smoke_report["kernel_rss_reset"] is True
                        and len(smoke_report["phase_kernel_rss"]) == 9
                        and smoke_identity["binary_sha256"] == digest(binary)
                        and smoke_identity["rows"] == 2 and smoke_identity["dimensions"] == 4, "CASE")
                atomic_json(work / "example-smoke-identity.json", smoke_identity)
                atomic_json(work / "example-smoke-summary.json", smoke_report)
                shutil.copyfile(smoke / "events.jsonl", work / "example-smoke-events.log")
                shutil.rmtree(smoke)
                receipt["smoke"] = {"rows": 2, "dimensions": 4, "phases": 9, "status": "passed",
                                    "runner_sha256": digest(runner),
                                    "events_sha256": digest(work / "example-smoke-events.log")}
                from hnsw_completion_smoke import verify as verify_completion
                receipt["completion_smoke"] = {}
                for mode in ("commit", "rollback"):
                    budget.before("collect")
                    budget.run([sys.executable, "-B", str(runner), str(binary), "--output", str(smoke),
                                "--rows", "2", "--dimensions", "4", "--timeout", "30",
                                "--rss-limit", "268435456", "--phase-kernel-rss", "--finish", mode],
                               timeout=60, log=work / (mode + "-smoke.log"), env=smoke_env)
                    mode_identity = json.loads((smoke / "identity.json").read_text())
                    mode_report = json.loads((smoke / "summary.json").read_text())
                    event_bytes = (smoke / "events.jsonl").read_bytes()
                    mode_events = [json.loads(line) for line in event_bytes.splitlines()]
                    checked = verify_completion(mode_identity, mode_report, mode_events,
                                                mode, digest(binary), digest(runner))
                    checked["events_sha256"] = hashlib.sha256(event_bytes).hexdigest()
                    receipt["completion_smoke"][mode] = checked
                    atomic_json(work / (mode + "-smoke-identity.json"), mode_identity)
                    atomic_json(work / (mode + "-smoke-summary.json"), mode_report)
                    (work / (mode + "-smoke-events.log")).write_bytes(event_bytes)
                    shutil.rmtree(smoke)
                atomic_json(receipt_file, receipt)
                for row in manifest["files"]:
                    path = assets / row["path"]
                    row.update(size=path.stat().st_size, sha256=digest(path))
                require(sum(row["size"] for row in manifest["files"]) <= config["asset_expanded_bytes"], "CAPACITY")
                atomic_json(assets / "assets.json", manifest)
                public["assets_digest"] = digest(assets / "assets.json")
            except BaseException as error:
                status = "failed"
                atomic_json(work / "smoke-failure.json", {"error": repr(error), "detail": getattr(error, "detail", None)})
        atomic_json(work / "storage-after-smoke.json", {"allocated_bytes": allocated_bytes(roots)})
        archive = work / "evidence.tar.gz"
        budget.before("collect")
        with tarfile.open(archive, "w:gz") as output:
            for path in sorted(work.iterdir()):
                if path.is_file() and path.suffix in (".json", ".log"):
                    budget.check(); output.add(path, arcname=path.name)
                elif path.is_dir() and path.name == "smoke":
                    children = list(path.rglob("*"))
                    require(not any(child.is_symlink() for child in children)
                            and sum(child.stat().st_size for child in children if child.is_file()) <= 16_000_000, "CAPACITY")
                    budget.check(); output.add(path, arcname=path.name)
        command(["openssl", "cms", "-encrypt", "-aes-256-cbc", "-binary", "-outform", "DER", "-in", str(archive),
                 "-out", str(export / "private-evidence.cms"), str(repo / ".github/ops/native-evidence-cert.pem")], "collect", "encrypt-evidence")
        if (work / "smoke").exists(): shutil.rmtree(work / "smoke")
        if status != "passed":
            atomic_json(export / "result.json", {"schema": "alopex.hnsw-example-assets/v1", "status": "failed"})
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
