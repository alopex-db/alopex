"""Build source-bound comparison assets from public inputs on a managed VM."""
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
        require(re.fullmatch(r"[a-f0-9]{40}", config["source_commit"]) and
                re.fullmatch(r"[a-f0-9]{40}", config["harness_commit"]), "INPUT")
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
        published_wheel = config.get("published_wheel")
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
        command(["rustup", "toolchain", "install", config["rust_version"], "--profile", "minimal"], "tools", "rust-install", 600)
        command(["rustc", "-Vv"], "tools", "rust-version")
        rust_version = (work / "rust-version.log").read_text()
        require("release: " + config["rust_version"] + "\n" in rust_version and
                "host: " + record["target"] + "\n" in rust_version, "IDENTITY")
        command([sys.executable, "-m", "venv", str(scratch / "venv")], "tools", "venv")
        python = scratch / "venv/bin/python"
        if published_wheel is None:
            command([str(python), "-m", "pip", "install", "--only-binary=:all:", "--report", str(work / "tool-install.json"),
                     "maturin==" + config["maturin_version"]], "tools", "maturin-install", 300)
        os.chdir(source)
        command(["cargo", "build", "--locked", "--release", "-p", "alopex-server", "--bin", "alopex-server"],
                "server", "server-build", config["build_seconds"])
        assets = private_dir(export / "assets")
        server = private_dir(assets / "bin") / "alopex-server"
        shutil.copyfile(target / "release/alopex-server", server); server.chmod(0o700)
        wheel_directory = private_dir(assets / "wheels")
        if published_wheel is None:
            command([str(scratch / "venv/bin/maturin"), "build", "--locked", "--release", "--compatibility", "linux",
                     "--manifest-path", "crates/alopex-py/Cargo.toml", "--interpreter", str(python),
                     "--out", str(wheel_directory)], "wheel", "wheel-build", config["build_seconds"])
        else:
            fetch(published_wheel["url"], wheel_directory / published_wheel["filename"],
                  published_wheel["sha256"], published_wheel["size"])
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
                             "wheel": "maturin build --locked --release --compatibility linux" if published_wheel is None else
                             {"mode": "published_distribution", **published_wheel}},
                   "qualification": "development comparison asset; not RC qualification"}
        receipt_file = private_dir(assets / "receipts") / "build.json"; atomic_json(receipt_file, receipt)
        files = [{"role": role, "path": str(path.relative_to(assets)), "size": path.stat().st_size, "sha256": digest(path)}
                 for role, path in (("server", server), ("wheel", wheel), ("library", lib_out), ("receipt", receipt_file), ("harness", harness))]
        manifest = {"schema": "alopex.test-env-assets/v1", "platform": config["platform"], "source_commit": identity["commit"],
                    "harness_commit": config["harness_commit"], "expanded_limit_bytes": config["asset_expanded_bytes"], "files": files}
        atomic_json(assets / "assets.json", manifest); verify_assets(assets / "assets.json", config["platform"])
        public = {"schema": "alopex.native-assets/v1", "status": "passed", "source": identity,
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
