"""Build a source-bound comparison wheel in ordinary runner storage."""
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


def validate_storage_boundary(scratch, roots, planned_bytes, reserve_bytes):
    counted = [path.resolve() for path in roots]
    if (not isinstance(planned_bytes, dict) or
            set(planned_bytes) != {"prepare", "tools", "wheel", "collect"} or
            any(type(value) is not int or value <= 0 for value in planned_bytes.values()) or
            type(reserve_bytes) is not int or reserve_bytes <= 0 or
            sum(planned_bytes.values()) + reserve_bytes >= 40_000_000_000 or
            not any(scratch.resolve().is_relative_to(path) for path in counted) or
            scratch.is_symlink()):
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



def verify_wheel_assets(root, manifest, config):
    def check(value):
        if not value:
            raise ValueError('wheel asset identity differs')
    check(manifest['schema'] == 'alopex.native-wheel-assets/v1'
          and manifest['platform'] == config['platform']
          and manifest['source_commit'] == config['source_commit']
          and manifest['source_tree'] == config['source_tree'])
    files = manifest['files']
    check(len(files) == 3 and {f['role'] for f in files} == {'wheel', 'library', 'receipt'})
    check(len({f['path'] for f in files}) == 3)
    check(sum(f['size'] for f in files) <= 100_000_000)
    by_role = {}
    for item in files:
        relative = PurePosixPath(item['path'])
        check(not relative.is_absolute() and '..' not in relative.parts and '\\' not in item['path'])
        path = root.joinpath(*relative.parts)
        check(path.resolve().is_relative_to(root.resolve()) and path.is_file() and not path.is_symlink())
        check(path.stat().st_size == item['size'] and hashlib.sha256(path.read_bytes()).hexdigest() == item['sha256'])
        by_role[item['role']] = path
    receipt = json.loads(by_role['receipt'].read_bytes())
    check(receipt['schema'] == 'alopex.performance-wheel-build/v1'
          and receipt['source'] == dict(commit=config['source_commit'], tree=config['source_tree'], profile='release')
          and receipt['tools'] == dict(rust=config['rust_version'], maturin=config['maturin_version']))
    for role, field in (('wheel','python_wheel_sha256'), ('library','parser_sha256')):
        check(receipt[field] == hashlib.sha256(by_role[role].read_bytes()).hexdigest())


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
    scratch = Path(os.environ["RUNNER_TEMP"]) / "alopex-native-build"
    require(not scratch.exists(), "STATE")
    # Count the ordinary runner workspace and temporary storage, including
    # compiler output, without creating a mount or changing isolation settings.
    roots = [repo.parent, Path(os.environ["RUNNER_TEMP"])]
    for path in (repo.parent.parent / "_actions", Path.home() / ".cargo", Path.home() / ".cache"):
        if path.exists(): roots.append(path)
    planned = config["planned_bytes"]
    validate_storage_boundary(scratch, roots, planned, config["reserve_bytes"])
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
                re.fullmatch(r"[a-f0-9]{40}", config["source_tree"]), "INPUT")
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
                    {"scratch_owner_is_runner": True, "scratch_mode": "0700",
                     "mount_changed": False})
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
        command([str(python), "-m", "pip", "install", "--only-binary=:all:", "--report", str(work / "tool-install.json"),
                 "maturin==" + config["maturin_version"]], "tools", "maturin-install", 300)
        os.chdir(source)
        assets = private_dir(export / "assets")
        wheel_directory = private_dir(assets / "wheels")
        command([str(scratch / "venv/bin/maturin"), "build", "--locked", "--release", "--compatibility", "linux",
                 "--manifest-path", "crates/alopex-py/Cargo.toml", "--interpreter", str(python),
                 "--out", str(wheel_directory)], "wheel", "wheel-build", config["build_seconds"])
        wheels = list((assets / "wheels").glob("*.whl")); require(len(wheels) == 1, "IDENTITY")
        wheel = wheels[0]
        command([str(python), "-m", "pip", "install", "--no-index", "--no-deps", str(wheel)], "collect", "wheel-install")
        command([str(python), "-c", "import alopex, importlib.metadata; assert importlib.metadata.version('alopex') == " + repr(version)],
                "collect", "wheel-import")
        require(git_value("diff", "--name-only") == "", "IDENTITY")
        lib_out = private_dir(assets / "lib") / library.name; shutil.copyfile(library, lib_out)
        receipt = {"schema": "alopex.performance-wheel-build/v1", "version": version, "source": identity,
                   "python_wheel_sha256": digest(wheel), "parser_sha256": digest(lib_out),
                   "parser_contract": record["contract_version"], "parser_record_sha256": digest(record_file),
                   "cargo_lock_sha256": digest(source / "Cargo.lock"),
                   "tools": {"rust": config["rust_version"], "maturin": config["maturin_version"]},
                   "python_version": platform.python_version(),
                   "build": {"incremental": False, "release_debug": False,
                             "path_remap": {"control": "/alopex-build/control", "work": "/alopex-build/work"},
                             "wheel": "maturin build --locked --release --compatibility linux"},
                   "qualification": "development comparison asset; not RC qualification"}
        receipt_file = private_dir(assets / "receipts") / "build.json"; atomic_json(receipt_file, receipt)
        files = [{"role": role, "path": str(path.relative_to(assets)), "size": path.stat().st_size, "sha256": digest(path)}
                 for role, path in (("wheel", wheel), ("library", lib_out), ("receipt", receipt_file))]
        manifest = {"schema": "alopex.native-wheel-assets/v1", "platform": config["platform"], "source_commit": identity["commit"],
                    "source_tree": identity["tree"], "files": files}
        atomic_json(assets / "assets.json", manifest); verify_wheel_assets(assets, manifest, config)
        public = {"schema": "alopex.native-wheel-assets-result/v1", "status": "passed", "source": identity,
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
        except BaseException as error:
            cleanup_error = repr(error)
            status = "failed"
        atomic_json(work / "filesystem-cleanup.json",
                    {"scratch_removed": not scratch.exists(), "mount_changed": False,
                     "error": cleanup_error})
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
            atomic_json(export / "result.json", {"schema": "alopex.native-wheel-assets-result/v1", "status": "failed"})
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
