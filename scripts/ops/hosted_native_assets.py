"""Build source-bound comparison assets from public inputs on a managed VM."""
from __future__ import annotations

import base64
import ctypes
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
    roots = [repo.parent, Path(os.environ["RUNNER_TEMP"])]
    for path in (repo.parent.parent / "_actions", Path.home() / ".cargo", Path.home() / ".cache"):
        if path.exists(): roots.append(path)
    budget = Budget({"roots": list(map(str, roots)), "stop_bytes": 40_000_000_000,
                     "reserve_bytes": config["reserve_bytes"], "poll_seconds": 2,
                     "planned_bytes": config["planned_bytes"]})
    source = work / "source"
    target = work / "target"
    env = dict(os.environ, CARGO_HOME=str(work / "cargo"), RUSTUP_HOME=str(work / "rustup"),
               RUSTUP_TOOLCHAIN=config["rust_version"], CARGO_TARGET_DIR=str(target),
               CARGO_INCREMENTAL="0", CARGO_PROFILE_DEV_DEBUG="0", CARGO_PROFILE_TEST_DEBUG="0",
               CARGO_PROFILE_RELEASE_DEBUG="0", CARGO_BUILD_JOBS="2", PIP_NO_CACHE_DIR="1",
               PYTHONDONTWRITEBYTECODE="1", TMPDIR=str(work / "tmp"))
    env.pop("GH_TOKEN", None)
    env.pop("GITHUB_TOKEN", None)
    env["RUSTFLAGS"] = "--remap-path-prefix=" + str(repo) + "=/alopex-build"
    env.pop("CARGO_ENCODED_RUSTFLAGS", None)
    def command(args, phase, name, seconds=120):
        budget.before(phase)
        budget.run(args, timeout=seconds, log=work / (name + ".log"), env=env)
    def fetch(url, output, expected, limit):
        require(url.startswith("https://github.com/alopex-db/alopex/releases/download/v0.8.16/"), "POLICY")
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
        require(initial + sum(config["planned_bytes"].values()) + config["reserve_bytes"] < 40_000_000_000, "CAPACITY")
        atomic_json(work / "storage-before.json", {"allocated_bytes": initial, "roots": list(map(str, roots))})
        for tool in ("git", "rustup", "cargo", "cc", "c++", "cmake", "pkg-config", "openssl"):
            require(shutil.which(tool) is not None, "PREREQUISITE")
        for path in (source, target, work / "tmp", work / "cargo", work / "rustup"):
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
        record_file = work / "parser-record.json"
        parser_archive = work / "parser.tar.gz"
        fetch(config["parser_record_url"], record_file, config["parser_record_sha256"], 65536)
        record = json.loads(record_file.read_text())
        fetch(config["parser_archive_url"], parser_archive, record["archive"]["sha256"], record["archive"]["size"])
        parser_root = private_dir(work / "parser")
        extract_regular(parser_archive, parser_root, record["packing"]["decompressed_size_limit"])
        for kind in ("library", "static_library", "build_identity"):
            item = record[kind]; path = parser_root / item["path"]
            require(path.stat().st_size == item["size"] and digest(path) == item["sha256"], "IDENTITY")
        library = parser_root / record["library"]["path"]
        lib = ctypes.CDLL(str(library)); lib.alopex_parser_init()
        lib.alopex_parser_version.restype = ctypes.c_char_p
        require(lib.alopex_parser_version().decode() == record["contract_version"], "IDENTITY")
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
        command([sys.executable, "-m", "venv", str(work / "venv")], "tools", "venv")
        python = work / "venv/bin/python"
        command([str(python), "-m", "pip", "install", "--only-binary=:all:", "--report", str(work / "tool-install.json"),
                 "maturin==" + config["maturin_version"]], "tools", "maturin-install", 300)
        os.chdir(source)
        command(["cargo", "build", "--locked", "--release", "-p", "alopex-server", "--bin", "alopex-server"],
                "server", "server-build", config["build_seconds"])
        assets = private_dir(export / "assets")
        server = private_dir(assets / "bin") / "alopex-server"
        shutil.copyfile(target / "release/alopex-server", server); server.chmod(0o700)
        command([str(work / "venv/bin/maturin"), "build", "--locked", "--release", "--compatibility", "linux",
                 "--manifest-path", "crates/alopex-py/Cargo.toml", "--interpreter", str(python),
                 "--out", str(private_dir(assets / "wheels"))], "wheel", "wheel-build", config["build_seconds"])
        wheels = list((assets / "wheels").glob("*.whl")); require(len(wheels) == 1, "IDENTITY")
        wheel = wheels[0]
        command([str(python), "-m", "pip", "install", "--no-index", "--no-deps", str(wheel)], "collect", "wheel-install")
        command([str(python), "-c", "import alopex, importlib.metadata; assert importlib.metadata.version('alopex') == " + repr(version)],
                "collect", "wheel-import")
        command([str(server), "--version"], "collect", "server-version")
        docs = private_dir(work / "docs")
        command(["git", "-C", str(docs), "init", "--quiet"], "collect", "docs-init")
        command(["git", "-C", str(docs), "fetch", "--quiet", "--depth=1", "https://github.com/alopex-db/docs.git", config["harness_commit"]], "collect", "docs-fetch")
        harness = assets / "harness.tar"
        command(["git", "-C", str(docs), "archive", "--format=tar", "--output", str(harness), "FETCH_HEAD", "scripts/performance"], "collect", "harness-export")
        lib_out = private_dir(assets / "lib") / library.name; shutil.copyfile(library, lib_out)
        receipt = {"schema": "alopex.performance-build/v1", "version": version, "source": identity,
                   "server_sha256": digest(server), "python_wheel_sha256": digest(wheel), "parser_sha256": digest(lib_out),
                   "parser_contract": record["contract_version"], "parser_record_sha256": digest(record_file),
                   "cargo_lock_sha256": digest(source / "Cargo.lock"),
                   "tools": {"rust": config["rust_version"], "maturin": config["maturin_version"]},
                   "build": {"incremental": False, "release_debug": False, "path_remap": "/alopex-build",
                             "server": ["cargo", "build", "--locked", "--release", "-p", "alopex-server", "--bin", "alopex-server"],
                             "wheel": "maturin build --locked --release --compatibility linux"},
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
        for path in (target, work / "cargo", work / "rustup", work / "venv", source, work / "docs", work / "tmp"):
            if path.exists(): shutil.rmtree(path)
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
