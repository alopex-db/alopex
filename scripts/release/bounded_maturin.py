"""Execute the pinned upstream maturin action under the host storage watchdog."""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import re
import subprocess
import sys

ACTION_SHA = "e83996d129638aa358a18fbd1dfb82f0b0fb5d3b"
MATURIN_VERSION = "1.14.1"
IMAGE = "quay.io/pypa/manylinux2014_x86_64:latest"
PROFILES = {
    "linux": ("build", "-m crates/alopex-py/Cargo.toml --release --out dist", "x86_64", "auto"),
    "macos": ("build", "-m crates/alopex-py/Cargo.toml --release --auditwheel repair --out dist", "aarch64", ""),
    "windows": ("build", "-m crates/alopex-py/Cargo.toml --release --out dist", "", ""),
    "sdist": ("sdist", "-m crates/alopex-py/Cargo.toml --out dist", "", ""),
}


def output(*command):
    return subprocess.check_output(command, text=True, timeout=30).strip()


def action_environment(kind, owner):
    command, args, target, manylinux = PROFILES[kind]
    env = dict(os.environ)
    env.update({"INPUT_COMMAND": command, "INPUT_ARGS": args, "INPUT_TARGET": target,
                "INPUT_MANYLINUX": manylinux, "INPUT_MATURIN-VERSION": MATURIN_VERSION,
                "INPUT_SCCACHE": "false", "INPUT_CONTAINER": IMAGE if kind == "linux" else "off",
                "INPUT_WORKING-DIRECTORY": "", "INPUT_BEFORE-SCRIPT-LINUX": "",
                "INPUT_HOST-HOME-MOUNT": "", "INPUT_DOCKER-OPTIONS": ""})
    if kind == "linux":
        env["INPUT_DOCKER-OPTIONS"] = (
            f"--name alopex-python-{owner} --label alopex.release-owner={owner} --init"
        )
    return env


def cleanup_docker(workspace, owner, image_was_present):
    name = f"alopex-python-{owner}"
    found = output("docker", "ps", "-aq", "--filter", f"name=^/{name}$")
    if found:
        label = output("docker", "inspect", "--format", '{{index .Config.Labels "alopex.release-owner"}}', name)
        if label != owner:
            raise RuntimeError("refusing to remove a Docker container owned by another run")
        subprocess.run(["docker", "rm", "--force", name], check=True, timeout=30)
    # A killed Docker client can leave a running container and root-owned bind
    # outputs. Remove only our labelled container, then repair only our outputs.
    paths = [workspace / name for name in ("target", "dist")]
    if any(path.is_symlink() for path in paths):
        raise RuntimeError("refusing symlinked Python release outputs")
    existing = [str(path) for path in paths if path.exists()]
    if existing:
        subprocess.run(["sudo", "-n", "chown", "-R", f"{os.getuid()}:{os.getgid()}", "--", *existing],
                       check=True, timeout=60)
    if image_was_present is False and output("docker", "image", "ls", "--quiet", "--no-trunc", IMAGE):
        subprocess.run(["docker", "image", "rm", IMAGE], check=True, timeout=60)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--kind", choices=PROFILES, required=True)
    parser.add_argument("--cleanup", action="store_true")
    args = parser.parse_args(argv)
    if os.environ.get("RUNNER_ENVIRONMENT") != "github-hosted":
        raise RuntimeError("bounded maturin requires an isolated GitHub-hosted runner")
    if sys.platform != {"linux": "linux", "macos": "darwin", "windows": "win32", "sdist": "linux"}[args.kind]:
        raise RuntimeError("qualification profile does not match this host")
    workspace = Path(os.environ["GITHUB_WORKSPACE"])
    if not workspace.is_absolute() or Path(os.environ["CARGO_TARGET_DIR"]) != workspace / "target":
        raise RuntimeError("Python release requires the single workspace Cargo target")
    owner = os.environ["GITHUB_RUN_ID"] + "-" + os.environ["GITHUB_RUN_ATTEMPT"]
    if not re.fullmatch(r"[1-9][0-9]*-[1-9][0-9]*", owner):
        raise RuntimeError("invalid run identity")
    directory = workspace / "artifacts/python-release"
    receipt = directory / "maturin.json"
    if args.cleanup:
        if args.kind == "linux" and receipt.is_file():
            record = json.loads(receipt.read_text(encoding="utf-8"))
            if record["owner"] != owner or record["kind"] != args.kind:
                raise RuntimeError("cleanup receipt belongs to another run")
            cleanup_docker(workspace, owner, record["image_was_present"])
        return 0
    action = workspace / ".maturin-action"
    if output("git", "-C", str(action), "rev-parse", "HEAD") != ACTION_SHA:
        raise RuntimeError("unexpected maturin action source")
    subprocess.run(["git", "-C", str(action), "diff", "--exit-code", "HEAD", "--", "dist/index.js", "action.yml"], check=True)
    if not output("node", "--version").startswith("v24."):
        raise RuntimeError("the pinned action requires Node 24")
    directory.mkdir(parents=True, exist_ok=True)
    record = {"owner": owner, "kind": args.kind, "action_sha": ACTION_SHA,
              "maturin_version": MATURIN_VERSION, "source_sha": output("git", "rev-parse", "HEAD"),
              "image_was_present": None, "build_exit": None}
    if args.kind == "linux":
        if sys.platform != "linux":
            raise RuntimeError("manylinux qualification requires Linux")
        if output("docker", "ps", "-aq", "--filter", f"name=^/alopex-python-{owner}$"):
            raise RuntimeError("owned container name already exists; inspect before retry")
        record["image_was_present"] = bool(output("docker", "image", "ls", "--quiet", "--no-trunc", IMAGE))
    receipt.write_text(json.dumps(record, indent=2) + "\n", encoding="utf-8")
    command = [sys.executable, str(Path(__file__).with_name("release_storage.py"))]
    if args.kind == "linux":
        command.append("--docker")
    command += ["--log", str(directory / "build.jsonl"), "--additional-bytes",
                "2000000000" if args.kind == "sdist" else "20000000000",
                "--", "node", str(action / "dist/index.js")]
    try:
        result = subprocess.run(command, cwd=workspace, env=action_environment(args.kind, owner))
        record["build_exit"] = result.returncode
        return result.returncode
    finally:
        record["cleanup"] = "failure"
        try:
            if args.kind == "linux":
                try:
                    image_ids = output("docker", "image", "ls", "--quiet", "--no-trunc", IMAGE)
                    if image_ids:
                        record["image_identity"] = json.loads(output("docker", "image", "inspect", IMAGE))[0]["RepoDigests"]
                finally:
                    cleanup_docker(workspace, owner, record["image_was_present"])
            record["cleanup"] = "success"
        finally:
            receipt.write_text(json.dumps(record, indent=2) + "\n", encoding="utf-8")


if __name__ == "__main__":
    sys.exit(main())
