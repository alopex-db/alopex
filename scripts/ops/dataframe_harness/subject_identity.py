"""Check a manually qualified measurement subject before fixtures or timers.

The build receipt is provenance supplied by the build/qualification step. This
module verifies its identity and installed bytes; it does not prove a build by
looking at a version string, and never installs or launches a product.
"""
import functools
from email.parser import BytesParser
import hashlib
import importlib.metadata
import importlib.util
import json
import os
from pathlib import Path, PurePosixPath
import re
import sys
from urllib.parse import urlsplit
import zipfile

MANIFEST_ENV = "ALOPEX_PERF_SUBJECT_MANIFEST"
DIGEST_ENV = "ALOPEX_PERF_SUBJECT_SHA256"


def digest(path):
    with Path(path).open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def _require(condition, message):
    if not condition:
        raise RuntimeError(message)


def _artifact(root, record):
    path = Path(record["path"])
    path = path if path.is_absolute() else root / path
    _require(re.fullmatch(r"[0-9a-f]{64}", record["sha256"]), "Invalid artifact digest")
    _require(digest(path) == record["sha256"], f"Artifact digest mismatch: {path.name}")
    return path.resolve()


def _package_files(directory):
    return {p.relative_to(directory).as_posix() for p in directory.rglob("*")
            if p.is_file() and "__pycache__" not in p.parts and p.suffix != ".pyc"}


def _installed_wheel(wheel, version):
    dist = importlib.metadata.distribution("alopex")
    _require(dist.version == version, f"Expected alopex {version}, found {dist.version}")
    package = Path(dist.locate_file("alopex")).resolve()
    spec = importlib.util.find_spec("alopex")
    _require(spec is not None and spec.origin is not None
             and Path(spec.origin).resolve() == package / "__init__.py",
             "Imported alopex is shadowed or outside the selected distribution")
    names = set()
    with zipfile.ZipFile(wheel) as archive:
        metadata = [i for i in archive.infolist() if i.filename.endswith(".dist-info/METADATA")]
        _require(len(metadata) == 1, "Wheel must have one distribution metadata record")
        headers = BytesParser().parsebytes(archive.read(metadata[0]), headersonly=True)
        _require(headers["Name"] == "alopex" and headers["Version"] == version,
                 "Wheel metadata does not identify the selected version")
        for info in archive.infolist():
            path = PurePosixPath(info.filename)
            if info.is_dir() or not path.parts or path.parts[0] != "alopex":
                continue
            _require(".." not in path.parts and not path.is_absolute(), "Invalid wheel member")
            relative = PurePosixPath(*path.parts[1:]).as_posix()
            _require(relative not in names, "Duplicate wheel member")
            names.add(relative)
            with archive.open(info) as stream:
                expected = hashlib.file_digest(stream, "sha256").hexdigest()
            _require(digest(package / relative) == expected,
                     f"Installed wheel file mismatch: {relative}")
    _require("__init__.py" in names and any(name.endswith((".so", ".pyd")) for name in names),
             "Wheel must contain the Python package and native extension")
    _require(names == _package_files(package), "Installed package file inventory differs from wheel")
    return {"version": dist.version, "package_path": str(package), "verified_files": len(names)}


def _start_ticks(proc):
    # comm may contain spaces or parentheses. Fields after its final ')' start
    # with field 3 (state); field 22 is the process start time.
    return int((proc / "stat").read_text().rsplit(")", 1)[1].split()[19])


def cpu_assignment(subject):
    """Optional manifest allocation; preserve the historical v0.8.16 topology."""
    allocation = subject.get("cpu", {"server": [0], "client": [1]})
    _require(isinstance(allocation, dict) and set(allocation) == {"server", "client"},
             "CPU assignment requires server and client sets")
    for role in ("server", "client"):
        values = allocation[role]
        _require(isinstance(values, list) and values
                 and all(type(value) is int and value >= 0 for value in values)
                 and len(set(values)) == len(values), "Invalid CPU affinity assignment")
    _require(not set(allocation["server"]) & set(allocation["client"]),
             "Server and client CPU affinity must not overlap")
    return {role: sorted(values) for role, values in allocation.items()}


def _server(record, binary, proc_root=Path("/proc"), cpu=None):
    _require(sys.platform == "linux", "Server identity requires the original local Linux topology")
    url = urlsplit(os.environ.get("ALOPEX_PERF_SERVER_URL", ""))
    _require(url.scheme == "http" and url.hostname == "127.0.0.1" and url.port is not None
             and url.path in ("", "/") and not url.query and not url.fragment
             and url.username is None and url.password is None,
             "Use the qualified local server's http://127.0.0.1:PORT endpoint")
    _require(record["url"] == os.environ["ALOPEX_PERF_SERVER_URL"], "Server endpoint mismatch")
    _require(type(record["pid"]) is int and record["pid"] > 0, "Invalid server PID")
    proc = proc_root / str(record["pid"])
    before = _start_ticks(proc)
    _require(before == record["start_ticks"], "Server process was replaced")
    _require(digest(proc / "exe") == digest(binary), "Running server executable mismatch")
    expected = cpu_assignment({"cpu": cpu})["server"] if cpu is not None else cpu_assignment({})["server"]
    _require(os.sched_getaffinity(record["pid"]) == set(expected), "Server CPU affinity differs from manifest")
    sockets = {p.readlink().as_posix() for p in (proc / "fd").iterdir() if p.is_symlink()}
    listening = False
    for line in (proc / "net/tcp").read_text().splitlines()[1:]:
        fields = line.split()
        address, port = fields[1].split(":")
        if (fields[3] == "0A" and address in ("0100007F", "00000000")
                and int(port, 16) == url.port and f"socket:[{fields[9]}]" in sockets):
            listening = True
    _require(listening and _start_ticks(proc) == before, "Endpoint is not owned by the qualified server")
    return {"pid": record["pid"], "start_ticks": before, "url": record["url"],
            "sha256": digest(binary), "cpu_affinity": expected}


@functools.lru_cache(maxsize=4)
def _verify(path_string, expected_digest, require_server):
    path = Path(path_string)
    raw = path.read_bytes()
    _require(hashlib.sha256(raw).hexdigest() == expected_digest, "Subject manifest changed")
    manifest = json.loads(raw)
    _require(manifest["schema"] == "alopex.performance-subject/v1", "Unknown subject schema")
    version, source = manifest["version"], manifest["source"]
    allocation = cpu_assignment(manifest)
    _require(re.fullmatch(r"[0-9]+\.[0-9]+\.[0-9]+", version), "Invalid subject version")
    _require(all(re.fullmatch(r"[0-9a-f]{40}", source[k]) for k in ("commit", "tree")),
             "Exact source commit/tree required")
    _require(source["profile"] == "release", "Final comparison requires a release build")
    receipt_path = _artifact(path.parent, manifest["build_receipt"])
    receipt = json.loads(receipt_path.read_bytes())
    receipt_schema = receipt["schema"]
    _require(receipt_schema in ("alopex.performance-build/v1", "alopex.performance-wheel-build/v1")
             and receipt["source"] == source and receipt["version"] == version,
             "Build receipt source/version mismatch")
    _require(not require_server or receipt_schema == "alopex.performance-build/v1",
             "Server comparison requires a server build receipt")
    wheel = _artifact(path.parent, manifest["python_wheel"])
    _require(receipt["python_wheel_sha256"] == manifest["python_wheel"]["sha256"],
             "Wheel is not identified by the build receipt")
    installed = _installed_wheel(wheel, version)
    result = {"schema": manifest["schema"], "manifest_sha256": expected_digest,
              "version": version, "source": source,
              "build_receipt_sha256": manifest["build_receipt"]["sha256"],
              "python_wheel_sha256": manifest["python_wheel"]["sha256"], "installed": installed,
              "cpu": allocation}
    if require_server:
        binary = _artifact(path.parent, manifest["server"]["binary"])
        _require(receipt["server_sha256"] == manifest["server"]["binary"]["sha256"],
                 "Server is not identified by the build receipt")
        result["server"] = _server(manifest["server"], binary, cpu=allocation)
    result["identity_checker_sha256"] = digest(__file__)
    return result


def verify_subject(require_server=False):
    path = os.environ.get(MANIFEST_ENV)
    if not path:
        _require(DIGEST_ENV not in os.environ, "Subject digest requires a manifest")
        version = importlib.metadata.version("alopex")
        _require(version == "0.8.16", f"Expected historical alopex 0.8.16, found {version}; supply a subject manifest")
        return {"version": version, "identity_status": "historical-version-only"}
    path = str(Path(path).resolve())
    current = digest(path)
    expected = os.environ.setdefault(DIGEST_ENV, current)
    _require(current == expected, "Subject manifest changed")
    os.environ[MANIFEST_ENV] = path  # Workers may have a different cwd.
    return _verify(path, expected, require_server)


def worker_command(command, require_server=False):
    if MANIFEST_ENV not in os.environ:
        return command
    return [sys.executable, "-B", str(Path(__file__).resolve()),
            "server" if require_server else "python", *command]


if __name__ == "__main__":
    _require(len(sys.argv) > 2 and sys.argv[1] in ("python", "server"), "Expected python/server and worker command")
    verify_subject(require_server=sys.argv[1] == "server")
    os.execv(sys.argv[2], sys.argv[2:])
