"""Private configuration, bounded inputs and fixed public error codes."""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import stat
import tarfile
import tempfile
import contextlib
import zipfile

ERRORS = {
    "INPUT": 64, "IDENTITY": 64, "ARCHIVE": 64, "PREREQUISITE": 78,
    "CAPACITY": 78, "MEASUREMENT": 78, "POLICY": 78, "COMMAND": 1,
    "CASE": 1, "INCOMPLETE": 1, "COLLECT": 1, "CLEANUP": 1,
    "STATE": 1, "TIMEOUT": 124, "INTERRUPTED": 1, "INTERNAL": 1,
}
CAPABILITIES = ("CHOWN", "DAC_OVERRIDE", "FOWNER", "SETUID", "SETGID", "AUDIT_WRITE")
CASES = ("E01", "E02", "E03", "E04", "E05", "E06", "E07", "S01", "S02", "S03",
         "F01", "F02", "R01", "P01")


class Failure(Exception):
    def __init__(self, code, detail=""):
        self.code = code if code in ERRORS else "INTERNAL"
        self.detail = detail
        super().__init__(self.code)


def require(condition, code="INPUT"):
    if not condition:
        raise Failure(code)


def digest(path):
    h = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def sha256(value):
    require(isinstance(value, str) and re.fullmatch(r"[a-f0-9]{64}", value))
    return value


@contextlib.contextmanager
def host_lock(directory=None):
    """Serialize growing operations on a physical host, across profiles."""
    if directory is None:
        directory = Path.home() / ".cache" / "alopex-test-env"
    path = private_dir(directory) / "host.lock"
    with path.open("a+b") as stream:
        path.chmod(0o600)
        if os.name == "posix":
            import fcntl
            try:
                fcntl.flock(stream, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError as error:
                raise Failure("STATE") from error
            try:
                yield
            finally:
                fcntl.flock(stream, fcntl.LOCK_UN)
        else:
            raise Failure("PREREQUISITE")


def private_dir(path):
    path = Path(path)
    require(not path.is_symlink())
    path.mkdir(mode=0o700, parents=True, exist_ok=True)
    path.chmod(0o700)
    return path


def atomic_json(path, value):
    path = Path(path)
    private_dir(path.parent)
    fd, temporary = tempfile.mkstemp(dir=path.parent, prefix=".pending-")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            json.dump(value, stream, ensure_ascii=False, sort_keys=True, indent=2)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        Path(temporary).unlink(missing_ok=True)


def read_json(path, *, private=False):
    path = Path(path)
    require(path.is_file() and not path.is_symlink())
    if private and os.name == "posix":
        info = path.stat()
        require(info.st_uid == os.getuid() and not info.st_mode & 0o077)
    require(path.stat().st_size <= 4 * 1024 * 1024)
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (ValueError, UnicodeError) as error:
        raise Failure("INPUT") from error


def relative(value):
    require(isinstance(value, str) and bool(value) and "\\" not in value)
    path = PurePosixPath(value)
    require(not path.is_absolute() and all(p not in ("", ".", "..") for p in value.split("/")))
    return path


def contained(root, name):
    path = Path(root).joinpath(*relative(name).parts)
    require(path.resolve().is_relative_to(Path(root).resolve()), "ARCHIVE")
    cursor = path
    while cursor != Path(root):
        require(not cursor.is_symlink(), "ARCHIVE")
        cursor = cursor.parent
    return path


def inspect_archive(path, maximum):
    """Reject links, device nodes, duplicates and decompression outside a bound."""
    total, seen = 0, set()
    if zipfile.is_zipfile(path):
        with zipfile.ZipFile(path) as archive:
            for item in archive.infolist():
                name = item.filename.rstrip("/")
                relative(name)
                mode = item.external_attr >> 16
                require(not stat.S_ISLNK(mode), "ARCHIVE")
                require(stat.S_IFMT(mode) in (0, stat.S_IFREG, stat.S_IFDIR), "ARCHIVE")
                require(name not in seen and not item.flag_bits & 1, "ARCHIVE")
                seen.add(name)
                total += item.file_size
                require(total <= maximum, "ARCHIVE")
    else:
        with tarfile.open(path) as archive:
            for item in archive:
                relative(item.name.rstrip("/"))
                require(item.isfile() or item.isdir(), "ARCHIVE")
                require(item.name.rstrip("/") not in seen, "ARCHIVE")
                seen.add(item.name.rstrip("/"))
                total += item.size
                require(total <= maximum, "ARCHIVE")
    return total


def elf_signature(path):
    with Path(path).open("rb") as stream:
        header = stream.read(20)
    require(len(header) == 20 and header[:4] == b"\x7fELF" and
            header[4] in (1, 2) and header[5] in (1, 2), "IDENTITY")
    endian = "little" if header[5] == 1 else "big"
    require(int.from_bytes(header[16:18], endian) in (2, 3), "IDENTITY")
    return header[4], header[5], int.from_bytes(header[18:20], endian)


def load_profile(path):
    profile = read_json(path, private=True)
    require(profile.get("schema") == "alopex.test-env-profile/v1")
    require(re.fullmatch(r"[a-z0-9][a-z0-9-]{0,47}", profile.get("run_id", "")))
    require(re.fullmatch(r"linux/[a-z0-9_]+(?:/[a-z0-9]+)?", profile.get("platform", "")))
    require(profile.get("execution_mode") in ("native", "emulated"))
    for key in ("docker_context", "image_lock", "assets", "private_output", "cache"):
        require(isinstance(profile.get(key), str) and bool(profile[key]))
    for group in ("server", "client"):
        cpus = profile["cpu"][group]
        require(isinstance(cpus, list) and cpus and len(set(cpus)) == len(cpus))
        require(all(type(cpu) is int and cpu >= 0 for cpu in cpus))
    require(not set(profile["cpu"]["server"]) & set(profile["cpu"]["client"]))
    for key in ("memory_bytes", "pids", "case_seconds", "run_seconds", "stop_seconds"):
        require(type(profile["limits"].get(key)) is int and profile["limits"][key] > 0)
    require(profile["limits"]["swap_bytes"] == 0 or
            type(profile["limits"]["swap_bytes"]) is int and profile["limits"]["swap_bytes"] > 0)
    budget = profile["storage"]
    require(isinstance(budget["roots"], list) and budget["roots"])
    require(isinstance(budget.get("runtime_storage_roots"), list) and budget["runtime_storage_roots"])
    for root in budget["runtime_storage_roots"]:
        resolved = Path(root).resolve(strict=True)
        require(any(resolved.is_relative_to(Path(parent).resolve(strict=True)) for parent in budget["roots"]), "MEASUREMENT")
    for key in ("stop_bytes", "reserve_bytes", "poll_seconds"):
        require(type(budget[key]) is int and budget[key] > 0)
    require(budget["stop_bytes"] <= 40_000_000_000)
    require(isinstance(budget["planned_bytes"], dict))
    for operation in ("fetch", "build", "runtime", "accept", "collect"):
        require(type(budget["planned_bytes"].get(operation)) is int and
                budget["planned_bytes"][operation] > 0)
    policy = profile["security"]
    require(policy["lsm_kind"] in ("apparmor", "selinux"))
    for key in ("lsm_profile", "seccomp_profile"):
        require(isinstance(policy[key], str) and bool(policy[key]))
    sha256(policy["seccomp_sha256"])
    return profile


def verify_assets(manifest_path, platform):
    manifest_path = Path(manifest_path)
    manifest = read_json(manifest_path)
    require(manifest.get("schema") == "alopex.test-env-assets/v1")
    require(manifest.get("platform") == platform, "IDENTITY")
    require(re.fullmatch(r"[a-f0-9]{40}", manifest.get("source_commit", "")))
    require(re.fullmatch(r"[a-f0-9]{40}", manifest.get("harness_commit", "")))
    require(isinstance(manifest.get("files"), list) and manifest["files"])
    seen, roles = set(), set()
    for item in manifest["files"]:
        name = item["path"]
        require(name not in seen)
        seen.add(name)
        roles.add(item["role"])
        path = contained(manifest_path.parent, name)
        require(path.is_file() and type(item["size"]) is int and item["size"] >= 0)
        require(path.stat().st_size == item["size"] and digest(path) == sha256(item["sha256"]), "IDENTITY")
        if item["role"] in ("wheel", "harness"):
            inspect_archive(path, manifest["expanded_limit_bytes"])
    require({"server", "wheel", "receipt", "harness"}.issubset(roles))
    require(all(sum(item["role"] == role for item in manifest["files"]) == 1
                for role in ("server", "wheel", "receipt", "harness")))
    receipt_entries = [i for i in manifest["files"] if i["role"] == "receipt"]
    require(len(receipt_entries) == 1)
    receipt = read_json(contained(manifest_path.parent, receipt_entries[0]["path"]))
    # The receipt adapter is explicit: arbitrary old receipts must not silently
    # pass as identity evidence merely because their byte hash is correct.
    if receipt.get("schema") == "alopex.performance-build/v1":
        require(receipt["source"]["commit"] == manifest["source_commit"], "IDENTITY")
        for item in manifest["files"]:
            if item["role"] in ("server", "wheel"):
                key = "server_sha256" if item["role"] == "server" else "python_wheel_sha256"
                require(receipt[key] == item["sha256"], "IDENTITY")
            elif item["role"] == "library":
                expected = receipt.get("runtime_libraries", {}).get(item["path"])
                # Existing performance receipts bind the parser separately.
                # Preserve the receipt and recognize only that named library;
                # unrelated libraries still require their own explicit binding.
                if expected is None and Path(item["path"]).name == "libalopex_sql_parser.so":
                    expected = receipt.get("parser_sha256")
                require(expected == item["sha256"], "IDENTITY")
    else:
        require(receipt.get("source_commit") == manifest["source_commit"], "IDENTITY")
        require(receipt.get("platform") == platform, "IDENTITY")
        for item in manifest["files"]:
            if item["role"] in ("server", "wheel", "library"):
                require(receipt.get("artifacts", {}).get(item["path"]) == item["sha256"], "IDENTITY")
    return manifest


def public_result(private):
    """Construct public output; never copy free-form strings from a report."""
    result = {"schema": "alopex.test-env-public/v1", "status": "failed", "cases": []}
    for key in ("image_digest", "assets_digest", "harness_digest", "controller_digest"):
        if key in private:
            result[key] = sha256(private[key])
    if "controller_commit" in private:
        require(isinstance(private["controller_commit"], str) and
                re.fullmatch(r"[a-f0-9]{40}", private["controller_commit"]))
        result["controller_commit"] = private["controller_commit"]
    for row in private.get("cases", []):
        require(row.get("id") in CASES)
        require(row.get("status") in ("passed", "failed", "not_run"))
        code = row.get("error_code")
        require(code is None or code in ERRORS)
        result["cases"].append({"id": row["id"], "status": row["status"], "error_code": code})
    if private.get("status") == "passed" and result["cases"] and all(
            row["status"] == "passed" and row["error_code"] is None for row in result["cases"]):
        result["status"] = "passed"
    return result
