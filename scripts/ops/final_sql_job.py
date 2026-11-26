"""Same-job, independently pinned public final SQL handoff.

The workflow installs a reviewed lock, performs #804 qualification, then calls
``run``. This CLI never constructs the qualified image or uploads an artifact.
"""
from __future__ import annotations

import argparse
import base64
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import sys

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO))

from scripts.ops.final_sql_runtime import (
    container, docs_harness, prepared_entrypoint, product_harness, public_reference_download,
    public_upload, reference_public, same_job, selected_profile, subject_binding,
)
from scripts.ops.four_case_diagnostic import entrypoint as four_case_entrypoint
from scripts.ops.four_case_diagnostic import qualification as four_case_qualification

SCHEMA = "alopex.public-final-sql-lock/v1"
LOCK_DIR = "alopex-final-sql-lock"
HEX64 = re.compile(r"[0-9a-f]{64}\Z")
FIELDS = {"schema", "qualification_expected", "reference_artifacts",
          "reviewed_products", "worker_sha256", "supervisor_sha256",
          "controller_digest", "declared_seconds", "trusted_download_hosts"}
SOURCE_BYTES = 1_000_000_000
FINAL_BYTES = 4_000_000_000
RESERVE_BYTES = 1_000_000_000
STOP_BYTES = 40_000_000_000


def require(value):
    if not value:
        raise ValueError("PUBLIC_INCOMPLETE")


def _unique(pairs):
    result = {}
    for key, value in pairs:
        require(key not in result)
        result[key] = value
    return result


def _canonical(value):
    return (json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2) + "\n").encode()


def _lock(raw, digest):
    require(type(raw) is bytes and 0 < len(raw) <= 65_536 and
            type(digest) is str and HEX64.fullmatch(digest) and
            hashlib.sha256(raw).hexdigest() == digest)
    value = json.loads(raw, object_pairs_hook=_unique)
    require(type(value) is dict and set(value) == FIELDS and
            value["schema"] == SCHEMA and raw == _canonical(value))
    expected = value["qualification_expected"]
    require(type(expected) is dict and set(expected) == {
        "lock_sha256", "source_commit", "source_tree", "controller_commit",
        "operator_sha256"} and
        all(type(expected[key]) is str and HEX64.fullmatch(expected[key])
            for key in ("lock_sha256",)) and
        all(reference_public.hex_digest(expected[key], 40)
            for key in ("source_commit", "source_tree", "controller_commit")) and
        type(expected["operator_sha256"]) is dict and
        set(expected["operator_sha256"]) == four_case_qualification.REQUIRED_OPERATORS and
        all(type(item) is str and HEX64.fullmatch(item)
            for item in expected["operator_sha256"].values()))
    require(all(type(value[key]) is str and HEX64.fullmatch(value[key])
                for key in ("worker_sha256", "supervisor_sha256", "controller_digest")))
    refs = value["reference_artifacts"]
    require(type(refs) is dict and set(refs) == set(reference_public.SUBJECTS))
    for subject, pin in refs.items():
        reference_public._pin(subject, pin)
    products = value["reviewed_products"]
    require(type(products) is dict and set(products) == set(subject_binding.PRODUCTS))
    for subject, pin in products.items():
        require(type(pin) is dict and set(pin) == {
            "source", "assets_digest", "files", "platform", "version"} and
            pin["version"] == subject_binding.VERSIONS[subject] and
            pin["platform"] == "linux/amd64" and
            reference_public.hex_digest(pin["assets_digest"], 64) and
            type(pin["source"]) is dict and
            set(pin["source"]) == {"commit", "tree", "profile"} and
            pin["source"]["commit"] == subject_binding.SOURCE_COMMITS[subject] and
            reference_public.hex_digest(pin["source"]["tree"], 40) and
            pin["source"]["profile"] == "release" and
            type(pin["files"]) is list and len(pin["files"]) == 4 and
            {row.get("role") for row in pin["files"] if type(row) is dict} ==
            subject_binding.ROLES)
        for row in pin["files"]:
            require(type(row) is dict and set(row) == {"role", "path", "size", "sha256"} and
                    type(row["path"]) is str and not row["path"].startswith("/") and
                    ".." not in Path(row["path"]).parts and
                    type(row["size"]) is int and 0 < row["size"] <= 268_435_456 and
                    reference_public.hex_digest(row["sha256"], 64))
    for subject in reference_public.SUBJECTS:
        pin = refs[subject]
        reviewed = products[subject]
        require(reviewed["assets_digest"] == pin["manifest_sha256"] and
                reviewed["source"]["commit"] == pin["source_commit"] and
                reviewed["source"]["tree"] == pin["source_tree"] and
                reviewed["files"] == [row for row in pin["files"]
                                      if row["role"] != "harness"])
    seconds = value["declared_seconds"]
    require(type(seconds) is dict and set(seconds) == {"preflight", "cell"} and
            all(type(number) is int and 0 < number <= 7200
                for number in seconds.values()) and
            4 * seconds["preflight"] + 48 * seconds["cell"] < 7200)
    hosts = value["trusted_download_hosts"]
    require(type(hosts) is list and 0 < len(hosts) <= 8 and
            hosts == sorted(set(hosts)) and
            all(type(host) is str and
                public_reference_download.https.HOST.fullmatch(host)
                for host in hosts))
    return value


def _runner_temp(environment):
    raw = environment.get("RUNNER_TEMP")
    require(type(raw) is str and Path(raw).is_absolute())
    path = Path(raw)
    require(path.is_dir() and not path.is_symlink() and path.resolve(strict=True) == path)
    return path


def _new_private_dir(path):
    path.mkdir(mode=0o700)
    info = path.lstat()
    require(stat.S_ISDIR(info.st_mode) and info.st_uid == os.geteuid() and
            stat.S_IMODE(info.st_mode) == 0o700 and not any(path.iterdir()))
    return path


def _read_private(path):
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    try:
        before = os.fstat(fd)
        require(stat.S_ISREG(before.st_mode) and before.st_nlink == 1 and
                before.st_uid == os.geteuid() and
                stat.S_IMODE(before.st_mode) == 0o600 and
                0 < before.st_size <= 65_536)
        raw = os.read(fd, 65_537)
        after = os.fstat(fd)
        require(len(raw) == before.st_size and
                (before.st_dev, before.st_ino, before.st_size,
                 before.st_mtime_ns, before.st_ctime_ns) ==
                (after.st_dev, after.st_ino, after.st_size,
                 after.st_mtime_ns, after.st_ctime_ns))
        return raw
    finally:
        os.close(fd)


def install(environment):
    """Install canonical, SHA-pinned lock bytes in a fixed private directory."""
    root = _runner_temp(environment)
    encoded = environment.get("ALOPEX_FINAL_SQL_LOCK_B64")
    digest = environment.get("ALOPEX_FINAL_SQL_LOCK_SHA256")
    require(type(encoded) is str and 0 < len(encoded) <= 90_000)
    raw = base64.b64decode(encoded, validate=True)
    require(base64.b64encode(raw).decode() == encoded)
    lock = _lock(raw, digest)
    directory = _new_private_dir(root / LOCK_DIR)
    path = directory / "lock.json"
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    with os.fdopen(fd, "wb") as output:
        output.write(raw)
        output.flush()
        os.fsync(output.fileno())
    require(_lock(_read_private(path), digest) == lock)


def _installed(environment):
    path = _runner_temp(environment) / LOCK_DIR / "lock.json"
    return _lock(_read_private(path), environment.get("ALOPEX_FINAL_SQL_LOCK_SHA256"))


def _qualification_args(lock, environment):
    root = _runner_temp(environment)
    require(environment.get("GITHUB_WORKSPACE") == str(REPO) and
            environment.get("GITHUB_REPOSITORY") == "alopex-db/alopex" and
            environment.get("GITHUB_SHA") ==
            lock["qualification_expected"]["controller_commit"] and
            environment.get("EXPECTED_CONTROLLER_SHA") == environment["GITHUB_SHA"] and
            environment.get("ALOPEX_FOUR_CASE_LOCK_SHA256") ==
            lock["qualification_expected"]["lock_sha256"])
    return {"lock_path": root / "alopex-712-lock/lock.json",
            "expected": lock["qualification_expected"],
            "repo": REPO, "runner_temp": root, "environment": environment}


def _admit_storage(qualification_args, *, source_bytes, select=selected_profile.from_live,
                   runtime=container._runtime):
    """Reserve checkout and final-run growth against live qualified storage."""
    require(type(source_bytes) is int and source_bytes in (0, SOURCE_BYTES))
    profile_path, _profile_sha, backing = select(qualification_args)
    common, capacity, *_ = runtime()
    profile = common.load_profile(profile_path)
    storage = profile["storage"]
    require(storage["runtime_storage_roots"] == [str(backing)] and
            type(storage["stop_bytes"]) is int and
            0 < storage["stop_bytes"] <= STOP_BYTES and
            type(storage["reserve_bytes"]) is int and
            storage["reserve_bytes"] >= RESERVE_BYTES)
    used = capacity.Budget(storage).used()
    additional = source_bytes + FINAL_BYTES + storage["reserve_bytes"]
    require(type(used) is int and used >= 0 and
            used + additional < storage["stop_bytes"] and
            used + additional < STOP_BYTES and
            used + additional < 50_000_000_000)
    for root in (qualification_args["runner_temp"], qualification_args["repo"], backing):
        disk = os.statvfs(root)
        require(disk.f_bavail * disk.f_frsize > additional)
    return True


def admit_sources(environment):
    """Stop before additional source checkouts when storage cannot fit."""
    lock = _installed(environment)
    qualification_args = _qualification_args(lock, environment)
    return _admit_storage(qualification_args, source_bytes=SOURCE_BYTES)


def run(environment, *, retrieve=public_reference_download.retrieve,
        prepared=prepared_entrypoint.run, live=same_job.attest_live,
        profile=selected_profile.from_live):
    """Acquire the two public references and execute from admitted same-job inputs.

    Callback substitution is for offline focused tests. Production uses the
    module defaults and fixed current-job paths only.
    """
    lock = _installed(environment)
    qualification_args = _qualification_args(lock, environment)
    root = qualification_args["runner_temp"]
    repo = REPO
    require(type(environment.get("GH_TOKEN")) is str and environment["GH_TOKEN"])
    four_case_lock = four_case_entrypoint.load_reviewed_lock(
        qualification_args["lock_path"], lock["qualification_expected"]["lock_sha256"])
    require(four_case_lock["pin"]["controller_digest"] == lock["controller_digest"])
    proof = live(**qualification_args)
    require(type(proof) is dict and
            proof.get("controller_digest") == lock["controller_digest"] and
            proof.get("assets_digest") ==
            lock["reviewed_products"]["candidate"]["assets_digest"] and
            proof.get("source_commit") ==
            lock["reviewed_products"]["candidate"]["source"]["commit"] and
            proof.get("source_tree") ==
            lock["reviewed_products"]["candidate"]["source"]["tree"] and
            reference_public.hex_digest(proof.get("image_digest"), 64))
    profile(qualification_args)
    _admit_storage(qualification_args, source_bytes=0)
    docs_checkout = repo / "alopex-final-docs"
    product_checkout = repo / "alopex-final-product"
    docs_harness._source(docs_checkout)
    product_harness.verify(product_checkout)
    destination = _new_private_dir(root / "alopex-final-sql-references")
    refs = retrieve(
        pins=lock["reference_artifacts"], destination=destination,
        private_roots=(str(root), str(repo)),
        trusted_download_hosts=set(lock["trusted_download_hosts"]),
        token=environment["GH_TOKEN"])
    require(type(refs) is dict and set(refs) == set(reference_public.SUBJECTS))
    work_dir = _new_private_dir(root / "alopex-final-sql-work")
    profile_dir = _new_private_dir(root / "alopex-final-sql-profiles")
    staging_root = _new_private_dir(root / "alopex-final-sql-staging")
    public_dir = _new_private_dir(repo / "native-final-sql-export")
    try:
        path = prepared(
            qualification_args=qualification_args,
            manifests={**refs, "candidate": root / "alopex-804-candidate/assets/assets.json"},
            reviewed_pins=lock["reviewed_products"],
            selected_image_digest=proof["image_digest"],
            controller_digest=lock["controller_digest"],
            worker_path=repo / "scripts/ops/final_sql_runtime/worker.py",
            worker_sha256=lock["worker_sha256"],
            controller_path=repo / "scripts/ops/final_sql_runtime/supervisor.py",
            controller_sha256=lock["supervisor_sha256"],
            docs_checkout=docs_checkout, product_checkout=product_checkout,
            docs_archive=work_dir / "docs.tar",
            harness_path=work_dir / "harness.tar",
            profile_dir=profile_dir, staging_root=staging_root,
            public_dir=public_dir, declared_seconds=lock["declared_seconds"])
        require(path == public_dir / "public.json")
    except BaseException:
        (public_dir / "public.json").unlink(missing_ok=True)
        raise
    return path


def main(argv=None, environment=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("phase", choices=("install", "admit-sources", "run"))
    args = parser.parse_args(argv)
    environment = dict(os.environ) if environment is None else environment
    try:
        {"install": install, "admit-sources": admit_sources,
         "run": run}[args.phase](environment)
        return 0
    except BaseException as error:
        # Even a setup failure emits the same public status and no private cause.
        from scripts.ops.final_sql_runtime.controller import ControllerFailure
        line = (error.public_line if isinstance(error, prepared_entrypoint.PreparedFailure)
                else public_upload.failure_log(ControllerFailure(
                    "PUBLIC_INCOMPLETE", 0, ValueError("PUBLIC_INCOMPLETE"))))
        sys.stdout.buffer.write(line)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
