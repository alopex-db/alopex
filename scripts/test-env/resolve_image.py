"""Resolve an image lock from signed APT indexes in a disposable base container."""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
import re
import shlex
import shutil
import ssl
import urllib.parse
import urllib.request

from common import (CAPABILITIES, Failure, atomic_json, contained, digest, private_dir,
                    read_json, require, sha256)
from docker_backend import OWNER
from lifecycle import Run

HERE = Path(__file__).resolve().parent
PGDG_KEY = "https://www.postgresql.org/media/keys/ACCC4CF8.asc"
PGDG_SHA256 = "0144068502a1eddd2a0280ede10ef607d1ec592ce819940991203941564e8e76"
APT_HOSTS = {"archive.ubuntu.com", "security.ubuntu.com", "ports.ubuntu.com",
             "apt-archive.postgresql.org"}
PG_VERSION = "16.14-1.pgdg24.04+1"


def paragraphs(text):
    """Read Debian control paragraphs, including continuation lines."""
    result, row, key = [], {}, None
    for line in [*text.splitlines(), ""]:
        if not line:
            if row:
                result.append(row)
            row, key = {}, None
        elif line[0].isspace():
            require(key is not None, "IDENTITY")
            row[key] += "\n" + line
        else:
            require(":" in line, "IDENTITY")
            key, value = line.split(":", 1)
            require(key not in row, "IDENTITY")
            row[key] = value.strip()
    return result


def apt_variant(directory):
    directory = Path(directory)
    packages = {}
    for line in (directory / "base.tsv").read_text().splitlines():
        name, version = line.split("\t")
        require(name not in packages and name and version, "IDENTITY")
        packages[name] = version
    files, names, selected = [], set(), set()
    for line in (directory / "uris.txt").read_text().splitlines():
        if not line.startswith("'"):
            continue
        fields = shlex.split(line)
        require(len(fields) == 4, "IDENTITY")
        url, filename, size, _ = fields  # APT may print MD5; use signed-index SHA256.
        require(re.fullmatch(r"[A-Za-z0-9+.%:_~-]+\.deb", filename), "IDENTITY")
        package = filename.split("_", 1)[0]
        parsed = urllib.parse.urlsplit(url)
        require(parsed.scheme == "https" and parsed.hostname in APT_HOSTS and
                not parsed.username and not parsed.password and
                parsed.port in (None, 443), "POLICY")
        require(filename not in names and package not in selected and int(size) > 0, "IDENTITY")
        names.add(filename)
        selected.add(package)
        metadata = contained(directory / "metadata", package + ".control").read_text()
        matches = [row for row in paragraphs(metadata)
                   if row.get("Package") == package and row.get("Size") == size and
                   row.get("Filename") and
                   urllib.parse.unquote(parsed.path).endswith("/" + row["Filename"])]
        identities = {(row.get("Version"), row.get("SHA256")) for row in matches}
        require(len(identities) == 1, "IDENTITY")
        version, checksum = identities.pop()
        require(isinstance(version, str) and bool(version), "IDENTITY")
        sha256(checksum)
        if package in ("postgresql-16", "postgresql-client-16", "libpq5"):
            require(parsed.hostname == "apt-archive.postgresql.org" and version == PG_VERSION, "IDENTITY")
        packages[package] = version
        files.append({"path": "deps/" + filename, "url": url, "size": int(size), "sha256": checksum})
    required = {"python3.12", "python3.12-venv", "python3-pip", "postgresql-16",
                "postgresql-client-16", "libpq5", "sudo", "ca-certificates", "procps",
                "util-linux", "iproute2"}
    require(files and required.issubset(packages), "INCOMPLETE")
    require(all(packages[name] == PG_VERSION for name in
                ("postgresql-16", "postgresql-client-16", "libpq5")), "IDENTITY")
    return {"files": files, "packages": packages, "allowed_hosts": []}


def acquire(url, hosts, maximum, budget):
    from envctl import LockedRedirect
    parsed = urllib.parse.urlsplit(url)
    require(parsed.scheme == "https" and parsed.hostname in hosts, "POLICY")
    opener = urllib.request.build_opener(LockedRedirect(hosts))
    chunks, size = [], 0
    budget.check()
    with opener.open(url, timeout=30) as response:
        while chunk := response.read(min(65536, maximum + 1 - size)):
            size += len(chunk)
            require(size <= maximum, "IDENTITY")
            chunks.append(chunk)
            budget.check()
    return b"".join(chunks)


def wheel_records(requirements, fetch_metadata):
    records, names = [], set()
    for line in requirements.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        require(re.fullmatch(r"[A-Za-z0-9_.-]+==[0-9][A-Za-z0-9_.+-]*", line))
        name, version = line.split("==")
        normalized = re.sub(r"[-_.]+", "-", name).lower()
        require(normalized not in names)
        names.add(normalized)
        metadata = fetch_metadata(f"https://pypi.org/pypi/{name}/{version}/json")
        require(re.sub(r"[-_.]+", "-", metadata["info"]["name"]).lower() == normalized and
                metadata["info"]["version"] == version, "IDENTITY")
        choices = [row for row in metadata["urls"] if row["filename"].endswith("-py3-none-any.whl")
                   and row["packagetype"] == "bdist_wheel" and not row.get("yanked")]
        require(len(choices) == 1, "PREREQUISITE")
        row = choices[0]
        parsed = urllib.parse.urlsplit(row["url"])
        require(parsed.scheme == "https" and parsed.hostname == "files.pythonhosted.org", "POLICY")
        require(type(row["size"]) is int and row["size"] > 0, "IDENTITY")
        require("/" not in row["filename"] and "\\" not in row["filename"], "IDENTITY")
        records.append({"path": "wheelhouse/" + row["filename"], "url": row["url"],
                        "size": row["size"], "sha256": sha256(row["digests"]["sha256"])})
    require({"psycopg", "typing-extensions"}.issubset(names), "INCOMPLETE")
    return records


def resolve(profile, docker, budget):
    from envctl import collect, recover
    budget.before("fetch")
    docker.prerequisites()
    lock = read_json(profile["image_lock"])
    require(lock.get("schema") == "alopex.test-env-image/v1" and "@sha256:" in lock["base"])
    sha256(lock["base"].rsplit("@sha256:", 1)[1])
    policy = docker.policy()
    run_id = profile["run_id"] + "-resolve"
    run = Run(profile["private_output"], run_id).create({"base": lock["base"],
        "resolver_sha256": digest(HERE / "resolve_image.sh"),
        "resolver_controller_sha256": digest(HERE / "resolve_image.py"),
        "requirements_sha256": digest(HERE / "requirements.lock")})
    name = "alopex-test-" + run_id
    bootstrap = private_dir(run.root / "bootstrap")
    try:
        key = acquire(PGDG_KEY, {"www.postgresql.org"}, 65536, budget)
        require(hashlib.sha256(key).hexdigest() == PGDG_SHA256, "IDENTITY")
        (bootstrap / "pgdg.asc").write_bytes(key)
        certificates = ssl.create_default_context().get_ca_certs(binary_form=True)
        require(certificates, "PREREQUISITE")
        (bootstrap / "ca.pem").write_text("".join(ssl.DER_cert_to_PEM_cert(cert) for cert in certificates))
        shutil.copyfile(HERE / "resolve_image.sh", bootstrap / "resolve.sh")
        # Public keys/CA certificates must be readable by APT's _apt sandbox UID.
        bootstrap.chmod(0o755)
        for path in bootstrap.iterdir():
            path.chmod(0o644)
        budget.run([*docker.prefix, "pull", "--platform", profile["platform"], lock["base"]],
                   timeout=profile["limits"]["run_seconds"], log=run.root / "stage.log")
        limits = profile["limits"]
        args = ["create", "--name", name, "--pull=never", "--platform", profile["platform"],
                "--init", "--network=bridge", "--ipc=private", "--cap-drop=ALL", "--user=root",
                "--label", OWNER, "--label", "org.alopex.test-env.run=" + run_id,
                "--label", "org.alopex.test-env.token=" + run.record["ownership_token"],
                "--memory", str(limits["memory_bytes"]), "--memory-swap", str(limits["memory_bytes"]),
                "--cpuset-cpus", ",".join(map(str, sorted(set(profile["cpu"]["server"] + profile["cpu"]["client"])))),
                "--pids-limit", str(limits["pids"]), "--log-driver=local", "--log-opt=max-size=1m",
                "--log-opt=max-file=1", "--log-opt=compress=false"]
        for capability in CAPABILITIES:
            args.extend(["--cap-add", capability])
        args.extend([*policy, "--entrypoint", "/bin/sh", lock["base"], "/resolution/resolve.sh"])
        run.intent("container", name)
        identity = docker.call(*args)
        run.resource("container", name, identity)
        budget.run([*docker.prefix, "cp", str(bootstrap), identity + ":/resolution"],
                   timeout=limits["case_seconds"], log=run.root / "stage.log")
        run.transition("running")
        budget.run([*docker.prefix, "start", "--attach", identity], timeout=limits["run_seconds"],
                   log=run.root / "container.log", cancel=lambda: docker.stop(identity))
        state = json.loads(docker.call("container", "inspect", identity))[0]["State"]
        require(state["ExitCode"] == 0 and not state["OOMKilled"], "COMMAND")
        run.transition("stopped")
        collect(profile, docker, budget, run_id)
        variant = apt_variant(run.root / "collected")
        # The second online acquisition is small PyPI metadata, covered by fetch.
        budget.before("fetch")
        variant["files"].extend(wheel_records((HERE / "requirements.lock").read_text(),
            lambda url: json.loads(acquire(url, {"pypi.org"}, 4 * 1024 * 1024, budget))))
        variant["allowed_hosts"] = sorted({urllib.parse.urlsplit(f["url"]).hostname for f in variant["files"]})
        lock.setdefault("platforms", {})[profile["platform"]] = variant
        atomic_json(profile["image_lock"], lock)
        run.load()
        run.record["image_lock_sha256"] = digest(profile["image_lock"])
        run.save()
        run.clean(docker.inspect, docker.remove)
    except BaseException as error:
        atomic_json(run.root / "failure.json", {"error_code": getattr(error, "code", "INTERRUPTED"),
                                                "detail": repr(error)})
        recover(run.load(), docker)
        raise
    finally:
        shutil.rmtree(bootstrap)
