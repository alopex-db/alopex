"""In-container identity and report primitives for an owned #712 pair.

The launch sequence requires final image, wheel, harness and asset pins.
Importing this module never starts a server or relaxes #804 policy.
"""
from __future__ import annotations

import os
from pathlib import Path
import re
import signal
import subprocess
import sys
import json
import time

from . import supervisor

NAMESPACE = re.compile(r"(?:pid|net):\[[1-9][0-9]*\]\Z")


def process_identity(pid):
    """Capture a live process and its namespace identity without PID reuse."""
    supervisor.require(type(pid) is int and pid > 0, "PAIR_IDENTITY")
    root = Path("/proc") / str(pid)
    try:
        before = (root / "stat").read_text()
        fields = before.rsplit(")", 1)[1].split()
        start_ticks = int(fields[19])
        pid_namespace = os.readlink(root / "ns/pid")
        net_namespace = os.readlink(root / "ns/net")
        after = (root / "stat").read_text()
    except (OSError, IndexError, ValueError) as error:
        raise ValueError("PAIR_IDENTITY") from error
    supervisor.require(before == after and start_ticks > 0 and
                       NAMESPACE.fullmatch(pid_namespace) and
                       NAMESPACE.fullmatch(net_namespace) and
                       pid_namespace.startswith("pid:") and
                       net_namespace.startswith("net:"), "PAIR_IDENTITY")
    return {"pid": pid, "start_ticks": start_ticks,
            "pid_namespace": pid_namespace, "net_namespace": net_namespace}


def validate_process_pair(server, client, url):
    """Require server and client to share PID and network namespaces."""
    supervisor.require(type(server) is dict and type(client) is dict and
                       set(server) == {"pid", "start_ticks", "pid_namespace",
                                       "net_namespace"} and
                       set(client) == set(server) and
                       all(type(item[key]) is int and item[key] > 0
                           for item in (server, client)
                           for key in ("pid", "start_ticks")) and
                       all(type(item[key]) is str and
                           NAMESPACE.fullmatch(item[key]) is not None and
                           item[key].startswith(prefix)
                           for item in (server, client)
                           for key, prefix in (("pid_namespace", "pid:"),
                                               ("net_namespace", "net:"))) and
                       server["pid"] != client["pid"] and
                       server["pid_namespace"] == client["pid_namespace"] and
                       server["net_namespace"] == client["net_namespace"] and
                       type(url) is str and re.fullmatch(
                           r"http://127\.0\.0\.1:[1-9][0-9]{0,4}", url) is not None and
                       1 <= int(url.rsplit(":", 1)[1]) <= 65535,
                       "PAIR_IDENTITY")


def paired_report(config, *, server, client, url, image_digest,
                  profile_sha256, resource_manifest_sha256, cleanup):
    """Form the exact report after server and worker cleanup is proven.

    The caller must independently verify package, assets, manifests, output
    and Docker policy. Process identity alone cannot establish those facts.
    """
    validate_process_pair(server, client, url)
    supervisor.require(cleanup == {"server_reaped": True,
                                   "worker_reaped": True,
                                   "process_groups_empty": True} and
                       all(supervisor._hex(value, 64) for value in
                           (image_digest, profile_sha256,
                            resource_manifest_sha256)), "PAIR_IDENTITY")
    return {"schema": "alopex.sql-four-case-container-pair/v1",
            "status": "passed", "config": config,
            "image_digest": image_digest, "profile_sha256": profile_sha256,
            "server": dict(server, url=url), "client": client,
            "resource_manifest_sha256": resource_manifest_sha256,
            "cleanup": cleanup}


def main():
    """Run one source-pinned pair wholly inside the qualified container."""
    if sys.platform != "linux" or os.geteuid() == 0:
        raise ValueError("POLICY")
    sys.path.insert(0, "/tool")
    from common import atomic_json, digest, private_dir, read_json, verify_assets
    from entrypoint import WORK, INPUT, alopex, prepare
    from observe import process
    from verify_environment import privileges, resource_limits, files

    os.umask(0o077)
    staged = INPUT / "four-case"
    request = read_json(INPUT / "request.json")
    extra = read_json(staged / "stage.json")
    config = extra["config"]
    supervisor.validate_config(config, config["source"], config["assets_digest"],
                               config["same_job_id"])
    supervisor.require(request["schema"] == "alopex.test-env-request/v1" and
                       request["cases"] == [] and
                       extra["schema"] == "alopex.sql-four-case-stage/v1" and
                       extra["assets_digest"] == config["assets_digest"] and
                       all(supervisor._hex(extra[key], 64) for key in
                           ("image_digest", "profile_sha256")), "MISSING_PIN")
    assets = verify_assets(INPUT / "assets.json", extra["platform"])
    supervisor.require(digest(INPUT / "assets.json") == config["assets_digest"],
                       "ASSET_IDENTITY")
    # The host copied only named files from the reviewed manifest. Verify
    # their exact bytes again after the root-owned stage helper normalized modes.
    expected = extra["files"]
    supervisor.require(type(expected) is dict and expected and
                       all(type(name) is str and name and not name.startswith("/") and
                           ".." not in Path(name).parts and
                           supervisor._hex(value, 64) for name, value in expected.items()),
                       "MISSING_PIN")
    actual = {p.relative_to(staged).as_posix() for p in staged.rglob("*") if p.is_file()}
    supervisor.require(actual == set(expected) | {"stage.json"} and
                       all(not p.is_symlink() for p in staged.rglob("*")) and
                       all(digest(staged / name) == value for name, value in expected.items()),
                       "ASSET_IDENTITY")
    output = private_dir(WORK / "results")
    private_dir(WORK / "databases")
    state = private_dir(WORK / "databases/pair")
    for mode in ("on", "off"):
        private_dir(output / mode)
    for key in ("OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS",
                "RAYON_NUM_THREADS", "TOKIO_WORKER_THREADS", "POLARS_MAX_THREADS"):
        os.environ[key] = "1"
    privileges(request)
    resource_limits(request)
    python, env = prepare(assets, request)
    files(WORK)
    server = None
    child = None
    server_identity = None
    client_identity = None
    worker_reaped = False
    server_reaped = False
    groups_empty = False
    try:
        env.update(ALOPEX_SQL_TIMING="1", RUST_LOG="warn,alopex_sql_private_timing=info",
                   NO_COLOR="1")
        server, ports = alopex(output, request, assets, env, state_directory=state)
        server_identity = process_identity(server.child.pid)
        supervisor.require(server_identity["start_ticks"] ==
                           process(server.child.pid)["start_ticks"], "SERVER_IDENTITY")
        url = f'http://127.0.0.1:{ports["http"]}'
        roles = {item["role"]: item for item in assets["files"]}
        receipt = read_json(INPUT / roles["receipt"]["path"])
        artifact = lambda role: {"path": str(INPUT / roles[role]["path"]),
                                 "sha256": roles[role]["sha256"]}
        subject = {"schema": "alopex.performance-subject/v1",
                   "version": receipt["version"], "source": config["source"],
                   "cpu": request["cpu"], "build_receipt": artifact("receipt"),
                   "python_wheel": artifact("wheel"),
                   "server": {"pid": server_identity["pid"],
                              "start_ticks": server_identity["start_ticks"],
                              "binary": artifact("server"), "url": url}}
        atomic_json(output / "subject.json", subject)
        atomic_json(output / "resources.json", {
            "schema": "alopex.sql-resource-servers/v1",
            "servers": {"alopex-server": {"pid": server_identity["pid"],
                                           "start_ticks": server_identity["start_ticks"]}}})
        worker = dict(config, outputs={mode: {
            "raw": str(output / mode / "raw.ndjson"),
            "client": str(output / mode / "client.ndjson"),
            "result": str(output / mode / "measurement.json")}
            for mode in ("on", "off")},
            docs_harness_root=str(staged / "docs"),
            product_harness_root=str(staged / "product"),
            docs_files=extra["docs_files"], product_files=extra["product_files"])
        atomic_json(output / "worker.json", worker)
        worker_env = dict(env, ALOPEX_PERF_SERVER_URL=url,
                          ALOPEX_PERF_SUBJECT_MANIFEST=str(output / "subject.json"),
                          ALOPEX_PERF_SUBJECT_SHA256=digest(output / "subject.json"),
                          ALOPEX_PERF_SQL_RESOURCE_MANIFEST=str(output / "resources.json"),
                          PYTHONPATH=str(staged))
        with (output / "worker.log").open("xb") as log:
            child = subprocess.Popen([str(python), "-B", "-P", "-m",
                "four_case_diagnostic.worker", str(output / "worker.json")],
                stdout=log, stderr=log, env=worker_env, start_new_session=True,
                preexec_fn=lambda: os.sched_setaffinity(0, request["cpu"]["client"]))
            client_identity = process_identity(child.pid)
            validate_process_pair(server_identity, client_identity, url)
            supervisor.require(child.wait(timeout=supervisor.WORKER_SECONDS) == 0,
                               "WORKER_EXIT")
            worker_reaped = True
        supervisor.require(not supervisor._clear_group(child.pid), "WORKER_ORPHAN")
        supervisor.verify_output(config, output,
            {"pid": server_identity["pid"], "start_ticks": server_identity["start_ticks"]},
            digest(output / "resources.json"), stage_mode="create")
    finally:
        if child is not None and child.poll() is None:
            supervisor._terminate_worker(child)
            worker_reaped = True
        if server is not None:
            server.stop()
            server_reaped = server.child.poll() is not None
        groups_empty = (child is None or not supervisor._group_members(child.pid)) and \
                       (server is None or not supervisor._group_members(server.child.pid))
    supervisor.require(worker_reaped and server_reaped and groups_empty,
                       "CLEANUP")
    report = paired_report(config, server=server_identity, client=client_identity,
                           url=url, image_digest=extra["image_digest"],
                           profile_sha256=extra["profile_sha256"],
                           resource_manifest_sha256=digest(output / "resources.json"),
                           cleanup={"server_reaped": True, "worker_reaped": True,
                                    "process_groups_empty": True})
    atomic_json(output / "result.json", report)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
