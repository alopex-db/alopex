"""In-container identity and report primitives for an owned #712 pair.

The launch sequence requires final image, wheel, harness and asset pins.
Importing this module never starts a server or relaxes #804 policy.
"""
from __future__ import annotations

import os
from pathlib import Path
import re

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
    """Refuse execution until the source-pinned launch path is installed."""
    raise ValueError("MISSING_PIN")


if __name__ == "__main__":
    main()
