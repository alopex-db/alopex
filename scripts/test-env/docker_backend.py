"""Docker operations against an existing context; never provision a VM."""
from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess

from common import CAPABILITIES, Failure, atomic_json, digest, require

OWNER = "org.alopex.test-env.owner=envctl"


class Docker:
    def __init__(self, profile):
        self.profile = profile
        self.prefix = ["docker", "--context", profile["docker_context"]]

    def call(self, *args, missing=False):
        result = subprocess.run([*self.prefix, *args], capture_output=True, text=True, timeout=30)
        if result.returncode:
            if missing and ("No such container:" in result.stderr or
                            "no such volume" in result.stderr.lower()):
                return None
            raise Failure("PREREQUISITE", result.stderr)
        return result.stdout.strip()

    def inspect(self, item):
        raw = self.call(item["kind"], "inspect", item["name"], missing=True)
        if raw is None:
            return None
        live = json.loads(raw)[0]
        if item["kind"] == "volume":
            # Volume API has no immutable ID; CreatedAt prevents name reuse.
            return {"id": live["Name"] + "@" + live["CreatedAt"],
                    "labels": live.get("Labels") or {}, "running": False}
        return {"id": live["Id"], "labels": live["Config"].get("Labels") or {},
                "running": live["State"]["Running"]}

    def remove(self, item):
        self.call(item["kind"], "rm", item["id"] if item["kind"] == "container" else item["name"])

    def prerequisites(self):
        context = json.loads(self.call("context", "inspect", self.profile["docker_context"]))[0]
        require(context["Endpoints"]["docker"]["Host"].startswith("unix://"), "PREREQUISITE")
        native = self.call("version", "--format", "{{.Server.Os}}/{{.Server.Arch}}")
        selected = "/".join(self.profile["platform"].split("/")[:2])
        require((self.profile["execution_mode"] == "native") == (selected == native), "IDENTITY")

    def policy(self):
        security = self.profile["security"]
        require(digest(security["seccomp_profile"]) == security["seccomp_sha256"], "POLICY")
        options = ["--security-opt", "seccomp=" + security["seccomp_profile"]]
        if security["lsm_kind"] == "apparmor":
            options.extend(["--security-opt", "apparmor=" + security["lsm_profile"]])
        else:
            parts = security["lsm_profile"].split(":", 3)
            require(len(parts) == 4, "POLICY")
            for field, value in zip(("user", "role", "type", "level"), parts):
                options.extend(["--security-opt", "label=" + field + ":" + value])
        return options

    def arguments(self, run_id, image, input_volume, work_volume, token):
        p, limits = self.profile, self.profile["limits"]
        cpus = sorted(set(p["cpu"]["server"] + p["cpu"]["client"]))
        args = ["create", "--name", "alopex-test-" + run_id, "--init", "--pull=never",
                "--platform", p["platform"], "--network=none", "--ipc=private",
                "--cap-drop=ALL", "--user", "alopex", "--label", OWNER,
                "--label", "org.alopex.test-env.run=" + run_id,
                "--label", "org.alopex.test-env.token=" + token,
                "--memory", str(limits["memory_bytes"]),
                "--memory-swap", str(limits["memory_bytes"] + limits["swap_bytes"]),
                "--pids-limit", str(limits["pids"]), "--cpuset-cpus", ",".join(map(str, cpus)),
                "--log-driver", "local", "--log-opt", "max-size=1m", "--log-opt", "max-file=1",
                "--mount", f"type=volume,src={input_volume},dst=/input,readonly,volume-nocopy",
                "--mount", f"type=volume,src={work_volume},dst=/work,volume-nocopy"]
        for capability in CAPABILITIES:
            args.extend(["--cap-add", capability])
        args.extend(self.policy())
        args.extend([image])
        return args

    def create_volume(self, run, suffix):
        name = "alopex-test-" + run.run_id + "-" + suffix
        require(self.inspect({"kind": "volume", "name": name}) is None, "STATE")
        run.intent("volume", name)
        self.call("volume", "create", "--label", OWNER,
                  "--label", "org.alopex.test-env.run=" + run.run_id,
                  "--label", "org.alopex.test-env.token=" + run.record["ownership_token"], name)
        live = self.inspect({"kind": "volume", "name": name})
        run.resource("volume", name, live["id"])
        return name

    def stop(self, name):
        live = self.inspect({"kind": "container", "name": name})
        if live and live["running"]:
            self.call("stop", "--time", str(self.profile["limits"]["stop_seconds"]), name)
        live = self.inspect({"kind": "container", "name": name})
        require(live is None or not live["running"], "CLEANUP")

    def validate_container(self, name):
        live = json.loads(self.call("container", "inspect", name))[0]
        host, config = live["HostConfig"], live["Config"]
        require(not host["Privileged"] and host["NetworkMode"] == "none", "POLICY")
        require(host["PidMode"] in ("", "private") and host["IpcMode"] == "private", "POLICY")
        require(not host.get("Devices") and not host.get("PortBindings") and not host.get("Binds"), "POLICY")
        normalize = lambda values: {v.removeprefix("CAP_").upper() for v in (values or [])}
        require(normalize(host["CapAdd"]) == set(CAPABILITIES), "POLICY")
        require(normalize(host["CapDrop"]) == {"ALL"}, "POLICY")
        require(config["User"] == "alopex", "POLICY")
        security = host.get("SecurityOpt") or []
        require(not any(value.startswith("no-new-privileges") for value in security), "POLICY")
        supplied = next((value.split("=", 1)[1] for value in security if value.startswith("seccomp=")), None)
        require(supplied is not None, "POLICY")
        from common import read_json
        require(json.loads(supplied) == read_json(self.profile["security"]["seccomp_profile"]), "POLICY")
        mounts = {m["Destination"]: m for m in live["Mounts"]}
        require(set(mounts) == {"/input", "/work"}, "POLICY")
        require(not mounts["/input"]["RW"] and mounts["/work"]["RW"], "POLICY")
        require(all(m["Type"] == "volume" for m in mounts.values()), "POLICY")
        limits = self.profile["limits"]
        require(host["Memory"] == limits["memory_bytes"] and
                host["MemorySwap"] == limits["memory_bytes"] + limits["swap_bytes"] and
                host["PidsLimit"] == limits["pids"], "POLICY")
        expected = ",".join(map(str, sorted(set(self.profile["cpu"]["server"] + self.profile["cpu"]["client"]))))
        require(host["CpusetCpus"] == expected, "POLICY")
        return live
