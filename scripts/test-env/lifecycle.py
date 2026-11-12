"""Durable run ownership and collection-before-deletion protocol."""
from __future__ import annotations

from pathlib import Path
import shutil
import stat
import secrets

from common import Failure, atomic_json, digest, private_dir, read_json, require


class Run:
    def __init__(self, output, run_id):
        self.root = Path(output) / run_id
        self.path = self.root / "run.json"
        self.run_id = run_id

    def create(self, identities):
        require(not self.root.exists(), "STATE")
        private_dir(self.root)
        self.record = {"schema": "alopex.test-env-run/v1", "run_id": self.run_id,
                       "phase": "preparing", "resources": [], "identities": identities,
                       "ownership_token": secrets.token_hex(16)}
        self.save()
        return self

    def load(self):
        self.record = read_json(self.path, private=True)
        require(self.record["run_id"] == self.run_id, "STATE")
        return self

    def save(self):
        atomic_json(self.path, self.record)

    def resource(self, kind, name, identity):
        require(kind in ("container", "volume") and identity, "STATE")
        for item in self.record["resources"]:
            if item["kind"] == kind and item["name"] == name:
                require(item["id"] in (None, identity), "STATE")
                item["id"] = identity
                break
        else:
            self.record["resources"].append({"kind": kind, "name": name, "id": identity})
        self.save()

    def intent(self, kind, name):
        require(not any(item["name"] == name for item in self.record["resources"]), "STATE")
        self.record["resources"].append({"kind": kind, "name": name, "id": None})
        self.save()

    def transition(self, phase):
        edges = {"preparing": {"running", "stopped"}, "running": {"stopped"},
                 "stopped": {"collected"}, "collected": {"deleted"}, "deleted": set()}
        require(phase in edges[self.record["phase"]], "STATE")
        self.record["phase"] = phase
        self.save()

    def collect(self, copier):
        require(self.record["phase"] in ("stopped", "collected"), "STATE")
        if self.record["phase"] == "collected":
            self.verify_collection()
            return
        staging = self.root / "collection.pending"
        if staging.exists():
            shutil.rmtree(staging)
        private_dir(staging)
        copier(staging)
        manifest = {}
        for path in sorted(staging.rglob("*")):
            info = path.lstat()
            require(stat.S_ISREG(info.st_mode) or stat.S_ISDIR(info.st_mode), "COLLECT")
            path.chmod(0o700 if path.is_dir() else 0o600)
            if path.is_file():
                manifest[str(path.relative_to(staging))] = digest(path)
        require(manifest and "result.json" in manifest, "COLLECT")
        # Verify JSON before publishing a collection receipt, even on failed runs.
        read_json(staging / "result.json")
        final = self.root / "collected"
        if final.exists():
            shutil.rmtree(final)  # Only an uncommitted local collection copy.
        staging.rename(final)
        atomic_json(self.root / "collection.json", manifest)
        self.transition("collected")

    def verify_collection(self):
        manifest = read_json(self.root / "collection.json", private=True)
        require(manifest, "COLLECT")
        from common import contained
        for name, expected in manifest.items():
            require(digest(contained(self.root / "collected", name)) == expected, "COLLECT")

    def clean(self, inspect, remove):
        if self.record["phase"] == "deleted":
            return
        require(self.record["phase"] == "collected", "STATE")
        self.verify_collection()
        # Inspect every surviving resource before deleting any resource.
        owned = []
        for item in self.record["resources"]:
            live = inspect(item)
            if live is None:  # Supports retry after a partially completed cleanup.
                # An unbound create may still publish later; absence alone is
                # not proof that this ownership intent has been settled.
                require(item["id"] is not None, "CLEANUP")
                continue
            require(live["id"] == item["id"] and
                    live["labels"].get("org.alopex.test-env.run") == self.run_id and
                    live["labels"].get("org.alopex.test-env.owner") == "envctl" and
                    live["labels"].get("org.alopex.test-env.token") == self.record["ownership_token"] and
                    not live.get("running", False), "CLEANUP")
            owned.append(item)
        for item in sorted(owned, key=lambda i: i["kind"] != "container"):
            remove(item)
        self.transition("deleted")
