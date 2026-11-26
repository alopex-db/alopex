"""Join one hosted #681 build pair to one public completion-mode run."""

from __future__ import annotations

from pathlib import Path
import os
import stat
import tempfile
import time

import issue681_hosted_acquire_all as acquire_all
import issue681_hosted_build_join as build_join
import issue681_hosted_build_guard as capacity
import issue681_public_admission as admission
import issue681_public_pair as pair
import issue681_public_run as run
import issue681_public_stage_audit as stage_audit

ERROR = "PUBLIC_STAGE"
MEASURE_DIAG_CODES = frozenset({
    "MEASURE_ADMIT",
    "MEASURE_ADMISSION_GATE",
    "MEASURE_RUN_CASES",
    "MEASURE_ASSETS",
    "MEASURE_STAGE_ADMIT",
    "MEASURE_CASE_ADMIT",
    "MEASURE_CASE_RUN",
    "MEASURE_CASE_COMPLETE",
    "MEASURE_CASE_AUDIT",
    "MEASURE_PAIR_WRITE",
    "MEASURE_VERIFY_EXPORT",
})


def _set_diag(diag, code):
    if isinstance(diag, dict) and code in MEASURE_DIAG_CODES:
        diag["code"] = code


def require(value):
    if not value:
        raise ValueError(ERROR)


def _private_dir(path):
    build_join._private_dir(path)


def _disjoint(paths):
    require(all(left != right and left not in right.parents and
                right not in left.parents
                for index, left in enumerate(paths)
                for right in paths[index + 1:]))


def _admit(acquired, built, workspace, measure_parent, finish):
    require(type(acquired) is dict and set(acquired) ==
            build_join.ACQUIRED_KEYS and
            type(built) is dict and
            set(built) == {"pair", "build_task", "guard", "cargo_home"} and
            type(acquired["guard"]) is capacity.HostStorageGuard and
            built["guard"] is acquired["guard"] and
            built["cargo_home"] == acquired["cargo_home"] and
            type(built["pair"]) is dict and
            set(built["pair"]) == {"pins", "staging"} and
            finish in ("commit", "rollback"))
    pair._pins(built["pair"]["pins"])
    for arm in pair.ARMS:
        pin = built["pair"]["pins"][arm]
        source = build_join.acquire_source.PINS[arm]
        require(pin["source_commit"] == source["commit"] and
                pin["source_tree"] == source["tree"] and
                pin["example_sha256"] == source["example"] and
                pin["cargo_lock_sha256"] == source["lock"])
    for path in (acquired["task"], acquired["cargo_home"],
                 built["build_task"], built["pair"]["staging"],
                 measure_parent):
        _private_dir(path)
    require(isinstance(workspace, Path) and workspace.is_absolute() and
            workspace.is_dir() and not workspace.is_symlink() and
            workspace.resolve(strict=True) == workspace and
            not any(measure_parent.iterdir()) and
            built["pair"]["staging"].parent ==
            built["build_task"] / "bundles" and
            acquired["sources"] == {
                arm: acquired["task"] / "sources" / arm
                for arm in pair.ARMS})
    _disjoint((workspace, acquired["task"], acquired["cargo_home"],
               built["build_task"], measure_parent))
    for arm in pair.ARMS:
        _private_dir(built["pair"]["staging"] / arm)


def _guarded(guard, callback):
    def check(*args):
        guard()
        return callback(*args)
    return check


def measure(acquired, built, workspace: Path, measure_parent: Path,
            profile: dict, finish: str, *, on_substage=None, diag=None):
    """Return one audited public file, retaining private receipts separately."""
    owned = None
    identity = None
    substage = "admit"
    substage_started = time.monotonic()
    measure_diag = diag if isinstance(diag, dict) else ({} if on_substage is not None else None)

    def emit(name, status, detail=None):
        if on_substage is not None:
            elapsed_s = max(0, int(time.monotonic() - substage_started))
            on_substage(name, status, detail, elapsed_s)

    try:
        _set_diag(measure_diag, "MEASURE_ADMIT")
        _admit(acquired, built, workspace, measure_parent, finish)
        guard = acquired["guard"]
        guard()
        owned = Path(tempfile.mkdtemp(prefix="issue681-measure-",
                                      dir=measure_parent))
        identity = acquire_all._identity(owned)
        for name in ("work", "export", "admission"):
            (owned / name).mkdir(mode=0o700)
            _private_dir(owned / name)
        work, export, admitted = (owned / name for name in
                                  ("work", "export", "admission"))
        staged = built["pair"]["staging"]
        assets = {arm: staged / arm for arm in pair.ARMS}
        roots = (workspace, acquired["task"], acquired["cargo_home"],
                 built["build_task"], owned)
        protected = tuple(str(path) for path in roots)
        guard()
        substage = "admission_gate"
        substage_started = time.monotonic()
        _set_diag(measure_diag, "MEASURE_ADMISSION_GATE")
        with admission.Admission(profile, roots=roots, log_dir=admitted) as gate:
            emit("admission_gate", "passed")
            guard()
            substage = "cases"
            substage_started = time.monotonic()
            _set_diag(measure_diag, "MEASURE_RUN_CASES")
            run_kwargs = dict(
                pins=built["pair"]["pins"], asset_locations=assets,
                work=work, export=export, finish=finish,
                private_roots=protected, admission_dir=admitted,
                additional_bytes=profile["additional_bytes"],
                admit_stage=_guarded(guard, gate.admit_stage),
                admit_case=_guarded(guard, gate.admit_case),
                complete_case=_guarded(guard, gate.complete_case),
                inventory_roots=roots, host_guard=guard)
            if measure_diag is not None:
                run_kwargs["diag"] = measure_diag
            result = run._run_mode_fixed(**run_kwargs)
            emit("cases", "passed")
            guard()
            substage = "verify_export"
            substage_started = time.monotonic()
            _set_diag(measure_diag, "MEASURE_VERIFY_EXPORT")
            locations = [
                {"arm": arm, "rows": rows, "trial": trial,
                 "public": work / f"case-{position:02d}" / "public",
                 "private": work / f"case-{position:02d}" / "private"}
                for position, (rows, trial, arm) in enumerate(pair._order())]

            def independent_audit(value):
                return stage_audit.audit_stage(
                    value, case_locations=locations, asset_locations=assets,
                    pins=built["pair"]["pins"], finish=finish,
                    private_roots=protected, admission_dir=admitted,
                    additional_bytes=profile["additional_bytes"])

            require(pair.verify(export, pins=built["pair"]["pins"],
                                independent_audit=independent_audit) == result)
            emit("verify_export", "passed")
            guard()
        guard()
        if isinstance(measure_diag, dict):
            measure_diag.pop("code", None)
        return {"public": export / "public.json", "measure_task": owned,
                "finish": finish, "guard": guard}
    except BaseException:
        detail = None
        if isinstance(measure_diag, dict):
            code = measure_diag.get("code")
            if isinstance(code, str) and code in MEASURE_DIAG_CODES:
                detail = code
        emit(substage, "failed", detail or ERROR)
        if owned is not None:
            try:
                if identity is None:
                    require(owned.parent == measure_parent and
                            owned.name.startswith("issue681-measure-"))
                    _private_dir(measure_parent)
                    info = owned.lstat()
                    require(stat.S_ISDIR(info.st_mode) and
                            info.st_uid == os.geteuid() and
                            stat.S_IMODE(info.st_mode) == 0o700)
                    identity = (info.st_dev, info.st_ino)
                acquire_all._remove_owned(owned, identity)
            except BaseException:
                pass
        raise ValueError(ERROR) from None
