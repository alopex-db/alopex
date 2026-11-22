"""Internal staged #681 public case controller.

The selected-host entry must supply concrete stage and case admission checks.
This module alone does not qualify an environment or publish a report.
"""

from __future__ import annotations

import os
from pathlib import Path
import stat

import issue681_public as case
import issue681_public_admission as admission
import issue681_public_assets as assets
import issue681_public_audit as audit
import issue681_public_pair as pair
import issue681_public_stage_audit as stage_audit

TIMEOUTS = {1_000: 120, 10_000: 300, 100_000: 1_800}
RSS_LIMIT = 4 * 1024 ** 3


def require(value):
    if not value:
        raise ValueError("PUBLIC_STAGE")


def _empty_private(directory):
    require(isinstance(directory, Path) and directory.is_absolute() and
            directory.is_dir() and not directory.is_symlink() and
            directory.stat().st_uid == os.geteuid() and
            stat.S_IMODE(directory.stat().st_mode) == 0o700 and
            not any(directory.iterdir()))


def _run_mode(*, pins, asset_locations, work, export, finish, private_roots,
              admission_dir, additional_bytes, admit_stage, admit_case,
              complete_case,
              inventory_roots=(), case_runner=case.run_case,
              host_guard=None):
    """Collect, audit and export exactly one completion mode in this job."""
    pair._pins(pins)
    require(finish in ("commit", "rollback") and
            type(asset_locations) is dict and set(asset_locations) == set(pair.ARMS) and
            type(private_roots) is tuple and callable(admit_stage) and
            callable(admit_case) and callable(complete_case) and
                callable(case_runner) and
                (host_guard is None or callable(host_guard)))
    _empty_private(work)
    _empty_private(export)
    require(work != export and work not in export.parents and
            export not in work.parents)
    protected_roots = tuple(dict.fromkeys((*private_roots, str(work),
                                          str(export), str(admission_dir),
                                          *(str(asset_locations[arm])
                                            for arm in pair.ARMS))))
    admitted = {arm: assets.verify(asset_locations[arm], pins[arm],
                                   private_roots=protected_roots)
                for arm in pair.ARMS}
    reports = []
    locations = []
    stage_rows = None
    for position, (rows, trial, arm) in enumerate(pair._order()):
        if rows != stage_rows:
            require(admit_stage(rows) is True)
            stage_rows = rows
        require(admit_case(rows, trial, arm) is True)
        root = work / f"case-{position:02d}"
        root.mkdir(mode=0o700)
        public = root / "public"
        private = root / "private"
        public.mkdir(mode=0o700)
        private.mkdir(mode=0o700)
        case_inputs = dict(
            binary=admitted[arm]["binary"], directory=public,
            private_dir=private, source_commit=pins[arm]["source_commit"],
            binary_sha256=pins[arm]["binary_sha256"], rows=rows,
            dimensions=384, trial=trial, finish=finish,
            timeout=TIMEOUTS[rows], rss_limit=RSS_LIMIT,
            inventory_roots=inventory_roots)
        if host_guard is not None:
            case_inputs["host_guard"] = host_guard
        report = case_runner(**case_inputs)
        require(type(report) is dict and report.get("status") == "measured")
        require(complete_case(rows, trial, arm) is True)
        require(audit.audit_case(public, private, admitted[arm]["binary"],
                                 pins[arm], rows, trial, finish) == report)
        reports.append(report)
        locations.append({"arm": arm, "rows": rows, "trial": trial,
                          "public": public, "private": private})
    def independent_audit(value):
        return stage_audit.audit_stage(
            value, case_locations=locations, asset_locations=asset_locations,
            pins=pins, finish=finish, private_roots=protected_roots,
            admission_dir=admission_dir, additional_bytes=additional_bytes)
    return pair.write(reports, pins, export, finish=finish,
                      independent_audit=independent_audit)


def _run_mode_fixed(**kwargs):
    """Keep the injectable stage engine private and its failures fixed."""
    try:
        return _run_mode(**kwargs)
    except Exception:
        raise ValueError("PUBLIC_STAGE") from None


def run_selected_mode(*, profile, pins, asset_locations, work, export,
                      admission_dir, finish, private_roots, extra_roots=()):
    """Bind actual host admission to the fixed public case controller."""
    try:
        require(type(asset_locations) is dict and
                set(asset_locations) == set(pair.ARMS) and
                isinstance(admission_dir, Path) and admission_dir.is_absolute() and
                admission_dir != export and admission_dir != work and
                admission_dir not in export.parents and
                export not in admission_dir.parents and
                admission_dir not in work.parents and
                work not in admission_dir.parents and
                type(extra_roots) is tuple and
                all(isinstance(root, Path) and root.is_absolute()
                    for root in extra_roots))
        roots = (work, export, admission_dir,
                 *(asset_locations[arm] for arm in pair.ARMS),
                 *extra_roots)
        with admission.Admission(profile, roots=roots,
                                 log_dir=admission_dir) as gate:
            return _run_mode_fixed(
                pins=pins, asset_locations=asset_locations, work=work,
                export=export, finish=finish, private_roots=private_roots,
                admission_dir=admission_dir,
                additional_bytes=profile["additional_bytes"],
                admit_stage=gate.admit_stage, admit_case=gate.admit_case,
                complete_case=gate.complete_case,
                inventory_roots=roots)
    except Exception:
        raise ValueError("PUBLIC_STAGE") from None
