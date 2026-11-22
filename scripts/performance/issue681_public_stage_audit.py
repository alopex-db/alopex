"""Rejoin one selected job's thirty #681 public cases to private evidence."""

from __future__ import annotations

from pathlib import Path

import issue681_public_assets as assets
import issue681_public_audit as audit
import issue681_public_pair as pair


def require(value):
    if not value:
        raise ValueError("STAGE_AUDIT")


def _audit_stage(report, case_locations, asset_locations, pins, finish,
                 private_roots):
    pair._pins(pins)
    require(finish in ("commit", "rollback") and
            type(case_locations) is list and len(case_locations) == 30 and
            type(asset_locations) is dict and set(asset_locations) == set(pair.ARMS) and
            type(private_roots) is tuple and
            all(type(value) is str and value.startswith("/") and len(value) >= 8
                for value in private_roots))
    require(type(report) is dict and report == pair.assemble(
        [row["report"] for row in report["cases"]], pins, finish))
    admitted = {}
    for arm in pair.ARMS:
        root = asset_locations[arm]
        require(isinstance(root, Path) and root.is_absolute())
        admitted[arm] = assets.verify(root, pins[arm],
                                      private_roots=private_roots)
    for position, ((rows, trial, arm), location) in enumerate(
            zip(pair._order(), case_locations)):
        require(type(location) is dict and
                set(location) == {"arm", "rows", "trial", "public", "private"} and
                location["arm"] == arm and type(location["rows"]) is int and
                location["rows"] == rows and type(location["trial"]) is int and
                location["trial"] == trial and
                isinstance(location["public"], Path) and
                isinstance(location["private"], Path) and
                location["public"].is_absolute() and
                location["private"].is_absolute() and
                location["public"] != location["private"] and
                report["cases"][position]["arm"] == arm)
        reconstructed = audit.audit_case(
            location["public"], location["private"],
            admitted[arm]["binary"], pins[arm], rows, trial, finish)
        require(reconstructed == report["cases"][position]["report"])
    return True


def audit_stage(report, *, case_locations, asset_locations, pins, finish,
                private_roots):
    """Return true only after all exact assets and cases pass independent audit."""
    try:
        return _audit_stage(report, case_locations, asset_locations, pins,
                            finish, private_roots)
    except Exception:
        raise ValueError("STAGE_AUDIT") from None
