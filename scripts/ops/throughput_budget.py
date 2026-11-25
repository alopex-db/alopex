"""Private, pure admission and checkpoint planning for complete release jobs.

Callers supply measured or representative-pilot observations for *every* stage.
This module never publishes diagnostics or guesses a rate from a timeout.
"""

from __future__ import annotations

from dataclasses import dataclass
import math


@dataclass(frozen=True)
class Stage:
    name: str
    total_units: int
    completed_units: int
    observed_units: int
    observed_seconds: float
    group_size: int = 1
    checkpoint_size: int | None = None


@dataclass(frozen=True)
class Budget:
    stages: tuple[Stage, ...]
    elapsed_seconds: float
    deadline_seconds: float
    setup_remaining_seconds: float
    audit_seconds: float
    cleanup_seconds: float
    margin_seconds: float
    conservative_fraction: float
    storage_used_bytes: int
    storage_remaining_peak_bytes: int
    storage_cap_bytes: int


@dataclass(frozen=True)
class Decision:
    proceed: bool
    code: str
    private_diagnostics: dict[str, object]


def _positive_int(value: object) -> bool:
    return type(value) is int and value > 0


def _nonnegative_int(value: object) -> bool:
    return type(value) is int and value >= 0


def _finite(value: object, *, positive: bool = False) -> bool:
    if type(value) not in (int, float):
        return False
    try:
        return math.isfinite(value) and (value > 0 if positive else value >= 0)
    except (OverflowError, ValueError):
        return False


def _stop(code: str, **private: object) -> Decision:
    return Decision(False, code, private)


def evaluate(budget: Budget) -> Decision:
    """Project the complete remaining job; return a fixed code and private detail.

    Stage observations may come from comparable source-bound runs or a bounded
    representative pilot. ``completed_units`` describes the *current* job.
    The caller must supply the stage manifest and observations before dispatch.
    """
    if (not isinstance(budget, Budget) or type(budget.stages) is not tuple
            or not budget.stages):
        return _stop("INVALID_PLAN")
    if (not all(_finite(value) for value in (
            budget.elapsed_seconds, budget.setup_remaining_seconds,
            budget.audit_seconds, budget.cleanup_seconds))
            or not all(_finite(value, positive=True) for value in (
                budget.deadline_seconds, budget.margin_seconds,
                budget.conservative_fraction))
            or budget.conservative_fraction > 1
            or not all(_nonnegative_int(value) for value in (
                budget.storage_used_bytes, budget.storage_remaining_peak_bytes))
            or not _positive_int(budget.storage_cap_bytes)):
        return _stop("INVALID_PLAN")

    names: set[str] = set()
    for stage in budget.stages:
        if not isinstance(stage, Stage):
            return _stop("INVALID_PLAN")
        checkpoint_size = (stage.group_size if stage.checkpoint_size is None
                           else stage.checkpoint_size)
        if (not isinstance(stage.name, str) or not stage.name or stage.name in names
                or not _positive_int(stage.total_units)
                or not _nonnegative_int(stage.completed_units)
                or stage.completed_units > stage.total_units
                or not _positive_int(stage.group_size)
                or stage.total_units % stage.group_size != 0
                or not _positive_int(checkpoint_size)
                or stage.group_size % checkpoint_size != 0):
            return _stop("INVALID_PLAN")
        names.add(stage.name)
        if stage.completed_units % checkpoint_size != 0:
            return _stop("INCOMPLETE_GROUP", stage=stage.name)
        if (not _positive_int(stage.observed_units)
                or not _finite(stage.observed_seconds, positive=True)):
            return _stop("RATE_MISSING", stage=stage.name)
        if stage.observed_units % stage.group_size != 0:
            return _stop("INCOMPLETE_GROUP", stage=stage.name)

    if (budget.storage_used_bytes + budget.storage_remaining_peak_bytes
            >= budget.storage_cap_bytes):
        return _stop("STORAGE_BUDGET", used_bytes=budget.storage_used_bytes,
                     remaining_peak_bytes=budget.storage_remaining_peak_bytes)

    remaining_seconds = (budget.setup_remaining_seconds + budget.audit_seconds
                         + budget.cleanup_seconds)
    stages: list[dict[str, object]] = []
    for stage in budget.stages:
        try:
            rate = stage.observed_units / stage.observed_seconds
            conservative_rate = rate * budget.conservative_fraction
        except (OverflowError, ZeroDivisionError):
            return _stop("RATE_MISSING", stage=stage.name)
        if not math.isfinite(rate) or not math.isfinite(conservative_rate) or conservative_rate <= 0:
            return _stop("RATE_MISSING", stage=stage.name)
        units_left = stage.total_units - stage.completed_units
        try:
            stage_seconds = units_left / conservative_rate
        except (OverflowError, ZeroDivisionError):
            return _stop("RATE_MISSING", stage=stage.name)
        if not math.isfinite(stage_seconds):
            return _stop("RATE_MISSING", stage=stage.name)
        remaining_seconds += stage_seconds
        stages.append({"name": stage.name, "remaining_units": units_left,
                       "observed_rate": rate, "conservative_rate": conservative_rate,
                       "projected_seconds": stage_seconds})

    projected_finish = budget.elapsed_seconds + remaining_seconds + budget.margin_seconds
    detail: dict[str, object] = {"projected_finish_seconds": projected_finish,
                                 "deadline_seconds": budget.deadline_seconds,
                                 "remaining_seconds": remaining_seconds,
                                 "stages": stages}
    if not math.isfinite(projected_finish) or projected_finish >= budget.deadline_seconds:
        return _stop("TIME_BUDGET", **detail)
    return Decision(True, "READY", detail)
