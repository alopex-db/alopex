"""Local, fail-closed orchestration of the fixed #712 SQL comparison.

The caller owns the reviewed source pins, same-job qualification, and the
container callbacks. This module neither chooses a host nor changes its
security contract. A declared bound is a hard timeout supplied to execute;
the 300/420 second limits are ceilings, not an implicit way to fit 52 runs
inside the 7,200 second matrix window.
"""
from __future__ import annotations

import math

from . import matrix, same_job, worker

MATRIX_SECONDS = 7200
WORKER_SECONDS = 300
RUN_SECONDS = 420


class OperationFailure(RuntimeError):
    """Failure with all completed, source-bound observations retained."""

    def __init__(self, phase, evidence, cause):
        super().__init__(f"{phase}: {cause}")
        self.phase = phase
        self.evidence = evidence
        self.cause = cause


def _require(condition, reason):
    if not condition:
        raise ValueError(reason)


def _identity(row):
    return (row["case"], row["trial"], row["subject"])


def run(*, qualification, subjects, product_manifests, destination_for,
        harness, harness_record, operators, prepare, execute, collect, cleanup,
        capacity, clock, declared_seconds):
    """Run four select-all preflights, then exactly the 48 planned cells.

    ``qualification`` is the output of ``same_job.attest_live`` from this job.
    ``subjects`` contains four independently reviewed pins. ``prepare`` is
    normally ``matrix.prepare_cell``; it receives its exact argument list.
    ``execute`` receives the staged assets and strict worker/run timeouts.
    ``collect`` returns a dict: preflight needs ``rows`` and ``result``;
    matrix cells also need ``cell_result``, ``raw`` and ``resource_digest``.
    ``cleanup`` is called even after failed execution or collection. All
    callbacks operate on caller-owned resources and must enforce their timeout.
    """
    evidence = {"preflights": [], "cells": [], "public_operations": [],
                "verified_runs": 0,
                "performance_acceptance": False}
    phase = "admission"
    try:
        _require(type(qualification) is dict and
                 qualification.get("qualification_cases") == sorted(same_job.qualification.CASES) and
                 type(qualification.get("job_id")) is str and
                 qualification["job_id"].isdecimal() and int(qualification["job_id"]) > 0 and
                 type(qualification.get("run_attempt")) is str and
                 qualification["run_attempt"].isdecimal() and
                 int(qualification["run_attempt"]) > 0,
                 "missing same-job qualification")
        _require(type(subjects) is dict and set(subjects) == set(matrix.SUBJECTS) and
                 type(product_manifests) is dict and
                 set(product_manifests) == set(matrix.SUBJECTS), "subject inventory mismatch")
        rows = matrix.plan()
        _require(len(rows) == 48 and len({_identity(row) for row in rows}) == 48,
                 "matrix plan mismatch")
        preflights = [next(row for row in rows if row["case"] == "select-all" and
                           row["trial"] == 0 and row["subject"] == subject)
                      for subject in matrix.SUBJECTS]
        schedule = [("preflight", row) for row in preflights] + [("cell", row) for row in rows]
        _require(type(declared_seconds) is dict and
                 set(declared_seconds) == {"preflight", "cell"}, "missing run bounds")
        for bound in declared_seconds.values():
            _require(type(bound) in (int, float) and math.isfinite(bound) and
                     0 < bound <= RUN_SECONDS, "invalid run bound")
        required = sum(declared_seconds[kind] for kind, _ in schedule)
        _require(required <= MATRIX_SECONDS, "declared matrix exceeds 7200 seconds")
        start = clock()
        _require(type(start) in (int, float) and math.isfinite(start), "invalid clock")
        for index, (kind, row) in enumerate(schedule):
            phase = f"{kind}:{row['id']}"
            now = clock()
            remaining = MATRIX_SECONDS - (now - start)
            future = sum(declared_seconds[item[0]] for item in schedule[index:])
            _require(type(now) in (int, float) and math.isfinite(now) and
                     now >= start and remaining >= future,
                     "insufficient matrix time for declared bounds")
            capacity(kind, row, future)
            run_bound = declared_seconds[kind]
            staged = None
            primary = None
            observed = None
            try:
                staged, config = prepare(product_manifests[row["subject"]],
                                         destination_for(kind, row), harness,
                                         harness_record, row, operators,
                                         subjects[row["subject"]], kind=kind)
                _require(type(config) is dict and
                         config.get("phase") == kind and
                         all(config.get(key) == value for key, value in row.items()),
                         "staged coordinate mismatch")
                execution = execute(staged, config, kind=kind,
                                    worker_timeout_seconds=min(WORKER_SECONDS, run_bound),
                                    run_timeout_seconds=run_bound)
                observed = collect(execution, kind, row)
                _require(type(observed) is dict, "missing collected result")
                if kind == "preflight":
                    checksum = worker.validate_exact("select-all", observed["result"],
                                                     observed["rows"])
                    evidence["preflights"].append({"subject": row["subject"],
                                                    "result_checksum": checksum})
                else:
                    worker.verify_cell(observed["cell_result"], observed["raw"],
                                       config, observed["resource_digest"])
                    public = observed["public"]
                    _require(type(public) is list and len(public) == 21 and
                             all(item == worker.public_record(config, sample,
                                 observed["raw"][sample]["seconds"],
                                 observed["raw"][sample]["resources"])
                                 for sample, item in enumerate(public)),
                             "public/private observation mismatch")
                    evidence["public_operations"].extend(public)
                    evidence["cells"].append(dict(row, status="passed",
                        result_checksum=worker.result_digest(row["case"]),
                        trial_median_seconds=observed["cell_result"]["latency_p50_ms"] / 1000))
            except Exception as error:
                primary = error
            finally:
                try:
                    cleanup(staged, kind, row)
                except Exception as error:
                    if primary is None:
                        primary = error
                    else:
                        primary = ExceptionGroup("execution and cleanup failed", [primary, error])
            if primary is not None:
                raise primary
            after = clock()
            _require(type(after) in (int, float) and math.isfinite(after) and
                     after >= now and after - now <= run_bound and
                     after - start <= MATRIX_SECONDS, "run or matrix deadline exceeded")
            evidence["verified_runs"] += 1
        phase = "summary"
        _require(len(evidence["preflights"]) == 4 and
                 {item["subject"] for item in evidence["preflights"]} == set(matrix.SUBJECTS),
                 "incomplete or duplicate preflights")
        _require(evidence["verified_runs"] == 52, "incomplete verified run schedule")
        _require(len(evidence["cells"]) == 48 and
                 [_identity(item) for item in evidence["cells"]] ==
                 [_identity(item) for item in rows], "incomplete or reordered matrix")
        _require(len(evidence["public_operations"]) == 1008,
                 "incomplete public observations")
        summary = matrix.summarize(evidence["cells"])
        _require(summary.get("performance_acceptance") is False,
                 "unexpected performance acceptance")
        return {"evidence": evidence, "summary": summary}
    except Exception as error:
        if isinstance(error, OperationFailure):
            raise
        raise OperationFailure(phase, evidence, error) from error
