"""Run one private four-case on/off pair against one already qualified server.

The caller supplies the pinned harness, remote driver, observer, and unique raw
paths. Process isolation and the hard 300-second worker kill belong to its
supervisor; the deadline here also stops a pair between SQL operations.
"""
from contextlib import ExitStack
import json
import os
from pathlib import Path
import resource
import stat
import statistics
import time

from . import adapter, operation, oracle, public_timing

PAIR_LIMIT_SECONDS = 420


def _open_private_raw(path):
    """Create a private raw file relative to an owned, nonsymlink 0700 dir."""
    path = Path(path)
    directory = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        info = os.fstat(directory)
        if not stat.S_ISDIR(info.st_mode) or info.st_uid != os.geteuid() or \
                stat.S_IMODE(info.st_mode) != 0o700:
            raise ValueError("RAW_PARENT")
        fd = os.open(path.name, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
                     0o600, dir_fd=directory)
    finally:
        os.close(directory)
    try:
        os.fchmod(fd, 0o600)
        return os.fdopen(fd, "w", encoding="utf-8")
    except BaseException:
        os.close(fd)
        raise


def measure_pair(module, case, trial, raw_paths, *, connect, remote_type, url,
                 txn_mode=None, observer=None, timing_root=None,
                 pair_limit_seconds=PAIR_LIMIT_SECONDS, evidence_mode="public",
                 debug_root=None):
    """Return per-mode summaries and callback records; write 21 raw rows each.

    ``connect`` accepts ``(url, keep_alive=True, sql_timing_sink=...)``. Only
    the on connection prepares the fixture. The required observer has begin/end
    methods matching the frozen OperationResources boundary.
    """
    if case not in adapter.CASES or type(trial) is not int or trial not in range(3):
        raise ValueError("PAIR_IDENTITY")
    if type(raw_paths) is not dict or set(raw_paths) != {"on", "off"} or \
            len({str(Path(path).resolve()) for path in raw_paths.values()}) != 2:
        raise ValueError("RAW_PATHS")
    if not 0 < pair_limit_seconds <= PAIR_LIMIT_SECONDS:
        raise ValueError("PAIR_LIMIT")
    if observer is None or not callable(getattr(observer, "begin", None)) or \
            not callable(getattr(observer, "end", None)):
        raise ValueError("RESOURCE_OBSERVER")
    if case == "insert-rollback" and txn_mode is None:
        raise ValueError("TXN_MODE")
    if evidence_mode == "public":
        if timing_root is None or debug_root is not None:
            raise ValueError("TIMING_ROOT")
    elif evidence_mode == "debug":
        if timing_root is not None or debug_root is None or \
                os.environ.get("GITHUB_ACTIONS") == "true" or \
                os.environ.get("CI") == "true":
            raise ValueError("DEBUG_MODE")
        root = Path(debug_root)
        try:
            info = root.lstat()
        except OSError as error:
            raise ValueError("DEBUG_ROOT") from error
        if not root.is_absolute() or not stat.S_ISDIR(info.st_mode) or \
                info.st_uid != os.geteuid() or stat.S_IMODE(info.st_mode) != 0o700 or \
                any(Path(path).parent != root / mode for mode, path in raw_paths.items()):
            raise ValueError("DEBUG_ROOT")
    else:
        raise ValueError("EVIDENCE_MODE")
    started = time.monotonic()

    def deadline():
        if time.monotonic() - started >= pair_limit_seconds:
            raise TimeoutError("PAIR_LIMIT")

    cells = {mode: adapter.CellAdapter(case, trial, mode, oracle.operation)
             for mode in ("on", "off")}
    clients = {}
    primary_error = None
    try:
        for mode in adapter.pair_order(trial, 0):
            deadline()
            options = {"keep_alive": True}
            if mode == "on":
                options["sql_timing_sink"] = cells[mode].sink
            clients[mode] = connect(url, **options)
        if any(not isinstance(client, remote_type) for client in clients.values()):
            raise ValueError("REMOTE_REQUIRED")
        deadline()
        prepared = operation.prepare(case, module, clients["on"], cells["on"].setup,
                                     txn_mode=txn_mode)
        operations = {mode: (lambda mode=mode: prepared(clients[mode]))
                      for mode in ("on", "off")}
        for mode in adapter.pair_order(trial, 0):
            deadline()
            cells[mode].call("precheck", operations[mode])
            for index in range(3):
                deadline()
                cells[mode].call("warmup", operations[mode], sample=index)
        samples = {mode: [] for mode in clients}
        with ExitStack() as opened:
            streams = {mode: opened.enter_context(_open_private_raw(raw_paths[mode]))
                       for mode in clients}
            timing = ({mode: opened.enter_context(public_timing.TimingCellWriter(
                timing_root, case, trial, mode)) for mode in clients}
                      if evidence_mode == "public" else {})
            for index in range(21):
                for position, mode in enumerate(adapter.pair_order(trial, index)):
                    deadline()
                    before = observer.begin()
                    value, elapsed = cells[mode].call("measured", operations[mode],
                                                      sample=index, measure=True,
                                                      defer_oracle=True)
                    ordinal = 2 * index + position
                    if evidence_mode == "public":
                        timing[mode].append(index, ordinal, elapsed)
                    cells[mode].calls[-1]["pair_ordinal"] = ordinal
                    row = {"sample": index, "seconds": elapsed,
                           "pair_ordinal": ordinal}
                    try:
                        row["resources"] = observer.end(before)
                    except Exception:
                        row["resources"] = {"status": "failed", "error_code": "resource_capture_failed"}
                        streams[mode].write(json.dumps(row, allow_nan=False) + "\n")
                        streams[mode].flush()
                        raise
                    streams[mode].write(json.dumps(row, allow_nan=False) + "\n")
                    streams[mode].flush()
                    cells[mode].finalize_result(value)
                    samples[mode].append(elapsed)
        if case == "insert-rollback":
            for mode in adapter.pair_order(trial, 0):
                deadline()
                cells[mode].call("postcheck", lambda mode=mode: operation.postcheck(
                    case, clients[mode]))
        for cell in cells.values():
            cell.finish()
        if cells["on"].expected_checksum != cells["off"].expected_checksum:
            raise ValueError("PAIR_ORACLE")
        return ({mode: {"case": case, "engine": "alopex-server", "server_client": True,
                        "fixture_rows": 1 if case == "insert-rollback" else oracle.ROWS,
                        "warmups": 3, "samples": 21,
                        "result_sha256": cells[mode].expected_checksum,
                        "rollback_sha256": oracle.ROLLBACK_SHA256 if case == "insert-rollback" else None,
                        "latency_p50_ms": statistics.median(samples[mode]) * 1000,
                        "client_peak_rss_bytes": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * 1024}
                 for mode in clients}, {mode: cells[mode].calls for mode in clients})
    except BaseException as error:
        primary_error = error
        raise
    finally:
        cleanup_errors = []
        for client in clients.values():
            try:
                client.close()
            except BaseException as error:
                cleanup_errors.append(error)
        if cleanup_errors:
            if primary_error is not None:
                raise BaseExceptionGroup("PAIR_PRIMARY_AND_CLEANUP", [primary_error, *cleanup_errors]) \
                    from primary_error
            raise BaseExceptionGroup("PAIR_CLEANUP", cleanup_errors)
