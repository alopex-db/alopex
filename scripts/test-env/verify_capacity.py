"""Active watchdog faults using one 16 KiB fixture, never disk exhaustion."""
import os
from pathlib import Path
import shutil
import signal
import sys

from capacity import Budget, allocated_bytes
from common import Failure, private_dir, require

FIXTURE_BYTES = 16 * 1024


def active_faults(work):
    results = {}
    for mode, expected in (("capacity", "CAPACITY"), ("measurement", "MEASUREMENT")):
        root = private_dir(work / ("watchdog-" + mode))
        data = private_dir(root / "data")
        retained = root / "retained"
        retained.write_bytes(b"original")
        initial = allocated_bytes([data])
        config = {"roots": [str(data)], "stop_bytes": initial + FIXTURE_BYTES,
                  "reserve_bytes": 4096, "poll_seconds": 0.05,
                  "planned_bytes": {"probe": 8192}}
        budget = Budget(config)
        budget.before("probe")
        try:
            try:
                budget.run([sys.executable, __file__, mode, str(root)], timeout=5,
                           log=root / "child.log")
            except Failure as error:
                require(error.code == expected and not getattr(error, "cleanup_errors", []), "CASE")
            else:
                raise Failure("CASE")
            # The helper records its own PID before the fault. Budget.run must
            # already have waited for it, not merely sent a termination signal.
            pid = int((root / "pid").read_text())
            try:
                os.waitpid(pid, os.WNOHANG)
            except ChildProcessError:
                pass
            else:
                raise Failure("CLEANUP")
            require(retained.read_bytes() == b"original", "CASE")
            if mode == "capacity":
                require((data / "fixture").stat().st_size == FIXTURE_BYTES, "CASE")
                require(allocated_bytes([data]) + config["reserve_bytes"] >= config["stop_bytes"], "CASE")
            else:
                require(not data.exists(), "CASE")
            results[mode] = {"error_code": expected, "reaped": True, "original_data_retained": True}
        finally:
            shutil.rmtree(root)
    return {"kind": "live_watchdog_faults", "maximum_fixture_bytes": FIXTURE_BYTES, "cases": results}


if __name__ == "__main__":
    os.umask(0o077)
    mode, root = sys.argv[1], Path(sys.argv[2])
    (root / "pid").write_text(str(os.getpid()))
    if mode == "capacity":
        # Publish a complete fsynced fixture into the watched directory. No
        # partial-file timing race is needed to trigger the small threshold.
        fixture = root / "fixture.pending"
        with fixture.open("xb") as stream:
            stream.write(bytes(range(256)) * (FIXTURE_BYTES // 256))
            stream.flush()
            os.fsync(stream.fileno())
        fixture.rename(root / "data/fixture")
    elif mode == "measurement":
        (root / "data").rmdir()
    else:
        raise Failure("INPUT")
    signal.pause()
