"""Observe a known-broken legacy reader without losing the upgrade fixture.

Only the old columnar self-read is diagnostic. Fixture creation and every
candidate assertion remain ordinary, required matrix phases.
"""

import json
from pathlib import Path
import subprocess
import sys


def observe(command, timeout=30):
    try:
        result = subprocess.run(
            [sys.executable, str(Path(__file__).resolve()), "--child", *command],
            capture_output=True,
            text=True,
            timeout=timeout,
        )
    except subprocess.TimeoutExpired:
        return {"status": "timeout", "timeout_seconds": timeout}
    if result.returncode:
        return {
            "status": "process_failed",
            "returncode": result.returncode,
            "stdout": result.stdout[-12000:],
            "stderr": result.stderr[-12000:],
        }
    try:
        observation = json.loads(result.stdout)
    except ValueError:
        return {
            "status": "invalid_report",
            "stdout": result.stdout[-12000:],
            "stderr": result.stderr[-12000:],
        }
    return {"status": "observed", "result": observation}


if __name__ == "__main__":
    import os
    import resource

    assert sys.argv[1] == "--child"
    # A SIGABRT must not leave a multi-GB core file on the CI host.
    resource.setrlimit(resource.RLIMIT_CORE, (0, 0))
    if sys.platform == "linux":
        # The legacy Linux decoder can interpret malformed lengths as a 42 GiB
        # allocation. Keep enough address space for Python/NumPy startup while
        # bounding that diagnostic process; do not constrain the candidate.
        limit = 4 * 1024**3
        _, hard = resource.getrlimit(resource.RLIMIT_AS)
        if hard != resource.RLIM_INFINITY:
            limit = min(limit, hard)
        resource.setrlimit(resource.RLIMIT_AS, (limit, limit))
    os.execvpe(sys.argv[2], sys.argv[2:], os.environ)
