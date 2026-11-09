"""Execute the fixed 20-row SQL preflight against the real candidate server."""
from __future__ import annotations

import importlib.util
import importlib.metadata
import json
import os
from pathlib import Path
import signal
import sys

ROOT = Path(__file__).resolve().parents[2]
WORK = Path(os.environ["ALOPEX_PREFLIGHT_WORK"])
sys.path.insert(0, str(ROOT / "scripts/ops/preflight"))
import parity_v0811_measure as harness
import server_compare


def load_fixtures():
    path = ROOT / "crates/alopex-py/tests/conftest.py"
    spec = importlib.util.spec_from_file_location("candidate_server_fixtures", path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def timeout(_signum, _frame):
    raise TimeoutError("20-row functional preflight exceeded 120 seconds")


def main():
    import alopex

    assert importlib.metadata.version("alopex") == "0.8.17"
    work = WORK / "server-fixture"
    work.mkdir(mode=0o700)
    fixtures = load_fixtures()
    server = fixtures._start_server(Path(os.environ["ALOPEX_SERVER_BIN"]), work, None)
    os.environ["ALOPEX_PERF_SERVER_URL"] = server.http_base
    signal.signal(signal.SIGALRM, timeout)
    signal.alarm(120)
    try:
        result = server_compare.preflight(harness, "alopex-server")
    finally:
        signal.alarm(0)
        server.stop()
    assert server.process.poll() is not None
    assert result["engine"] == "alopex-server" and result["server_client"] is True
    assert result["checks"] == ["insert_returning", "upsert_returning", "merge_results",
                                "ddl_rollback", "sequence_advances",
                                "complete_existing_workload_and_rollback_state"]
    assert result["result_sha256"] == os.environ["ALOPEX_PREFLIGHT_EXPECTED"]
    (WORK / "preflight-result.json").write_text(json.dumps({"rows": 20, "timing_samples": 0,
          "checks": result["checks"], "result_sha256": result["result_sha256"],
          "server_stopped": True}, sort_keys=True) + "\n")
    print(json.dumps({"status": "passed", "checks": len(result["checks"])}))


if __name__ == "__main__":
    main()
