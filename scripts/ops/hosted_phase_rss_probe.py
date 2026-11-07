"""Verify #809 kernel counters inside the existing isolated image probe."""
import hashlib
import json
import os
from pathlib import Path
import sys

import hosted_native_image as image
import hosted_native_sql as sql


def configure():
    repo = Path(os.environ["GITHUB_WORKSPACE"])
    files = {name: (repo / "scripts/performance" / (name + ".py")).read_text()
             for name in ("phase_rss", "phase_rss_probe")}
    marker = "print(json.dumps(result, sort_keys=True))"
    if image.PROBE.count(marker) != 1:
        raise ValueError("probe contract changed")
    addition = ("import types\n"
                "_observer = types.ModuleType('phase_rss')\n"
                "exec(compile(" + repr(files["phase_rss"]) + ", 'phase_rss.py', 'exec'), _observer.__dict__)\n"
                "sys.modules['phase_rss'] = _observer\n"
                "_probe = {'__name__': 'phase_rss_probe'}\n"
                "exec(compile(" + repr(files["phase_rss_probe"]) + ", 'phase_rss_probe.py', 'exec'), _probe)\n"
                "result['phase_rss'] = _probe['run_probe']()\n" + marker)
    image.PROBE = image.PROBE.replace(marker, addition)
    return {name: hashlib.sha256(content.encode()).hexdigest() for name, content in files.items()}


def operation(repo, config):
    def run(profile, docker, budget, work, export, public):
        from common import read_json, require
        from lifecycle import Run
        probe = Run(profile["private_output"], profile["run_id"])
        result = read_json(probe.root / "probe-result.json")["evidence"]["phase_rss"]
        require(result["status"] == "passed" and result["child_reaped"] and
                result["exited_child_rejected"] and result["allocation_bytes"] == 8 * 1024 * 1024, "CASE")
        require(SOURCE_HASHES == config["phase_probe_hashes"], "IDENTITY")
        public.update(schema="alopex.native-phase-rss/v1", phase_kernel_rss=True,
                      probe_source=config["phase_probe_source"],
                      probe_controller=os.environ["GITHUB_SHA"], probe_sources=SOURCE_HASHES)
    return run


if __name__ == "__main__":
    try:
        SOURCE_HASHES = configure()
        sql.operation = operation
        raise SystemExit(sql.main())
    except SystemExit:
        raise
    except BaseException:
        print(json.dumps({"status": "failed", "error_code": "CONSTRUCTION"}))
        raise SystemExit(1)
