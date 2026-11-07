"""Small manual Linux counter probe; no database or performance workload."""
import argparse
import json
import os
import selectors
import subprocess
import sys

from phase_rss import PhaseRss, PhaseRssError

ALLOCATION_BYTES = 8 * 1024 * 1024
CHILD = r'''
import mmap, os, sys
print("ready", flush=True)
assert sys.stdin.readline().strip() == "continue"
size = int(sys.argv[1])
value = mmap.mmap(-1, size)
for offset in range(0, size, os.sysconf("SC_PAGE_SIZE")):
    value[offset] = 1
value.close()
print("released", flush=True)
assert sys.stdin.readline().strip() == "continue"
print("empty", flush=True)
assert sys.stdin.readline().strip() == "continue"
'''


def run_probe():
    if sys.platform != "linux" or os.getuid() == 0:
        raise PhaseRssError("normal Linux user required")
    child = subprocess.Popen([sys.executable, "-B", "-c", CHILD, str(ALLOCATION_BYTES)],
                             stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                             stderr=subprocess.DEVNULL, text=True, bufsize=1)
    observer = None
    selector = selectors.DefaultSelector()
    selector.register(child.stdout, selectors.EVENT_READ)

    def receive(expected):
        if not selector.select(timeout=10) or child.stdout.readline().strip() != expected:
            raise PhaseRssError("probe handshake failed")

    def resume():
        child.stdin.write("continue\n")
        child.stdin.flush()

    try:
        receive("ready")
        observer = PhaseRss(child.pid)
        observer.begin("transient")
        resume()
        receive("released")
        transient = observer.finish("transient")
        # The mapping was freed before observation: a current-RSS-only sampler
        # cannot supply the same peak. Use a wide margin for RSS accounting.
        if (transient["kernel_reported_peak_rss_bytes"] -
                transient["after"]["rss_bytes"] < ALLOCATION_BYTES // 2):
            raise PhaseRssError("transient allocation not captured")
        observer.begin("empty")
        resume()
        receive("empty")
        empty = observer.finish("empty")
        if (empty["before_reset"]["high_water_bytes"] -
                empty["after_reset"]["high_water_bytes"] < ALLOCATION_BYTES // 2):
            raise PhaseRssError("RSS high-water reset not observed")
        resume()
        if child.wait(timeout=5) != 0:
            raise PhaseRssError("probe child failed")
        try:
            observer.snapshot()
        except PhaseRssError:
            pass
        else:
            raise PhaseRssError("exited child still accepted")
        return {"status": "passed", "allocation_bytes": ALLOCATION_BYTES,
                "transient": transient, "empty": empty, "child_reaped": True,
                "exited_child_rejected": True}
    finally:
        if child.poll() is None:
            child.terminate()
            try:
                child.wait(timeout=5)
            except subprocess.TimeoutExpired:
                child.kill()
                child.wait(timeout=5)
        if observer is not None:
            observer.close()
        selector.close()
        child.stdin.close()
        child.stdout.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manual", action="store_true", required=True)
    parser.parse_args()
    try:
        print(json.dumps(run_probe(), sort_keys=True))
    except Exception:
        print(json.dumps({"status": "failed", "error_code": "PHASE_RSS"}))
        raise SystemExit(1)
