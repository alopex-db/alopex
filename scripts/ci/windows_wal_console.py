"""Emit UTF-8 CI log excerpts independently of the Windows console code page."""
import sys


def print_log_tail(contents):
    # The raw artifact is already UTF-8. A redirected Windows stdout can still
    # default to cp1252; displaying a compiler warning must not mask its exit code.
    sys.stdout.reconfigure(encoding="utf-8", errors="backslashreplace")
    print(contents[-6000:], flush=True)
