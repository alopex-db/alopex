"""Retired verification launcher; retained to reject historical invocations."""
import sys


def main():
    print(
        "Refusing retired issue577 verification launcher: its repository-only "
        "storage measurement cannot prove the per-host Alopex storage budget. "
        "Before using verification_watchdog.run_guarded, inventory all Alopex "
        "worktrees, outputs, temporary directories and caches on the execution "
        "host (including Windows and WSL together), enforce the 40 GB stop "
        "threshold, and record the bounded command and cleanup plan.",
        file=sys.stderr,
    )
    return 125


if __name__ == "__main__":
    raise SystemExit(main())
