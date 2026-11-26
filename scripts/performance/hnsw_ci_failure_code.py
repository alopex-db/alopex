"""Emit one fixed, source-controlled HNSW CI failure group without test output."""

from pathlib import Path
import re
import sys

TESTS = Path(__file__).resolve().parent / "tests"
HEADER = re.compile(r"^(?:FAIL|ERROR): ([^\s(]+) \(([^)]+)\)")
UNKNOWN = "TEST_UNCLASSIFIED"


def classify(log: str, allowed: set[str]) -> str:
    for line in log.splitlines():
        match = HEADER.match(line)
        if match is None:
            continue
        location = match.group(2)
        module = (match.group(1) if location.startswith("unittest.loader._FailedTest.")
                  else location.split(".", 1)[0])
        if module == "test_phase_rss":
            return "TEST_PHASE_RSS"
        if module in allowed:
            return "TEST_" + module.removeprefix("test_").upper()
    return UNKNOWN


def main() -> None:
    code = UNKNOWN
    try:
        if len(sys.argv) == 2:
            path = Path(sys.argv[1])
            if path.is_file() and path.stat().st_size <= 1_000_000:
                allowed = {item.stem for item in TESTS.glob("test_issue681_*.py")
                           if len(item.stem) <= 64}
                code = classify(path.read_text(errors="replace"), allowed)
    except BaseException:
        pass
    print(code)


if __name__ == "__main__":
    main()
