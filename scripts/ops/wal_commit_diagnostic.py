"""Manual WAL diagnosis; traced durations are never performance acceptance.

Use strace -f -ttt -T -yy -s 128 -e trace=fsync,fdatasync,write,pwrite64,writev.
Raw paths and process records remain private. No process/VM setting is changed.
"""
from decimal import Decimal
import os
from pathlib import Path
import re

BEGIN = b"ALOPEX_COST_COMMIT_BEGIN\n"
END = b"ALOPEX_COST_COMMIT_END\n"
CALLS = ("fsync", "fdatasync", "write", "pwrite64", "writev", "pwritev", "pwritev2")
RSS = ("VmRSS", "RssAnon", "RssFile", "RssShmem")
ROLLUP = ("Rss", "Anonymous", "Pss_Anon", "Pss_File", "Pss_Shmem")


def require(value, message):
    if not value:
        raise ValueError(message)


def trace_command(command, destination):
    require(command and all(isinstance(arg, str) and arg for arg in command), "invalid child command")
    return ["strace", "-f", "-q", "-ttt", "-T", "-yy", "-s", "128", "-o", str(destination),
            "-e", "trace=" + ",".join(CALLS), "--", *command]


def fields(text, required):
    result = {}
    for line in text.splitlines():
        name, _, value = line.partition(":")
        if name not in required:
            continue
        require(name not in result and re.fullmatch(r"\s*\d+ kB\s*", value),
                "duplicate or invalid memory field")
        result[name] = int(value.split()[0]) * 1024
    require(set(result) == set(required), "missing memory field")
    return result


def memory(raw):
    status = fields(raw["status"], RSS)
    rollup = fields(raw["smaps_rollup"], ROLLUP)
    require(status["VmRSS"] == sum(status[k] for k in RSS[1:]), "RSS parts differ")
    require(rollup["Anonymous"] <= rollup["Rss"], "anonymous RSS exceeds total")
    return dict(status_bytes=status, smaps_rollup_bytes=rollup)


def snapshot():
    # Read our own records: no ptrace capability or cgroup delegation is needed.
    # Keep the raw readings; parse only after the second boundary to reduce work
    # between readings. The observer's own allocations still affect this process.
    return {name: Path("/proc/self", name).read_text()
            for name in ("status", "smaps_rollup")}


def observe(action, measure, *, read_memory=snapshot, write=os.write):
    before = read_memory()
    require(write(2, BEGIN) == len(BEGIN), "short begin marker")
    result = measure(action)
    require(write(2, END) == len(END), "short end marker")
    after = read_memory()
    parsed_before, parsed_after = memory(before), memory(after)
    return result, dict(
        schema="alopex.wal-commit-diagnostic/v1", raw_before=before, raw_after=after,
        before=parsed_before, after=parsed_after,
        delta_bytes={group: {key: parsed_after[group][key] - value for key, value in values.items()}
                     for group, values in parsed_before.items()},
        timer_nanoseconds=result[1], performance_acceptance=False,
        scope="pending public commit; memory and marker writes outside original timer",
        memory_scope="boundary snapshots, not peak or WAL allocations; includes observer allocations; "
                     "status RSS is approximate, smaps Anonymous is RSS, Pss_* is proportional memory")


def nanoseconds(value):
    number = Decimal(value) * 1_000_000_000
    require(number.is_finite() and number >= 0 and number == number.to_integral_value(),
            "invalid trace time")
    return int(number)


def events(text):
    """Reassemble interleaved -f output before deciding interval membership."""
    require(0 < len(text.encode()) <= 1_048_576, "trace size bound exceeded")
    pending, result = {}, []
    previous = {}
    for line in text.splitlines():
        match = re.fullmatch(r"(\d+)\s+(\d+\.\d+)\s+(.+)", line)
        require(match, "trace PID/timestamp missing")
        pid, timestamp, call = match.groups()
        pid, start = int(pid), nanoseconds(timestamp)
        require(start >= previous.get(pid, 0), "trace clock moved backwards")
        previous[pid] = start
        if call.startswith(("--- ", "+++ ")):
            continue
        resumed = re.fullmatch(r"<\.\.\. (\w+) resumed>(.*)", call)
        if resumed:
            require(pid in pending, "orphan resumed syscall")
            old_start, prefix = pending.pop(pid)
            require(prefix.startswith(resumed[1] + "("), "resumed syscall mismatch")
            start, call = old_start, prefix + resumed[2]
        elif call.endswith("<unfinished ...>"):
            require(pid not in pending and any(call.startswith(name + "(") for name in CALLS),
                    "unexpected unfinished syscall")
            pending[pid] = (start, call[:-len("<unfinished ...>")])
            continue
        else:
            require(pid not in pending, "unfinished syscall lost")
        parsed = re.fullmatch(r"(\w+)\((.*)\)\s+= (.*?)\s+<(\d+\.\d+)>", call)
        require(parsed and parsed[1] in CALLS, "unparsed syscall or missing duration")
        name, args, returned, duration = parsed.groups()
        args = args.strip()
        result.append(dict(pid=pid, start_ns=start, duration_ns=nanoseconds(duration),
                           syscall=name, args=args, result=returned))
    require(not pending, "truncated unfinished syscall")
    return sorted(result, key=lambda event: (event["start_ns"], event["pid"]))


def marker(event, value):
    escaped = value.decode().replace("\n", r"\n")
    return (event["syscall"] == "write" and
            re.fullmatch(r'2<[^>]+>, "' + re.escape(escaped) + r'", ' + str(len(value)), event["args"]))


def summarize_trace(text, database):
    """Return complete private events; unknown commit writes fail closed."""
    calls = events(text)
    begins = [e for e in calls if marker(e, BEGIN)]
    ends = [e for e in calls if marker(e, END)]
    require(len(begins) == len(ends) == 1, "commit marker count differs")
    begin, end = begins[0], ends[0]
    require(begin["pid"] == end["pid"] and begin["start_ns"] < end["start_ns"],
            "commit markers unordered or from different processes")
    require(begin["result"] == str(len(BEGIN)) and end["result"] == str(len(END)),
            "commit marker write failed")
    left = begin["start_ns"] + begin["duration_ns"]
    right = end["start_ns"]
    require(left <= right, "marker interval overlaps")
    prefix = str(database) + ".d/"
    selected = []
    for event in calls:
        if event is begin or event is end:
            continue
        start, finish = event["start_ns"], event["start_ns"] + event["duration_ns"]
        if finish < left or start > right:
            continue
        require(left <= start and finish <= right, "syscall crosses commit boundary")
        fd = re.match(r"\d+<([^>]+)>(?:, |$)", event["args"])
        require(fd and fd[1].startswith(prefix), "unclassified commit syscall target")
        suffix = fd[1][len(prefix):]
        if suffix == "lsm.wal":
            target = "wal"
        elif suffix == "overflow":
            target = "directory"
        elif re.fullmatch(r"overflow/[^/]+\.tmp", suffix):
            target = "spool"
        else:
            raise ValueError("unclassified commit syscall target")
        require(re.fullmatch(r"\d+", event["result"]), "syscall failure")
        if event["syscall"] in ("fsync", "fdatasync"):
            require(event["result"] == "0", "sync failed")
        elif event["syscall"] == "write":
            requested = re.search(r", (\d+)$", event["args"])
            require(requested and int(event["result"]) == int(requested[1]), "short write")
        # pwrite/writev are retained but cannot be silently treated as the expected
        # scalar-write path: a new codec path needs an explicit diagnostic review.
        else:
            raise ValueError("unexpected write syscall")
        selected.append(dict(event, target=target))
    require(selected and any(e["target"] == "wal" and e["syscall"] == "fdatasync"
                             for e in selected), "WAL synchronization missing")
    totals = {}
    for event in selected:
        key = event["target"] + "/" + event["syscall"]
        total = totals.setdefault(key, dict(count=0, duration_ns=0))
        total["count"] += 1
        total["duration_ns"] += event["duration_ns"]
    return dict(events=selected, totals=totals, marker_interval_ns=right-left, marker_pid=begin["pid"],
                performance_acceptance=False,
                scope="strace-observed syscall durations including tracing/scheduling effects; "
                      "cross-thread sums are not elapsed wall time or physical device latency")
