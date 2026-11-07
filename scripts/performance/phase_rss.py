"""Process-scoped Linux RSS high-water marks for bounded manual measurements.

The child must be waiting at the runner's phase handshake. These are kernel
accounting values, not exact allocation peaks; the window includes the child's
end-event serialization. No cgroup or host configuration is changed.
"""
import os


class PhaseRssError(RuntimeError):
    pass


def parse_status(text):
    values = {}
    for line in text.splitlines():
        key, separator, value = line.partition(":")
        if key not in ("VmRSS", "VmHWM"):
            continue
        fields = value.split()
        if (not separator or key in values or len(fields) != 2 or
                not fields[0].isdigit() or fields[1] != "kB"):
            raise PhaseRssError("invalid RSS counter")
        values[key] = int(fields[0]) * 1024
    if set(values) != {"VmRSS", "VmHWM"} or values["VmHWM"] < values["VmRSS"]:
        raise PhaseRssError("missing or inconsistent RSS counters")
    return {"rss_bytes": values["VmRSS"], "high_water_bytes": values["VmHWM"]}


def start_ticks(text):
    # comm may contain spaces or parentheses; fields after its closing ')' start
    # at state (field 3), so starttime (field 22) has offset 19.
    prefix, closing, tail = text.rpartition(")")
    fields = tail.split()
    if not closing or "(" not in prefix or len(fields) < 20 or not fields[19].isdigit():
        raise PhaseRssError("invalid process identity")
    return int(fields[19])


class PhaseRss:
    def __init__(self, pid, proc_root="/proc"):
        if type(pid) is not int or pid <= 0:
            raise PhaseRssError("invalid child PID")
        self.directory = None
        self.active = None
        self.pending_observation = None
        try:
            self.directory = os.open(os.path.join(proc_root, str(pid)),
                                     os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
            if os.fstat(self.directory).st_uid != os.getuid():
                raise PhaseRssError("child must belong to runner UID")
            self.identity = start_ticks(self._read("stat"))
            self.snapshot()
        except BaseException as error:
            self.close()
            if not isinstance(error, Exception):
                raise
            raise PhaseRssError("RSS observation unavailable") from None

    def _read(self, name):
        try:
            fd = os.open(name, os.O_RDONLY | os.O_NOFOLLOW, dir_fd=self.directory)
            try:
                value = os.read(fd, 65537)
            finally:
                os.close(fd)
            if len(value) > 65536:
                raise PhaseRssError("oversized process record")
            return value.decode("utf-8", errors="replace")
        except OSError:
            raise PhaseRssError("process counter unavailable") from None

    def snapshot(self):
        if self.directory is None or start_ticks(self._read("stat")) != self.identity:
            raise PhaseRssError("process identity changed")
        result = parse_status(self._read("status"))
        if start_ticks(self._read("stat")) != self.identity:
            raise PhaseRssError("process identity changed")
        return result

    def begin(self, phase):
        if self.active is not None or not isinstance(phase, str) or not phase:
            raise PhaseRssError("invalid phase start")
        before_reset = self.snapshot()
        self.pending_observation = {"phase": phase, "before_reset": before_reset}
        try:
            fd = os.open("clear_refs", os.O_WRONLY | os.O_NOFOLLOW,
                         dir_fd=self.directory)
            try:
                if os.write(fd, b"5\n") != 2:
                    raise PhaseRssError("incomplete RSS reset")
            finally:
                os.close(fd)
        except OSError:
            raise PhaseRssError("RSS reset unavailable") from None
        after_reset = self.snapshot()
        self.pending_observation["after_reset"] = after_reset
        self.active = (phase, before_reset, after_reset)

    def finish(self, phase):
        if self.active is None or phase != self.active[0]:
            raise PhaseRssError("phase end does not match start")
        after = self.snapshot()
        self.pending_observation["after"] = after
        _, previous, before = self.active
        # VmHWM combines a stored high-water value with approximate RSS counters.
        # Releasing pages can lower the reported value without another reset.
        # Preserve both readings and their maximum rather than discarding a valid
        # interval or claiming that the final reading covers its starting RSS.
        decreased = after["high_water_bytes"] < before["high_water_bytes"]
        observed_peak = max(before["high_water_bytes"], after["high_water_bytes"])
        self.active = None
        self.pending_observation = None
        return {"phase": phase, "before_reset": previous, "after_reset": before,
                "after": after, "kernel_reported_peak_rss_bytes": observed_peak,
                "kernel_high_water_decreased": decreased,
                "aggregation": "maximum of reset and end kernel high-water readings",
                "scope": "child process between phase handshakes, including end-event serialization",
                "counter": "Linux VmHWM after clear_refs=5; kernel RSS accounting accuracy applies"}

    def close(self):
        if self.directory is not None:
            os.close(self.directory)
            self.directory = None
