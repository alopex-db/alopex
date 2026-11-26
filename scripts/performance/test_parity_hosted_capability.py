"""Focused preconstruction checks for the hosted parity capability probe."""

from contextlib import ExitStack, redirect_stderr
from io import StringIO
import json
from pathlib import Path
from unittest import TestCase
from unittest.mock import patch

from scripts.performance import parity_hosted_capability as target


SHA = "a" * 40
ENV = {
    "RUNNER_ENVIRONMENT": "github-hosted",
    "PARITY_ISSUE_NUMBER": "712",
    "GITHUB_EVENT_NAME": "workflow_dispatch",
    "GITHUB_SHA": SHA,
    "RUNNER_TEMP": "/runner-temp",
}
INFO = {
    "OSType": "linux", "Architecture": "x86_64", "CgroupVersion": "2",
    "SecurityOptions": ["name=apparmor", "name=seccomp,profile=builtin"],
}


class HostedCapabilityTest(TestCase):
    def setUp(self):
        self.stack = ExitStack()
        self.addCleanup(self.stack.close)
        self.stack.enter_context(patch.object(target.sys, "platform", "linux"))
        self.stack.enter_context(patch.object(target.platform, "machine", return_value="x86_64"))
        self.stack.enter_context(patch.object(target.platform, "release", return_value="test"))
        self.stack.enter_context(patch.dict(target.os.environ, ENV, clear=True))
        self.stack.enter_context(patch.object(target.subprocess, "check_output", return_value=SHA + "\n"))
        self.stack.enter_context(patch.object(Path, "read_text", autospec=True,
                                              side_effect=self.read_text))
        self.stack.enter_context(patch.object(Path, "is_file", return_value=True))
        self.sibling = self.stack.enter_context(patch.object(target, "sibling_io"))
        self.command = self.stack.enter_context(patch.object(target, "command", side_effect=self.command_result))
        self.endpoint = "unix:///var/run/docker.sock"

    def read_text(self, path):
        if str(path).startswith("/boot/config-"):
            return "CONFIG_TASK_IO_ACCOUNTING=y\n"
        if str(path) == "/sys/kernel/security/lsm":
            return "lockdown,apparmor,bpf\n"
        raise AssertionError("unexpected file read")

    def command_result(self, *args):
        if args == ("docker", "context", "show"):
            return "default"
        if args == ("docker", "context", "inspect", "default"):
            return json.dumps([{"Endpoints": {"docker": {"Host": self.endpoint}}}])
        if args == ("docker", "info", "--format", "{{json .}}"):
            return json.dumps(INFO)
        raise AssertionError("unexpected command")

    def test_observation_uses_only_read_only_docker_queries(self):
        target.probe()
        self.assertEqual([call.args for call in self.command.call_args_list], [
            ("docker", "context", "show"),
            ("docker", "context", "inspect", "default"),
            ("docker", "info", "--format", "{{json .}}"),
        ])
        self.sibling.assert_called_once()

    def test_wrong_job_or_missing_counter_stops_before_docker(self):
        target.os.environ["RUNNER_ENVIRONMENT"] = "self-hosted"
        with self.assertRaises(target.ProbeFailure) as wrong_job:
            target.probe()
        self.assertEqual(wrong_job.exception.code, "IDENTITY")
        self.command.assert_not_called()
        target.os.environ["RUNNER_ENVIRONMENT"] = "github-hosted"
        target.os.environ["PARITY_ISSUE_NUMBER"] = "804"
        with self.assertRaises(target.ProbeFailure) as wrong_issue:
            target.probe()
        self.assertEqual(wrong_issue.exception.code, "IDENTITY")
        self.command.assert_not_called()
        target.os.environ["PARITY_ISSUE_NUMBER"] = "712"
        self.sibling.side_effect = target.ProbeFailure("COUNTER")
        with self.assertRaises(target.ProbeFailure) as counter:
            target.probe()
        self.assertEqual(counter.exception.code, "COUNTER")
        self.command.assert_not_called()

    def test_remote_docker_endpoint_is_rejected(self):
        self.endpoint = "tcp://127.0.0.1:2375"
        with self.assertRaises(target.ProbeFailure) as remote:
            target.probe()
        self.assertEqual(remote.exception.code, "DOCKER")

    def test_public_failure_is_fixed_code(self):
        with patch.object(target, "probe", side_effect=target.ProbeFailure("DOCKER")):
            output = StringIO()
            with redirect_stderr(output):
                self.assertEqual(target.main(), 1)
        self.assertEqual(output.getvalue(), "PARITY_HOSTED_CAPABILITY_DOCKER\n")
