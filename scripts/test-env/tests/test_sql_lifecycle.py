import json
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import entrypoint
import envctl
from common import Failure, atomic_json, read_json
from lifecycle import Run


class SQLLifecycle(unittest.TestCase):
    def setUp(self):
        previous = os.umask(0o077)
        self.addCleanup(os.umask, previous)
        self.temp = tempfile.TemporaryDirectory(prefix="alopex-sql-lifecycle-")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)

    def test_database_pages_and_sockets_are_not_result_collection_inputs(self):
        servers = []

        def server(directory, request, *args, state_directory):
            self.assertFalse(state_directory.is_relative_to(self.root / "results"))
            (state_directory / "data").mkdir()
            (state_directory / "data/wal").write_bytes(b"x" * 4096)
            (state_directory / "socket").mkdir()
            (directory / "server.log").write_text("started\n")
            instance = Mock()
            instance.child.pid = 1
            servers.append(instance)
            return instance

        def worker(argv, log, seconds, env):
            config = json.loads(Path(argv[-1]).read_text())
            self.assertFalse(Path(config["socket"]).is_relative_to(self.root / "results"))
            atomic_json(config["result"], {"status": "passed", "engine": config["engine"]})

        request = {"pids": 4, "cpu": {"client": [0]}, "limits": {"pids": 4, "case_seconds": 1}}
        with patch.object(entrypoint, "WORK", self.root), \
             patch("verify_environment.ownership"), \
             patch.object(entrypoint, "postgres", side_effect=server), \
             patch.object(entrypoint, "alopex", side_effect=lambda *a, **k: (server(*a, **k), {"http": 1})), \
             patch.object(entrypoint, "command", side_effect=worker):
            results = entrypoint.sql_case("S01", request, {}, Path("python"), {})
        self.assertEqual(len(results), 2)
        self.assertEqual(len(list((self.root / "databases").rglob("wal"))), 2)
        self.assertEqual(list((self.root / "results").rglob("wal")), [])
        self.assertEqual(len(list((self.root / "results").rglob("worker.json"))), 2)
        for instance in servers:
            instance.stop.assert_called_once()

    def test_postgres_stop_uses_its_actual_database_directory(self):
        logs, data = self.root / "results", self.root / "databases/pg/data"
        logs.mkdir()
        with patch.object(entrypoint.subprocess, "Popen") as launch, \
             patch.object(entrypoint, "command") as command:
            launch.return_value.poll.return_value = None
            launch.return_value.wait.return_value = 0
            server = entrypoint.Server(["postgres"], logs, {"limits": {"stop_seconds": 1}},
                                      postgres=True, data_directory=data)
            server.stop()
        argv = command.call_args.args[0]
        self.assertEqual(argv[argv.index("-D") + 1], str(data))

    def test_stage_stop_failure_does_not_replace_copy_failure(self):
        run = Run(self.root, 'stage-failure').create({})
        profile = {'assets': 'unused', 'platform': 'linux/test',
                   'limits': {'memory_bytes': 4096, 'pids': 4, 'case_seconds': 1}}
        docker, budget = Mock(), Mock()
        docker.prefix = ['docker']
        docker.policy.return_value = []
        docker.call.return_value = 'stage-id'
        docker.stop.side_effect = Failure('CLEANUP', 'stop failed')
        original = Failure('MEASUREMENT', 'copy measurement failed')
        budget.run.side_effect = original
        with patch.object(envctl, 'verify_assets', return_value={'files': []}), \
             patch.object(envctl, 'runtime_request', return_value={}):
            with self.assertRaises(Failure) as caught:
                envctl.stage(profile, docker, budget, run, 'image', 'input', 'work', ['E01'])
        self.assertIs(caught.exception, original)
        self.assertEqual(original.cleanup_errors[0]['operation'], 'stop_stage')
        self.assertEqual(original.cleanup_errors[0]['detail'], 'stop failed')
        self.assertFalse((run.root / 'bundle').exists())

    def test_run_preserves_original_failure_and_records_each_stop_failure(self):
        profile = {'private_output': str(self.root), 'run_id': 'run-failure', 'assets': 'unused',
                   'platform': 'linux/test', 'limits': {'run_seconds': 1}}
        docker, budget = Mock(), Mock()
        docker.prefix = ['docker']
        docker.arguments.return_value = ['create']
        docker.call.return_value = 'main-id'
        docker.stop.side_effect = Failure('CLEANUP', 'stop failed')
        original = Failure('MEASUREMENT', 'measurement failed')
        budget.run.side_effect = original
        def stage(p, d, b, run, *args):
            run.resource('container', 'stage', 'stage-id')
        with patch.object(envctl, 'verified_image', return_value='sha256:' + 'a' * 64), \
             patch.object(envctl, 'verify_assets', return_value={'files': [{'role': 'harness', 'sha256': 'b' * 64}]}), \
             patch.object(envctl, 'digest', return_value='c' * 64), \
             patch.object(envctl, 'source_identity', return_value={}), \
             patch.object(envctl, 'stage', side_effect=stage):
            with self.assertRaises(Failure) as caught:
                envctl.execute(profile, docker, budget, ['E01'])
        self.assertIs(caught.exception, original)
        run = Run(self.root, profile['run_id']).load()
        self.assertEqual(run.record['phase'], 'running')
        self.assertEqual(read_json(run.root / 'failure.json')['error_code'], 'MEASUREMENT')
        errors = read_json(run.root / 'cleanup-errors.json')
        self.assertEqual(len(errors), 2)
        self.assertTrue(all(row['error_code'] == 'CLEANUP' for row in errors))


if __name__ == "__main__":
    unittest.main()
