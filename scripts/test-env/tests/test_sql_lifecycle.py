import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import entrypoint
from common import atomic_json


class SQLLifecycle(unittest.TestCase):
    def setUp(self):
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


if __name__ == "__main__":
    unittest.main()
