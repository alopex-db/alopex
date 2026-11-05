"""Minimal SQL worker; fixture and result checks stay outside observation."""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import socket
import importlib.util

from common import atomic_json, require
from observe import Observation, process


def rows(result):
    if hasattr(result, "fetchall"):
        result = result.fetchall()
    return [(row["id"], row["value"]) if isinstance(row, dict) else tuple(row) for row in result]


def worker(config):
    os.sched_setaffinity(0, config["cpu"])
    require(os.sched_getaffinity(0) == set(config["cpu"]), "POLICY")
    engine, case = config["engine"], config["case"]
    if engine == "postgresql":
        import psycopg
        require(psycopg.__version__ == "3.2.10", "IDENTITY")
        connect = lambda: psycopg.connect(host=config["socket"], dbname="acceptance", user="alopex", autocommit=True)
        db = connect()
        execute = db.execute
        backend = db.execute("SELECT pg_backend_pid()").fetchone()[0]
    else:
        import alopex
        connect = lambda: alopex.connect(config["url"], keep_alive=True)
        db = connect()
        require(type(db).__name__ == "RemoteDatabase", "IDENTITY")
        execute, backend = db.execute_sql, None
    try:
        if case == "E05":
            if engine == "postgresql":
                require(db.execute("SHOW server_version").fetchone()[0].startswith("16.14"), "IDENTITY")
                for database, role in (("postgres", "alopex"), ("acceptance", "denied_role")):
                    try:
                        other = psycopg.connect(host=config["socket"], dbname=database, user=role, connect_timeout=2)
                    except psycopg.OperationalError:
                        pass
                    else:
                        other.close()
                        raise __import__("common").Failure("POLICY")
                with socket.socket() as sock:
                    sock.settimeout(1)
                    require(sock.connect_ex(("127.0.0.1", 5432)) != 0, "POLICY")
            with socket.socket() as sock:
                sock.settimeout(1)
                require(sock.connect_ex(("192.0.2.1", 443)) != 0, "POLICY")
            return {"engine": engine, "case": case, "status": "passed"}
        execute("CREATE TABLE acceptance_case (id INTEGER PRIMARY KEY, value TEXT)")
        execute("INSERT INTO acceptance_case VALUES (1, 'one')")
        if case == "S01":
            execute("INSERT INTO acceptance_case VALUES (2, NULL)")
        # Warm the same path before observing, without changing the fixture.
        rows(execute("SELECT id, value FROM acceptance_case ORDER BY id"))
        module_path = Path(os.environ["ALOPEX_TEST_RESOURCE_MODULE"])
        spec = importlib.util.spec_from_file_location("qualified_sql_resources", module_path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        start = process(config["server_pid"])["start_ticks"]
        qualified = module.OperationResources(config["server_pid"], start)
        observer = Observation(config["server_pid"], config["pids"], backend, qualified)
        observer.begin()
        if case == "S01":
            actual = rows(execute("SELECT id, value FROM acceptance_case ORDER BY id"))
        elif engine == "postgresql":
            execute("BEGIN")
            actual = rows(execute("INSERT INTO acceptance_case VALUES (2, 'added') RETURNING id, value"))
            execute("COMMIT" if case == "S02" else "ROLLBACK")
        else:
            txn = db.begin(alopex.TxnMode.READ_WRITE)
            actual = rows(txn.execute_sql("INSERT INTO acceptance_case VALUES (2, 'added') RETURNING id, value"))
            txn.commit() if case == "S02" else txn.rollback()
        observation = observer.end()
        expected = [(1, "one"), (2, None)] if case == "S01" else [(2, "added")]
        require(actual == expected and all(type(row[0]) is int for row in actual), "CASE")
    finally:
        db.close()
    db = connect()
    try:
        actual = rows(db.execute("SELECT id, value FROM acceptance_case ORDER BY id") if engine == "postgresql"
                      else db.execute_sql("SELECT id, value FROM acceptance_case ORDER BY id"))
        expected = [(1, "one"), (2, None)] if case == "S01" else (
            [(1, "one"), (2, "added")] if case == "S02" else [(1, "one")])
        require(actual == expected, "CASE")
    finally:
        db.close()
    return {"engine": engine, "case": case, "status": "passed", "resources": observation,
            "reconnected_rows": actual}


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("configuration")
    args = parser.parse_args()
    config = json.loads(Path(args.configuration).read_text())
    atomic_json(config["result"], worker(config))
