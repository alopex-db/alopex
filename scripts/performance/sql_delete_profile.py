#!/usr/bin/env python3
"""Manual public SQL diagnosis for #712; not a product-comparison benchmark."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import random
import resource
import socket
import subprocess
import time
import urllib.error
import urllib.request


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def emit(value):
    print(json.dumps(value, allow_nan=False), flush=True)


def timed_operation(client, mode, key, operation, clock=time.perf_counter_ns):
    # A previous transaction must already be destroyed before entering here.
    started = clock()
    transaction = client.begin(mode)
    begun = clock()
    try:
        sql = (f"DELETE FROM bench_write WHERE id = {key}" if operation == "delete" else
               f"SELECT id, value FROM bench_write WHERE id = {key}")
        result = transaction.execute_sql(sql) if operation != "noop" else None
        executed = clock()
        transaction.commit()
        committed = clock()
    except BaseException:
        try:
            transaction.rollback()
        except Exception:
            pass
        raise
    finally:
        # Keep native/session destruction outside every measured interval.
        del transaction
    return result, dict(begin_ns=begun-started, execute_ns=executed-begun,
                        commit_ns=committed-executed, total_ns=committed-started)


def write(client, mode, sqls):
    transaction = client.begin(mode)
    try:
        for sql in sqls:
            transaction.execute_sql(sql)
        transaction.commit()
    except BaseException:
        try:
            transaction.rollback()
        except Exception:
            pass
        raise
    finally:
        del transaction


def validate_rows(actual, expected):
    require(actual == expected, "complete result mismatch")
    for row, wanted in zip(actual, expected):
        require(set(row) == set(wanted), "column mismatch")
        require(all(type(row[k]) is type(v) for k, v in wanted.items()), "value type mismatch")


def stop(process):
    if process.poll() is None:
        process.terminate()
        try:
            process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=10)


def start_server(binary, work):
    sockets = [socket.socket() for _ in range(3)]
    try:
        for sock in sockets:
            sock.bind(("127.0.0.1", 0))
        http, grpc, admin = [sock.getsockname()[1] for sock in sockets]
    finally:
        for sock in sockets:
            sock.close()
    config = work / "alopex.toml"
    config.write_text(f'http_bind = "127.0.0.1:{http}"\n'
                      f'grpc_bind = "127.0.0.1:{grpc}"\n'
                      f'admin_bind = "127.0.0.1:{admin}"\n'
                      f'data_dir = "{work / "data"}"\n'
                      'auth_mode = { type = "none" }\n')
    with (work / "server.stdout").open("w") as out, (work / "server.stderr").open("w") as err:
        process = subprocess.Popen([str(binary), "--config", str(config)], stdout=out, stderr=err)
    base = f"http://127.0.0.1:{http}"
    try:
        deadline = time.monotonic()+45
        while time.monotonic() < deadline:
            require(process.poll() is None, "server exited during startup")
            try:
                for url in [f"http://127.0.0.1:{admin}/healthz", base+"/api/admin/health"]:
                    with urllib.request.urlopen(url, timeout=1) as response:
                        require(response.status == 200, "server health failed")
                return process, base
            except (urllib.error.URLError, OSError):
                time.sleep(.1)
        raise RuntimeError("server startup deadline")
    except BaseException:
        stop(process)
        raise


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manual", action="store_true", required=True)
    parser.add_argument("--surface", choices=["embedded", "server"], required=True)
    parser.add_argument("--server-bin", type=Path)
    parser.add_argument("--work", type=Path, required=True)
    parser.add_argument("--rows", type=int, choices=[3, 1000, 10000], required=True)
    parser.add_argument("--operation", choices=["noop", "query", "delete"], default="delete")
    parser.add_argument("--profile", action="store_true", help="separate 4-second mixed operation/restore loop")
    args = parser.parse_args()
    require(not os.environ.get("CI") and not os.environ.get("GITHUB_ACTIONS"), "manual diagnosis forbidden in CI")
    require(not args.profile or args.operation == "delete", "profile covers the mixed DELETE/restore workload")
    require(args.surface != "server" or args.server_bin is not None, "server binary required")
    args.work.mkdir(parents=True, exist_ok=False)
    import alopex
    mode = alopex.TxnMode.READ_WRITE
    server = None
    client = None
    try:
        if args.surface == "server":
            server, url = start_server(args.server_bin.resolve(), args.work)
            client = alopex.connect(url, keep_alive=True, timeout=30)
            require(isinstance(client, alopex.RemoteDatabase), "embedded fallback forbidden")
        else:
            client = alopex.Database.open(str(args.work / "database"))
        emit(dict(event="started", pid=os.getpid(), server_pid=server.pid if server else None))
        client.execute_sql("CREATE TABLE bench_write (id INTEGER PRIMARY KEY, value TEXT)")
        expected = {key: f"{key:032x}" for key in range(args.rows)}
        write(client, mode, ["INSERT INTO bench_write (id, value) VALUES "+",".join(
            f"({key}, '{expected[key]}')" for key in range(start, min(start+1000, args.rows)))
            for start in range(0, args.rows, 1000)])
        emit(dict(event="ready", pid=os.getpid(), server_pid=server.pid if server else None,
                  surface=args.surface, rows=args.rows, operation=args.operation, profile=args.profile,
                  native_sha256=hashlib.sha256(Path(alopex._alopex.__file__).read_bytes()).hexdigest(),
                  driver_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest()))
        rng = random.Random(42)
        if args.profile:
            time.sleep(1)  # Let the external sampler attach after fixture setup.
        start = time.monotonic()
        iteration = 0
        pending_restore = None
        keys = []
        while True:
            if pending_restore is not None:
                key, value = pending_restore
                write(client, mode, [f"INSERT INTO bench_write VALUES ({key}, '{value}')"])
                expected[key] = value
                pending_restore = None
            key = rng.randrange(args.rows)
            keys.append(key)
            result, intervals = timed_operation(client, mode, key, args.operation)
            if not args.profile:
                emit(dict(event="sample", iteration=iteration, phase="warmup" if iteration < 3 else "measure",
                          key=key, **intervals))
            if args.operation == "delete":
                pending_restore = (key, expected.pop(key))
                validate_rows(client.execute_sql(f"SELECT id, value FROM bench_write WHERE id = {key}"), [])
            elif args.operation == "query":
                validate_rows(result, [dict(id=key, value=expected[key])])
            iteration += 1
            if (args.profile and time.monotonic()-start >= 4) or (not args.profile and iteration == 24):
                break
        # Reconnect/reopen before the final whole-dataset verification.
        client.close()
        del client
        client = None
        client = (alopex.connect(url, keep_alive=True, timeout=30) if args.surface == "server" else
                  alopex.Database.open(str(args.work / "database")))
        final = client.execute_sql("SELECT id, value FROM bench_write ORDER BY id")
        validate_rows(final, [dict(id=k, value=v) for k, v in sorted(expected.items())])
        emit(dict(event="complete", iterations=iteration, final_rows=len(final), result_sha256=digest(final),
                  input_keys_sha256=digest(keys), client_cpu_lifetime_seconds=sum(resource.getrusage(resource.RUSAGE_SELF)[:2]),
                  validation="per-operation result and full state after reconnect/reopen",
                  scope="manual internal diagnosis; not reference-product or published-version comparison"))
    finally:
        try:
            if client is not None:
                client.close()
        finally:
            if server is not None:
                stop(server)
                emit(dict(event="server_stopped", returncode=server.returncode,
                          server_cpu_lifetime_seconds=sum(resource.getrusage(resource.RUSAGE_CHILDREN)[:2])))


if __name__ == "__main__":
    main()
