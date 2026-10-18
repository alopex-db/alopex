"""Public-API fork scenarios isolated from the pytest process."""
import gc
import os
import select
import signal
import subprocess
import sys
import time

from alopex import AlopexError, Database, TxnMode


def wait_child(pid):
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        waited, status = os.waitpid(pid, os.WNOHANG)
        if waited:
            assert os.WIFEXITED(status) and os.WEXITSTATUS(status) == 0, status
            return
        time.sleep(0.01)
    os.kill(pid, signal.SIGKILL)
    os.waitpid(pid, 0)
    raise AssertionError("fork child did not exit within 10 seconds")


def stop_child(pid):
    try:
        waited, _ = os.waitpid(pid, os.WNOHANG)
        if not waited:
            os.kill(pid, signal.SIGKILL)
            os.waitpid(pid, 0)
    except ChildProcessError:
        pass


def check_process(path, expected):
    result = subprocess.run(
        [sys.executable, __file__, expected, path],
        capture_output=True, text=True, timeout=10,
    )
    assert result.returncode == 0, result.stdout + result.stderr


def put(db, key, value):
    txn = db.begin(TxnMode.READ_WRITE)
    txn.put(key, value)
    txn.commit()


def check_open(path, expected):
    if expected == "locked":
        try:
            opened = Database.open(path)
        except AlopexError as error:
            assert "already open" in str(error), str(error)
        else:
            opened.close()
            raise AssertionError("independent process acquired the parent's database")
    else:
        opened = Database.open(path)
        try:
            with opened.begin(TxnMode.READ_ONLY) as txn:
                assert txn.get(b"before") == b"kept"
                if expected == "persisted":
                    assert txn.get(b"after") == b"also kept"
        finally:
            opened.close()


def child_drop(path, finish):
    db = Database.open(path)
    put(db, b"before", b"kept")
    pid = os.fork()
    if pid == 0:
        status = 0
        try:
            if finish == "close":
                db.close()
            else:
                del db
                gc.collect()
        except BaseException:
            status = 1
        os._exit(status)
    try:
        wait_child(pid)
        check_process(path, "locked")
        put(db, b"after", b"also kept")
    finally:
        stop_child(pid)
        db.close()
    check_process(path, "persisted")


def parent_close(path):
    db = Database.open(path)
    put(db, b"before", b"kept")
    ready_read, ready_write = os.pipe()
    resume_read, resume_write = os.pipe()
    pid = os.fork()
    if pid == 0:
        os.close(ready_read)
        os.close(resume_write)
        os.write(ready_write, b"1")
        readable, _, _ = select.select([resume_read], [], [], 10)
        os._exit(0 if readable and os.read(resume_read, 1) == b"1" else 1)
    os.close(ready_write)
    os.close(resume_read)
    reopened = None
    try:
        readable, _, _ = select.select([ready_read], [], [], 10)
        assert readable and os.read(ready_read, 1) == b"1", "child not ready"
        db.close()
        check_process(path, "reopened")
        reopened = Database.open(path)
        os.write(resume_write, b"1")
        wait_child(pid)
        check_process(path, "locked")
    finally:
        stop_child(pid)
        os.close(ready_read)
        os.close(resume_write)
        db.close()
        if reopened is not None:
            reopened.close()


if __name__ == "__main__":
    mode, database_path = sys.argv[1:]
    if mode in ("locked", "reopened", "persisted"):
        check_open(database_path, mode)
    elif mode == "parent_close":
        parent_close(database_path)
    else:
        child_drop(database_path, mode)
