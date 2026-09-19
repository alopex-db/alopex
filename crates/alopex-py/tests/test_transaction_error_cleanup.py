import subprocess
import sys
import textwrap


def test_transaction_errors_release_native_locks_before_returning_to_python():
    scenario = textwrap.dedent(
        """
        from alopex import AlopexError, Database, TxnMode


        def assert_cleanup(statement, params, error_type):
            db = Database.new()
            db.execute_sql("CREATE TABLE t (id INTEGER)")
            try:
                with db.begin(TxnMode.READ_WRITE) as txn:
                    txn.execute_sql(statement, params)
            except error_type as exc:
                if error_type is AlopexError:
                    assert "ALOPEX-T001" in str(exc)
                    assert "type mismatch: expected INTEGER, found BIGINT" in str(exc)
            else:
                raise AssertionError(f"{error_type.__name__} was not raised")
            assert db.execute_sql("SELECT id FROM t") == []
            with db.begin(TxnMode.READ_WRITE) as txn:
                txn.execute_sql("INSERT INTO t VALUES (1)")
                txn.commit()
            assert db.execute_sql("SELECT id FROM t") == [{"id": 1}]


        assert_cleanup("INSERT INTO t VALUES (?)", [1, 2], ValueError)
        assert_cleanup("INSERT INTO t VALUES (?)", [3_000_000_000], AlopexError)

        db = Database.new()
        db.execute_sql("CREATE TABLE t (id INTEGER)")
        try:
            with db.begin(TxnMode.READ_WRITE) as txn:
                txn.execute_sql("INSERT INTO t VALUES (1)")
                raise RuntimeError("body failure")
        except RuntimeError:
            pass
        assert db.execute_sql("SELECT id FROM t") == []
        print("ERROR_CLEANUP_OK", flush=True)

        """
    )

    completed = subprocess.run(
        [sys.executable, "-c", scenario],
        capture_output=True,
        text=True,
        timeout=20,
        check=False,
    )
    assert completed.returncode == 0, completed.stderr
    assert completed.stdout.splitlines() == [
        "ERROR_CLEANUP_OK",
    ]
