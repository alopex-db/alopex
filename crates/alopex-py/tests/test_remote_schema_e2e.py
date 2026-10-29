"""One-row schema contracts shared by embedded and the real HTTP server."""
from __future__ import annotations

import uuid
import pytest
from alopex import AlopexError, Database, RemoteDatabase, TxnMode

pytestmark = pytest.mark.requires_server


@pytest.mark.parametrize("case", ["add_column", "drop_column", "column_type", "rename", "truncate", "view"])
@pytest.mark.parametrize("commit", [False, True])
def test_schema_transaction_matches_embedded(alopex_server, case, commit):
    name = f"schema_{uuid.uuid4().hex[:12]}"
    destination = name + "_changed"
    initial = [{"id": 1, "value": 7}]
    sql, expected, changed_name = {
        "add_column": (f"ALTER TABLE {name} ADD COLUMN label TEXT DEFAULT 'seed'", [{"id": 1, "value": 7, "label": "seed"}], name),
        "drop_column": (f"ALTER TABLE {name} DROP COLUMN value", [{"id": 1}], name),
        "column_type": (f"ALTER TABLE {name} ALTER COLUMN value SET DATA TYPE TEXT", [{"id": 1, "value": "7"}], name),
        "rename": (f"ALTER TABLE {name} RENAME TO {destination}", initial, destination),
        "truncate": (f"TRUNCATE TABLE {name}", [], name),
        "view": (f"CREATE VIEW {destination} AS SELECT id, value FROM {name}", initial, destination),
    }[case]
    embedded = Database.new()
    remote = RemoteDatabase(alopex_server.http_base)
    try:
        observed = []
        for surface in (embedded, remote):
            surface.execute_sql(f"CREATE TABLE {name} (id INTEGER, value INTEGER)")
            surface.execute_sql(f"INSERT INTO {name} VALUES (1, 7)")
            txn = surface.begin(TxnMode.READ_WRITE)
            finished = False
            try:
                txn.execute_sql(sql)
                inside = txn.execute_sql(f"SELECT * FROM {changed_name}")
                assert inside == expected
                if commit:
                    txn.commit()
                else:
                    txn.rollback()
                finished = True
            finally:
                if not finished:
                    txn.rollback()
            after_name = changed_name if commit else name
            after = surface.execute_sql(f"SELECT * FROM {after_name}")
            assert after == (expected if commit else initial)
            if not commit and case in ("rename", "view"):
                with pytest.raises(AlopexError):
                    surface.execute_sql(f"SELECT * FROM {destination}")
            observed.append((inside, after))
        assert observed[0] == observed[1]
    finally:
        remote.close()
        embedded.close()


@pytest.mark.parametrize("case", ["view_dependency", "indexed_column", "truncate_view"])
def test_schema_failure_matches_embedded_and_preserves_rows(alopex_server, case):
    name = f"schema_{uuid.uuid4().hex[:12]}"
    embedded = Database.new()
    remote = RemoteDatabase(alopex_server.http_base)
    try:
        errors = []
        for surface in (embedded, remote):
            surface.execute_sql(f"CREATE TABLE {name} (id INTEGER, value INTEGER)")
            surface.execute_sql(f"INSERT INTO {name} VALUES (1, 7)")
            if case == "indexed_column":
                surface.execute_sql(f"CREATE INDEX {name}_idx ON {name} (value)")
                sql, reason = f"ALTER TABLE {name} DROP COLUMN value", "referenced by index"
            else:
                surface.execute_sql(f"CREATE VIEW {name}_view AS SELECT id, value FROM {name}")
                sql, reason = (f"DROP TABLE {name}", "referenced by view") if case == "view_dependency" else (f"TRUNCATE TABLE {name}_view", "is a view")
            with pytest.raises(AlopexError, match=reason) as error:
                surface.execute_sql(sql)
            errors.append((type(error.value), error.value.code))
            assert surface.execute_sql(f"SELECT * FROM {name}") == [{"id": 1, "value": 7}]
        assert errors[0] == errors[1]
    finally:
        remote.close()
        embedded.close()
