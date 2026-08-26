"""`alopex.connect()` の分岐と、両サーフェスの構造的一致（D3 / D4）。

RemoteDatabase は遅延接続なのでサーバーが無くても構築テストができる。
実サーバーを使った「同じコードが両サーフェスで同じ結果を返す」確認は
tests/test_remote_e2e.py::test_connect_switches_surface_by_target。
"""

from __future__ import annotations

import pytest

import alopex
from alopex import (
    Database,
    DatabaseLike,
    RemoteDatabase,
    RemoteTransaction,
    Transaction,
    TransactionLike,
    TxnMode,
)


def test_memory_target_is_the_default_and_opens_embedded():
    db = alopex.connect()
    try:
        assert isinstance(db, Database)
        assert db.execute_sql("SELECT 1 AS one") == [{"one": 1}]
    finally:
        db.close()

    db = alopex.connect(":memory:")
    try:
        assert isinstance(db, Database)
    finally:
        db.close()


def test_filesystem_path_and_file_url_open_embedded(tmp_path):
    path = tmp_path / "connect-target-db"
    db = alopex.connect(str(path))
    try:
        assert isinstance(db, Database)
        db.execute_sql("CREATE TABLE t (id INTEGER PRIMARY KEY)")
    finally:
        db.close()

    reopened = alopex.connect(path.as_uri())
    try:
        assert isinstance(reopened, Database)
    finally:
        reopened.close()


def test_http_targets_build_a_server_client():
    for url in ("http://127.0.0.1:8080", "https://alopex.example.com:8443"):
        client = alopex.connect(url)
        assert isinstance(client, RemoteDatabase)
        assert client.url == url


def test_server_options_reach_the_server_client():
    client = alopex.connect("http://127.0.0.1:8080", api_key="k", timeout=1.5)
    assert isinstance(client, RemoteDatabase)
    assert client.sql_path == "/sql"


def test_server_only_options_are_refused_for_embedded_targets(tmp_path):
    with pytest.raises(ValueError) as raised:
        alopex.connect(str(tmp_path / "db"), api_key="k")
    assert raised.value.code == "ALOPEX-PY205"


def test_thread_mode_still_reaches_the_embedded_constructor():
    db = alopex.connect(":memory:", thread_mode="single")
    try:
        assert repr(db.thread_mode) == "ThreadMode.SINGLE"
    finally:
        db.close()


def test_object_store_targets_are_explicitly_unimplemented():
    with pytest.raises(NotImplementedError) as raised:
        alopex.connect("s3://bucket/prefix")
    assert raised.value.code == "ALOPEX-PY204"


@pytest.mark.parametrize("target", ["ftp://host/db", "redis://host", ""])
def test_unusable_targets_are_value_errors(target):
    with pytest.raises(ValueError) as raised:
        alopex.connect(target)
    assert raised.value.code == "ALOPEX-PY205"


def test_both_surfaces_satisfy_database_like():
    embedded = Database.new()
    try:
        assert isinstance(embedded, DatabaseLike)
    finally:
        embedded.close()
    assert isinstance(RemoteDatabase("http://127.0.0.1:1"), DatabaseLike)
    assert issubclass(Database, DatabaseLike)
    assert issubclass(RemoteDatabase, DatabaseLike)


def test_both_transaction_surfaces_satisfy_transaction_like():
    db = Database.new()
    try:
        txn = db.begin(TxnMode.READ_WRITE)
        assert isinstance(txn, TransactionLike)
        txn.rollback()
    finally:
        db.close()
    assert issubclass(Transaction, TransactionLike)
    assert issubclass(RemoteTransaction, TransactionLike)


def test_public_surface_names_are_exported():
    for name in (
        "connect",
        "RemoteDatabase",
        "RemoteTransaction",
        "DatabaseLike",
        "TransactionLike",
    ):
        assert name in alopex.__all__
        assert hasattr(alopex, name)
