"""実 alopex-server に対する e2e。中核は「同じ SQL を両サーフェスで実行して
戻り値リスト全体を == で比較する」対照テスト（issue #182）。

`requires_server` マーカーが付いたテストは、サーバーバイナリが無い環境では
skip され、`ALOPEX_REQUIRE_SERVER_E2E=1` の CI では fail する（conftest 参照）。
"""

from __future__ import annotations

import datetime as _dt
import gc
import uuid

import pytest

from alopex import AlopexError, Database, RemoteDatabase, TxnMode

pytestmark = pytest.mark.requires_server


def _script(table: str):
    """DDL -> INSERT(全型) -> INSERT(NULL) -> SELECT -> UPDATE -> COUNT -> DELETE。"""
    stamp = _dt.datetime(2024, 5, 4, 3, 2, 1)
    return [
        (
            f"CREATE TABLE {table} ("
            "id INTEGER PRIMARY KEY, big BIGINT, ratio FLOAT, precise DOUBLE, "
            "label TEXT, flag BOOLEAN, at TIMESTAMP, embedding VECTOR(3))",
            None,
        ),
        (
            f"INSERT INTO {table} (id, big, ratio, precise, label, flag, at, embedding) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            # ratio / embedding には binary32 で表現しきれない 0.1 を入れる。
            # サーバーは f32 を「f32 として往復する最短表記」= 0.1 で送るので、
            # クライアントが binary32 へ戻さない限り組み込みの
            # f64::from(0.1f32) と一致しない（D19 を実際に踏むための値）。
            [1, 2**40, 0.1, 0.1, "a'b", True, stamp, [0.5, -1.5, 0.1]],
        ),
        (
            f"INSERT INTO {table} (id, big, ratio, precise, label, flag, at, embedding) "
            "VALUES (2, NULL, NULL, NULL, NULL, NULL, NULL, NULL)",
            None,
        ),
        (f"SELECT * FROM {table} ORDER BY id", None),
        (f"UPDATE {table} SET label = ? WHERE id = ?", ["updated", 1]),
        (f"SELECT COUNT(*) AS total FROM {table}", None),
        (f"DELETE FROM {table} WHERE id = ?", [2]),
        (f"SELECT id, label FROM {table} ORDER BY id", None),
    ]


def _run(surface, script):
    return [surface.execute_sql(sql, params) for sql, params in script]


def _table_name() -> str:
    return f"remote_{uuid.uuid4().hex[:12]}"


def test_same_sql_script_returns_identical_values_on_both_surfaces(alopex_server):
    table = _table_name()
    embedded = Database.new()
    remote = RemoteDatabase(alopex_server.http_base)
    try:
        embedded_results = _run(embedded, _script(table))
        remote_results = _run(remote, _script(table))
    finally:
        remote.close()
        embedded.close()

    assert remote_results == embedded_results
    # 3 分岐が全部登場していることを固定しておく（== が空振りしないため）。
    assert embedded_results[0] is None  # DDL
    assert embedded_results[1] == 1  # DML
    assert isinstance(embedded_results[3], list) and len(embedded_results[3]) == 2
    assert embedded_results[5] == [{"total": 2}]

    # FLOAT / VECTOR は f32 なので、Python float へ広げた値は binary32 由来の
    # 0.10000000149011612 になる。ここが 0.1 なら D19 の再狭化が抜けている。
    first_row = remote_results[3][0]
    narrowed = 0.10000000149011612
    assert first_row["ratio"] == narrowed
    assert first_row["precise"] == 0.1  # DOUBLE は f64 のまま
    assert first_row["embedding"] == [0.5, -1.5, narrowed]
    assert first_row["label"] == "a'b"  # 引用符のエスケープ
    assert first_row["big"] == 2**40
    assert first_row["flag"] is True
    assert remote_results[3][1] == {
        key: None for key in first_row if key != "id"
    } | {"id": 2}


def test_column_order_is_identical(alopex_server):
    table = _table_name()
    embedded = Database.new()
    remote = RemoteDatabase(alopex_server.http_base)
    try:
        for surface in (embedded, remote):
            surface.execute_sql(
                f"CREATE TABLE {table} (zeta TEXT, alpha INTEGER, mid BIGINT)"
            )
            surface.execute_sql(
                f"INSERT INTO {table} (zeta, alpha, mid) VALUES ('z', 1, 2)"
            )
        embedded_rows = embedded.execute_sql(f"SELECT * FROM {table}")
        remote_rows = remote.execute_sql(f"SELECT * FROM {table}")
    finally:
        remote.close()
        embedded.close()

    # dict.__eq__ は順序を無視するので列順は別 assert で押さえる。
    assert list(remote_rows[0].keys()) == list(embedded_rows[0].keys())
    assert list(embedded_rows[0].keys()) == ["zeta", "alpha", "mid"]


def test_placeholder_binding_is_identical(alopex_server):
    table = _table_name()
    embedded = Database.new()
    remote = RemoteDatabase(alopex_server.http_base)
    try:
        for surface in (embedded, remote):
            surface.execute_sql(f"CREATE TABLE {table} (id INTEGER PRIMARY KEY, name TEXT)")
            surface.execute_sql(f"INSERT INTO {table} (id, name) VALUES (?, ?)", [1, "o'ne"])
            surface.execute_sql(f"INSERT INTO {table} (id, name) VALUES (?, ?)", [2, "two"])

        # 引用符・行コメント・ブロックコメント内の `?` はプレースホルダではない。
        tricky = (
            f"SELECT id, name FROM {table} "
            "WHERE name = ? /* not a ? placeholder */ -- nor this ?\n"
            "ORDER BY id"
        )
        assert remote.execute_sql(tricky, ["o'ne"]) == embedded.execute_sql(
            tricky, ["o'ne"]
        )

        for params, exc_type in (
            ([1], ValueError),  # 個数不一致（プレースホルダ 2 個）
            ([1, b"blob"], NotImplementedError),  # BLOB リテラル未対応
            ([1, float("nan")], ValueError),  # 非有限 float
        ):
            sql = f"SELECT id FROM {table} WHERE id = ? AND name = ?"
            with pytest.raises(exc_type) as embedded_err:
                embedded.execute_sql(sql, params)
            with pytest.raises(exc_type) as remote_err:
                remote.execute_sql(sql, params)
            assert type(remote_err.value) is type(embedded_err.value)
            assert remote_err.value.code == embedded_err.value.code
    finally:
        remote.close()
        embedded.close()


#: サーバーのルーティング前段（async_plan_for_routing）が潰してしまうコード。
#: 詳細は下の test_planning_errors_lose_their_stable_code_on_the_server と
#: docs/python-server-client.md D20 を参照。
_ROUTING_FLATTENED_CODE = "ALOPEX-E999"


def test_error_type_and_code_match_across_surfaces(alopex_server):
    """実行中に起きるエラーは型も安定コードも両サーフェスで一致する（D11）。"""
    table = _table_name()
    embedded = Database.new()
    remote = RemoteDatabase(alopex_server.http_base)
    try:
        for surface in (embedded, remote):
            surface.execute_sql(f"CREATE TABLE {table} (id INTEGER PRIMARY KEY)")
            surface.execute_sql(f"INSERT INTO {table} (id) VALUES (1)")

        failures = [
            f"SELECT CAST('not-a-number' AS INTEGER) AS z FROM {table}",
            f"SELECT CAST('x' AS VECTOR(2)) AS z FROM {table}",
            f"SELECT 1 / 0 AS z FROM {table}",
        ]
        for sql in failures:
            with pytest.raises(AlopexError) as embedded_err:
                embedded.execute_sql(sql)
            with pytest.raises(AlopexError) as remote_err:
                remote.execute_sql(sql)
            # メッセージは比較しない。安定コードだけが契約（D11）。
            assert remote_err.value.code == embedded_err.value.code, sql
            assert remote_err.value.correlation_id
            assert remote_err.value.http_status >= 400
    finally:
        remote.close()
        embedded.close()


def test_planning_errors_lose_their_stable_code_on_the_server(alopex_server):
    """既知の非対称の固定（D20）。

    サーバーは `/sql` の前にルーティング用の解析を行い、その失敗を
    `ExecutorError::InvalidOperation` へ文字列化して包む
    （crates/alopex-sql/src/storage/async_storage.rs::async_plan_for_routing）。
    その結果、パース・カタログ・型検査のエラーはワイヤ上で ALOPEX-E999 に
    潰れ、本来の安定コードはメッセージの中にしか残らない。CLI 経由でも同じで、
    Python クライアントの問題ではない（クライアントはサーバーのコードを
    そのまま透過する契約 = D11）。

    サーバー側が修正されたらこのテストが落ちるので、そのとき
    test_error_type_and_code_match_across_surfaces へ統合すること。
    """
    table = _table_name()
    embedded = Database.new()
    remote = RemoteDatabase(alopex_server.http_base)
    try:
        for surface in (embedded, remote):
            surface.execute_sql(f"CREATE TABLE {table} (id INTEGER PRIMARY KEY)")

        planning_failures = [
            "SELEKT 1 FRUM nowhere",
            f"SELECT * FROM {table}_missing",
            f"CREATE TABLE {table} (id INTEGER PRIMARY KEY)",
        ]
        for sql in planning_failures:
            with pytest.raises(AlopexError) as embedded_err:
                embedded.execute_sql(sql)
            with pytest.raises(AlopexError) as remote_err:
                remote.execute_sql(sql)
            # 例外型は一致する。潰れるのはコードだけ。
            assert type(remote_err.value) is type(embedded_err.value)
            assert remote_err.value.code == _ROUTING_FLATTENED_CODE, sql
            assert embedded_err.value.code != _ROUTING_FLATTENED_CODE, sql
            # 本来のコードはメッセージにだけ残っている。
            assert embedded_err.value.code in str(remote_err.value), sql
    finally:
        remote.close()
        embedded.close()


def test_remote_transaction_commit_and_rollback(alopex_server):
    table = _table_name()
    remote = RemoteDatabase(alopex_server.http_base)
    try:
        remote.execute_sql(f"CREATE TABLE {table} (id INTEGER PRIMARY KEY)")

        txn = remote.begin()
        # D22: 組み込みの active な Transaction と同じ値になる。
        embedded = Database.new()
        try:
            embedded_txn = embedded.begin(TxnMode.READ_WRITE)
            assert txn.status == embedded_txn.status
            embedded_txn.rollback()
        finally:
            embedded.close()
        assert txn.status == {"state": "active", "stream_effect": "committable"}
        txn.execute_sql(f"INSERT INTO {table} (id) VALUES (1)")
        txn.rollback()
        assert txn.status["state"] == "rolled_back"
        assert remote.execute_sql(f"SELECT id FROM {table}") == []

        txn = remote.begin()
        txn.execute_sql(f"INSERT INTO {table} (id) VALUES (2)")
        txn.commit()
        assert txn.status["state"] == "committed"
        assert remote.execute_sql(f"SELECT id FROM {table} ORDER BY id") == [{"id": 2}]

        # with を抜けるとき未完了なら rollback（組み込み Transaction と同じ）。
        with remote.begin() as scoped:
            scoped.execute_sql(f"INSERT INTO {table} (id) VALUES (3)")
        assert remote.execute_sql(f"SELECT id FROM {table} ORDER BY id") == [{"id": 2}]

        with pytest.raises(AlopexError) as raised:
            txn.commit()
        assert raised.value.code == "ALOPEX-PY999"

        with pytest.raises(NotImplementedError) as unsupported:
            remote.begin(TxnMode.READ_ONLY)
        assert unsupported.value.code == "ALOPEX-PY204"

        assert remote.begin(TxnMode.READ_WRITE).session_id
    finally:
        remote.close()


def test_closing_mid_transaction_kills_the_server_session(alopex_server):
    """D21 の回帰: close() 後に session が commit 可能なまま残ってはいけない。

    修正前は `db.close()` が接続を落とすだけだったので、session id を知って
    いれば後から `/session/<id>/commit` が通り、書きかけの行が可視になった
    （組み込みの `PyTransaction::drop` は即 rollback する）。
    """
    table = _table_name()
    remote = RemoteDatabase(alopex_server.http_base)
    remote.execute_sql(f"CREATE TABLE {table} (id INTEGER PRIMARY KEY)")

    txn = remote.begin()
    session_id = txn.session_id
    txn.execute_sql(f"INSERT INTO {table} (id) VALUES (1)")
    remote.close()

    probe = RemoteDatabase(alopex_server.http_base)
    try:
        with pytest.raises(AlopexError):
            probe._session_action(session_id, "commit")
        assert probe.execute_sql(f"SELECT id FROM {table}") == []

        # ハンドルを捨てただけの場合もファイナライザが同じ後始末をする。
        gc_txn = probe.begin()
        gc_session = gc_txn.session_id
        gc_txn.execute_sql(f"INSERT INTO {table} (id) VALUES (2)")
        del gc_txn
        gc.collect()
        with pytest.raises(AlopexError):
            probe._session_action(gc_session, "commit")
        assert probe.execute_sql(f"SELECT id FROM {table}") == []
    finally:
        probe.close()


@pytest.mark.parametrize("sql", ["", "   ", "\n\t "])
def test_blank_sql_returns_none_on_both_surfaces(alopex_server, sql):
    """D23 の回帰: 空文はサーバーの INVALID_REQUEST ではなく None を返す。"""
    embedded = Database.new()
    remote = RemoteDatabase(alopex_server.http_base)
    try:
        assert remote.execute_sql(sql) == embedded.execute_sql(sql)
        assert remote.execute_sql(sql) is None
    finally:
        remote.close()
        embedded.close()


@pytest.mark.parametrize(
    "sql",
    [
        "PRAGMA cache_size = 16",
        "PRAGMA io_stats",
        "SELECT clear_cache() AS cleared",
        ";",
    ],
)
def test_known_value_divergences_are_pinned(alopex_server, sql):
    """docs/python-server-client.md「既知の値の差異」を固定する。

    どれもサーバー側のエンジンの穴で、クライアントの変換の穴ではない
    （D24）。サーバー側が直ったらこのテストが落ちて、ドキュメントの
    リストを消す番だと分かる。
    """

    def outcome(surface):
        try:
            return ("value", surface.execute_sql(sql))
        except AlopexError as exc:
            return ("error", exc.code)

    embedded = Database.new()
    remote = RemoteDatabase(alopex_server.http_base)
    try:
        assert outcome(remote) != outcome(embedded), (
            f"{sql!r} no longer diverges; drop it from the known-divergence "
            "list in docs/python-server-client.md"
        )
    finally:
        remote.close()
        embedded.close()


def test_connect_switches_surface_by_target(alopex_server, tmp_path):
    import alopex

    table = _table_name()

    def exercise(handle):
        handle.execute_sql(f"CREATE TABLE {table} (id INTEGER PRIMARY KEY, name TEXT)")
        handle.execute_sql(f"INSERT INTO {table} (id, name) VALUES (?, ?)", [7, "x"])
        return handle.execute_sql(f"SELECT id, name FROM {table}")

    embedded = alopex.connect(str(tmp_path / "connect-db"))
    remote = alopex.connect(alopex_server.http_base)
    try:
        assert exercise(remote) == exercise(embedded)
    finally:
        remote.close()
        embedded.close()


def test_remote_surface_reports_cluster_status_matching_fixture(alopex_server):
    from test_surface_consistency import (
        _assert_with_diff,
        _load_cluster_expected,
        _stable_cluster_status_fields,
    )

    expected = _load_cluster_expected()["server_cluster_status"]["single_node"]
    remote = RemoteDatabase(alopex_server.http_base)
    try:
        actual = _stable_cluster_status_fields(remote.cluster_status())
    finally:
        remote.close()
    _assert_with_diff(expected, actual)


def test_remote_sql_rows_match_cross_surface_fixture(alopex_server):
    from test_surface_consistency import _assert_with_diff, _load_expected

    table = _table_name()
    expected = _load_expected()["sql_rows"]
    remote = RemoteDatabase(alopex_server.http_base)
    try:
        remote.execute_sql(f"CREATE TABLE {table} (id INTEGER PRIMARY KEY, name TEXT)")
        remote.execute_sql(f"INSERT INTO {table} (id, name) VALUES (1, 'alpha')")
        remote.execute_sql(f"INSERT INTO {table} (id, name) VALUES (2, 'beta')")
        rows = remote.execute_sql(f"SELECT id, name FROM {table} ORDER BY id")
    finally:
        remote.close()
    assert len(rows) == len(expected)
    for expected_row, actual_row in zip(expected, rows):
        _assert_with_diff(expected_row, actual_row)


def test_api_key_authentication(alopex_server_with_auth, alopex_server_api_key):
    base = alopex_server_with_auth.http_base

    anonymous = RemoteDatabase(base)
    try:
        with pytest.raises(AlopexError) as raised:
            anonymous.execute_sql("SELECT 1 AS one")
    finally:
        anonymous.close()
    assert raised.value.code == "UNAUTHORIZED"
    assert raised.value.http_status == 401

    wrong = RemoteDatabase(base, api_key="not-the-key")
    try:
        with pytest.raises(AlopexError) as raised:
            wrong.execute_sql("SELECT 1 AS one")
    finally:
        wrong.close()
    assert raised.value.http_status == 401

    with RemoteDatabase(base, api_key=alopex_server_api_key) as authorized:
        assert authorized.execute_sql("SELECT 1 AS one") == [{"one": 1}]

    bearer = RemoteDatabase(
        base, headers={"Authorization": f"Bearer {alopex_server_api_key}"}
    )
    try:
        assert bearer.execute_sql("SELECT 1 AS one") == [{"one": 1}]
    finally:
        bearer.close()


def test_alternate_sql_path_is_the_same_handler(alopex_server):
    remote = RemoteDatabase(alopex_server.http_base, sql_path="/api/sql/query")
    try:
        assert remote.execute_sql("SELECT 1 AS one") == [{"one": 1}]
    finally:
        remote.close()


def test_routing_diagnostics_are_exposed_as_remote_only_state(alopex_server):
    remote = RemoteDatabase(alopex_server.http_base)
    try:
        remote.execute_sql("SELECT 1 AS one")
        assert isinstance(remote.last_routing_diagnostics, list)
        with pytest.raises(NotImplementedError) as raised:
            remote.routing_diagnostics()
    finally:
        remote.close()
    assert raised.value.code == "ALOPEX-PY204"
