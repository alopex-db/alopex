"""RemoteDatabase の単体テスト。サーバーバイナリは不要で、スタブ HTTP と
生ソケットのサーバーだけを使う（cargo ビルド不要なので速い）。

実サーバーとの突き合わせは tests/test_remote_e2e.py。
"""

from __future__ import annotations

import gc
import json
import socket
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

import alopex
from alopex import AlopexError, Database, RemoteDatabase, RemoteTransaction, Transaction


# ---------------------------------------------------------------------------
# スタブ HTTP サーバー
# ---------------------------------------------------------------------------


class StubState:
    def __init__(self):
        self.status = 200
        self.body = {"columns": [], "rows": [], "affected_rows": None}
        self.raw_body = None
        self.delay = 0.0
        self.requests = []
        self.connections = 0


class _StubHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *_args):  # noqa: D401 - silence the test server
        pass

    def _respond(self):
        state = self.server.state  # type: ignore[attr-defined]
        length = int(self.headers.get("Content-Length") or 0)
        payload = self.rfile.read(length) if length else b""
        state.requests.append(
            {
                "method": self.command,
                "path": self.path,
                "headers": dict(self.headers),
                "body": json.loads(payload) if payload else None,
            }
        )
        if state.delay:
            time.sleep(state.delay)
        if state.raw_body is not None:
            body = state.raw_body.encode("utf-8")
            content_type = "text/plain"
        else:
            body = json.dumps(state.body).encode("utf-8")
            content_type = "application/json"
        self.send_response(state.status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    do_GET = _respond
    do_POST = _respond


class _StubServer(ThreadingHTTPServer):
    daemon_threads = True

    def __init__(self, address, handler, state):
        self.state = state
        super().__init__(address, handler)

    def get_request(self):
        request = super().get_request()
        self.state.connections += 1
        return request


@pytest.fixture()
def stub():
    state = StubState()
    server = _StubServer(("127.0.0.1", 0), _StubHandler, state)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    state.url = f"http://127.0.0.1:{server.server_address[1]}"
    try:
        yield state
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


def _query_body(columns, rows):
    return {
        "columns": [{"name": name, "data_type": "TEXT"} for name in columns],
        "rows": rows,
        "affected_rows": None,
    }


# ---------------------------------------------------------------------------
# 値の正規化（D5 / D17 / D19）
# ---------------------------------------------------------------------------


VALUE_CASES = [
    ("Null", None),
    ({"Integer": 1}, 1),
    ({"BigInt": 2**40}, 2**40),
    # serde_json は f32 を「f32 として往復する最短表記」で書くので、binary32 へ
    # 戻してから広げると組み込みの f64::from(f32) とビット単位で一致する（D19）。
    ({"Float": 0.1}, 0.10000000149011612),
    ({"Double": 0.1}, 0.1),
    ({"Text": "x"}, "x"),
    ({"Blob": [1, 2, 255]}, b"\x01\x02\xff"),
    ({"Boolean": True}, True),
    ({"Timestamp": 1736937000123456}, 1736937000123456),
    ({"Vector": [0.25, -1.5]}, [0.25, -1.5]),
]


@pytest.mark.parametrize("wire,expected", VALUE_CASES)
def test_sql_value_wire_forms_unwrap_to_embedded_values(stub, wire, expected):
    # BLOB は SQL パーサーが BLOB リテラルを持たないため e2e で往復できない。
    # {"Blob": [...]} -> bytes の変換はこの単体テストが唯一のカバレッジ。
    stub.body = _query_body(["v"], [[wire]])
    with RemoteDatabase(stub.url) as db:
        rows = db.execute_sql("SELECT v FROM t")
    assert rows == [{"v": expected}]
    assert type(rows[0]["v"]) is type(expected)


BAD_VALUE_CASES = [
    {"Decimal": 1},  # 未知タグ
    {"Integer": 1, "Text": "x"},  # 単一キーでない
    "Unknown",  # 未知 unit variant
    {"Double": None},  # 非有限 float が null に潰れている（D17）
    {"Float": None},
    {"Vector": [None]},
    123,  # タグなし
]


@pytest.mark.parametrize("wire", BAD_VALUE_CASES)
def test_unknown_sql_value_wire_forms_are_protocol_errors(stub, wire):
    stub.body = _query_body(["v"], [[wire]])
    with RemoteDatabase(stub.url) as db:
        with pytest.raises(AlopexError) as raised:
            db.execute_sql("SELECT v FROM t")
    assert raised.value.code == "ALOPEX-PY203"


# ---------------------------------------------------------------------------
# 戻り値の 3 分岐（D6）
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "body,expected",
    [
        (
            _query_body(["id", "name"], [[{"Integer": 1}, {"Text": "a"}]]),
            [{"id": 1, "name": "a"}],
        ),
        ({"columns": [], "rows": [], "affected_rows": 3}, 3),
        ({"columns": [], "rows": [], "affected_rows": 0}, 0),
        ({"columns": [], "rows": [], "affected_rows": None}, None),
    ],
)
def test_result_shape_matches_the_cli_branch_order(stub, body, expected):
    stub.body = body
    with RemoteDatabase(stub.url) as db:
        assert db.execute_sql("SELECT 1") == expected


def test_zero_affected_rows_is_not_confused_with_ddl(stub):
    stub.body = {"columns": [], "rows": [], "affected_rows": 0}
    with RemoteDatabase(stub.url) as db:
        result = db.execute_sql("DELETE FROM t")
    assert result == 0
    assert result is not None


def test_column_order_and_duplicate_columns_match_embedded(stub):
    stub.body = _query_body(
        ["zeta", "alpha", "zeta"],
        [[{"Text": "first"}, {"Integer": 2}, {"Text": "last"}]],
    )
    with RemoteDatabase(stub.url) as db:
        rows = db.execute_sql("SELECT zeta, alpha, zeta FROM t")
    # PyDict::set_item と同じく、位置は初出、値は後勝ち。
    assert list(rows[0].keys()) == ["zeta", "alpha"]
    assert rows[0]["zeta"] == "last"


def test_row_arity_mismatch_is_a_protocol_error(stub):
    stub.body = _query_body(["a", "b"], [[{"Integer": 1}]])
    with RemoteDatabase(stub.url) as db:
        with pytest.raises(AlopexError) as raised:
            db.execute_sql("SELECT a, b FROM t")
    assert raised.value.code == "ALOPEX-PY203"


# ---------------------------------------------------------------------------
# エラー応答（D11）
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "status,code",
    [
        (400, "INVALID_REQUEST"),
        (401, "UNAUTHORIZED"),
        (404, "NOT_FOUND"),
        (408, "QUERY_TIMEOUT"),
        (409, "CONFLICT"),
        (410, "SESSION_EXPIRED"),
        (413, "PAYLOAD_TOO_LARGE"),
        (500, "INTERNAL"),
        (501, "NOT_IMPLEMENTED"),
        (503, "SERVER_BACKPRESSURE"),
        (409, "ALOPEX-S001"),
        (400, "ALOPEX-P042"),
        (418, "A_CODE_THIS_CLIENT_HAS_NEVER_SEEN"),
    ],
)
def test_server_error_codes_are_forwarded_verbatim(stub, status, code):
    stub.status = status
    stub.body = {
        "error": {
            "code": code,
            "message": "boom",
            "correlation_id": "cid-1234",
        }
    }
    with RemoteDatabase(stub.url) as db:
        with pytest.raises(AlopexError) as raised:
            db.execute_sql("SELECT 1")
    assert raised.value.code == code
    assert raised.value.http_status == status
    assert raised.value.correlation_id == "cid-1234"
    assert str(raised.value) == "boom"


def test_non_json_error_body_is_a_protocol_error(stub):
    stub.status = 502
    stub.raw_body = "<html>bad gateway</html>"
    with RemoteDatabase(stub.url) as db:
        with pytest.raises(AlopexError) as raised:
            db.execute_sql("SELECT 1")
    assert raised.value.code == "ALOPEX-PY203"


def test_non_json_success_body_is_a_protocol_error(stub):
    stub.raw_body = "not json"
    with RemoteDatabase(stub.url) as db:
        with pytest.raises(AlopexError) as raised:
            db.execute_sql("SELECT 1")
    assert raised.value.code == "ALOPEX-PY203"


# ---------------------------------------------------------------------------
# リクエストの形
# ---------------------------------------------------------------------------


def test_request_payload_and_api_key_header(stub):
    with RemoteDatabase(stub.url, api_key="s3cret") as db:
        db.execute_sql("SELECT * FROM t WHERE id = ?", [7])
    request = stub.requests[-1]
    assert request["path"] == "/sql"
    assert request["body"] == {
        "sql": "SELECT * FROM t WHERE id = 7",
        "session_id": None,
        "streaming": False,
    }
    assert request["headers"]["x-api-key"] == "s3cret"


def test_api_prefix_from_url_path_applies_to_every_endpoint(stub):
    stub.body = {"session_id": "sess-1", "expires_at": "2026-01-01T00:00:00Z"}
    with RemoteDatabase(stub.url + "/api/v1") as db:
        assert db.sql_path == "/api/v1/sql"
        txn = db.begin()
        assert stub.requests[-1]["path"] == "/api/v1/session/begin"
        stub.body = {"columns": [], "rows": [], "affected_rows": None}
        txn.commit()
        assert stub.requests[-1]["path"] == "/api/v1/session/sess-1/commit"


def test_session_id_is_attached_inside_a_transaction(stub):
    stub.body = {"session_id": "sess-9", "expires_at": None}
    with RemoteDatabase(stub.url) as db:
        txn = db.begin()
        stub.body = {"columns": [], "rows": [], "affected_rows": 1}
        assert txn.execute_sql("INSERT INTO t VALUES (1)") == 1
        assert stub.requests[-1]["body"]["session_id"] == "sess-9"
    # 未完了のまま close すると D21 のロールバックが最後のリクエストになる。
    assert stub.requests[-1]["path"] == "/session/sess-9/rollback"


def test_placeholder_expansion_reuses_the_embedded_binder(stub):
    from alopex._alopex import _bind_sql_params

    sql = "SELECT * FROM t WHERE a = ? AND b = ? -- ? stays\n"
    params = [1, "o'ne"]
    with RemoteDatabase(stub.url) as db:
        db.execute_sql(sql, params)
    assert stub.requests[-1]["body"]["sql"] == _bind_sql_params(sql, params)


def test_binder_errors_are_raised_before_any_request(stub):
    with RemoteDatabase(stub.url) as db:
        with pytest.raises(ValueError):
            db.execute_sql("SELECT ?", [])
        with pytest.raises(NotImplementedError):
            db.execute_sql("SELECT ?", [b"blob"])
        with pytest.raises(TypeError):
            db.execute_sql("SELECT ?", "not-a-sequence")
    assert stub.requests == []


# ---------------------------------------------------------------------------
# トランスポート（D13 / D15 / D16）
# ---------------------------------------------------------------------------


def test_connection_refused_is_a_connect_error():
    # ポートを確保して即座に閉じ、確実に接続拒否になるアドレスを作る。
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]
    db = RemoteDatabase(f"http://127.0.0.1:{port}")
    try:
        with pytest.raises(AlopexError) as raised:
            db.execute_sql("SELECT 1")
    finally:
        db.close()
    assert raised.value.code == "ALOPEX-PY201"


def test_slow_response_is_a_client_timeout(stub):
    stub.delay = 1.5
    db = RemoteDatabase(stub.url, timeout=0.25)
    try:
        with pytest.raises(AlopexError) as raised:
            db.execute_sql("SELECT 1")
    finally:
        db.close()
    assert raised.value.code == "ALOPEX-PY202"


def test_per_call_timeout_overrides_the_default(stub):
    stub.delay = 1.0
    db = RemoteDatabase(stub.url, timeout=30.0)
    try:
        with pytest.raises(AlopexError) as raised:
            db.execute_sql("SELECT 1", timeout=0.2)
    finally:
        db.close()
    assert raised.value.code == "ALOPEX-PY202"


class _SilentServer:
    """リクエストを読み取ってから応答せずに切る（送信済みリクエストを作る）。"""

    def __init__(self):
        self.sock = socket.socket()
        self.sock.bind(("127.0.0.1", 0))
        self.sock.listen(8)
        self.url = f"http://127.0.0.1:{self.sock.getsockname()[1]}"
        self.received = 0
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._serve, daemon=True)
        self._thread.start()

    def _serve(self):
        while not self._stop.is_set():
            try:
                conn, _ = self.sock.accept()
            except OSError:
                return
            try:
                data = conn.recv(65536)
                if data:
                    self.received += 1
            except OSError:
                pass
            finally:
                conn.close()

    def close(self):
        self._stop.set()
        self.sock.close()
        self._thread.join(timeout=5)


def test_a_sent_request_is_never_resent():
    server = _SilentServer()
    db = RemoteDatabase(server.url, retries=3)
    try:
        with pytest.raises(AlopexError) as raised:
            db.execute_sql("INSERT INTO t VALUES (1)")
    finally:
        db.close()
        server.close()
    # INSERT が二重適用されないことがこのテストの本題（D13）。
    assert server.received == 1
    assert raised.value.code == "ALOPEX-PY201"


def test_keep_alive_reuses_one_connection(stub):
    with RemoteDatabase(stub.url) as db:
        for _ in range(3):
            db.execute_sql("SELECT 1")
    assert stub.connections == 1
    assert len(stub.requests) == 3


def test_keep_alive_disabled_opens_a_connection_per_call(stub):
    with RemoteDatabase(stub.url, keep_alive=False) as db:
        for _ in range(3):
            db.execute_sql("SELECT 1")
    assert stub.connections == 3


def test_zero_idle_reconnect_window_reconnects_every_call(stub):
    with RemoteDatabase(stub.url, idle_reconnect_seconds=0) as db:
        for _ in range(3):
            db.execute_sql("SELECT 1")
    assert stub.connections == 3


def test_plaintext_http_to_a_non_loopback_host_is_refused():
    with pytest.raises(ValueError) as raised:
        RemoteDatabase("http://alopex.example.com:8080")
    assert raised.value.code == "ALOPEX-PY205"

    # loopback は開発と e2e のため無条件に許可（D16）。
    for url in ("http://127.0.0.1:8080", "http://localhost:8080", "http://[::1]:8080"):
        assert RemoteDatabase(url).url.startswith("http://")

    explicit = RemoteDatabase("http://alopex.example.com:8080", insecure=True)
    assert explicit.url == "http://alopex.example.com:8080"


@pytest.mark.parametrize(
    "url",
    [
        "",
        "   ",
        "ftp://host/db",
        "/var/lib/alopex",
        "http://",
        "https://",
        # 明示的な :0 は「ポート指定なし」ではない。`parts.port or default` は 0 が
        # falsy なので黙って 80/443 に差し替えていた（回帰）。
        "http://127.0.0.1:0",
        "https://alopex.example.com:0",
    ],
)
def test_invalid_server_urls_are_rejected(url):
    with pytest.raises(ValueError) as raised:
        RemoteDatabase(url)
    assert raised.value.code == "ALOPEX-PY205"


def test_default_ports_still_apply_when_the_url_names_none():
    assert RemoteDatabase("https://alopex.example.com")._transport._target.port == 443
    assert RemoteDatabase("http://127.0.0.1")._transport._target.port == 80
    assert RemoteDatabase("http://127.0.0.1:8080")._transport._target.port == 8080


@pytest.mark.parametrize(
    "kwargs",
    [
        {"timeout": 0},
        {"timeout": -1},
        # isinstance(True, int) が True なので、フラグの取り違えが 1 秒の
        # デッドラインとして通っていた（回帰）。retries は元から bool を弾く。
        {"timeout": True},
        {"timeout": False},
        {"retries": -1},
        {"retries": True},
    ],
)
def test_invalid_client_options_are_rejected(kwargs):
    with pytest.raises(ValueError) as raised:
        RemoteDatabase("http://127.0.0.1:8080", **kwargs)
    assert raised.value.code == "ALOPEX-PY205"


def test_constructing_a_client_opens_no_socket(monkeypatch):
    def forbidden(*_args, **_kwargs):
        raise AssertionError("RemoteDatabase construction must not touch the network")

    monkeypatch.setattr(socket, "create_connection", forbidden)
    monkeypatch.setattr(socket.socket, "connect", forbidden)

    client = RemoteDatabase("http://127.0.0.1:9")
    assert client.url == "http://127.0.0.1:9"
    assert alopex.connect(":memory:") is not None


def test_closed_client_matches_the_embedded_closed_handle_error(stub):
    db = RemoteDatabase(stub.url)
    db.execute_sql("SELECT 1")
    db.close()
    db.close()  # idempotent
    with pytest.raises(AlopexError) as raised:
        db.execute_sql("SELECT 1")
    assert raised.value.code == "ALOPEX-PY999"
    assert str(raised.value) == "database is closed"


def test_completed_transaction_rejects_further_work(stub):
    stub.body = {"session_id": "sess-2", "expires_at": None}
    with RemoteDatabase(stub.url) as db:
        txn = db.begin()
        stub.body = {"columns": [], "rows": [], "affected_rows": None}
        txn.commit()
        for call in (txn.commit, txn.rollback, lambda: txn.execute_sql("SELECT 1")):
            with pytest.raises(AlopexError) as raised:
                call()
            assert raised.value.code == "ALOPEX-PY999"


def test_transaction_context_manager_rolls_back_when_incomplete(stub):
    stub.body = {"session_id": "sess-3", "expires_at": None}
    with RemoteDatabase(stub.url) as db:
        with db.begin() as txn:
            stub.body = {"columns": [], "rows": [], "affected_rows": None}
            txn.execute_sql("INSERT INTO t VALUES (1)")
        assert txn.status["state"] == "rolled_back"
    assert stub.requests[-1]["path"].endswith("/rollback")


def test_close_rolls_back_every_still_active_session(stub):
    """D21: close() が生きているセッションを必ず終わらせる（回帰）。

    修正前は close() が接続を落とすだけで、放置された書き込みトランザクション
    がサーバー側に残り、session id を知っていれば後から commit できた。
    """
    stub.body = {"session_id": "sess-leak", "expires_at": None}
    db = RemoteDatabase(stub.url)
    txn = db.begin()
    stub.body = {"columns": [], "rows": [], "affected_rows": 1}
    txn.execute_sql("INSERT INTO t VALUES (1)")

    db.close()

    rollbacks = [r for r in stub.requests if r["path"].endswith("/rollback")]
    assert [r["path"] for r in rollbacks] == ["/session/sess-leak/rollback"]
    assert txn.status["state"] == "rolled_back"
    with pytest.raises(AlopexError) as raised:
        txn.commit()
    assert raised.value.code == "ALOPEX-PY999"

    # 二度目の close は追加のロールバックを出さない（冪等）。
    db.close()
    assert len([r for r in stub.requests if r["path"].endswith("/rollback")]) == 1


def test_dropping_a_transaction_rolls_it_back(stub):
    """D21: ファイナライザが PyTransaction::drop と同じ後始末をする（回帰）。"""
    stub.body = {"session_id": "sess-gc", "expires_at": None}
    db = RemoteDatabase(stub.url)
    txn = db.begin()
    stub.body = {"columns": [], "rows": [], "affected_rows": 1}
    txn.execute_sql("INSERT INTO t VALUES (1)")

    del txn
    gc.collect()

    assert stub.requests[-1]["path"] == "/session/sess-gc/rollback"
    # 回収済みなので close() が二重にロールバックすることはない。
    db.close()
    assert len([r for r in stub.requests if r["path"].endswith("/rollback")]) == 1


def test_finalizer_does_not_raise_when_the_session_is_unreachable(stub):
    """後始末経路は例外を出さない（サーバーが落ちていても）。"""
    stub.body = {"session_id": "sess-dead", "expires_at": None}
    db = RemoteDatabase(stub.url)
    txn = db.begin()
    stub.status = 500
    stub.raw_body = "boom"
    db.close()  # ロールバックが 500 で失敗しても close は成功する
    assert txn.status["state"] == "rolled_back"


def test_cleanup_never_re_enters_an_exchange_in_flight(stub):
    """D21: ファイナライザが実行中のやり取りに割り込まない。

    `_HttpTransport._lock` は RLock なので、同じスレッドでリクエスト中に
    `__del__` が走ると同一コネクション上で 2 つのやり取りが混ざる。混ざる
    代わりに保留し、close() で流し切る。
    """
    stub.body = {"session_id": "sess-busy", "expires_at": None}
    db = RemoteDatabase(stub.url)
    txn = db.begin()
    stub.body = {"columns": [], "rows": [], "affected_rows": None}

    db._transport._in_flight = True
    try:
        txn._rollback_quietly()
        assert db._pending_rollbacks == ["sess-busy"]
        assert not [r for r in stub.requests if r["path"].endswith("/rollback")]
    finally:
        db._transport._in_flight = False

    db.close()
    assert stub.requests[-1]["path"] == "/session/sess-busy/rollback"
    assert db._pending_rollbacks == []


def test_committed_transaction_is_not_rolled_back_by_close(stub):
    stub.body = {"session_id": "sess-done", "expires_at": None}
    db = RemoteDatabase(stub.url)
    txn = db.begin()
    stub.body = {"columns": [], "rows": [], "affected_rows": None}
    txn.commit()
    db.close()
    assert not [r for r in stub.requests if r["path"].endswith("/rollback")]


def test_transaction_status_matches_the_embedded_surface(stub):
    """D22: stream_effect は組み込みと同じ値を返す（回帰）。"""
    embedded = Database.new()
    embedded_txn = embedded.begin()
    try:
        stub.body = {"session_id": "sess-status", "expires_at": None}
        with RemoteDatabase(stub.url) as db:
            remote_txn = db.begin()
            assert remote_txn.status == embedded_txn.status
            assert remote_txn.status == {
                "state": "active",
                "stream_effect": "committable",
            }
            stub.body = {"columns": [], "rows": [], "affected_rows": None}
            remote_txn.rollback()
            embedded_txn.rollback()
            assert remote_txn.status == embedded_txn.status
            assert remote_txn.status["stream_effect"] == "closed"
    finally:
        embedded.close()


@pytest.mark.parametrize("sql", ["", "   ", "\n\t "])
def test_blank_sql_matches_the_embedded_none_without_a_request(stub, sql):
    """D23: 空文の戻り値を両面で揃える（回帰）。

    修正前はサーバーの INVALID_REQUEST（"sql must not be empty"）が上がって
    いた。空白のみの文はリクエストを出さずに組み込みと同じ None を返す。
    """
    embedded = Database.new()
    try:
        with RemoteDatabase(stub.url) as db:
            assert db.execute_sql(sql) is None
            txn_body = {"session_id": "sess-blank", "expires_at": None}
            stub.body = txn_body
            txn = db.begin()
            stub.body = {"columns": [], "rows": [], "affected_rows": None}
            assert txn.execute_sql(sql) is None
        assert embedded.execute_sql(sql) is None
        assert not [r for r in stub.requests if r["path"] == "/sql"]
    finally:
        embedded.close()


def test_blank_sql_still_reports_binder_errors(stub):
    with RemoteDatabase(stub.url) as db:
        with pytest.raises(ValueError):
            db.execute_sql("   ", [1])


def test_session_begin_without_session_id_is_a_protocol_error(stub):
    stub.body = {"expires_at": None}
    with RemoteDatabase(stub.url) as db:
        with pytest.raises(AlopexError) as raised:
            db.begin()
    assert raised.value.code == "ALOPEX-PY203"


def test_cluster_status_reads_the_admin_status_cluster_field(stub):
    stub.body = {"cluster": {"schema_version": 1, "mode": "single_node"}}
    with RemoteDatabase(stub.url) as db:
        assert db.cluster_status() == {"schema_version": 1, "mode": "single_node"}
    assert stub.requests[-1]["path"] == "/api/admin/status"

    stub.body = {"no_cluster_here": True}
    with RemoteDatabase(stub.url) as db:
        with pytest.raises(AlopexError) as raised:
            db.cluster_status()
    assert raised.value.code == "ALOPEX-PY203"


def test_routing_diagnostics_from_the_last_response_are_kept(stub):
    stub.body = {
        "columns": [],
        "rows": [],
        "affected_rows": None,
        "routing_diagnostics": [{"decision": "local_only"}],
    }
    with RemoteDatabase(stub.url) as db:
        db.execute_sql("SELECT 1")
        assert db.last_routing_diagnostics == [{"decision": "local_only"}]


# ---------------------------------------------------------------------------
# 未対応 API の明示エラー（D7 / D8 / D9）と surface completeness
# ---------------------------------------------------------------------------


UNSUPPORTED_DATABASE_CALLS = [
    ("execute_sql_stream", ("SELECT 1",)),
    ("query_stream", (None,)),
    ("execute_shared", (None,)),
    ("prepare", ("SELECT 1",)),
    ("copy_from_csv", ("items", None)),
    ("copy_to_csv", ("items", None)),
    ("list_sequences", ()),
    ("flush", ()),
    ("memory_usage", ()),
    ("routing_diagnostics", ()),
    ("create_hnsw_index", ("idx", None)),
    ("search_hnsw", ("idx", [1.0], 1)),
    ("drop_hnsw_index", ("idx",)),
    ("get_hnsw_stats", ("idx",)),
    ("open", ("/tmp/db",)),
    ("new", ()),
    ("open_in_memory", ()),
    ("open_with_config", (None,)),
]


@pytest.mark.parametrize("name,args", UNSUPPORTED_DATABASE_CALLS)
def test_unsupported_database_methods_raise_with_a_reason(name, args):
    db = RemoteDatabase("http://127.0.0.1:9")
    with pytest.raises(NotImplementedError) as raised:
        getattr(db, name)(*args)
    assert raised.value.code == "ALOPEX-PY204"
    assert "has no equivalent on the Alopex server" in str(raised.value)
    assert str(raised.value).split(": ", 1)[1]


def test_thread_mode_is_refused_with_a_reason():
    db = RemoteDatabase("http://127.0.0.1:9")
    with pytest.raises(NotImplementedError) as raised:
        db.thread_mode
    assert raised.value.code == "ALOPEX-PY204"


UNSUPPORTED_TRANSACTION_CALLS = [
    ("execute_sql_stream", ("SELECT 1",)),
    ("query_stream", (None,)),
    ("get", (b"k",)),
    ("put", (b"k", b"v")),
    ("delete", (b"k",)),
    ("upsert_vector", (b"k", None, [1.0], None)),
    ("search_similar", ([1.0], None, 1)),
    ("get_vector", (b"k", None)),
    ("upsert_to_hnsw", ("idx", b"k", [1.0])),
    ("delete_from_hnsw", ("idx", b"k")),
]


@pytest.mark.parametrize("name,args", UNSUPPORTED_TRANSACTION_CALLS)
def test_unsupported_transaction_methods_raise_with_a_reason(stub, name, args):
    stub.body = {"session_id": "sess-4", "expires_at": None}
    with RemoteDatabase(stub.url) as db:
        txn = db.begin()
        with pytest.raises(NotImplementedError) as raised:
            getattr(txn, name)(*args)
    assert raised.value.code == "ALOPEX-PY204"


def _public_methods(cls):
    return {name for name in dir(cls) if not name.startswith("_")}


def test_remote_surface_covers_every_public_database_method():
    missing = _public_methods(Database) - _public_methods(RemoteDatabase)
    assert not missing, (
        "RemoteDatabase must either implement or explicitly refuse every public "
        f"Database method; missing: {sorted(missing)}"
    )


def test_remote_transaction_covers_every_public_transaction_method():
    missing = _public_methods(Transaction) - _public_methods(RemoteTransaction)
    assert not missing, (
        "RemoteTransaction must either implement or explicitly refuse every "
        f"public Transaction method; missing: {sorted(missing)}"
    )


def test_client_error_codes_are_registered_in_the_public_registry():
    from alopex.remote import CLIENT_ERROR_CODES, ERROR_CLOSED

    registry = set(alopex.ALOPEX_ERROR_CODES)
    for code in (*CLIENT_ERROR_CODES, ERROR_CLOSED):
        assert code in registry, code


def test_private_binder_is_not_re_exported_as_public_api():
    assert "_bind_sql_params" not in alopex.__all__


def test_url_does_not_echo_userinfo_back():
    # 認証情報は api_key / headers を通す。URL に userinfo が混ざっていても
    # .url やエラーメッセージへ流出させない。
    client = RemoteDatabase("https://user:pass@alopex.example.com:8443")
    assert client.url == "https://alopex.example.com:8443"
    assert "pass" not in client.url

    default_port = RemoteDatabase("https://alopex.example.com")
    assert default_port.url == "https://alopex.example.com"
