"""Server client for Alopex, shaped like the embedded ``Database`` API.

``RemoteDatabase`` talks to a running ``alopex-server`` over its HTTP ``/sql``
endpoint while returning exactly what ``alopex.Database`` returns for the same
statement: ``list[dict[str, Any]]`` for SELECT (in column order), ``int`` for
DML, ``None`` for DDL.

Design decisions (issue #182, v0.8.8):

D1  Pure Python on top of :mod:`http.client`.  The client adds no dependency and
    keeps network stacks (tokio/hyper/rustls) out of the abi3 wheel that
    ``alopex-py-release.yml`` builds for three operating systems.
D2  ``?`` placeholder expansion is *not* reimplemented here.  It calls the
    embedded binder exported as ``_alopex._bind_sql_params``, so quoting,
    comment handling, NUL rejection, and literal rendering cannot drift between
    the two surfaces (a drift here would be an injection-shaped defect).
D5  The server returns ``SqlValue`` in serde's externally tagged form
    (``"Null"``, ``{"BigInt": 6}``, ...).  Unwrapping lives in exactly one place
    (:func:`_unwrap_sql_value`) and refuses unknown tags instead of passing them
    through silently.
D6  The three-way return shape is decided from the flattened response fields in
    the same order as ``alopex-cli``: non-empty ``columns`` wins, then
    ``affected_rows``, then ``None``.
D11 Server error codes are forwarded verbatim onto ``AlopexError.code`` so the
    same invalid statement reports the same stable code on both surfaces.
D13 Requests are never resent.  ``/sql`` is not idempotent, so only a connection
    that failed before a single byte was written may be retried, and only when
    ``retries`` is set above its default of ``0``.
D17 Non-finite floats arrive as JSON ``null`` (serde_json cannot encode them) and
    are rejected rather than silently turned into ``NaN``.
D19 ``Float`` (f32) values are re-narrowed through IEEE-754 binary32 before being
    widened to a Python float.  serde_json writes the shortest text that
    round-trips as f32, so narrowing recovers the exact f32 the server held and
    the widened result matches the embedded ``f64::from(f32)`` bit for bit.
"""

from __future__ import annotations

import ipaddress
import json
import socket
import struct
import threading
import time
from http import client as _http_client
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple, Union
from urllib.parse import unquote, urlsplit

from ._alopex import AlopexError as AlopexError
from ._alopex import _bind_sql_params
from ._alopex import types as _types

TxnMode = _types.TxnMode

#: Connection could not be established (DNS, refused, TLS handshake, reset).
ERROR_CONNECT_FAILED = "ALOPEX-PY201"
#: Client-side deadline expired before the server answered.
ERROR_TIMEOUT = "ALOPEX-PY202"
#: The peer did not speak the documented protocol (non-JSON body, unknown
#: ``SqlValue`` tag, row/column arity mismatch, non-finite float placeholder).
ERROR_PROTOCOL = "ALOPEX-PY203"
#: The embedded API has no equivalent on the server.
ERROR_UNSUPPORTED = "ALOPEX-PY204"
#: The connection target is not a target this client can use.
ERROR_INVALID_TARGET = "ALOPEX-PY205"
#: Operating a closed handle. Deliberately the same code and message the
#: embedded bindings use, so closed-handle errors look identical (D12).
ERROR_CLOSED = "ALOPEX-PY999"

#: Client codes this module can raise. ``alopex.ALOPEX_ERROR_CODES`` must be a
#: superset; ``tests/test_compatibility_contract.py`` asserts that.
CLIENT_ERROR_CODES = (
    ERROR_CONNECT_FAILED,
    ERROR_TIMEOUT,
    ERROR_PROTOCOL,
    ERROR_UNSUPPORTED,
    ERROR_INVALID_TARGET,
)

#: Twice the server's own ``query_timeout`` default (30s) so the server's
#: classified ``QUERY_TIMEOUT`` wins the race instead of a bare socket timeout
#: (D14).
DEFAULT_TIMEOUT = 60.0

#: Reconnect before sending when the pooled connection has been idle this long,
#: rather than resending a request that a half-closed keep-alive ate (D13).
DEFAULT_IDLE_RECONNECT_SECONDS = 5.0

#: ``SqlValue`` variants in serde's externally tagged encoding. Mirrors
#: ``crates/alopex-sql/src/storage/value.rs``; ``Null`` is a unit variant and
#: therefore arrives as the bare JSON string ``"Null"``.
_SQL_VALUE_TAGS = frozenset(
    (
        "Integer",
        "BigInt",
        "Float",
        "Double",
        "Text",
        "Blob",
        "Boolean",
        "Timestamp",
        "Vector",
    )
)

SqlResult = Union[List[Dict[str, Any]], int, None]


# ---------------------------------------------------------------------------
# Errors
# ---------------------------------------------------------------------------


def _alopex_error(code: str, message: str, **attrs: Any) -> AlopexError:
    error = AlopexError(message)
    error.code = code
    for name, value in attrs.items():
        setattr(error, name, value)
    return error


def _protocol_error(message: str) -> AlopexError:
    return _alopex_error(ERROR_PROTOCOL, message)


def _closed_error() -> AlopexError:
    # Same message and code the embedded bindings raise for a closed handle.
    return _alopex_error(ERROR_CLOSED, "database is closed")


def _unsupported(operation: str, reason: str) -> NotImplementedError:
    error = NotImplementedError(
        f"{operation} has no equivalent on the Alopex server: {reason}"
    )
    error.code = ERROR_UNSUPPORTED  # type: ignore[attr-defined]
    return error


def _invalid_target(message: str) -> ValueError:
    error = ValueError(message)
    error.code = ERROR_INVALID_TARGET  # type: ignore[attr-defined]
    return error


_STREAM_REASON = (
    "the server's JSONL stream carries no column metadata "
    "(crates/alopex-server/src/http/sql.rs::StreamItem is {row, error, done}), "
    "so it cannot yield the dicts the embedded stream API promises; "
    "stream APIs stay embedded-local in v0.8"
)
_HNSW_REASON = (
    "the server's /hnsw endpoints take neither HnswConfig (m, ef_construction) "
    "nor ef_search and never return HnswStats alongside results, so the embedded "
    "signature cannot be honoured"
)
_KV_REASON = (
    "the server's /kv/txn/* endpoints use a transaction id space of their own, "
    "separate from SQL sessions; mixing both into one RemoteTransaction would "
    "misreport which transaction the operation joined"
)
_LOCAL_ONLY_REASON = "it reports process-local engine state that a remote server does not expose for its caller"
_CONSTRUCTOR_REASON = (
    "the server client is constructed from a URL; use alopex.connect(url) or "
    "RemoteDatabase.connect(url)"
)


# ---------------------------------------------------------------------------
# Wire value normalization (D5 / D17 / D19)
# ---------------------------------------------------------------------------


def _narrow_to_f32(value: float) -> float:
    """Round-trip through IEEE-754 binary32 (see D19)."""
    try:
        return float(struct.unpack("<f", struct.pack("<f", value))[0])
    except (OverflowError, struct.error) as exc:  # pragma: no cover - guard
        raise _protocol_error(
            f"FLOAT value is out of binary32 range: {value!r} ({exc})"
        ) from exc


def _unwrap_sql_value(value: Any) -> Any:
    """Turn one externally tagged ``SqlValue`` into the embedded Python value."""
    if isinstance(value, str):
        if value == "Null":
            return None
        raise _protocol_error(
            f"unknown SqlValue unit variant: {value!r} "
            "(the wire form is 'Null' or a single-key object)"
        )
    if not isinstance(value, dict):
        raise _protocol_error(
            f"invalid SqlValue wire form: {type(value).__name__} ({value!r})"
        )
    if len(value) != 1:
        raise _protocol_error(f"SqlValue is not a single-key object: {value!r}")
    ((tag, inner),) = value.items()
    if tag not in _SQL_VALUE_TAGS:
        raise _protocol_error(
            f"unknown SqlValue tag: {tag!r} ({value!r}). When alopex-sql gains a "
            "variant, update _SQL_VALUE_TAGS in alopex/remote.py."
        )
    if tag in ("Integer", "BigInt", "Timestamp"):
        if not isinstance(inner, int) or isinstance(inner, bool):
            raise _protocol_error(f"{tag} payload is not an integer: {inner!r}")
        return inner
    if tag == "Boolean":
        if not isinstance(inner, bool):
            raise _protocol_error(f"Boolean payload is not a bool: {inner!r}")
        return inner
    if tag == "Text":
        if not isinstance(inner, str):
            raise _protocol_error(f"Text payload is not a string: {inner!r}")
        return inner
    if tag in ("Float", "Double"):
        number = _require_finite_number(tag, inner)
        return _narrow_to_f32(number) if tag == "Float" else number
    if tag == "Blob":
        if not isinstance(inner, list) or any(
            not isinstance(b, int) or isinstance(b, bool) or not 0 <= b <= 255
            for b in inner
        ):
            raise _protocol_error(f"Blob payload is not a byte list: {inner!r}")
        return bytes(inner)
    # Vector
    if not isinstance(inner, list):
        raise _protocol_error(f"Vector payload is not a list: {inner!r}")
    return [_narrow_to_f32(_require_finite_number("Vector", item)) for item in inner]


def _require_finite_number(tag: str, inner: Any) -> float:
    if inner is None:
        # serde_json encodes NaN / +-Inf as null, which erases which of the
        # three it was. Failing loudly beats inventing a value (D17).
        raise _protocol_error(
            f"{tag} value arrived as JSON null: serde_json encodes non-finite "
            "floats (NaN, +Inf, -Inf) as null, so the original value cannot be "
            "recovered. Avoid non-finite results over the server client."
        )
    if isinstance(inner, bool) or not isinstance(inner, (int, float)):
        raise _protocol_error(f"{tag} payload is not a number: {inner!r}")
    return float(inner)


def _column_names(body: Mapping[str, Any]) -> List[str]:
    columns = body.get("columns")
    if columns is None:
        return []
    if not isinstance(columns, list):
        raise _protocol_error(f"'columns' is not a list: {columns!r}")
    names: List[str] = []
    for column in columns:
        if not isinstance(column, dict) or not isinstance(column.get("name"), str):
            raise _protocol_error(f"invalid column descriptor: {column!r}")
        names.append(column["name"])
    return names


def _rows_to_dicts(names: Sequence[str], rows: Any) -> List[Dict[str, Any]]:
    if rows is None:
        return []
    if not isinstance(rows, list):
        raise _protocol_error(f"'rows' is not a list: {rows!r}")
    out: List[Dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, list):
            raise _protocol_error(f"row is not a list: {row!r}")
        if len(row) != len(names):
            raise _protocol_error(
                f"row has {len(row)} value(s) but the result declares "
                f"{len(names)} column(s): {row!r}"
            )
        # Column order and last-wins duplicate handling match the embedded
        # PyDict::set_item loop in embedded/sql.rs::execution_result_to_py.
        record: Dict[str, Any] = {}
        for name, value in zip(names, row):
            record[name] = _unwrap_sql_value(value)
        out.append(record)
    return out


def _result_from_body(body: Mapping[str, Any]) -> SqlResult:
    names = _column_names(body)
    if names:
        return _rows_to_dicts(names, body.get("rows"))
    affected = body.get("affected_rows")
    if affected is None:
        return None
    if not isinstance(affected, int) or isinstance(affected, bool):
        raise _protocol_error(f"'affected_rows' is not an integer: {affected!r}")
    return affected


def _parse_json(text: str, *, context: str) -> Any:
    try:
        return json.loads(text)
    except ValueError as exc:
        raise _protocol_error(
            f"{context} response body is not JSON: {exc} ({text[:200]!r})"
        ) from exc


def _raise_for_error(status: int, text: str) -> None:
    """Translate an error response into ``AlopexError`` with the server's code."""
    body: Any = None
    try:
        body = json.loads(text)
    except ValueError:
        body = None
    error = body.get("error") if isinstance(body, dict) else None
    if not isinstance(error, dict) or not isinstance(error.get("code"), str):
        raise _protocol_error(
            f"server returned HTTP {status} without the documented "
            f"{{'error': {{'code', 'message', 'correlation_id'}}}} body: {text[:200]!r}"
        )
    message = error.get("message")
    raise _alopex_error(
        error["code"],
        message if isinstance(message, str) and message else f"server returned HTTP {status}",
        correlation_id=error.get("correlation_id"),
        http_status=status,
    )


# ---------------------------------------------------------------------------
# Connection target
# ---------------------------------------------------------------------------


def _is_loopback(host: str) -> bool:
    if host in ("localhost", "localhost.localdomain"):
        return True
    candidate = host[1:-1] if host.startswith("[") and host.endswith("]") else host
    try:
        return ipaddress.ip_address(candidate).is_loopback
    except ValueError:
        return False


#: Target kinds returned by :func:`resolve_connect_target`.
TARGET_REMOTE = "remote"
TARGET_MEMORY = "memory"
TARGET_PATH = "path"

#: Keyword options that mean something to the embedded constructors. Every other
#: option belongs to the server client and is refused on an embedded target.
EMBEDDED_CONNECT_OPTIONS = ("thread_mode",)


def resolve_connect_target(
    target: str, options: Mapping[str, Any]
) -> Tuple[str, str]:
    """Classify a connection target for :func:`alopex.connect` (D3).

    Returns ``(kind, value)`` where *kind* is :data:`TARGET_REMOTE` (value is the
    URL), :data:`TARGET_MEMORY`, or :data:`TARGET_PATH` (value is a filesystem
    path). Target parsing lives here so the scheme rules and the stable codes
    they raise stay next to the client that defines them.

    Raises:
        NotImplementedError: ``s3://`` targets (``ALOPEX-PY204``).
        ValueError: An unusable target, or a server-only option on an embedded
            target (``ALOPEX-PY205``).
    """
    if not isinstance(target, str) or not target.strip():
        raise _invalid_target("connection target must be a non-empty string")
    target = target.strip()
    parts = urlsplit(target)
    # A single-letter scheme is a Windows drive letter, not a URL scheme.
    scheme = parts.scheme.lower() if len(parts.scheme) > 1 else ""

    if scheme in ("http", "https"):
        return (TARGET_REMOTE, target)

    for option in options:
        if option not in EMBEDDED_CONNECT_OPTIONS:
            raise _invalid_target(
                f"option {option!r} is only meaningful for a server target; "
                f"embedded targets accept {list(EMBEDDED_CONNECT_OPTIONS)}"
            )

    if scheme == "s3":
        raise _unsupported(
            f"connect({target!r})",
            "the Python bindings do not expose the embedded object-store URI "
            "opener yet; open the database locally or point at a server URL",
        )
    if scheme == "file":
        path = unquote(parts.path)
        if not path:
            raise _invalid_target(f"file:// target has no path: {target!r}")
        return (TARGET_PATH, path)
    if scheme:
        raise _invalid_target(
            f"unsupported connection scheme {scheme!r}: expected http, https, "
            "file, a filesystem path, or ':memory:'"
        )
    if target == ":memory:":
        return (TARGET_MEMORY, target)
    return (TARGET_PATH, target)


class _Target:
    """A validated http(s) endpoint plus the API prefix carried in its path."""

    __slots__ = ("scheme", "host", "port", "prefix", "url")

    def __init__(self, url: str, *, insecure: bool, api_prefix: Optional[str]) -> None:
        if not isinstance(url, str) or not url.strip():
            raise _invalid_target("server URL must be a non-empty string")
        parts = urlsplit(url.strip())
        if parts.scheme not in ("http", "https"):
            raise _invalid_target(
                f"server URL must use http:// or https://, got {url!r}"
            )
        if not parts.hostname:
            raise _invalid_target(f"server URL has no host: {url!r}")
        self.scheme = parts.scheme
        self.host = parts.hostname
        try:
            self.port = parts.port or (443 if parts.scheme == "https" else 80)
        except ValueError as exc:
            raise _invalid_target(f"server URL has an invalid port: {url!r}") from exc
        if self.scheme == "http" and not insecure and not _is_loopback(self.host):
            # Same posture as the CLI's validate_base_url: https is required off
            # loopback unless the caller opts out explicitly (D16).
            raise _invalid_target(
                f"plaintext http:// to a non-loopback host is refused ({self.host}); "
                "use https:// or pass insecure=True to accept an unencrypted link"
            )
        prefix = api_prefix if api_prefix is not None else parts.path
        prefix = prefix.rstrip("/")
        if prefix and not prefix.startswith("/"):
            prefix = "/" + prefix
        self.prefix = prefix
        # Rebuild the authority from host and port instead of reusing netloc:
        # any userinfo in the URL is meaningless to this client (credentials go
        # through api_key/headers) and must not be echoed back in `url` or in
        # the error messages that quote it.
        host_part = f"[{self.host}]" if ":" in self.host else self.host
        default_port = 443 if self.scheme == "https" else 80
        authority = host_part if self.port == default_port else f"{host_part}:{self.port}"
        self.url = f"{self.scheme}://{authority}{self.prefix}"

    def path(self, suffix: str) -> str:
        return f"{self.prefix}{suffix}"


# ---------------------------------------------------------------------------
# Transport
# ---------------------------------------------------------------------------


class _HttpTransport:
    """One connection per instance, serialized by a lock (D15).

    Share nothing across threads: build one ``RemoteDatabase`` per thread when
    concurrency matters.
    """

    def __init__(
        self,
        target: _Target,
        *,
        timeout: float,
        headers: Mapping[str, str],
        ssl_context: Any,
        retries: int,
        keep_alive: bool,
        idle_reconnect_seconds: Optional[float],
    ) -> None:
        self._target = target
        self._timeout = timeout
        self._headers = dict(headers)
        self._ssl_context = ssl_context
        self._retries = retries
        self._keep_alive = keep_alive
        self._idle_reconnect_seconds = idle_reconnect_seconds
        self._lock = threading.RLock()
        self._conn: Optional[_http_client.HTTPConnection] = None
        self._last_used = 0.0

    # -- connection lifecycle ------------------------------------------------

    def _new_connection(self, timeout: float) -> _http_client.HTTPConnection:
        if self._target.scheme == "https":
            # Imported lazily: minimal Python builds may ship without _ssl, and
            # plain http (including the loopback e2e lane) must still work.
            import ssl as _ssl

            context = self._ssl_context or _ssl.create_default_context()
            return _http_client.HTTPSConnection(
                self._target.host,
                self._target.port,
                timeout=timeout,
                context=context,
            )
        return _http_client.HTTPConnection(
            self._target.host, self._target.port, timeout=timeout
        )

    def _drop(self) -> None:
        if self._conn is not None:
            try:
                self._conn.close()
            except Exception:
                pass
            self._conn = None

    def _acquire(self, timeout: float) -> _http_client.HTTPConnection:
        if self._conn is not None:
            idle = time.monotonic() - self._last_used
            stale = self._idle_reconnect_seconds is not None and (
                idle >= self._idle_reconnect_seconds
            )
            if stale:
                self._drop()
        if self._conn is None:
            self._conn = self._new_connection(timeout)
        else:
            self._conn.timeout = timeout
            if self._conn.sock is not None:
                self._conn.sock.settimeout(timeout)
        return self._conn

    def close(self) -> None:
        with self._lock:
            self._drop()

    # -- requests ------------------------------------------------------------

    def request(
        self,
        method: str,
        path: str,
        payload: Optional[Mapping[str, Any]] = None,
        *,
        timeout: Optional[float] = None,
    ) -> Tuple[int, str]:
        effective = self._timeout if timeout is None else float(timeout)
        body = None if payload is None else json.dumps(payload).encode("utf-8")
        headers = dict(self._headers)
        if body is not None:
            headers["Content-Type"] = "application/json"
        headers.setdefault("Accept", "application/json")
        if not self._keep_alive:
            headers["Connection"] = "close"
        with self._lock:
            return self._send(method, path, body, headers, effective)

    def _send(
        self,
        method: str,
        path: str,
        body: Optional[bytes],
        headers: Dict[str, str],
        timeout: float,
    ) -> Tuple[int, str]:
        attempts = max(1, self._retries + 1)
        last: Optional[BaseException] = None
        for _ in range(attempts):
            conn = self._acquire(timeout)
            # Connecting is a separate step on purpose: only a failure here is
            # retryable, because not one byte of the request has been written.
            if conn.sock is None:
                try:
                    conn.connect()
                except (socket.timeout, TimeoutError) as exc:
                    self._drop()
                    raise _alopex_error(
                        ERROR_TIMEOUT,
                        f"connecting to {self._target.url} timed out after {timeout}s",
                    ) from exc
                except (OSError, _http_client.HTTPException) as exc:
                    self._drop()
                    last = exc
                    continue
            try:
                conn.request(method, path, body=body, headers=headers)
                response = conn.getresponse()
                raw = response.read()
                should_close = response.will_close
            except (socket.timeout, TimeoutError) as exc:
                self._drop()
                raise _alopex_error(
                    ERROR_TIMEOUT,
                    f"{method} {path} timed out after {timeout}s "
                    f"(server {self._target.url})",
                ) from exc
            except (OSError, _http_client.HTTPException) as exc:
                # The request is already on the wire. Alopex writes are not
                # idempotent, so this never becomes a retry (D13).
                self._drop()
                raise _alopex_error(
                    ERROR_CONNECT_FAILED,
                    f"{method} {path} failed against {self._target.url} after the "
                    f"request was sent; it was not retried because Alopex writes "
                    f"are not idempotent: {exc}",
                ) from exc
            self._last_used = time.monotonic()
            if should_close or not self._keep_alive:
                self._drop()
            return response.status, raw.decode("utf-8", errors="replace")
        raise _alopex_error(
            ERROR_CONNECT_FAILED,
            f"could not connect to {self._target.url}: {last}",
        )


# ---------------------------------------------------------------------------
# Transaction
# ---------------------------------------------------------------------------


class RemoteTransaction:
    """A server session transaction (``/session/begin`` … ``/commit``).

    The server always opens sessions read-write and has no read-only mode, so
    :meth:`RemoteDatabase.begin` refuses ``TxnMode.READ_ONLY`` rather than
    silently upgrading it (D10).  Sessions also expire after the server's
    ``session_ttl`` (300s by default); the resulting ``SESSION_EXPIRED`` code is
    forwarded unchanged.
    """

    def __init__(
        self, database: "RemoteDatabase", session_id: str, expires_at: Optional[str]
    ) -> None:
        self._database = database
        self._session_id = session_id
        self._expires_at = expires_at
        self._state = "active"

    @property
    def session_id(self) -> str:
        return self._session_id

    @property
    def expires_at(self) -> Optional[str]:
        """RFC 3339 expiry reported by ``/session/begin`` (remote only)."""
        return self._expires_at

    @property
    def status(self) -> Dict[str, str]:
        # Key set matches the embedded PyTransaction.status. A server session
        # owns no local stream, so stream_effect is always "closed".
        return {"state": self._state, "stream_effect": "closed"}

    def execute_sql(
        self,
        sql: str,
        params: Optional[Sequence[Any]] = None,
        *,
        timeout: Optional[float] = None,
    ) -> SqlResult:
        self._ensure_active()
        return self._database._execute_sql(
            sql, params, session_id=self._session_id, timeout=timeout
        )

    def commit(self) -> None:
        self._ensure_active()
        self._database._session_action(self._session_id, "commit")
        self._state = "committed"

    def rollback(self) -> None:
        self._ensure_active()
        self._database._session_action(self._session_id, "rollback")
        self._state = "rolled_back"

    def __enter__(self) -> "RemoteTransaction":
        return self

    def __exit__(self, exc_type: Any, exc: Any, traceback: Any) -> bool:
        if self._state == "active":
            self.rollback()
        return False

    def _ensure_active(self) -> None:
        if self._state != "active":
            raise _alopex_error(
                ERROR_CLOSED, f"transaction is already {self._state}"
            )

    # -- explicit refusals ---------------------------------------------------

    def execute_sql_stream(self, *_args: Any, **_kwargs: Any) -> Any:
        raise _unsupported("Transaction.execute_sql_stream", _STREAM_REASON)

    def query_stream(self, *_args: Any, **_kwargs: Any) -> Any:
        raise _unsupported("Transaction.query_stream", _STREAM_REASON)

    def get(self, *_args: Any, **_kwargs: Any) -> Any:
        raise _unsupported("Transaction.get", _KV_REASON)

    def put(self, *_args: Any, **_kwargs: Any) -> Any:
        raise _unsupported("Transaction.put", _KV_REASON)

    def delete(self, *_args: Any, **_kwargs: Any) -> Any:
        raise _unsupported("Transaction.delete", _KV_REASON)

    def upsert_vector(self, *_args: Any, **_kwargs: Any) -> Any:
        raise _unsupported("Transaction.upsert_vector", _KV_REASON)

    def search_similar(self, *_args: Any, **_kwargs: Any) -> Any:
        raise _unsupported("Transaction.search_similar", _KV_REASON)

    def get_vector(self, *_args: Any, **_kwargs: Any) -> Any:
        raise _unsupported("Transaction.get_vector", _KV_REASON)

    def upsert_to_hnsw(self, *_args: Any, **_kwargs: Any) -> Any:
        raise _unsupported("Transaction.upsert_to_hnsw", _HNSW_REASON)

    def delete_from_hnsw(self, *_args: Any, **_kwargs: Any) -> Any:
        raise _unsupported("Transaction.delete_from_hnsw", _HNSW_REASON)


# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------


class RemoteDatabase:
    """``alopex.Database``'s calling convention, executed on a server.

    Args:
        url: ``http(s)://host:port`` with an optional API prefix in the path.
        api_key: Sent as ``x-api-key`` when the server runs ``auth_mode = dev``.
        timeout: Default per-request deadline in seconds (D14).
        sql_path: Overrides the SQL endpoint (defaults to ``<prefix>/sql``;
            ``/api/sql/query`` is the same handler).
        api_prefix: Overrides the prefix taken from the URL path.
        headers: Extra headers, e.g. ``{"Authorization": "Bearer ..."}``.
        ssl_context: Replaces ``ssl.create_default_context()`` for https (mTLS).
        insecure: Allows plaintext http to a non-loopback host.
        retries: Retries for connection establishment only; never resends a
            request that was already written (D13).
        keep_alive: Reuse one connection across calls.
        idle_reconnect_seconds: Reconnect before sending when the connection has
            been idle at least this long. ``None`` disables the check.

    The constructor opens no socket, so building a client never blocks and never
    touches the network.
    """

    def __init__(
        self,
        url: str,
        *,
        api_key: Optional[str] = None,
        timeout: float = DEFAULT_TIMEOUT,
        sql_path: Optional[str] = None,
        api_prefix: Optional[str] = None,
        headers: Optional[Mapping[str, str]] = None,
        ssl_context: Any = None,
        insecure: bool = False,
        retries: int = 0,
        keep_alive: bool = True,
        idle_reconnect_seconds: Optional[float] = DEFAULT_IDLE_RECONNECT_SECONDS,
    ) -> None:
        if not isinstance(timeout, (int, float)) or not 0 < float(timeout) < float("inf"):
            raise _invalid_target("timeout must be a finite positive number of seconds")
        if not isinstance(retries, int) or isinstance(retries, bool) or retries < 0:
            raise _invalid_target("retries must be a non-negative integer")
        self._target = _Target(url, insecure=insecure, api_prefix=api_prefix)
        request_headers: Dict[str, str] = dict(headers or {})
        if api_key is not None:
            request_headers.setdefault("x-api-key", api_key)
        self._sql_path = sql_path if sql_path is not None else self._target.path("/sql")
        self._transport = _HttpTransport(
            self._target,
            timeout=float(timeout),
            headers=request_headers,
            ssl_context=ssl_context,
            retries=retries,
            keep_alive=keep_alive,
            idle_reconnect_seconds=idle_reconnect_seconds,
        )
        self._closed = False
        self._last_routing_diagnostics: List[Any] = []

    # -- construction --------------------------------------------------------

    @classmethod
    def connect(cls, url: str, **options: Any) -> "RemoteDatabase":
        """Build a client for ``url``. Alias of the constructor."""
        return cls(url, **options)

    @property
    def url(self) -> str:
        """Normalized base URL, API prefix included."""
        return self._target.url

    @property
    def sql_path(self) -> str:
        return self._sql_path

    @property
    def last_routing_diagnostics(self) -> List[Any]:
        """``routing_diagnostics`` from the most recent ``/sql`` response.

        Remote-only: the embedded ``routing_diagnostics()`` reports engine-local
        state and has no server equivalent.
        """
        return list(self._last_routing_diagnostics)

    # -- core API ------------------------------------------------------------

    def execute_sql(
        self,
        sql: str,
        params: Optional[Sequence[Any]] = None,
        *,
        timeout: Optional[float] = None,
    ) -> SqlResult:
        """Execute one or more statements and return the last result.

        Returns the same shapes as ``Database.execute_sql``:
        ``list[dict[str, Any]]`` for SELECT, ``int`` for DML, ``None`` for DDL.
        """
        return self._execute_sql(sql, params, session_id=None, timeout=timeout)

    def begin(self, mode: Optional[Any] = None) -> RemoteTransaction:
        """Open a server session transaction.

        ``TxnMode.READ_ONLY`` is refused: the server opens every session
        read-write and quietly upgrading the mode would be a correctness
        accident (D10).
        """
        self._ensure_open()
        if _is_read_only(mode):
            raise _unsupported(
                "Database.begin(TxnMode.READ_ONLY)",
                "server sessions are always read-write and refusing is safer "
                "than silently upgrading a read-only transaction",
            )
        body = self._request_json(
            "POST", self._target.path("/session/begin"), None, context="session begin"
        )
        session_id = body.get("session_id") if isinstance(body, dict) else None
        if not isinstance(session_id, str) or not session_id:
            raise _protocol_error(
                f"/session/begin returned no session_id: {str(body)[:200]!r}"
            )
        expires_at = body.get("expires_at")
        return RemoteTransaction(
            self, session_id, expires_at if isinstance(expires_at, str) else None
        )

    def cluster_status(self) -> Dict[str, Any]:
        """Return the server's ``ClusterStatusSnapshot``.

        Same JSON schema as the embedded ``Database.cluster_status()`` and the
        gRPC ``cluster_json`` field.
        """
        self._ensure_open()
        body = self._request_json(
            "GET",
            self._target.path("/api/admin/status"),
            None,
            context="admin status",
        )
        cluster = body.get("cluster") if isinstance(body, dict) else None
        if not isinstance(cluster, dict):
            raise _protocol_error(
                f"admin status has no 'cluster' object: {str(body)[:200]!r}"
            )
        return cluster

    def close(self) -> None:
        """Release the connection. Idempotent, like the embedded ``close()``."""
        self._closed = True
        self._transport.close()

    def __enter__(self) -> "RemoteDatabase":
        return self

    def __exit__(self, exc_type: Any, exc: Any, traceback: Any) -> bool:
        self.close()
        return False

    # -- internals -----------------------------------------------------------

    def _ensure_open(self) -> None:
        if self._closed:
            raise _closed_error()

    def _execute_sql(
        self,
        sql: str,
        params: Optional[Sequence[Any]],
        *,
        session_id: Optional[str],
        timeout: Optional[float],
    ) -> SqlResult:
        self._ensure_open()
        # The embedded binder is the single source of truth for `?` (D2).
        bound = _bind_sql_params(sql, params)
        body = self._request_json(
            "POST",
            self._sql_path,
            {"sql": bound, "session_id": session_id, "streaming": False},
            context="sql",
            timeout=timeout,
        )
        if not isinstance(body, dict):
            raise _protocol_error(f"sql response is not an object: {str(body)[:200]!r}")
        diagnostics = body.get("routing_diagnostics")
        self._last_routing_diagnostics = (
            list(diagnostics) if isinstance(diagnostics, list) else []
        )
        return _result_from_body(body)

    def _session_action(self, session_id: str, action: str) -> None:
        self._ensure_open()
        self._request_json(
            "POST",
            self._target.path(f"/session/{session_id}/{action}"),
            None,
            context=f"session {action}",
        )

    def _request_json(
        self,
        method: str,
        path: str,
        payload: Optional[Mapping[str, Any]],
        *,
        context: str,
        timeout: Optional[float] = None,
    ) -> Any:
        status, text = self._transport.request(method, path, payload, timeout=timeout)
        if status != 200:
            _raise_for_error(status, text)
        return _parse_json(text, context=context)

    # -- explicit refusals (D7 / D8 / D9) ------------------------------------

    @property
    def thread_mode(self) -> Any:
        raise _unsupported("Database.thread_mode", _LOCAL_ONLY_REASON)

    def execute_sql_stream(self, *_args: Any, **_kwargs: Any) -> Any:
        raise _unsupported("Database.execute_sql_stream", _STREAM_REASON)

    def query_stream(self, *_args: Any, **_kwargs: Any) -> Any:
        raise _unsupported("Database.query_stream", _STREAM_REASON)

    def flush(self, *_args: Any, **_kwargs: Any) -> Any:
        raise _unsupported(
            "Database.flush",
            "durability is the server's own concern and it exposes no client-triggered flush",
        )

    def memory_usage(self, *_args: Any, **_kwargs: Any) -> Any:
        raise _unsupported(
            "Database.memory_usage",
            "the server reports process memory through /api/admin/metrics, which "
            "is not the embedded MemoryStats value",
        )

    def routing_diagnostics(self, *_args: Any, **_kwargs: Any) -> Any:
        raise _unsupported(
            "Database.routing_diagnostics",
            "the embedded accessor reports local engine state; read "
            "RemoteDatabase.last_routing_diagnostics for what the server "
            "attached to the most recent /sql response",
        )

    def create_hnsw_index(self, *_args: Any, **_kwargs: Any) -> Any:
        raise _unsupported("Database.create_hnsw_index", _HNSW_REASON)

    def search_hnsw(self, *_args: Any, **_kwargs: Any) -> Any:
        raise _unsupported("Database.search_hnsw", _HNSW_REASON)

    def drop_hnsw_index(self, *_args: Any, **_kwargs: Any) -> Any:
        raise _unsupported("Database.drop_hnsw_index", _HNSW_REASON)

    def get_hnsw_stats(self, *_args: Any, **_kwargs: Any) -> Any:
        raise _unsupported("Database.get_hnsw_stats", _HNSW_REASON)

    @classmethod
    def open(cls, *_args: Any, **_kwargs: Any) -> "RemoteDatabase":
        raise _unsupported("Database.open", _CONSTRUCTOR_REASON)

    @classmethod
    def new(cls, *_args: Any, **_kwargs: Any) -> "RemoteDatabase":
        raise _unsupported("Database.new", _CONSTRUCTOR_REASON)

    @classmethod
    def open_in_memory(cls, *_args: Any, **_kwargs: Any) -> "RemoteDatabase":
        raise _unsupported("Database.open_in_memory", _CONSTRUCTOR_REASON)

    @classmethod
    def open_with_config(cls, *_args: Any, **_kwargs: Any) -> "RemoteDatabase":
        raise _unsupported("Database.open_with_config", _CONSTRUCTOR_REASON)


def _is_read_only(mode: Any) -> bool:
    if mode is None:
        return False
    try:
        return bool(mode == TxnMode.READ_ONLY)
    except TypeError:
        # TxnMode.__richcmp__ only accepts TxnMode; anything else is not a mode.
        raise TypeError(
            f"mode must be a TxnMode, got {type(mode).__name__}"
        ) from None


__all__ = [
    "RemoteDatabase",
    "RemoteTransaction",
    "resolve_connect_target",
    "EMBEDDED_CONNECT_OPTIONS",
    "TARGET_REMOTE",
    "TARGET_MEMORY",
    "TARGET_PATH",
    "CLIENT_ERROR_CODES",
    "DEFAULT_TIMEOUT",
    "ERROR_CONNECT_FAILED",
    "ERROR_TIMEOUT",
    "ERROR_PROTOCOL",
    "ERROR_UNSUPPORTED",
    "ERROR_INVALID_TARGET",
]
