"""4 経路(embedded / cli / http / grpc)の実行とサーバー起動管理。

docs-public/specs/alopex-mode-parity-spec.md「実行系の構成」に従う。

経路と実行手段:
- embedded: ``cargo test -p alopex-embedded --test parity_corpus``(subprocess)。
  環境変数 PARITY_CORPUS_DIR / PARITY_DATA_DIR / PARITY_ROLE / PARITY_OUTPUT で
  制御する。cargo は必ず逐次(同時 1 プロセス)実行する。
- cli: 製品バイナリ ``alopex --batch --output json`` の subprocess。
- http: requests で SQL 実行エンドポイントへ POST。
- grpc: grpcio-tools で alopex.proto から実行時にスタブ生成
  (生成物は tempdir、コミットしない)。

本モジュールが返す「生レコード」(StatementRecord)の形:

    {
        "sql": str,
        "kind": "query" | "success" | "rows_affected" | "error",
        "columns": [str, ...] | None,   # kind == "query"(gRPC は常に None)
        "rows": [[...], ...] | None,    # kind == "query"(位置ベース)
        "affected_count": int | None,   # kind == "rows_affected"
        "error_message": str | None,    # kind == "error"
    }

生レコードは runner.normalize.record_to_entry / normalize_records で
expected/*.json と同形の結果バリアント(success / rows_affected / query /
error)へ正規化する。embedded 経路(PARITY_OUTPUT)は正規化済みの
実測出力スキーマ(per-statement キー ``actual``)を直接書き出す
(EmbeddedSurface の契約を参照)。

requests / grpcio は遅延 import とし、本モジュールの import は
標準ライブラリ + runner.normalize のみで完結する。
"""

from __future__ import annotations

import fcntl
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import threading
import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Sequence

from . import normalize

# ---------------------------------------------------------------------------
# 例外
# ---------------------------------------------------------------------------


class SurfaceError(Exception):
    """環境・起動エラー(exit code 2 相当)。"""


class SurfaceSkip(Exception):
    """明示的な SKIP(理由必須)。成功数には含めない。"""

    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


# ---------------------------------------------------------------------------
# パス・定数
# ---------------------------------------------------------------------------

#: 仕様書(alopex-mode-parity-spec)が規定する HTTP SQL 実行パス。
#: 現行の docs/server-guide.md は ``POST /sql``(+ 任意の api_prefix)を
#: 記載しており差異がある。サーバー側が仕様パスを提供するまでの間は
#: HttpSurface(sql_path=...) で上書きできる。
HTTP_SQL_PATH = "/api/sql/query"

#: 管理ポートのヘルスチェックパス(docs/server-guide.md「Admin endpoints」)。
HEALTH_PATH = "/healthz"

#: 製品バイナリ名(検証で使うのはこの 2 つのみ。検証用バイナリは追加しない)
PRODUCT_BIN_CLI = "alopex"
PRODUCT_BIN_SERVER = "alopex-server"

#: 検証クエリのファイル名(コーパス共通)
VERIFY_FILENAME = "99_verify.sql"


def repo_root() -> Path:
    """リポジトリルート(scripts/parity/runner/ から 3 階層上)。"""
    return Path(__file__).resolve().parents[3]


def nim_parser_dir(repo: Path) -> Path:
    return repo / "crates" / "alopex-sql" / "nim-sql-parser"


def product_env(repo: Path) -> Dict[str, str]:
    """製品バイナリ・cargo 実行用の環境変数。

    Nim 共有ライブラリ(``nimble lib`` の生成物)を解決できるよう、
    CI (.github/workflows/ci.yml) と同じく nim-sql-parser ディレクトリを
    LD_LIBRARY_PATH の先頭へ載せる。
    """
    env = dict(os.environ)
    nim_dir = str(nim_parser_dir(repo))
    current = env.get("LD_LIBRARY_PATH", "")
    env["LD_LIBRARY_PATH"] = f"{nim_dir}:{current}" if current else nim_dir
    return env


# ---------------------------------------------------------------------------
# SQL コーパスの読み込み・分割
# ---------------------------------------------------------------------------


def split_sql_statements(text: str) -> List[str]:
    """SQL テキストを文単位に分割する。

    文字列リテラル(' / ")内のセミコロン、行コメント(--)、
    ブロックコメント(/* */)を考慮する。コメントは文から除去する。
    """
    statements: List[str] = []
    buf: List[str] = []
    i = 0
    n = len(text)
    in_squote = False
    in_dquote = False
    while i < n:
        ch = text[i]
        if in_squote:
            buf.append(ch)
            if ch == "'":
                if i + 1 < n and text[i + 1] == "'":  # エスケープ ''
                    buf.append("'")
                    i += 2
                    continue
                in_squote = False
            i += 1
            continue
        if in_dquote:
            buf.append(ch)
            if ch == '"':
                in_dquote = False
            i += 1
            continue
        if ch == "'":
            in_squote = True
            buf.append(ch)
            i += 1
            continue
        if ch == '"':
            in_dquote = True
            buf.append(ch)
            i += 1
            continue
        if ch == "-" and text[i : i + 2] == "--":
            while i < n and text[i] != "\n":
                i += 1
            continue
        if ch == "/" and text[i : i + 2] == "/*":
            end = text.find("*/", i + 2)
            if end == -1:
                raise SurfaceError("閉じられていないブロックコメント")
            i = end + 2
            continue
        if ch == ";":
            statement = "".join(buf).strip()
            if statement:
                statements.append(statement)
            buf = []
            i += 1
            continue
        buf.append(ch)
        i += 1
    tail = "".join(buf).strip()
    if tail:
        statements.append(tail)
    return statements


def is_query_statement(sql: str) -> bool:
    """結果集合(行)を返す文かどうかの判定。"""
    first = sql.lstrip().split(None, 1)
    if not first:
        return False
    return first[0].upper() in {"SELECT", "WITH", "VALUES", "EXPLAIN", "SHOW"}


def is_dml_statement(sql: str) -> bool:
    """影響行数を返す DML(INSERT/UPDATE/DELETE)かどうかの判定。

    gRPC 経路の RPC ルーティング(ExecuteDml)に使う。
    """
    first = sql.lstrip().split(None, 1)
    if not first:
        return False
    return first[0].upper() in {"INSERT", "UPDATE", "DELETE"}


def corpus_files(corpus_dir: Path) -> List[Path]:
    """書き込みコーパス(01〜07 のみ)。

    - ``99_verify.sql`` は検証専用のため含めない。
    - ``08_server_insert.sql`` は S1 第 3 幕専用(サーバー HTTP 経由の
      追加 INSERT)のため含めない。実行順序契約は
      「01〜07 -> 99(基準検証) -> 08 -> 99(after_server_insert 検証)」で、
      08 の後に 03〜07 を再実行してはならない。
    """
    files = sorted(
        p
        for p in corpus_dir.glob("*.sql")
        if p.name[:2].isdigit() and 1 <= int(p.name[:2]) <= 7
    )
    if not files:
        raise SurfaceError(f"コーパス SQL (01〜07) が見つからない: {corpus_dir}")
    return files


def verify_sql_path(corpus_dir: Path) -> Path:
    path = corpus_dir / VERIFY_FILENAME
    if not path.is_file():
        raise SurfaceError(f"検証クエリが見つからない: {path}")
    return path


def load_statements(paths: Sequence[Path]) -> List[str]:
    statements: List[str] = []
    for path in paths:
        statements.extend(split_sql_statements(path.read_text(encoding="utf-8")))
    return statements


# ---------------------------------------------------------------------------
# cargo の逐次実行保証
# ---------------------------------------------------------------------------

# プロセス内(スレッド間)の逐次化
_CARGO_THREAD_LOCK = threading.Lock()


@contextmanager
def _cargo_process_lock(repo: Path) -> Iterator[None]:
    """プロセス間(verify.py と demo.py の同時起動等)の逐次化。

    target/ 配下のロックファイルへ flock を取り、cargo を同時に
    2 プロセス以上走らせない(並列 cargo 禁止のコードによる保証)。
    """
    lock_dir = repo / "target"
    lock_dir.mkdir(parents=True, exist_ok=True)
    lock_path = lock_dir / ".parity-cargo.lock"
    with open(lock_path, "w", encoding="utf-8") as lock_file:
        fcntl.flock(lock_file, fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(lock_file, fcntl.LOCK_UN)


def run_cargo(
    args: Sequence[str],
    *,
    repo: Path,
    env: Optional[Dict[str, str]] = None,
    timeout: float = 3600.0,
) -> subprocess.CompletedProcess:
    """cargo を逐次(同時 1 プロセス)で実行する唯一の入口。

    cargo の起動はすべて本関数を経由すること。スレッドロック + flock で
    プロセス内外の多重起動を禁止する。
    """
    command = ["cargo", *args]
    with _CARGO_THREAD_LOCK:
        with _cargo_process_lock(repo):
            return subprocess.run(
                command,
                cwd=repo,
                env=env or product_env(repo),
                capture_output=True,
                text=True,
                timeout=timeout,
            )


def _tail(text: str, lines: int = 30) -> str:
    return "\n".join(text.splitlines()[-lines:])


# ---------------------------------------------------------------------------
# 製品バイナリのビルド(単発・逐次)
# ---------------------------------------------------------------------------


def build_products(repo: Path, *, profile: str = "debug") -> Dict[str, Path]:
    """Nim パーサー + 製品バイナリ(alopex, alopex-server)を逐次ビルドする。

    - Nim パーサー: Makefile ``nim-parser`` ターゲットと同一手順
      (``cd crates/alopex-sql/nim-sql-parser && nimble lib``)。
    - 製品バイナリ: 1 回の cargo 呼び出しでまとめてビルドする
      (「ビルドは単発」要件。cargo の多重起動はしない)。
    """
    if shutil.which("nimble") is None:
        raise SurfaceError(
            "nimble が見つからない。検証コンテナ(scripts/parity/Dockerfile)内で"
            "実行すること。"
        )
    nim_dir = nim_parser_dir(repo)
    nimble = subprocess.run(
        ["nimble", "lib"],
        cwd=nim_dir,
        capture_output=True,
        text=True,
        timeout=1800.0,
    )
    if nimble.returncode != 0:
        raise SurfaceError(
            f"nimble lib 失敗 (cwd={nim_dir}):\n{_tail(nimble.stderr)}"
        )

    cargo_args = ["build", "-p", "alopex-cli", "-p", "alopex-server", "--bins"]
    if profile == "release":
        cargo_args.append("--release")
    elif profile != "debug":
        raise SurfaceError(f"未知のビルドプロファイル: {profile}")
    result = run_cargo(cargo_args, repo=repo)
    if result.returncode != 0:
        raise SurfaceError(f"cargo build 失敗:\n{_tail(result.stderr)}")

    target_dir = repo / "target" / profile
    binaries = {
        PRODUCT_BIN_CLI: target_dir / PRODUCT_BIN_CLI,
        PRODUCT_BIN_SERVER: target_dir / PRODUCT_BIN_SERVER,
    }
    for name, path in binaries.items():
        if not path.is_file():
            raise SurfaceError(f"ビルド成果物が見つからない: {path} ({name})")
    return binaries


# ---------------------------------------------------------------------------
# 経路 1: 組み込み API(cargo test -p alopex-embedded --test parity_corpus)
# ---------------------------------------------------------------------------

ROLE_WRITER = "writer"
ROLE_READER = "reader"


@dataclass
class EmbeddedRunResult:
    """embedded 経路の実行結果。

    テスト(parity_corpus.rs)は期待値との一致を自己アサートするため、
    出力 JSON が得られていれば cargo test の失敗は「検証不一致」として
    扱える(entries で相互 diff も行う)。出力がなければ環境エラー。
    """

    entries: List[Dict[str, Any]]  # 正規化済みエントリ({"index","sql","result"})
    cargo_ok: bool
    stderr_tail: str


class EmbeddedSurface:
    """組み込み API 経路。

    ``crates/alopex-embedded/tests/parity_corpus.rs`` との契約
    (Rust 側はこの契約に合わせて実装する。rust-dev に通知済みの
    オーケストレーター決定スキーマ):

    - ``PARITY_CORPUS_DIR``: コーパスディレクトリ(絶対パス)。
    - ``PARITY_DATA_DIR``  : データディレクトリ(絶対パス)。未設定なら
      インメモリ(SF-MEM)で実行する。
    - ``PARITY_ROLE``      : ``writer`` なら 01〜07 のコーパスを実行、
      ``reader`` なら 99_verify.sql のみを実行する。
    - ``PARITY_OUTPUT``    : 正規化済みの実測出力を書き出すファイル。
      スキーマは expected/*.json と同形で、per-statement キーのみ
      ``actual`` に変わる:

          {"corpus": "01_ddl.sql,02_dml.sql,...", "format": {...},
           "statements": [{"index": 1, "sql": "...", "actual": {...}}]}

      ``actual`` の値は success / rows_affected / query / error バリアント
      (runner.normalize のモジュール docstring 参照)。

      parity_corpus.rs 実装済みの解釈(rust-dev 確定):
        - writer ロール: 01〜07 の全文を連結した**単一ドキュメント**。
          ``index`` は 1 始まりの通し番号、``corpus`` はカンマ区切りの
          ファイル名リスト。
        - reader ロール: 99_verify.sql 単体の単一ドキュメント。
      比較側(verify.py / demo.py)は Python 経路も同じ文順・通し番号で
      正規化するため、位置ベースの zip 比較で整合する。ローダー
      (normalize.load_statements_file)はドキュメント配列も許容するが、
      実装済み契約は上記の単一ドキュメントである。
    """

    def __init__(self, repo: Path) -> None:
        self.repo = repo

    def run(
        self,
        corpus_dir: Path,
        *,
        role: str,
        data_dir: Optional[Path] = None,
        output_path: Path,
    ) -> EmbeddedRunResult:
        if role not in (ROLE_WRITER, ROLE_READER):
            raise SurfaceError(f"不正な PARITY_ROLE: {role}")
        if role == ROLE_READER and data_dir is None:
            raise SurfaceError("reader ロールはデータディレクトリが必須")

        # parity_corpus.rs は PARITY_OUTPUT へ直接書き込む(親ディレクトリは
        # 作成しない)ため、ハーネス側で必ず作成しておく。
        output_path.parent.mkdir(parents=True, exist_ok=True)

        env = product_env(self.repo)
        env["PARITY_CORPUS_DIR"] = str(corpus_dir.resolve())
        env["PARITY_ROLE"] = role
        env["PARITY_OUTPUT"] = str(output_path)
        env.pop("PARITY_DATA_DIR", None)
        if data_dir is not None:
            env["PARITY_DATA_DIR"] = str(data_dir)

        # cargo test は run_cargo 経由でのみ起動する(逐次実行の保証)。
        # テストバイナリ内も直列実行にするため --test-threads=1 を渡す。
        result = run_cargo(
            [
                "test",
                "-p",
                "alopex-embedded",
                "--test",
                "parity_corpus",
                "--",
                "--test-threads=1",
            ],
            repo=self.repo,
            env=env,
        )

        if not output_path.is_file():
            hint = ""
            combined = result.stderr + result.stdout
            if "no test target named" in combined:
                hint = (
                    "\ncrates/alopex-embedded/tests/parity_corpus.rs が未実装。"
                    "本ハーネスの契約(EmbeddedSurface docstring)に従って実装する。"
                )
            raise SurfaceError(
                "embedded 経路が出力 JSON を生成しなかった"
                f" (cargo exit={result.returncode}):\n{_tail(combined)}{hint}"
            )

        try:
            entries = normalize.load_statements_file(output_path)
        except normalize.NormalizeError as exc:
            raise SurfaceError(f"embedded 出力のスキーマ不正: {exc}") from exc
        return EmbeddedRunResult(
            entries=entries,
            cargo_ok=(result.returncode == 0),
            stderr_tail=_tail(result.stderr),
        )


# ---------------------------------------------------------------------------
# 経路 2: CLI バッチ(製品バイナリ alopex)
# ---------------------------------------------------------------------------


def _parse_json_documents(text: str) -> List[Any]:
    """連結された JSON ドキュメント列(配列が複数並ぶ出力)を順に読む。"""
    decoder = json.JSONDecoder()
    documents: List[Any] = []
    index = 0
    length = len(text)
    while index < length:
        while index < length and text[index].isspace():
            index += 1
        if index >= length:
            break
        obj, end = decoder.raw_decode(text, index)
        documents.append(obj)
        index = end
    return documents


#: CLI が DDL/DML 成功時に出力するステータス結果集合の列
#: (crates/alopex-cli/src/commands/sql.rs sql_status_columns())
_CLI_STATUS_COLUMNS = ["status", "message"]

#: DML 成功メッセージ "N row(s) affected"(同 sql.rs)
_CLI_AFFECTED_RE = re.compile(r"^(\d+) row\(s\) affected$")


def _record_from_json_array(sql: str, array: List[Dict[str, Any]]) -> Dict[str, Any]:
    """CLI の --output json(列名キーのオブジェクト配列)を生レコード化する。

    - DDL/DML 成功時、CLI は列 ``status`` / ``message`` の 1 行ステータス
      結果集合を出力する(sql_status_columns())。これを success /
      rows_affected(count はメッセージ "N row(s) affected" から抽出)へ
      写像する。コーパスの SELECT はこの列構成の結果を返さないこと
      (返すと誤ってステータスと解釈される。コーパス設計上の制約)。
    - それ以外は query として扱う。JSON オブジェクトのキー順は CLI 出力の
      列順を保持する(json.loads は挿入順を維持)。空の結果集合は列名情報を
      持たないため columns=None。
    """
    base = {
        "sql": sql,
        "columns": None,
        "rows": None,
        "affected_count": None,
        "error_message": None,
    }
    if not array:
        return {**base, "kind": "query", "rows": []}
    columns = list(array[0].keys())
    if (
        columns == _CLI_STATUS_COLUMNS
        and len(array) == 1
        and array[0].get("status") == "OK"
    ):
        message = array[0].get("message") or ""
        match = _CLI_AFFECTED_RE.match(message)
        if match:
            return {**base, "kind": "rows_affected", "affected_count": int(match.group(1))}
        return {**base, "kind": "success"}
    rows = [[obj.get(name) for name in columns] for obj in array]
    return {**base, "kind": "query", "columns": columns, "rows": rows}


class CliSurface:
    """CLI バッチ経路(``alopex --batch --output json``)。"""

    def __init__(self, binary: Path, repo: Path) -> None:
        self.binary = binary
        self.repo = repo
        self.env = product_env(repo)

    def _run(
        self, args: Sequence[str], *, timeout: float = 300.0
    ) -> subprocess.CompletedProcess:
        command = [str(self.binary), "--batch", "--output", "json", *args]
        return subprocess.run(
            command,
            env=self.env,
            capture_output=True,
            text=True,
            timeout=timeout,
        )

    def run_statement(self, sql: str, *, data_dir: Path) -> Dict[str, Any]:
        """1 文を 1 プロセスで実行する(SF-FILE。状態はデータディレクトリが持つ)。"""
        result = self._run(["--data-dir", str(data_dir), "sql", sql])
        if result.returncode != 0:
            message = result.stderr.strip() or result.stdout.strip()
            return {
                "sql": sql,
                "kind": "error",
                "columns": None,
                "rows": None,
                "affected_count": None,
                "error_message": message,
            }
        documents = _parse_json_documents(result.stdout)
        arrays = [doc for doc in documents if isinstance(doc, list)]
        if not arrays:
            # CLI は成功時に必ずステータス結果集合を出す(sql.rs)。
            # 出力なしは想定外だが、成功終了している以上 success として扱う。
            return {
                "sql": sql,
                "kind": "success",
                "columns": None,
                "rows": None,
                "affected_count": None,
                "error_message": None,
            }
        if len(arrays) > 1:
            raise SurfaceError(
                f"CLI が 1 文に対して複数の結果集合を出力した: {sql!r}"
            )
        return _record_from_json_array(sql, arrays[0])

    def run_statements(
        self, statements: Sequence[str], *, data_dir: Path
    ) -> List[Dict[str, Any]]:
        """複数文を 1 文 = 1 プロセスで順次実行する(SF-FILE)。

        プロセスを跨いだ状態はデータディレクトリのみが持つため、
        INV-1(順次アクセスでの可搬性)の検証条件とも整合する。
        """
        return [self.run_statement(sql, data_dir=data_dir) for sql in statements]

    def run_file_in_memory(self, sql_file: Path) -> List[Dict[str, Any]]:
        """SF-MEM: ``--in-memory sql -f <file>`` を単一プロセスで実行する。

        **既知の製品挙動(コンテナ実測 2026-07-13、別途バグ報告予定)**:
        CLI は複数文を 1 プロセスに渡すと**最後の文の結果のみ**を出力する。
        インメモリはプロセスを跨げないため文単位の分割起動もできず、
        複数文ファイルの文単位結果は本経路では取得できない。このため
        S1 第 1 幕の SF-MEM は組み込み API 経路(EmbeddedSurface の
        インメモリモード)を使う(demo.py act1_memory 参照)。

        本メソッドは「出力配列が文と 1:1 に対応する」場合(単文ファイル、
        または上記製品挙動が修正された場合)のみレコード列を返し、
        それ以外は SurfaceSkip で理由を明示する(偽装完了禁止)。
        """
        statements = split_sql_statements(sql_file.read_text(encoding="utf-8"))
        result = self._run(["--in-memory", "sql", "-f", str(sql_file)])
        if result.returncode != 0:
            raise SurfaceSkip(
                "CLI in-memory 経路はバッチ実行中のエラーを文単位に帰属できない"
                f" (exit={result.returncode}):\n{_tail(result.stderr)}"
            )
        documents = _parse_json_documents(result.stdout)
        arrays = [doc for doc in documents if isinstance(doc, list)]
        if len(arrays) != len(statements):
            raise SurfaceSkip(
                "CLI in-memory 出力の結果集合数が文数と一致しない"
                f"(結果={len(arrays)} / 文={len(statements)})。"
                " CLI は複数文入力に対し最後の文の結果のみを出力する製品挙動"
                "のため、複数文の SF-MEM 検証は組み込み API 経路を使うこと。"
            )
        return [
            _record_from_json_array(sql, array)
            for sql, array in zip(statements, arrays)
        ]


# ---------------------------------------------------------------------------
# サーバー起動管理(alopex-server)
# ---------------------------------------------------------------------------


def find_free_port() -> int:
    """空きポートを動的に割り当てる。"""
    import socket

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


#: 起動中の alopex-server の登録簿。シグナルハンドラ(verify.py)からの
#: 一括停止(cleanup_active_servers)に使う。
_ACTIVE_SERVERS: List["ServerHandle"] = []


def cleanup_active_servers() -> None:
    """起動中の全サーバーを確実に停止する(シグナルハンドラ用)。"""
    for handle in list(_ACTIVE_SERVERS):
        try:
            handle.stop()
        except Exception:  # noqa: BLE001 - 掃除は最後まで続行する
            pass


@dataclass(eq=False)  # 登録簿からの除去は同一性(identity)で判定する
class ServerHandle:
    """起動済み alopex-server のハンドル(確実な停止を担う)。"""

    process: subprocess.Popen
    http_base: str
    grpc_target: str
    admin_base: str
    work_dir: Path
    _log_files: List[Any] = field(default_factory=list)

    def stop(self, timeout: float = 10.0) -> None:
        """SIGTERM -> 待機 -> SIGKILL で確実に停止する(冪等)。"""
        try:
            if self.process.poll() is None:
                self.process.terminate()
                try:
                    self.process.wait(timeout=timeout)
                except subprocess.TimeoutExpired:
                    self.process.kill()
                    self.process.wait(timeout=timeout)
        finally:
            for fh in self._log_files:
                try:
                    fh.close()
                except OSError:
                    pass
            self._log_files.clear()
            if self in _ACTIVE_SERVERS:
                _ACTIVE_SERVERS.remove(self)

    def stderr_tail(self, lines: int = 30) -> str:
        path = self.work_dir / "server-stderr.log"
        if path.is_file():
            return _tail(path.read_text(encoding="utf-8", errors="replace"), lines)
        return ""

    def __enter__(self) -> "ServerHandle":
        return self

    def __exit__(self, *_exc: Any) -> None:
        self.stop()


def start_server(
    binary: Path,
    *,
    repo: Path,
    data_dir: Path,
    work_dir: Path,
    ready_timeout: float = 60.0,
) -> ServerHandle:
    """alopex-server を空きポートで起動し、/healthz が ready になるまで待つ。

    - ポート(http/grpc/admin)は動的割当。
    - 設定は work_dir/alopex.toml に書き、cwd=work_dir で起動する
      (docs/server-guide.md: カレントディレクトリの alopex.toml を読む)。
    - ready 確認は admin ポートの GET /healthz ポーリング。
    - 停止は ServerHandle.stop()(context manager 推奨)。
    """
    import requests

    http_port = find_free_port()
    grpc_port = find_free_port()
    admin_port = find_free_port()

    config = "\n".join(
        [
            f'http_bind = "127.0.0.1:{http_port}"',
            f'grpc_bind = "127.0.0.1:{grpc_port}"',
            f'admin_bind = "127.0.0.1:{admin_port}"',
            f'data_dir = "{data_dir}"',
            'auth_mode = { type = "none" }',
            "",
        ]
    )
    (work_dir / "alopex.toml").write_text(config, encoding="utf-8")

    stdout_log = open(work_dir / "server-stdout.log", "w", encoding="utf-8")
    stderr_log = open(work_dir / "server-stderr.log", "w", encoding="utf-8")
    try:
        process = subprocess.Popen(
            [str(binary)],
            cwd=work_dir,
            env=product_env(repo),
            stdout=stdout_log,
            stderr=stderr_log,
        )
    except OSError as exc:
        # Popen 失敗時に開いたログファイルをリークさせない
        stdout_log.close()
        stderr_log.close()
        raise SurfaceError(f"alopex-server の起動に失敗: {exc}") from exc
    handle = ServerHandle(
        process=process,
        http_base=f"http://127.0.0.1:{http_port}",
        grpc_target=f"127.0.0.1:{grpc_port}",
        admin_base=f"http://127.0.0.1:{admin_port}",
        work_dir=work_dir,
        _log_files=[stdout_log, stderr_log],
    )
    _ACTIVE_SERVERS.append(handle)

    deadline = time.monotonic() + ready_timeout
    health_url = handle.admin_base + HEALTH_PATH
    while time.monotonic() < deadline:
        if process.poll() is not None:
            tail = handle.stderr_tail()
            handle.stop()
            raise SurfaceError(
                f"alopex-server が起動直後に終了 (exit={process.returncode}):\n{tail}"
            )
        try:
            response = requests.get(health_url, timeout=1.0)
            if response.status_code == 200:
                return handle
        except requests.RequestException:
            pass
        time.sleep(0.2)

    tail = handle.stderr_tail()
    handle.stop()
    raise SurfaceError(
        f"alopex-server が {ready_timeout}s 以内に ready にならない"
        f" ({health_url}):\n{tail}"
    )


# ---------------------------------------------------------------------------
# 経路 3: HTTP(requests)
# ---------------------------------------------------------------------------


class HttpSurface:
    """HTTP 経路。仕様パスは HTTP_SQL_PATH を参照。"""

    def __init__(
        self, base_url: str, *, sql_path: str = HTTP_SQL_PATH, timeout: float = 60.0
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.sql_path = sql_path
        self.timeout = timeout

    def execute(self, sql: str) -> Dict[str, Any]:
        import requests

        try:
            response = requests.post(
                self.base_url + self.sql_path,
                json={"sql": sql, "session_id": None, "streaming": False},
                timeout=self.timeout,
            )
        except requests.RequestException as exc:
            raise SurfaceError(f"HTTP 経路の接続失敗: {exc}") from exc

        base = {
            "sql": sql,
            "columns": None,
            "rows": None,
            "affected_count": None,
            "error_message": None,
        }
        if response.status_code == 200:
            # map_execution_result (crates/alopex-server/src/http/sql.rs):
            #   Query        -> columns 非空
            #   RowsAffected -> columns 空 + affected_rows = Some(n)
            #   Success      -> columns 空 + affected_rows = None
            body = response.json()
            columns_meta = body.get("columns")
            affected = body.get("affected_rows")
            if columns_meta:
                columns = [col["name"] for col in columns_meta]
                # rows の値は SqlValue の serde 外部タグ形式
                # ({"BigInt": 6} / "Null" 等、normalize.HTTP_VALUE_TAGS 参照)。
                # 素の値へアンラップする。未知タグは明示エラーのまま
                # 環境エラー(SurfaceError)として報告する。
                try:
                    rows = [
                        [normalize.unwrap_http_value(v) for v in row]
                        for row in body.get("rows") or []
                    ]
                except normalize.NormalizeError as exc:
                    raise SurfaceError(
                        f"HTTP 値のアンラップ失敗 (sql={sql!r}): {exc}"
                    ) from exc
                return {**base, "kind": "query", "columns": columns, "rows": rows}
            if affected is not None:
                return {**base, "kind": "rows_affected", "affected_count": affected}
            return {**base, "kind": "success"}

        return {**base, "kind": "error", "error_message": self._extract_error(response)}

    @staticmethod
    def _extract_error(response: Any) -> str:
        try:
            body = response.json()
        except ValueError:
            return response.text
        if isinstance(body, dict):
            for key in ("error", "message", "detail"):
                value = body.get(key)
                if isinstance(value, str) and value:
                    return value
                if isinstance(value, dict):
                    inner = value.get("message")
                    if isinstance(inner, str) and inner:
                        return inner
        return response.text

    def run_statements(self, statements: Sequence[str]) -> List[Dict[str, Any]]:
        return [self.execute(sql) for sql in statements]


# ---------------------------------------------------------------------------
# 経路 4: gRPC(grpcio + 実行時スタブ生成)
# ---------------------------------------------------------------------------


class GrpcSurface:
    """gRPC 経路(AlopexService)。

    - スタブは grpcio-tools で ``crates/alopex-server/proto/alopex.proto`` から
      実行時に生成する。生成物は tempdir に置き、コミットしない。
    - RPC ルーティングは文種別で行う(サーバーの gRPC サーフェスの定義に従う):
        - 結果集合を返す文(is_query_statement) -> ``ExecuteSql``(stream Row)
        - DML(is_dml_statement)              -> ``ExecuteDml``(affected_rows)
        - それ以外(DDL)                       -> ``ExecuteDdl``(success)
    - proto の ``Row`` は列名メタデータを持たないため query の columns は
      常に None とし、正規化時に他経路または期待値の列名で補完する
      (runner.normalize 参照)。
    """

    def __init__(self, target: str, *, proto_path: Path, timeout: float = 60.0) -> None:
        self.target = target
        self.proto_path = proto_path
        self.timeout = timeout
        self._stub_dir: Optional[str] = None
        self._pb2: Any = None
        self._pb2_grpc: Any = None
        self._channel: Any = None
        self._stub: Any = None

    # -- スタブ生成 -----------------------------------------------------------

    def _ensure_stubs(self) -> None:
        if self._stub is not None:
            return
        if not self.proto_path.is_file():
            raise SurfaceError(f"proto ファイルが見つからない: {self.proto_path}")

        try:
            import grpc
            from grpc_tools import protoc
        except ImportError as exc:
            raise SurfaceError(
                f"grpcio / grpcio-tools が未インストール: {exc}。"
                " requirements.txt の依存を導入した環境で実行すること。"
            ) from exc

        stub_dir = tempfile.mkdtemp(prefix="alopex-parity-grpc-")
        self._stub_dir = stub_dir
        proto_dir = str(self.proto_path.parent)
        rc = protoc.main(
            [
                "protoc",
                f"-I{proto_dir}",
                f"--python_out={stub_dir}",
                f"--grpc_python_out={stub_dir}",
                str(self.proto_path),
            ]
        )
        if rc != 0:
            self.close()
            raise SurfaceError(f"gRPC スタブ生成失敗 (protoc exit={rc})")

        sys.path.insert(0, stub_dir)
        try:
            import importlib

            self._pb2 = importlib.import_module("alopex_pb2")
            self._pb2_grpc = importlib.import_module("alopex_pb2_grpc")
        except ImportError as exc:
            self.close()
            raise SurfaceError(f"生成スタブの import 失敗: {exc}") from exc

        self._channel = grpc.insecure_channel(self.target)
        self._stub = self._pb2_grpc.AlopexServiceStub(self._channel)

    # -- 値のデコード ----------------------------------------------------------

    def _decode_value(self, value: Any) -> Any:
        kind = value.WhichOneof("kind")
        if kind is None:
            return None  # 値未設定 = NULL
        if kind == "vector_value":
            return list(value.vector_value.values)
        if kind == "blob_value":
            # BLOB の正規化表現は未定義(normalize 側で明示エラーになる)
            return bytes(value.blob_value)
        return getattr(value, kind)

    def _decode_row(self, row: Any) -> List[Any]:
        return [self._decode_value(v) for v in row.values]

    # -- 実行 -----------------------------------------------------------------

    def execute(self, sql: str) -> Dict[str, Any]:
        self._ensure_stubs()
        import grpc

        base = {
            "sql": sql,
            "columns": None,
            "rows": None,
            "affected_count": None,
            "error_message": None,
        }
        try:
            if is_query_statement(sql):
                request = self._pb2.SqlRequest(sql=sql, session_id="")
                rows = [
                    self._decode_row(row)
                    for row in self._stub.ExecuteSql(request, timeout=self.timeout)
                ]
                # proto の Row は列名メタデータを持たない(columns=None)
                return {**base, "kind": "query", "rows": rows}
            if is_dml_statement(sql):
                request = self._pb2.DmlRequest(sql=sql, session_id="")
                response = self._stub.ExecuteDml(request, timeout=self.timeout)
                return {
                    **base,
                    "kind": "rows_affected",
                    "affected_count": int(response.affected_rows),
                }
            request = self._pb2.DdlRequest(sql=sql, session_id="")
            response = self._stub.ExecuteDdl(request, timeout=self.timeout)
            if not response.success:
                # エラーは grpc.RpcError で通知されるのが正常経路。
                # success=false の正常応答は契約外のため環境エラーとする。
                raise SurfaceError(
                    f"ExecuteDdl が success=false を返した(契約外): {sql!r}"
                )
            return {**base, "kind": "success"}
        except grpc.RpcError as exc:
            code = exc.code()
            if code is not None and code.name == "UNAVAILABLE":
                raise SurfaceError(f"gRPC 経路の接続失敗: {exc.details()}") from exc
            return {**base, "kind": "error", "error_message": exc.details() or str(exc)}

    def run_statements(self, statements: Sequence[str]) -> List[Dict[str, Any]]:
        return [self.execute(sql) for sql in statements]

    # -- 後始末 ---------------------------------------------------------------

    def close(self) -> None:
        if self._channel is not None:
            self._channel.close()
            self._channel = None
            self._stub = None
        if self._stub_dir is not None:
            if self._stub_dir in sys.path:
                sys.path.remove(self._stub_dir)
            shutil.rmtree(self._stub_dir, ignore_errors=True)
            self._stub_dir = None

    def __enter__(self) -> "GrpcSurface":
        return self

    def __exit__(self, *_exc: Any) -> None:
        self.close()
