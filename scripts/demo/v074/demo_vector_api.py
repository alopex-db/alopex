#!/usr/bin/env python3
"""ベクトル検索を Rust / Python の両方から実行するデモ。

「ケース 4: ベクトル検索を両言語から」に対応する。デモの目的はシナリオと
結果の提示であり、期待値アサートは行わない(呼び出した API 名・シグネチャ
と、その結果を出力する)。

コーパスは scripts/parity/corpus と同一の docs テーブル(99_verify.sql
実行時点で 4 行)を用いる。記事から同じ数字を引用できるよう、言語ごとに
別のデータを使わない。

| 場 | 経路                | 操作                                              |
|----|--------------------|---------------------------------------------------|
| 1  | Python (SQL)       | vector_distance(...) ORDER BY ... LIMIT           |
| 2  | Python (native)    | upsert_vector / search_similar / HNSW  ← issue #82 |
| 3  | Rust   (SQL)       | 同一 SQL を Rust の execute_sql() から             |
| 4  | Rust   (native)    | create_hnsw_index() / search_hnsw()                |

exit code: 成功 0 / 環境・起動エラー 2
"""

from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "v07"))

from _v07 import (  # noqa: E402
    BINARY_SOURCE_ENV,
    BINARY_SOURCE_RELEASED,
    EXIT_ENV,
    EXIT_OK,
    EnvError,
    banner,
    note,
    nim_parser_dir,
    repo_root,
)

#: 再実行ガード(LD_LIBRARY_PATH を整えて 1 回だけ exec し直す)。
REEXEC_MARKER = "V074_DEMO_VECTOR_REEXEC"

# ---------------------------------------------------------------------------
# コーパス(scripts/parity/corpus と同一の docs テーブル)
#
# 01_ddl.sql の docs 定義、02_dml.sql の 5 行投入、07_vector.sql の
# 「id=5 を [1.0, 0.25, 0.0] へ UPDATE、id=4 を DELETE」を適用した後の
# 状態(= 99_verify.sql が検証する 4 行)をそのまま再現する。
# ---------------------------------------------------------------------------

CORPUS_DDL = "CREATE TABLE docs (id INT PRIMARY KEY, title TEXT, embedding VECTOR(3, L2))"

CORPUS_DML = (
    "INSERT INTO docs (id, title, embedding) VALUES "
    "(1, 'alpha', [1.0, 0.0, 0.0]), "
    "(2, 'beta', [0.5, 1.0, 0.0]), "
    "(3, 'gamma', [0.0, 1.0, 1.0]), "
    "(5, 'echo', [1.0, 0.25, 0.0])"
)

#: クエリ点。07_vector.sql と同一。
QUERY_VECTOR = "[1.0, 0.0, 0.0]"

#: SQL 経路のベクトル検索(両言語で同一の文を実行する)。
VECTOR_SQL = (
    f"SELECT docs.id, vector_distance(docs.embedding, {QUERY_VECTOR}, 'l2') AS dist"
    " FROM docs"
    f" ORDER BY vector_distance(docs.embedding, {QUERY_VECTOR}, 'l2') ASC"
    " LIMIT 3"
)


def show_call(signature: str) -> None:
    """呼び出した API のシグネチャそのものを出力する。

    レポートから記事へコード例を引用できるよう、結果だけでなく
    「どう呼んだか」を必ず併記する。
    """
    print(f"  call> {signature}")


def show_rows(rows: Any) -> None:
    if rows is None:
        print("       -> None")
        return
    if isinstance(rows, int):
        print(f"       -> {rows} row(s) affected")
        return
    for row in rows:
        print(f"       {row}")


def ensure_alopex_importable(repo: Path) -> Any:
    """alopex Python パッケージを import する(demo_dataframe_p3.py と同一方針)。

    - 既定: リポジトリ内 crates/alopex-py/python を優先する。
    - ALOPEX_BINARY_SOURCE=released: PyPI 公開版(site-packages)を使う。
    - 拡張モジュールは libalopex_sql_parser を必要とする。wheel では
      package-local な alopex/native/ が rpath($ORIGIN/native)で解決される
      (.github/workflows/alopex-py.yml「Stage parser library ...」参照)。
      未解決の場合のみ LD_LIBRARY_PATH を整えて 1 回だけ exec し直す。
    """
    if os.environ.get(BINARY_SOURCE_ENV) != BINARY_SOURCE_RELEASED:
        package_dir = repo / "crates" / "alopex-py" / "python"
        if package_dir.is_dir():
            sys.path.insert(0, str(package_dir))
    try:
        import alopex  # noqa: PLC0415

        return alopex
    except ModuleNotFoundError as exc:
        raise EnvError(
            f"alopex Python パッケージが見つからない: {exc}。"
            " crates/alopex-py で `maturin develop` を実行してから再実行すること。"
        ) from exc
    except ImportError as exc:
        if "libalopex_sql_parser" in str(exc) and REEXEC_MARKER not in os.environ:
            nim_dir = str(nim_parser_dir(repo))
            env = dict(os.environ)
            current = env.get("LD_LIBRARY_PATH", "")
            env["LD_LIBRARY_PATH"] = f"{nim_dir}:{current}" if current else nim_dir
            env[REEXEC_MARKER] = "1"
            print(
                "  注記: libalopex_sql_parser 解決のため LD_LIBRARY_PATH="
                f"{nim_dir} で再実行する。"
            )
            sys.stdout.flush()
            os.execve(sys.executable, [sys.executable, *sys.argv], env)
        raise EnvError(
            f"alopex 拡張モジュールの import に失敗: {exc}。"
        ) from exc


# ---------------------------------------------------------------------------
# 場 1: Python から SQL 経由のベクトル検索
# ---------------------------------------------------------------------------


def scene1_python_sql(alopex: Any) -> None:
    banner(1, "Python 組み込み API — SQL 経由のベクトル検索")

    show_call("alopex.Database.new()")
    db = alopex.Database.new()

    show_call(f"db.execute_sql({CORPUS_DDL!r})")
    show_rows(db.execute_sql(CORPUS_DDL))

    show_call("db.execute_sql(<docs 4 行の INSERT>)")
    show_rows(db.execute_sql(CORPUS_DML))

    show_call(f"db.execute_sql({VECTOR_SQL!r})")
    show_rows(db.execute_sql(VECTOR_SQL))

    note(
        "docs は scripts/parity/corpus と同一(99_verify.sql 時点で 4 行)。"
        " クエリ点 [1.0, 0.0, 0.0] からの L2 距離昇順で Top-3 を取得する。"
    )


# ---------------------------------------------------------------------------
# 場 2: Python のネイティブベクトル API
#
# ---------------------------------------------------------------------------
# 本場は issue #82 の対応後に有効化する。
# https://github.com/alopex-db/alopex/issues/82
#
# 公開 wheel (PyPI v0.8.4) には以下のネイティブベクトル API が 1 つも
# 含まれていないため、現状このデモを実行すると AttributeError で落ちる。
#
#   Database.create_hnsw_index / search_hnsw / drop_hnsw_index / get_hnsw_stats
#   Transaction.upsert_vector / search_similar / get_vector
#                / upsert_to_hnsw / delete_from_hnsw
#
# 原因: これらは crates/alopex-py で #[cfg(feature = "numpy")] 配下にあり、
# Cargo.toml が default = [] であるうえ、alopex-py-release.yml の wheel
# ビルドが --features numpy を渡していない。したがって公開物では常に
# 欠落する。SQL 経由のベクトル演算(場 1)は影響を受けない。
#
# #82 の対応後、以下のコメントを解除する(API 名・シグネチャは
# crates/alopex-py/python/alopex/_alopex.pyi:491,492,538,545 に準拠)。
#
# def scene2_python_native(alopex: Any) -> None:
#     banner(2, "Python 組み込み API — ネイティブベクトル API")
#
#     show_call("alopex.Database.new()")
#     db = alopex.Database.new()
#
#     show_call("db.begin(alopex.TxnMode.READ_WRITE)")
#     tx = db.begin(alopex.TxnMode.READ_WRITE)
#
#     show_call(
#         "tx.upsert_vector(key=b'doc-1', metadata=None,"
#         " vector=[1.0, 0.0, 0.0], metric=alopex.Metric.L2)"
#     )
#     tx.upsert_vector(b"doc-1", None, [1.0, 0.0, 0.0], alopex.Metric.L2)
#
#     show_call(
#         "tx.search_similar(query=[1.0, 0.0, 0.0],"
#         " metric=alopex.Metric.L2, k=3)"
#     )
#     show_rows(tx.search_similar([1.0, 0.0, 0.0], alopex.Metric.L2, 3))
#
#     show_call("tx.commit()")
#     tx.commit()
#
#     # HnswConfig の実シグネチャは位置引数 (dim, m, ef_construction) である。
#     # .pyi スタブは dimension= と記載しており実体と乖離している (issue #82)。
#     show_call(
#         "db.create_hnsw_index('idx_docs_embedding',"
#         " alopex.HnswConfig(3, 8, 32))"
#     )
#     db.create_hnsw_index("idx_docs_embedding", alopex.HnswConfig(3, 8, 32))
#
#     show_call(
#         "db.search_hnsw('idx_docs_embedding',"
#         " query=[1.0, 0.0, 0.0], k=3, ef_search=None)"
#     )
#     results, stats = db.search_hnsw("idx_docs_embedding", [1.0, 0.0, 0.0], 3)
#     show_rows(results)
#     print(f"       stats: {stats}")
# ---------------------------------------------------------------------------


#: 場 2 が必要とするネイティブベクトル API。
#: (Database 側 / Transaction 側)
DATABASE_VECTOR_METHODS = ("create_hnsw_index", "search_hnsw")
TRANSACTION_VECTOR_METHODS = ("upsert_vector", "search_similar")


def scene2_blocked(alopex: Any) -> None:
    """ネイティブベクトル API の実在を確認し、無ければ SKIP を明示する。

    ハードコードで SKIP を出すと、issue #82 が解消された後もデモが
    「実行できない」と表示し続け、事実と乖離する。実際に API の有無を
    確認したうえで、無い場合のみ SKIP する。
    """
    banner(2, "Python 組み込み API — ネイティブベクトル API")

    db = alopex.Database.new()
    txn = db.begin()
    missing = [f"Database.{m}" for m in DATABASE_VECTOR_METHODS if not hasattr(db, m)]
    missing += [
        f"Transaction.{m}" for m in TRANSACTION_VECTOR_METHODS if not hasattr(txn, m)
    ]

    if not missing:
        # #82 解消後にここへ到達する。実装はコメントアウトしてあるため、
        # 誤って「成功」と表示しないよう、明示的に未実装として報告する。
        note(
            "ネイティブベクトル API が公開 wheel に存在する"
            "(issue #82 が解消された)。本場の実装コメントを解除すること。"
        )
        print("  SKIP (デモ未実装。成功数には数えない)")
        return

    note(
        "本場は issue #82 の対応後に有効化する"
        " (https://github.com/alopex-db/alopex/issues/82)。"
    )
    print("  公開 wheel に存在しないメソッド:")
    for name in missing:
        print(f"    - {name}")
    print(
        "  原因: これらは #[cfg(feature = \"numpy\")] 配下にあり、リリース\n"
        "  ビルドが --features numpy を渡していない。なお HnswConfig /\n"
        "  Metric / SearchResult などの型は公開されており構築できるため、\n"
        "  「型はあるが渡す先が無い」状態である。SQL 経由のベクトル検索\n"
        "  (場 1)は影響を受けず動作する。"
    )
    print("  SKIP (未実行。成功数には数えない)")


def main() -> int:
    repo = repo_root()
    print("ベクトル検索 API デモ (Rust / Python)")
    try:
        alopex = ensure_alopex_importable(repo)
        scene1_python_sql(alopex)
        scene2_blocked(alopex)
    except EnvError as exc:
        print(f"\n環境エラー: {exc}", file=sys.stderr)
        return EXIT_ENV

    print()
    print("=" * 72)
    print("デモ完了: 場 1 実行 / 場 2 SKIP (issue #82 待ち)")
    print("=" * 72)
    return EXIT_OK


if __name__ == "__main__":
    sys.exit(main())
