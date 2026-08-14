#!/usr/bin/env python3
"""組み込み API(Rust / Python)からの SQL 実行デモ。

「ケース 1: Rust 組み込み API から SQL を実行する」「ケース 2: Python
組み込み API から SQL を実行する」「ケース 5: 統計と PRAGMA を両言語から」
「ケース 6: 5 経路の等価性」に対応する。

デモの目的はシナリオと結果の提示であり、期待値アサートは行わない。
呼び出した API 名とシグネチャを結果と併記し、レポートから記事へコード例を
引用できるようにする(ケース 6 のみ、経路間の一致の有無を表示する)。

| 場 | 経路              | 操作                                              |
|----|------------------|---------------------------------------------------|
| 1  | Python (SF-MEM)  | Database.new() でコーパス実行 + 検証クエリ 8 文    |
| 2  | Python (SF-FILE) | Database.open(path) で開き直して検証クエリ         |
| 3  | Python           | Transaction の commit / rollback                   |
| 4  | Python           | execute_sql_stream() での反復取得                  |
| 5  | Python           | 統計関数と PRAGMA                                  |
| 6  | 5 経路           | CLI / HTTP / gRPC / Rust API / Python API の一致   |

コーパスは scripts/parity/corpus(共有)を用いる。検証クエリ 8 文の
結果は 99_verify.sql 時点の状態(users 6 / orders 7 / products 5 /
regions 3 / docs 4)である。demo.py 第 3 幕以降はサーバー経由の追加
INSERT 後の状態(orders 9 / 合計 351.0)になる点に注意する。

出力はすべて CLI 経路と同じ正規化規則(scripts/parity/runner/normalize.py:
有効数字 9 桁、null、エラー分類)を通す。

exit code: 成功 0 / 環境・起動エラー 2
"""

from __future__ import annotations

import sys
import tempfile
from pathlib import Path
from typing import Any, Dict, List, Sequence

PARITY_DIR = Path(__file__).resolve().parents[2] / "parity"
sys.path.insert(0, str(PARITY_DIR))

from runner import normalize, surfaces  # noqa: E402
from runner.report import EXIT_ENV, EXIT_OK, results_equal  # noqa: E402
from runner.surfaces import (  # noqa: E402
    CliSurface,
    EmbeddedSurface,
    GrpcSurface,
    HttpSurface,
    PythonSurface,
    SurfaceError,
    start_server,
)

#: 表示する行数の上限(人間向け表示)。
DISPLAY_ROW_LIMIT = 8


def banner(scene: int, title: str) -> None:
    print()
    print("=" * 72)
    print(f"場 {scene}: {title}")
    print("=" * 72)


def note(text: str) -> None:
    print(f"  注記: {text}")


def show_call(signature: str) -> None:
    """呼び出した API のシグネチャそのものを出力する。"""
    print(f"  call> {signature}")


def show_entries(entries: Sequence[Dict[str, Any]]) -> None:
    """正規化済みエントリを人間可読形式で出力する。"""
    for entry in entries:
        print(f"       sql: {entry['sql']}")
        result = entry["result"]
        kind = result["type"]
        if kind == "query":
            columns = result.get("columns")
            if columns:
                print(f"            columns: {', '.join(columns)}")
            rows = result["rows"]
            for row in rows[:DISPLAY_ROW_LIMIT]:
                print(f"            {row}")
            if len(rows) > DISPLAY_ROW_LIMIT:
                print(f"            ... 全 {len(rows)} 行")
        elif kind == "rows_affected":
            print(f"            -> {result['count']} row(s) affected")
        elif kind == "success":
            print("            -> OK")
        else:
            print(
                f"            -> ERROR {result['error_class']}"
                f" object={result['object']} code={result['code']}"
            )


def compare(label: str, left: Sequence[Dict[str, Any]], right: Sequence[Dict[str, Any]]) -> bool:
    """2 経路の正規化済み結果を比較し、一致の有無を表示する。

    デモはアサートしない(不一致でも exit を落とさない)。事実として
    一致/不一致を表示する。
    """
    if len(left) != len(right):
        print(f"  ✗ {label}: 文数不一致 ({len(left)} vs {len(right)})")
        return False
    notes: List[str] = []
    for index, (lhs, rhs) in enumerate(zip(left, right)):
        equal, note_text = results_equal(lhs, rhs)
        if not equal:
            print(f"  ✗ {label}: statement[{index}] が不一致")
            print(f"      sql: {lhs.get('sql', '')}")
            return False
        if note_text and note_text not in notes:
            notes.append(note_text)
    suffix = f" ({notes[0]})" if notes else ""
    print(f"  ✔ {label}: 全 {len(left)} 文が一致{suffix}")
    return True


# ---------------------------------------------------------------------------
# 場 1 / 2: Python 組み込み API(SF-MEM / SF-FILE)
# ---------------------------------------------------------------------------


def scene1_python_memory(
    py: PythonSurface,
    corpus_statements: Sequence[str],
    verify_statements: Sequence[str],
) -> List[Dict[str, Any]]:
    banner(1, "Python 組み込み API (SF-MEM) — Database.new() でコーパスを実行")

    show_call("alopex.Database.new()")
    db = py.open_in_memory()

    show_call(f"db.execute_sql(<コーパス {len(corpus_statements)} 文を順次>)")
    records = py.run_statements(db, corpus_statements)
    errors = [r for r in records if r["kind"] == "error"]
    print(f"       -> {len(records)} 文実行 / エラー {len(errors)} 件")
    for record in errors[:3]:
        print(f"          ERROR {record['sql'][:60]}: {record['error_message']}")

    show_call(f"db.execute_sql(<検証クエリ {len(verify_statements)} 文>)")
    entries = normalize.normalize_records(py.run_statements(db, verify_statements))
    show_entries(entries)
    note(
        "コーパス・検証クエリは scripts/parity/corpus と同一(CLI/HTTP/gRPC と共有)。"
        " 出力は runner.normalize による正規化済みの値である。"
    )
    return entries


def scene2_python_file(
    py: PythonSurface,
    corpus_statements: Sequence[str],
    verify_statements: Sequence[str],
    data_dir: Path,
) -> List[Dict[str, Any]]:
    banner(2, "Python 組み込み API (SF-FILE) — Database.open(path) で開き直す")
    print(f"  data-dir: {data_dir}")

    show_call(f"alopex.Database.open({str(data_dir)!r})")
    db = py.open_path(data_dir)
    show_call(f"db.execute_sql(<コーパス {len(corpus_statements)} 文を順次>)")
    records = py.run_statements(db, corpus_statements)
    errors = [r for r in records if r["kind"] == "error"]
    print(f"       -> {len(records)} 文実行 / エラー {len(errors)} 件")

    # 同一プロセス内でハンドルを落とし、開き直して永続性を示す。
    del db
    print("  (ハンドルを解放し、同一ディレクトリを開き直す)")
    show_call(f"alopex.Database.open({str(data_dir)!r})")
    reopened = py.open_path(data_dir)

    show_call(f"db.execute_sql(<検証クエリ {len(verify_statements)} 文>)")
    entries = normalize.normalize_records(py.run_statements(reopened, verify_statements))
    show_entries(entries)
    note(
        "ファイルパスを開く API は alopex.Database.open(path) である"
        "(メモリ専用ではない)。開き直した後も同一の結果が得られる。"
    )
    return entries


def scene3_python_transaction(py: PythonSurface, data_dir: Path) -> None:
    banner(3, "Python 組み込み API — Transaction の commit / rollback")

    show_call(f"alopex.Database.open({str(data_dir)!r})")
    db = py.open_path(data_dir)
    alopex = py.alopex

    show_call("db.execute_sql('CREATE TABLE txn_demo (id INT PRIMARY KEY, note TEXT)')")
    show_entries(
        normalize.normalize_records(
            [py.execute(db, "CREATE TABLE txn_demo (id INT PRIMARY KEY, note TEXT)")]
        )
    )

    # -- commit --
    show_call("db.begin(alopex.TxnMode.READ_WRITE)")
    txn = db.begin(alopex.TxnMode.READ_WRITE)
    insert_committed = "INSERT INTO txn_demo VALUES (1, 'committed')"
    show_call(f"txn.execute_sql({insert_committed!r})")
    print(f"       -> {txn.execute_sql(insert_committed)} row(s) affected")
    show_call("txn.commit()")
    txn.commit()
    print("       -> OK")

    # -- rollback --
    show_call("db.begin(alopex.TxnMode.READ_WRITE)")
    txn2 = db.begin(alopex.TxnMode.READ_WRITE)
    insert_rolled_back = "INSERT INTO txn_demo VALUES (2, 'rolled back')"
    show_call(f"txn2.execute_sql({insert_rolled_back!r})")
    print(f"       -> {txn2.execute_sql(insert_rolled_back)} row(s) affected")
    show_call("txn2.rollback()")
    txn2.rollback()
    print("       -> OK")

    show_call("db.execute_sql('SELECT id, note FROM txn_demo ORDER BY id')")
    show_entries(
        normalize.normalize_records(
            [py.execute(db, "SELECT id, note FROM txn_demo ORDER BY id")]
        )
    )
    note(
        "commit した行(id=1)のみが残り、rollback した行(id=2)は破棄される。"
        " TxnMode の値は READ_ONLY / READ_WRITE であり、既定 (引数なしの"
        " begin()) は READ_ONLY のため INSERT は拒否される。"
    )


def scene4_python_stream(py: PythonSurface, data_dir: Path) -> None:
    banner(4, "Python 組み込み API — execute_sql_stream() での反復取得")

    show_call(f"alopex.Database.open({str(data_dir)!r})")
    db = py.open_path(data_dir)

    # API の不在(AttributeError)と、API はあるが対象外 SQL で失敗する場合を
    # 区別する。両者を同じ except で握ると「メソッドが無い」ことを
    # 「SQL が対象外」と誤って説明してしまう(スキップを完了と偽らない)。
    if not hasattr(db, "execute_sql_stream"):
        note(
            "公開 wheel に execute_sql_stream / query_stream が存在しないため"
            " 実行できない (issue #82)。"
            " https://github.com/alopex-db/alopex/issues/82"
        )
        print("  SKIP (未実行。成功数には数えない)")
        return

    sql = "SELECT id, name, region FROM users"
    show_call(f"db.execute_sql_stream({sql!r})")
    try:
        stream = db.execute_sql_stream(sql)
        print(f"       -> {type(stream).__name__} (イテレータ)")
        for index, row in enumerate(stream, start=1):
            print(f"       [{index}] {row}")
    except Exception as exc:  # noqa: BLE001
        # API は存在するが実行に失敗した場合(対象外 SQL 等)。
        print(f"       -> ERROR {type(exc).__name__}: {exc}")
        note(
            "ストリーミングは単一テーブルの SELECT(行ローカル WHERE / 射影 /"
            " LIMIT / OFFSET)のみ対応する。対象外の文はエラーになる。"
        )
        return
    note(
        "execute_sql_stream() は結果を一括構築せずイテレータとして返す。"
        " 取得した行を順に出力した。"
    )


def scene5_stats_pragma(py: PythonSurface, data_dir: Path) -> None:
    banner(5, "統計関数と PRAGMA — Python の execute_sql() から実行")

    show_call(f"alopex.Database.open({str(data_dir)!r})")
    db = py.open_path(data_dir)

    statements = [
        "SELECT memory_stats() AS memory_stats",
        "SELECT io_stats() AS io_stats",
        "SELECT clear_cache() AS cleared_bytes",
        "PRAGMA cache_size = 8",
        "PRAGMA memory_limit = '64MiB'",
        "PRAGMA io_stats",
    ]
    for sql in statements:
        show_call(f"db.execute_sql({sql!r})")
        show_entries(normalize.normalize_records([py.execute(db, sql)]))

    note(
        "demo_sql_v074.sh は同じ関数・PRAGMA を CLI から実行している。"
        " ここでは同一のものを Python の execute_sql() から実行した。"
    )


# ---------------------------------------------------------------------------
# 場 6: 5 経路の等価性
# ---------------------------------------------------------------------------


def scene6_five_surfaces(
    repo: Path,
    py: PythonSurface,
    binaries: Dict[str, Path],
    corpus_dir: Path,
    corpus_statements: Sequence[str],
    verify_statements: Sequence[str],
    scratch: Path,
    python_entries: Sequence[Dict[str, Any]],
) -> None:
    banner(6, "5 経路の等価性 — CLI / HTTP / gRPC / Rust API / Python API")
    note(
        "従来の mode-parity は CLI・HTTP・gRPC・Rust(embedded)の 4 経路。"
        " ここに Python API を加えた 5 経路で、同一コーパスの正規化後の結果が"
        " 一致するかを表示する(デモのため不一致でも exit は落とさない)。"
    )

    results: Dict[str, List[Dict[str, Any]]] = {"Python API": list(python_entries)}

    # -- Rust API(embedded 経路) --
    print("\n  -- Rust 組み込み API (alopex-embedded) --")
    embedded_dir = scratch / "s6-embedded"
    show_call(
        "EmbeddedSurface(repo).run(corpus, role='writer', data_dir=None)"
        "  # PARITY_ROLE=writer / PARITY_DATA_DIR 未設定 = インメモリ"
    )
    try:
        embedded = EmbeddedSurface(repo).run(
            corpus_dir,
            role=surfaces.ROLE_WRITER,
            data_dir=None,
            output_path=embedded_dir / "out.json",
        )
        # writer ロールは 01〜07 の通し実行結果。末尾の検証クエリ相当は
        # 含まれないため、コーパス部分の文数で位置合わせする。
        rust_entries = embedded.entries[: len(corpus_statements)]
        print(f"       -> {len(embedded.entries)} 文の正規化出力を取得")
        results["Rust API"] = rust_entries
    except SurfaceError as exc:
        print(f"       SKIP: {exc}")

    # -- CLI / HTTP / gRPC --
    data_dir = scratch / "s6-data"
    data_dir.mkdir(parents=True, exist_ok=True)
    cli = CliSurface(binaries[surfaces.PRODUCT_BIN_CLI], repo)
    print("\n  -- CLI (alopex --batch --output json) --")
    show_call("CliSurface.run_statements(<コーパス>, data_dir=...)")
    cli.run_statements(corpus_statements, data_dir=data_dir)
    show_call("CliSurface.run_statements(<検証クエリ>, data_dir=...)")
    cli_entries = normalize.normalize_records(
        cli.run_statements(verify_statements, data_dir=data_dir)
    )
    show_entries(cli_entries)
    results["CLI"] = cli_entries

    work = scratch / "s6-server"
    work.mkdir(parents=True, exist_ok=True)
    with start_server(
        binaries[surfaces.PRODUCT_BIN_SERVER], repo=repo, data_dir=data_dir, work_dir=work
    ) as server:
        print(f"\n  -- HTTP ({server.http_base}) --")
        show_call("HttpSurface(base).run_statements(<検証クエリ>)")
        http_entries = normalize.normalize_records(
            HttpSurface(server.http_base).run_statements(verify_statements)
        )
        results["HTTP"] = http_entries

        print(f"  -- gRPC ({server.grpc_target}) --")
        show_call("GrpcSurface(target).run_statements(<検証クエリ>)")
        with GrpcSurface(
            server.grpc_target,
            proto_path=surfaces.server_proto_path(repo),
        ) as grpc_surface:
            grpc_raw = grpc_surface.run_statements(verify_statements)
        results["gRPC"] = normalize.normalize_records(
            grpc_raw, columns_source=cli_entries
        )

    # -- 一致の表示 --
    print("\n  -- 検証クエリ 8 文の一致(基準: CLI)--")
    baseline = results["CLI"]
    for name in ("HTTP", "gRPC", "Python API"):
        if name in results:
            compare(f"CLI <-> {name}", baseline, results[name])

    if "Rust API" in results:
        print("\n  -- Rust API はコーパス実行そのものの出力(検証クエリとは別)--")
        print(f"     Rust API entries = {len(results['Rust API'])} 文")
        note(
            "Rust(embedded)経路はコーパス 01〜07 を通し実行した結果を返す契約"
            "(EmbeddedSurface)であり、検証クエリ 8 文とは対象が異なる。"
            " 検証クエリでの 5 経路一致は verify.py (S2-a/S2-b) が担う。"
        )


def main() -> int:
    repo = surfaces.repo_root()
    corpus_dir = PARITY_DIR / "corpus"

    print("組み込み API サーフェスデモ (Rust / Python)")
    try:
        module = surfaces.import_alopex_module(repo)
        py = PythonSurface(module)
        print(f"  alopex パッケージ: {module.__file__}")

        corpus_statements = surfaces.load_statements(surfaces.corpus_files(corpus_dir))
        verify_statements = surfaces.load_statements(
            [surfaces.verify_sql_path(corpus_dir)]
        )

        with tempfile.TemporaryDirectory(prefix="v074-api-demo-") as tmp:
            scratch = Path(tmp)
            python_entries = scene1_python_memory(
                py, corpus_statements, verify_statements
            )

            file_dir = scratch / "python-data"
            file_dir.mkdir()
            scene2_python_file(py, corpus_statements, verify_statements, file_dir)
            scene3_python_transaction(py, file_dir)
            scene4_python_stream(py, file_dir)
            scene5_stats_pragma(py, file_dir)

            binaries = surfaces.build_products(repo)
            scene6_five_surfaces(
                repo,
                py,
                binaries,
                corpus_dir,
                corpus_statements,
                verify_statements,
                scratch,
                python_entries,
            )
    except (SurfaceError, normalize.NormalizeError) as exc:
        print(f"\n環境エラー: {exc}", file=sys.stderr)
        return EXIT_ENV

    print()
    print("=" * 72)
    print("デモ完了: 場 1〜6")
    print("=" * 72)
    return EXIT_OK


if __name__ == "__main__":
    sys.exit(main())
