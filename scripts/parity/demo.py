#!/usr/bin/env python3
"""S1 エントリポイント: ライフサイクル昇格デモ「One Engine, Four Forms」。

docs-public/specs/alopex-mode-parity-spec.md「シナリオ S1」:

| 幕 | サーフェス          | 操作                                             |
|----|--------------------|--------------------------------------------------|
| 1  | SF-MEM             | コーパスをインメモリで実行し 99_verify.sql を照合 |
| 2  | SF-FILE            | --data-dir で実行 → プロセス終了 → 再オープン照合 |
| 3  | SF-HTTP / SF-GRPC  | 同一ディレクトリでサーバー起動、照合 + 追加 INSERT|
| 4  | SF-FILE            | サーバー停止後に CLI で再オープン、追加行を照合   |
| 5  | SF-CLUSTER         | SKIP 表示(v0.7 で有効化)                        |

検証層(verify.py)と同一の runner(コーパス・期待値・正規化)を共有する。
各幕末の検証が失敗した場合は即座に非ゼロ exit で停止する。

exit code: 成功 0 / 検証不一致 1 / 環境・起動エラー 2
"""

from __future__ import annotations

import argparse
import shutil
import sys
import tempfile
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence

PARITY_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(PARITY_DIR))

from runner import normalize, surfaces  # noqa: E402
from runner.report import EXIT_ENV, EXIT_MISMATCH, EXIT_OK, record_diff, results_equal  # noqa: E402
from runner.surfaces import (  # noqa: E402
    CliSurface,
    EmbeddedSurface,
    GrpcSurface,
    HttpSurface,
    SurfaceError,
    SurfaceSkip,
    start_server,
)

#: 第 3 幕でサーバー(SF-HTTP)経由の追加 INSERT に使う S1 専用ファイル。
#: 実行順序契約: 01〜07 -> 99(基準検証) -> 08 -> 99(after 検証)。
#: 08 の後に 03〜07 を再実行してはならない(dave/frank の「注文なし」前提が
#: 変わり 04/06 の期待値が無効になるため)。
ACT3_INSERT_FILENAME = "08_server_insert.sql"

#: 第 3 幕の追加 INSERT の合計影響行数(expected/08_server_insert.json は
#: 存在せず、インラインで確認する契約)
ACT3_INSERT_TOTAL_AFFECTED = 2

#: 期待値ゴールデンのファイル名(runner.normalize.load_statements_file 形)
EXPECTED_VERIFY = "99_verify.json"
EXPECTED_AFTER_INSERT = "99_verify.after_server_insert.json"

#: 表示する行数の上限(人間向け表示。検証は全行で行う)
DISPLAY_ROW_LIMIT = 5


class ActFailure(Exception):
    """幕末検証の不一致(exit 1)。"""


# ---------------------------------------------------------------------------
# 幕ゲート(オーケストレーター決定)
#
# 幕の合否ゲートは**幕末検証のみ**(仕様どおり: 99_verify / after variant の
# 期待値一致)。コーパス実行中の文エラーは次のとおり分離する:
#
# - 読み取り専用文(SELECT)のエラー: 幕を中断せず、件数と内容を正直に表示
#   して続行する。INV-2 逸脱(サーフェス間の結果差)の失敗判定は verify.py
#   (S2-a)の責務であり、demo の各幕は状態のライフサイクル
#   (作成 → 永続化 → サーバー → 還流)を検証するため。
# - 状態変更文(CREATE/DROP/INSERT/UPDATE/DELETE)のエラー: 後続幕のデータ
#   状態=ストーリーの前提が壊れるため、従来どおり即失敗(exit 1)。
#
# 文種別の判定は先頭キーワードで行う(コーパスは自作で既知)。
# ---------------------------------------------------------------------------

#: 状態変更文の先頭キーワード(エラー時に幕を即失敗させる)
STATE_CHANGING_KEYWORDS = {"CREATE", "DROP", "ALTER", "TRUNCATE", "INSERT", "UPDATE", "DELETE"}


def is_state_changing_statement(sql: str) -> bool:
    first = sql.lstrip().split(None, 1)
    return bool(first) and first[0].upper() in STATE_CHANGING_KEYWORDS


def _one_line(text: str, limit: int = 100) -> str:
    line = " ".join((text or "").split())
    return line[:limit] + ("..." if len(line) > limit else "")


def gate_corpus_errors(act_label: str, records: Sequence[Dict[str, Any]]) -> None:
    """コーパス実行中の文エラーを幕ゲート方針(上記コメント)で処理する。

    ``records`` は surfaces の生レコード列(kind == "error" を検査する)。
    読み取り専用文のエラーは注記付きで表示して続行、状態変更文のエラーは
    ActFailure を送出する。
    """
    errors = [r for r in records if r["kind"] == "error"]
    if not errors:
        return
    read_only = [r for r in errors if not is_state_changing_statement(r["sql"])]
    state_changing = [r for r in errors if is_state_changing_statement(r["sql"])]

    if read_only:
        print(f"  ⚠ 読み取り専用文のエラー {len(read_only)} 件(幕は続行):")
        for record in read_only:
            print(f"     - sql: {_one_line(record['sql'], 80)}")
            print(f"       err: {_one_line(record['error_message'] or '')}")
        print("     注記: 読み取り専用文のエラーはサーフェス逸脱の兆候であり、")
        print("           verify.py の S2-a が INV-2 違反として検出・報告する。")
        print("           幕の合否は幕末検証(期待値一致)のみで判定する。")

    if state_changing:
        first = state_changing[0]
        raise ActFailure(
            f"{act_label}: 状態変更文のエラー {len(state_changing)} 件"
            "(ストーリーの前提が壊れるため即失敗)。最初のエラー:"
            f" {_one_line(first['sql'], 80)}: {_one_line(first['error_message'] or '')}"
        )


# ---------------------------------------------------------------------------
# 表示ヘルパ
# ---------------------------------------------------------------------------


def banner(act: int, surface: str, title: str) -> None:
    print()
    print("=" * 72)
    print(f"第 {act} 幕 [{surface}] {title}")
    print("=" * 72)


def show_records(entries: Sequence[Dict[str, Any]]) -> None:
    """正規化済みエントリを人間可読形式で表示する。"""
    for entry in entries:
        print(f"  sql> {entry['sql']}")
        result = entry["result"]
        if result["type"] == "query":
            rows = result["rows"]
            columns = result.get("columns")
            if columns:
                print(f"       columns: {', '.join(columns)}")
            for row in rows[:DISPLAY_ROW_LIMIT]:
                print(f"       {row}")
            if len(rows) > DISPLAY_ROW_LIMIT:
                print(f"       ... 全 {len(rows)} 行")
        elif result["type"] == "success":
            print("       OK")
        elif result["type"] == "rows_affected":
            print(f"       OK ({result['count']} row(s) affected)")
        else:
            print(
                "       ERROR "
                f"{result['error_class']} object={result['object']} code={result['code']}"
            )


def assert_match(
    act_label: str,
    expected: Sequence[Dict[str, Any]],
    actual: Sequence[Dict[str, Any]],
) -> None:
    """幕末検証。不一致なら差分付きで ActFailure を送出する。"""
    if len(expected) != len(actual):
        raise ActFailure(
            f"{act_label}: 文数不一致 expected={len(expected)} actual={len(actual)}"
        )
    for index, (exp, act) in enumerate(zip(expected, actual)):
        equal, _note = results_equal(exp, act)
        if not equal:
            diff = record_diff("expected", exp, "actual", act)
            raise ActFailure(
                f"{act_label}: statement[{index}] sql={exp.get('sql', '')!r} 不一致\n{diff}"
            )
    print(f"  ✔ 検証一致 ({len(actual)} 文)")


# ---------------------------------------------------------------------------
# 各幕
# ---------------------------------------------------------------------------


def act1_memory(
    repo: Path,
    corpus_dir: Path,
    expected_verify: List[Dict[str, Any]],
    scratch: Path,
) -> None:
    banner(1, "SF-MEM", "ライブラリ(インメモリ)— 永続化なしで全コーパスを実行")
    # SF-MEM は仕様上「alopex-embedded API / CLI --in-memory」の両経路を許容
    # する。第 1 幕は「ライブラリ」フォームファクタの実演であるため、
    # 組み込み API(インメモリ)経路で実行する。
    print("  補足: 本幕は「ライブラリ」フォームファクタの実演として"
          "組み込み API(インメモリ)で実行する。")

    # インメモリは永続化がなく全文を単一プロセスで実行する必要がある。
    # parity_corpus.rs の writer ロールは「先頭数字 1〜7 の *.sql を名前順に
    # 実行」するため、一時コーパスに 01〜07 のコピーと 99_verify.sql の
    # コピーを 07z_verify.sql(先頭数字 07 -> 番号 7、名前順で 07_vector の
    # 直後 = 末尾)として置くことで、同一プロセス内で全コーパス + 検証
    # クエリが実行される。
    act1_dir = scratch / "act1"
    act1_corpus = act1_dir / "corpus"
    act1_corpus.mkdir(parents=True)
    for path in surfaces.corpus_files(corpus_dir):
        shutil.copyfile(path, act1_corpus / path.name)
    shutil.copyfile(
        surfaces.verify_sql_path(corpus_dir), act1_corpus / "07z_verify.sql"
    )

    result = EmbeddedSurface(repo).run(
        act1_corpus,
        role=surfaces.ROLE_WRITER,
        data_dir=None,  # PARITY_DATA_DIR 未設定 = インメモリ(SF-MEM)
        output_path=act1_dir / "embedded-mem.json",
    )
    # 一時コーパスには隣接する expected/ を置かないため、parity_corpus 内の
    # 自己アサートは「期待値なし」で常に不成立になる(cargo_ok は本幕の判定に
    # 使わない)。幕末検証は仕様どおり 99_verify.sql の結果(末尾エントリ)と
    # 期待値ゴールデンの突合で行う。
    entries = result.entries

    if len(entries) < len(expected_verify):
        raise SurfaceError(
            f"第 1 幕: 結果数 {len(entries)} が検証クエリ数"
            f" {len(expected_verify)} より少ない"
        )
    tail = entries[-len(expected_verify):]
    show_records(tail)
    assert_match("第 1 幕 (SF-MEM)", expected_verify, tail)


def act2_file(
    cli: CliSurface,
    corpus_dir: Path,
    expected_verify: List[Dict[str, Any]],
    data_dir: Path,
    corpus_statements: Sequence[str],
    verify_statements: Sequence[str],
) -> None:
    banner(2, "SF-FILE", "組み込み(ファイル)— 同一 SQL を --data-dir で実行し永続化")
    print(f"  data-dir: {data_dir}")
    print(f"  コーパス {len(corpus_statements)} 文を実行(1 文 = 1 プロセス)...")
    write_records = cli.run_statements(corpus_statements, data_dir=data_dir)
    # 幕ゲート: 読み取り専用文のエラーは表示して続行、状態変更文のエラーは
    # 即失敗(gate_corpus_errors のモジュールコメント参照)。
    gate_corpus_errors("第 2 幕", write_records)
    print("  プロセス終了 → 再オープンして検証...")
    raw = cli.run_statements(verify_statements, data_dir=data_dir)
    normalized = normalize.normalize_records(raw)
    show_records(normalized)
    assert_match("第 2 幕 (SF-FILE, 再オープン後)", expected_verify, normalized)


def act3_server(
    repo: Path,
    binaries: Dict[str, Path],
    corpus_dir: Path,
    expected_verify: List[Dict[str, Any]],
    expected_after: List[Dict[str, Any]],
    data_dir: Path,
    verify_statements: Sequence[str],
    scratch: Path,
) -> None:
    banner(3, "SF-HTTP / SF-GRPC", "シングルノードサーバー — 第 2 幕のデータをそのまま開く")
    insert_path = corpus_dir / ACT3_INSERT_FILENAME
    if not insert_path.is_file():
        raise SurfaceError(
            f"第 3 幕の追加 INSERT ファイルがない: {insert_path}。"
            " コーパス整備タスクで S1 専用の追加 INSERT を提供すること。"
        )
    insert_statements = surfaces.split_sql_statements(
        insert_path.read_text(encoding="utf-8")
    )

    work = scratch / "act3-server"
    work.mkdir(parents=True, exist_ok=True)
    with start_server(
        binaries[surfaces.PRODUCT_BIN_SERVER],
        repo=repo,
        data_dir=data_dir,
        work_dir=work,
    ) as server:
        print(f"  server ready: http={server.http_base} grpc={server.grpc_target}")

        # 幕ゲート: 以降の 99_verify 実行はそれ自体が幕末検証(合否ゲート)。
        # 検証クエリ(SELECT のみ)がエラーになった場合は期待値との不一致
        # として assert_match が失敗させる(gate_corpus_errors の
        # モジュールコメント参照)。
        print("\n  -- SF-HTTP: 第 2 幕のデータが可視か検証 --")
        http = HttpSurface(server.http_base)
        http_normalized = normalize.normalize_records(
            http.run_statements(verify_statements)
        )
        show_records(http_normalized)
        assert_match("第 3 幕 (SF-HTTP)", expected_verify, http_normalized)

        print("\n  -- SF-GRPC: 同一クエリを gRPC でも検証 --")
        with GrpcSurface(
            server.grpc_target,
            proto_path=repo / "crates" / "alopex-server" / "proto" / "alopex.proto",
        ) as grpc_surface:
            grpc_raw = grpc_surface.run_statements(verify_statements)
        grpc_normalized = normalize.normalize_records(
            grpc_raw, columns_source=expected_verify
        )
        show_records(grpc_normalized)
        assert_match("第 3 幕 (SF-GRPC)", expected_verify, grpc_normalized)

        print(f"\n  -- サーバー経由で追加 INSERT ({len(insert_statements)} 文) --")
        # 幕ゲート: INSERT は状態変更文のため、エラーは gate_corpus_errors が
        # 即失敗させる(後続の after 検証・第 4 幕の前提が壊れるため)。
        insert_raw = http.run_statements(insert_statements)
        gate_corpus_errors("第 3 幕 (追加 INSERT)", insert_raw)
        # expected/08_server_insert.json は存在しない契約。INSERT 応答は
        # 合計 rows_affected = ACT3_INSERT_TOTAL_AFFECTED をインラインで確認する。
        insert_entries = normalize.normalize_records(insert_raw)
        show_records(insert_entries)
        total_affected = 0
        for entry in insert_entries:
            result = entry["result"]
            if result["type"] != "rows_affected":
                raise ActFailure(
                    f"第 3 幕: 追加 INSERT の応答が rows_affected でない:"
                    f" {entry['sql']!r} -> {result}"
                )
            total_affected += result["count"]
        if total_affected != ACT3_INSERT_TOTAL_AFFECTED:
            raise ActFailure(
                f"第 3 幕: 追加 INSERT の合計影響行数 {total_affected} が"
                f" 期待値 {ACT3_INSERT_TOTAL_AFFECTED} と不一致"
            )
        print(f"  ✔ 追加 INSERT rows_affected = {total_affected}")

        print("\n  -- 追加行を含む期待値と一致するか検証 --")
        after_normalized = normalize.normalize_records(
            http.run_statements(verify_statements)
        )
        show_records(after_normalized)
        assert_match("第 3 幕 (SF-HTTP, INSERT 後)", expected_after, after_normalized)
    print("  server stopped")


def act4_reopen(
    cli: CliSurface,
    expected_after: List[Dict[str, Any]],
    data_dir: Path,
    verify_statements: Sequence[str],
) -> None:
    banner(4, "SF-FILE", "サーバー停止後、同一ディレクトリを CLI で再オープン(INV-1 双方向性)")
    # 幕ゲート: 本幕の実行は幕末検証(99_verify の after 期待値一致)のみ。
    # 検証クエリ(SELECT のみ)のエラーは期待値との不一致として assert_match が
    # 失敗させる(gate_corpus_errors のモジュールコメント参照)。
    raw = cli.run_statements(verify_statements, data_dir=data_dir)
    normalized = normalize.normalize_records(raw)
    show_records(normalized)
    assert_match("第 4 幕 (SF-FILE, サーバー書込の可視性)", expected_after, normalized)


def act5_cluster() -> None:
    banner(5, "SF-CLUSTER", "クラスタ")
    print("  SKIP: SF-CLUSTER は v0.7 の cluster-aware リリース")
    print("        (query router / membership) で有効化される予約。")
    print("        本デモでは実行しない(スキップを完了と偽らない)。")


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--corpus",
        type=Path,
        default=PARITY_DIR / "corpus",
        help="SQL コーパスディレクトリ(default: scripts/parity/corpus)",
    )
    parser.add_argument(
        "--expected",
        type=Path,
        default=PARITY_DIR / "expected",
        help="正規化済み期待値ディレクトリ(default: scripts/parity/expected)",
    )
    args = parser.parse_args(argv)

    repo = surfaces.repo_root()
    if not args.corpus.is_dir():
        print(f"環境エラー: コーパスディレクトリがない: {args.corpus}", file=sys.stderr)
        return EXIT_ENV

    print("One Engine, Four Forms — alopex mode-parity デモ")

    try:
        expected_verify = normalize.load_statements_file(
            args.expected / EXPECTED_VERIFY
        )
        # after ファイルはトップレベルに variant / precondition キーを持つ
        # (ローダーは余剰キーを許容する)
        expected_after = normalize.load_statements_file(
            args.expected / EXPECTED_AFTER_INSERT
        )
    except (OSError, normalize.NormalizeError) as exc:
        print(f"環境エラー: 期待値ゴールデンを読めない: {exc}", file=sys.stderr)
        return EXIT_ENV

    try:
        # ビルドは単発・逐次。以後の幕はビルド済みバイナリを直接実行する。
        binaries = surfaces.build_products(repo)
        cli = CliSurface(binaries[surfaces.PRODUCT_BIN_CLI], repo)
        corpus_statements = surfaces.load_statements(
            surfaces.corpus_files(args.corpus)
        )
        verify_statements = surfaces.load_statements(
            [surfaces.verify_sql_path(args.corpus)]
        )

        with tempfile.TemporaryDirectory(prefix="parity-demo-") as tmp:
            base = Path(tmp)
            data_dir = base / "data"
            data_dir.mkdir()

            act1_memory(repo, args.corpus, expected_verify, base)
            act2_file(
                cli,
                args.corpus,
                expected_verify,
                data_dir,
                corpus_statements,
                verify_statements,
            )
            act3_server(
                repo,
                binaries,
                args.corpus,
                expected_verify,
                expected_after,
                data_dir,
                verify_statements,
                base,
            )
            act4_reopen(cli, expected_after, data_dir, verify_statements)
        act5_cluster()
    except ActFailure as exc:
        print(f"\n検証不一致: {exc}", file=sys.stderr)
        return EXIT_MISMATCH
    except SurfaceSkip as exc:
        # デモの必須幕が SKIP になる = デモを完遂できない環境
        print(f"\n環境エラー(必須幕が SKIP): {exc.reason}", file=sys.stderr)
        return EXIT_ENV
    except (SurfaceError, normalize.NormalizeError) as exc:
        print(f"\n環境エラー: {exc}", file=sys.stderr)
        return EXIT_ENV

    print()
    print("=" * 72)
    print("デモ完了: 第 1〜4 幕 PASS / 第 5 幕 SKIP (SF-CLUSTER, v0.7 予約)")
    print("=" * 72)
    return EXIT_OK


if __name__ == "__main__":
    sys.exit(main())
