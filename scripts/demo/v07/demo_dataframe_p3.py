#!/usr/bin/env python3
"""シナリオ D4: DataFrame P3(str / dt / list namespace と explode / implode)。

docs-public/specs/alopex-v07-feature-demo-spec.md「シナリオ D4」:

| 場 | 操作 | 検証 |
|----|------|------|
| 1 | 固定サンプル列に str.to_lowercase / contains / split / extract | 手計算期待値と一致 |
| 2 | 固定タイムスタンプ列に dt.year / month / weekday / convert_time_zone | 手計算期待値と一致 |
| 3 | リスト列に list.join / len / contains -> explode -> implode | 手計算期待値と一致、explode→implode の往復が元と等価 |
| 4 | 同一入力で 2 回実行 | 全出力がバイト単位で一致(決定性) |

期待値はすべて**手計算**で導出した(実行結果の写経ではない)。導出根拠は
各期待値の直上コメントに記す。セマンティクスの根拠実装:

- str.*: crates/alopex-core/src/dataframe/mod.rs(null 行は null 伝播、
  extract は不一致時 None、len_chars は文字数)
- dt.*: 同上(UTC 解釈、weekday は ISO で月=1..日=7、convert_time_zone は
  オフセット差をマイクロ秒値へ加算するウォールクロック変換)
- explode / implode: crates/alopex-dataframe/src/physical/operators.rs
  (explode は空リスト・null リストを 1 行の null に展開し他列を複製、
  implode は全行を単一行の List<Utf8> へ畳む)

exit code: 成功 0 / 検証不一致 1 / 環境・起動エラー 2
"""

from __future__ import annotations

import hashlib
import json
import os
import sys
from pathlib import Path
from typing import Any, Dict, Optional, Sequence

sys.path.insert(0, str(Path(__file__).resolve().parent))

from _v07 import (  # noqa: E402
    EXIT_ENV,
    EXIT_MISMATCH,
    EXIT_OK,
    DemoFailure,
    EnvError,
    banner,
    check,
    nim_parser_dir,
    repo_root,
)

#: 再実行ガード(LD_LIBRARY_PATH を整えて 1 回だけ exec し直す)。
REEXEC_MARKER = "V07_DEMO_DATAFRAME_REEXEC"


def ensure_alopex_importable(repo: Path) -> Any:
    """リポジトリ内の alopex Python パッケージを import する。

    - パッケージはリポジトリ内 crates/alopex-py/python を優先する
      (maturin develop / 事前ビルド済みの _alopex.abi3.so を含む)。
    - 拡張モジュールは libalopex_sql_parser.so(Nim パーサー)へ動的リンク
      するため、未解決なら LD_LIBRARY_PATH を整えて自分自身を exec し直す
      (動的リンカのパスはプロセス起動時にしか反映されないため)。
    """
    package_dir = repo / "crates" / "alopex-py" / "python"
    if package_dir.is_dir():
        sys.path.insert(0, str(package_dir))
    try:
        import alopex  # noqa: PLC0415

        return alopex
    except ModuleNotFoundError as exc:
        raise EnvError(
            f"alopex Python パッケージが見つからない: {exc}。"
            " crates/alopex-py で `maturin develop`(flock 経由・逐次)を実行"
            "してから再実行すること。"
        ) from exc
    except ImportError as exc:
        if "libalopex_sql_parser" in str(exc) and REEXEC_MARKER not in os.environ:
            nim_dir = str(nim_parser_dir(repo))
            env = dict(os.environ)
            current = env.get("LD_LIBRARY_PATH", "")
            env["LD_LIBRARY_PATH"] = f"{nim_dir}:{current}" if current else nim_dir
            env[REEXEC_MARKER] = "1"
            print(
                f"  注記: libalopex_sql_parser.so 解決のため LD_LIBRARY_PATH="
                f"{nim_dir} で再実行する。"
            )
            sys.stdout.flush()
            os.execve(sys.executable, [sys.executable, *sys.argv], env)
        raise EnvError(
            f"alopex 拡張モジュールの import に失敗: {exc}。"
            " Nim パーサー(crates/alopex-sql/nim-sql-parser の `nimble lib`)"
            "が未ビルドの可能性がある。"
        ) from exc


# ---------------------------------------------------------------------------
# 場 1: string namespace
# ---------------------------------------------------------------------------

#: 固定サンプル(null 行を含む)。
STR_INPUT = {"name": ["Fox-01", "alopex LAGOPUS", None, "tail-9"]}

#: 手計算期待値:
#: - to_lowercase: ASCII 小文字化のみ発生("Fox-01"->"fox-01",
#:   "alopex LAGOPUS"->"alopex lagopus")。null は null。
#: - contains r"\d+": 数字を含むか。"Fox-01" は "01" を含み True、
#:   "alopex LAGOPUS" は数字なしで False、null は null、"tail-9" は True。
#: - split "-": "Fox-01"->["Fox","01"]、"alopex LAGOPUS" は区切りなしで
#:   1 要素 ["alopex LAGOPUS"]、null は null、"tail-9"->["tail","9"]。
#: - extract r"([A-Za-z]+)-(\d+)" group=2: ハイフン後の数字列。
#:   "Fox-01"->"01"、"alopex LAGOPUS" は不一致で null、null は null、
#:   "tail-9"->"9"。
STR_EXPECTED = {
    "name": ["Fox-01", "alopex LAGOPUS", None, "tail-9"],
    "lower": ["fox-01", "alopex lagopus", None, "tail-9"],
    "has_digits": [True, False, None, True],
    "parts": [["Fox", "01"], ["alopex LAGOPUS"], None, ["tail", "9"]],
    "num": ["01", None, None, "9"],
}


def run_scene1(alopex: Any) -> Dict[str, Any]:
    df = alopex.DataFrame(dict(STR_INPUT))
    return (
        df.str("name")
        .to_lowercase("lower")
        .str("name")
        .contains(r"\d+", "has_digits")
        .str("name")
        .split("-", "parts")
        .str("name")
        .extract(r"([A-Za-z]+)-(\d+)", 2, "num")
        .to_dict()
    )


def scene1_string(alopex: Any) -> None:
    banner(1, "str namespace: to_lowercase / contains / split / extract")
    print(f"  入力: {STR_INPUT}")
    out = run_scene1(alopex)
    for column, expected in STR_EXPECTED.items():
        check(f"str.{column}", expected, out[column])
    if set(out) != set(STR_EXPECTED):
        raise DemoFailure(f"出力列が期待と異なる: {sorted(out)}")


# ---------------------------------------------------------------------------
# 場 2: datetime namespace
# ---------------------------------------------------------------------------

#: 固定タイムスタンプ(timestamp_micros, UTC):
#: - 0 = 1970-01-01T00:00:00Z(エポック)
#: - 1_711_927_800_000_000 µs = 2024-03-31T23:30:00Z。導出:
#:     1970-01-01 から 2024-01-01 までの日数 = 54 年 × 365
#:     + 閏年 13 回 (1972,1976,...,2020。2000 年は 400 の倍数で閏年)
#:     = 19710 + 13 = 19723 日
#:     2024-03-31 は年初から 90 日後(31+29+31 日目)= 19813 日
#:     19813 × 86400 s = 1_711_843_200 s、+ 23.5h (84600 s)
#:     = 1_711_927_800 s = 1_711_927_800_000_000 µs
#: - null
DT_INPUT_TS = [0, 1_711_927_800_000_000, None]

#: 手計算期待値:
#: - year: [1970, 2024, null]
#: - month: [1(1 月), 3(3 月), null]
#: - weekday (ISO 月=1..日=7): 1970-01-01 は木曜 = 4。
#:   2024-03-31: 経過日数 19813 = 7×2830 + 3 -> 木曜 + 3 = 日曜 = 7。
#: - convert_time_zone "Z" -> "+09:00": ウォールクロックを +9h ずらす
#:   = +32_400 s = +32_400_000_000 µs。
#:   0 -> 32_400_000_000
#:   1_711_927_800_000_000 -> 1_711_960_200_000_000
#:   (+09:00 表示では 2024-04-01T08:30:00、月替わりを跨ぐ例)
DT_EXPECTED = {
    "ts": [0, 1_711_927_800_000_000, None],
    "year": [1970, 2024, None],
    "month": [1, 3, None],
    "weekday": [4, 7, None],
    "tokyo": [32_400_000_000, 1_711_960_200_000_000, None],
}


def run_scene2(alopex: Any) -> Dict[str, Any]:
    df = alopex.DataFrame({"ts": list(DT_INPUT_TS)}, schema={"ts": "timestamp_micros"})
    return (
        df.dt("ts")
        .year("year")
        .dt("ts")
        .month("month")
        .dt("ts")
        .weekday("weekday")
        .dt("ts")
        .convert_time_zone("Z", "+09:00", "tokyo")
        .to_dict()
    )


def scene2_datetime(alopex: Any) -> None:
    banner(2, "dt namespace: year / month / weekday / convert_time_zone")
    print(f"  入力: ts = {DT_INPUT_TS} (timestamp_micros, UTC)")
    out = run_scene2(alopex)
    for column, expected in DT_EXPECTED.items():
        check(f"dt.{column}", expected, out[column])
    if set(out) != set(DT_EXPECTED):
        raise DemoFailure(f"出力列が期待と異なる: {sorted(out)}")


# ---------------------------------------------------------------------------
# 場 3: list namespace と explode / implode
# ---------------------------------------------------------------------------

#: 固定サンプル。str.split でリスト列 parts を作ってから list.* を適用する。
LIST_INPUT = {
    "id": ["r1", "r2", "r3", "r4"],
    "tags": ["red,green", "solo", None, "blue,"],
}

#: 手計算期待値:
#: - split ",": "red,green"->["red","green"]、"solo"->["solo"](区切りなし)、
#:   null->null、"blue,"->["blue",""](末尾区切りで空文字要素)。
#: - list.join "|"(null_value="NULL" はリスト内 null 要素の代替。今回の
#:   リストに null 要素は無い): ["red","green"]->"red|green"、
#:   ["solo"]->"solo"、null->null、["blue",""]->"blue|"。
#: - list.len: [2, 1, null, 2]。
#: - list.contains "red": 要素完全一致。[True, False, null, False]。
LIST_EXPECTED = {
    "id": ["r1", "r2", "r3", "r4"],
    "tags": ["red,green", "solo", None, "blue,"],
    "parts": [["red", "green"], ["solo"], None, ["blue", ""]],
    "joined": ["red|green", "solo", None, "blue|"],
    "n": [2, 1, None, 2],
    "has_red": [True, False, None, False],
}

#: explode("parts") の手計算期待値。展開規則(operators.rs explode_batches):
#: 各行のリスト要素数ぶん行を複製(他列は同値複製)、空リストと null リストは
#: 1 行(parts=null)に展開する。
#: r1: ["red","green"] -> 2 行 / r2: ["solo"] -> 1 行 /
#: r3: null -> 1 行(null) / r4: ["blue",""] -> 2 行
EXPLODED_EXPECTED = {
    "id": ["r1", "r1", "r2", "r3", "r4", "r4"],
    "tags": ["red,green", "red,green", "solo", None, "blue,", "blue,"],
    "parts": ["red", "green", "solo", None, "blue", ""],
    "joined": ["red|green", "red|green", "solo", None, "blue|", "blue|"],
    "n": [2, 2, 1, None, 2, 2],
    "has_red": [True, True, False, None, False, False],
}

#: explode→implode 往復の元データ(単一行のリスト列)。
#: explode で ["a","b","c"] が 3 行に展開され、implode で単一行の
#: List<Utf8> に畳まれて元と等価に戻る。
ROUNDTRIP_NESTED = {"items": [["a", "b", "c"]]}
ROUNDTRIP_FLAT = {"items": ["a", "b", "c"]}

#: implode→explode 側の往復(null 要素の保存を確認する)。
#: implode は全行を単一行 [["ember", null, "moss"]] に畳み、
#: explode で元の 3 行(null 含む)へ戻る。
WORDS_FLAT = {"word": ["ember", None, "moss"]}
WORDS_IMPLODED = {"word": [["ember", None, "moss"]]}


def run_scene3(alopex: Any) -> Dict[str, Dict[str, Any]]:
    listified = (
        alopex.DataFrame({k: list(v) for k, v in LIST_INPUT.items()})
        .str("tags")
        .split(",", "parts")
    )
    with_ops = (
        listified.list("parts")
        .join("|", "NULL", "joined")
        .list("parts")
        .len("n")
        .list("parts")
        .contains("red", "has_red")
    )
    exploded = with_ops.explode("parts")

    nested = alopex.DataFrame(
        {"items": [list(ROUNDTRIP_NESTED["items"][0])]}, schema={"items": "list_utf8"}
    )
    roundtrip_flat = nested.explode("items")
    roundtrip_back = roundtrip_flat.implode()

    words = alopex.DataFrame({"word": list(WORDS_FLAT["word"])})
    words_imploded = words.implode()
    words_back = words_imploded.explode("word")

    return {
        "list_ops": with_ops.to_dict(),
        "exploded": exploded.to_dict(),
        "roundtrip_flat": roundtrip_flat.to_dict(),
        "roundtrip_back": roundtrip_back.to_dict(),
        "words_imploded": words_imploded.to_dict(),
        "words_back": words_back.to_dict(),
    }


def scene3_list(alopex: Any) -> None:
    banner(3, "list namespace: join / len / contains -> explode -> implode 往復")
    print(f"  入力: {LIST_INPUT}")
    out = run_scene3(alopex)

    print("  -- list.join / len / contains --")
    for column, expected in LIST_EXPECTED.items():
        check(f"list_ops.{column}", expected, out["list_ops"][column])

    print("  -- explode(空リスト・null リストは 1 行の null、他列は複製) --")
    for column, expected in EXPLODED_EXPECTED.items():
        check(f"exploded.{column}", expected, out["exploded"][column])

    print("  -- explode → implode の往復が元と等価 --")
    check("explode(nested).items", ROUNDTRIP_FLAT["items"], out["roundtrip_flat"]["items"])
    check(
        "implode(explode(nested)).items(元と等価)",
        ROUNDTRIP_NESTED["items"],
        out["roundtrip_back"]["items"],
    )

    print("  -- implode → explode の往復(null 要素の保存) --")
    check("implode(words).word", WORDS_IMPLODED["word"], out["words_imploded"]["word"])
    check(
        "explode(implode(words)).word(元と等価)",
        WORDS_FLAT["word"],
        out["words_back"]["word"],
    )


# ---------------------------------------------------------------------------
# 場 4: 決定性(同一入力で 2 回実行しバイト単位一致)
# ---------------------------------------------------------------------------


def run_all(alopex: Any) -> bytes:
    """場 1〜3 の全出力を正規化 JSON バイト列に直列化する。"""
    outputs = {
        "scene1": run_scene1(alopex),
        "scene2": run_scene2(alopex),
        "scene3": run_scene3(alopex),
    }
    return json.dumps(outputs, sort_keys=True, ensure_ascii=False).encode("utf-8")


def scene4_determinism(alopex: Any) -> None:
    banner(4, "決定性: 同一入力で 2 回実行し、全出力がバイト単位で一致")
    first = run_all(alopex)
    second = run_all(alopex)
    digest1 = hashlib.sha256(first).hexdigest()
    digest2 = hashlib.sha256(second).hexdigest()
    print(f"  1 回目: {len(first)} bytes sha256={digest1}")
    print(f"  2 回目: {len(second)} bytes sha256={digest2}")
    if first != second:
        raise DemoFailure("2 回の実行出力がバイト単位で一致しない(決定性違反)")
    print("  ✔ 全出力がバイト単位で一致")


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------


def main(argv: Optional[Sequence[str]] = None) -> int:
    del argv
    repo = repo_root()

    print("v0.7 機能デモ D4: DataFrame P3 (str / dt / list, explode / implode)")

    try:
        alopex = ensure_alopex_importable(repo)
        print(f"  alopex パッケージ: {alopex.__file__}")
        scene1_string(alopex)
        scene2_datetime(alopex)
        scene3_list(alopex)
        scene4_determinism(alopex)
    except DemoFailure as exc:
        print(f"\n検証不一致: {exc}", file=sys.stderr)
        return EXIT_MISMATCH
    except EnvError as exc:
        print(f"\n環境エラー: {exc}", file=sys.stderr)
        return EXIT_ENV
    except Exception as exc:  # noqa: BLE001 - AlopexError 等は環境/実装起因
        print(f"\n環境エラー(予期しない例外): {type(exc).__name__}: {exc}", file=sys.stderr)
        return EXIT_ENV

    print()
    print("=" * 72)
    print("デモ完了: 場 1〜4 PASS")
    print("=" * 72)
    return EXIT_OK


if __name__ == "__main__":
    sys.exit(main())
