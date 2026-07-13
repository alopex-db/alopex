"""結果の正規化規則の実装。

docs-public/specs/alopex-mode-parity-spec.md「結果の正規化規則」を、
expected/*.json と同一のスキーマ(オーケストレーター決定、全経路共通)で
実装する。

ファイルスキーマ(期待値 / 実測出力の両方):

    {
        "corpus": "01_ddl.sql",
        "format": {...},                  # 説明用メタ(比較対象外)
        "statements": [
            {"index": 1, "sql": "...", "expected": {...結果...}},   # 期待値
            {"index": 1, "sql": "...", "actual":   {...結果...}},   # 実測出力
        ]
    }

- per-statement キーは期待値ファイルが ``expected``、実測出力(PARITY_OUTPUT を
  含む全サーフェスの正規化出力)が ``actual``。値の形は同一。
- ローダー(load_statements_file / parse_statements_document)は両キーを
  読める単一実装。トップレベルの余剰キー(``variant`` / ``precondition`` /
  ``status`` / ``known_gap_note`` 等)は拒否しない。

結果(``expected`` / ``actual`` の値)のバリアント:

    {"type": "success"}                                    # DDL 成功(結果セットなし)
    {"type": "rows_affected", "count": N}                  # DML 成功(影響行数)
    {"type": "query", "columns": [...], "rows": [{...}]}   # 結果セット
    {"type": "error", "error_class": "...",
     "object": "..." | null, "code": "ALOPEX-..." | null}  # エラー分類

正規化規則:
1. 行は配列、列は列名をキーとするオブジェクトで表現する(query.rows)。
2. 浮動小数点値は有効数字 9 桁へ丸める。
3. NULL は JSON ``null``(Python ``None``)。
4. エラーは「エラー分類コード + 対象オブジェクト名(+ エンジンエラーコード)」
   へ正規化する。メッセージ文字列全体の一致は要求しない。
5. 実行時間などのメタ情報は比較対象外(rows_affected の count は
   expected スキーマの一部であり比較対象)。

gRPC 経路(``ExecuteSql`` の ``stream Row``)は列名メタデータを持たないため、
query 結果は他経路の結果または期待値から列名を補完してから正規化する
(``columns_override`` / ``columns_source``)。
"""

from __future__ import annotations

import json
import math
import re
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence

#: 浮動小数点の丸め桁数(有効数字)
SIGNIFICANT_DIGITS = 9

#: 結果バリアント
RESULT_TYPES = ("success", "rows_affected", "query", "error")

#: per-statement の結果キー(期待値 / 実測)。単一ローダーで両方を読む。
STATEMENT_RESULT_KEYS = ("expected", "actual")


class NormalizeError(Exception):
    """正規化不能な入力(スキーマ不正・列名欠落・未定義の値型)。"""


class UnclassifiedErrorMessage(NormalizeError):
    """エラー分類表に一致しないメッセージ。UNKNOWN のような汎用コードへ
    吸収するとサーフェス間の実差分が偽陽性一致するため、明示的に失敗させる。
    分類表(ERROR_CLASSIFICATION)をコーパス整備に合わせて拡充すること。"""


# ---------------------------------------------------------------------------
# 値の正規化
# ---------------------------------------------------------------------------


def round_significant(value: float) -> Any:
    """浮動小数点値を有効数字 SIGNIFICANT_DIGITS 桁へ丸める(規則 2)。

    NaN / Inf は JSON で表現できないため、明示的な文字列タグへ写像する。
    """
    if math.isnan(value):
        return "NaN"
    if math.isinf(value):
        return "Infinity" if value > 0 else "-Infinity"
    return float(f"{value:.{SIGNIFICANT_DIGITS}g}")


def normalize_scalar(value: Any) -> Any:
    """単一の値を正規化する。

    - None -> None(規則 3)
    - bool / int / str -> そのまま
    - float -> 有効数字 9 桁へ丸め(規則 2)
    - list / tuple -> 各要素を再帰的に正規化(VECTOR 値)
    - bytes -> 未定義。コーパスは BLOB を含めない前提のため明示エラー
    """
    if value is None:
        return None
    # bool は int のサブクラスなので int より先に判定する
    if isinstance(value, bool):
        return value
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return round_significant(value)
    if isinstance(value, str):
        return value
    if isinstance(value, (list, tuple)):
        return [normalize_scalar(v) for v in value]
    if isinstance(value, bytes):
        raise NormalizeError(
            "BLOB 値の正規化表現は未定義(コーパスに BLOB を含めない前提)。"
            f" 値: {value!r:.64}"
        )
    raise NormalizeError(f"正規化未定義の値型: {type(value).__name__} ({value!r})")


# ---------------------------------------------------------------------------
# HTTP ワイヤ値のアンラップ(serde 外部タグ形式)
# ---------------------------------------------------------------------------

#: HTTP レスポンスの値タグ全一覧。裏取り:
#: - crates/alopex-server/src/http/sql.rs
#:     SqlResponse.rows: Vec<Vec<alopex_sql::storage::SqlValue>>(直接 serde)
#: - crates/alopex-sql/src/storage/value.rs
#:     enum SqlValue { Null, Integer(i32), BigInt(i64), Float(f32), Double(f64),
#:                     Text(String), Blob(Vec<u8>), Boolean(bool),
#:                     Timestamp(i64), Vector(Vec<f32>) }
#:     #[derive(Serialize)] のみ = serde の外部タグ表現:
#:       - unit variant ``Null``  -> JSON 文字列 "Null"
#:       - その他                 -> {"<タグ>": <中身>}(単一キーオブジェクト)
HTTP_VALUE_TAGS = (
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


def unwrap_http_value(value: Any) -> Any:
    """HTTP レスポンスの SqlValue(serde 外部タグ形式)を素の値へ戻す。

    - JSON 文字列 ``"Null"``(unit variant)-> None
    - ``{"BigInt": 6}`` 等の単一キーオブジェクト -> 中身の値
    - ``Blob`` は bytes へ変換する(BLOB の正規化表現は未定義のため、
      従来どおり normalize_scalar が明示エラーにする)
    - 未知タグ・タグなし文字列・その他の形は NormalizeError(黙って素通し
      にしない)
    """
    if isinstance(value, str):
        if value == "Null":
            return None
        raise NormalizeError(
            f"HTTP 値の未知 unit variant: {value!r}"
            "(SqlValue のワイヤ形式は 'Null' または単一キーオブジェクト)"
        )
    if isinstance(value, dict):
        if len(value) != 1:
            raise NormalizeError(
                f"HTTP 値が単一キーオブジェクトでない: {value!r}"
            )
        ((tag, inner),) = value.items()
        if tag not in HTTP_VALUE_TAGS:
            raise NormalizeError(
                f"HTTP 値の未知タグ: {tag!r} ({value!r})。"
                " SqlValue に variant が追加された場合は"
                " runner/normalize.py の HTTP_VALUE_TAGS を更新すること。"
            )
        if tag == "Blob":
            return bytes(inner)
        return inner
    raise NormalizeError(
        f"HTTP 値のワイヤ形式が不正: {type(value).__name__} ({value!r})"
    )


# ---------------------------------------------------------------------------
# エラー分類(規則 4)
# ---------------------------------------------------------------------------

#: エンジンエラーコード(メッセージ中に含まれる場合は抽出して使う)
_ENGINE_CODE_RE = re.compile(r"ALOPEX-[A-Z]\d{3}")

#: unsupported expression の既知 TypedExprKind variant。
#: 実測メッセージ 'unsupported expression: <TypedExprKind variant>' の
#: variant 名を「前方一致」で既知名へ正規化する
#: (expected/06_subquery.json の known_gap_note に基づく)。
UNSUPPORTED_EXPRESSION_OBJECTS = (
    "ScalarSubquery",
    "InSubquery",
    "Exists",
    "Quantified",
)

_OBJ = r"['\"`]?(?P<object>[A-Za-z_][A-Za-z0-9_]*)['\"`]?"

#: エラー分類表: (error_class, 既定エンジンコード, パターン)。
#: 上から順に最初の一致を採用する。named group ``object`` があれば
#: 対象オブジェクト名として抽出する。既定コードが None のエントリは
#: 対応する実測コードが未確認(コーパスが使い始める際に実測して埋める)。
ERROR_CLASSIFICATION = (
    # 本番 SELECT 実行経路の evaluator 未対応式(CLI 実測 2026-07-13、
    # expected/06_subquery.json が使用)
    (
        "UNSUPPORTED_EXPRESSION",
        "ALOPEX-E999",
        re.compile(
            r"unsupported expression:\s*(?P<object>[A-Za-z_][A-Za-z0-9_]*)", re.I
        ),
    ),
    # 列不在(ALOPEX-C003 実測済み)
    (
        "COLUMN_NOT_FOUND",
        "ALOPEX-C003",
        re.compile(rf"column {_OBJ}\s*(?:not found|does not exist)", re.I),
    ),
    ("TABLE_NOT_FOUND", None, re.compile(rf"table {_OBJ}\s*(?:not found|does not exist)", re.I)),
    ("TABLE_ALREADY_EXISTS", None, re.compile(rf"table {_OBJ}\s*already exists", re.I)),
    ("INDEX_NOT_FOUND", None, re.compile(rf"index {_OBJ}\s*(?:not found|does not exist)", re.I)),
    ("INDEX_ALREADY_EXISTS", None, re.compile(rf"index {_OBJ}\s*already exists", re.I)),
    ("PARSE_ERROR", None, re.compile(r"(?:parse|syntax)\s+error", re.I)),
    ("TYPE_MISMATCH", None, re.compile(r"type\s+(?:mismatch|error)", re.I)),
)


def classify_error(message: str) -> Dict[str, Optional[str]]:
    """エラーメッセージを error バリアントの中身へ正規化する。

    戻り値: {"error_class": ..., "object": ... | None, "code": ... | None}

    - code はメッセージ中の ``ALOPEX-Xnnn`` を優先し、なければ分類表の
      既定コードを使う。
    - UNSUPPORTED_EXPRESSION の object は既知 variant 名への前方一致で
      正規化する(例: 'QuantifiedComparison' -> 'Quantified')。
    - 分類表に一致しない場合は UnclassifiedErrorMessage を送出する。
    """
    code_match = _ENGINE_CODE_RE.search(message)
    for error_class, default_code, pattern in ERROR_CLASSIFICATION:
        match = pattern.search(message)
        if not match:
            continue
        obj = match.groupdict().get("object")
        if error_class == "UNSUPPORTED_EXPRESSION" and obj:
            for known in UNSUPPORTED_EXPRESSION_OBJECTS:
                if obj.startswith(known):
                    obj = known
                    break
        return {
            "error_class": error_class,
            "object": obj,
            "code": code_match.group(0) if code_match else default_code,
        }
    raise UnclassifiedErrorMessage(
        f"エラー分類表に一致しないメッセージ: {message!r}。"
        " runner/normalize.py の ERROR_CLASSIFICATION を拡充すること。"
    )


# ---------------------------------------------------------------------------
# 結果バリアントの正規化(期待値・実測の共通検証点)
# ---------------------------------------------------------------------------


def normalize_result_variant(variant: Dict[str, Any]) -> Dict[str, Any]:
    """結果バリアントを検証し、値を正規化して返す。

    期待値ファイルのロードと実測レコードの正規化の両方がここを通る
    (単一ソース)。float の丸めは冪等なため、既に正規化済みの入力にも
    安全に適用できる。
    """
    if not isinstance(variant, dict):
        raise NormalizeError(f"結果バリアントが dict でない: {variant!r}")
    vtype = variant.get("type")

    if vtype == "success":
        return {"type": "success"}

    if vtype == "rows_affected":
        count = variant.get("count")
        if isinstance(count, bool) or not isinstance(count, int) or count < 0:
            raise NormalizeError(f"rows_affected.count が非負整数でない: {variant!r}")
        return {"type": "rows_affected", "count": count}

    if vtype == "query":
        columns = variant.get("columns")
        rows = variant.get("rows")
        if not isinstance(rows, list):
            raise NormalizeError(f"query.rows がリストでない: {variant!r}")
        if columns is None:
            # 列名情報を持たない出力形式(CLI --output json の空結果集合)は
            # 空行のみ許容する。比較側は「片側欠落時は列名を比較しない」。
            if rows:
                raise NormalizeError(
                    "列名メタデータのない非空 query 結果は正規化できない"
                    "(gRPC 経路は columns_override で列名を補完する)"
                )
            return {"type": "query", "columns": None, "rows": []}
        if not isinstance(columns, list) or not all(
            isinstance(c, str) for c in columns
        ):
            raise NormalizeError(f"query.columns が文字列リストでない: {variant!r}")
        normalized_rows: List[Dict[str, Any]] = []
        for row_index, row in enumerate(rows):
            if not isinstance(row, dict):
                raise NormalizeError(
                    f"query.rows[{row_index}] が列名キーのオブジェクトでない: {row!r}"
                )
            if set(row.keys()) != set(columns):
                raise NormalizeError(
                    f"query.rows[{row_index}] のキー {sorted(row)} が"
                    f" columns {columns} と一致しない"
                )
            normalized_rows.append(
                {name: normalize_scalar(row[name]) for name in columns}
            )
        return {"type": "query", "columns": list(columns), "rows": normalized_rows}

    if vtype == "error":
        error_class = variant.get("error_class")
        if not isinstance(error_class, str) or not error_class:
            raise NormalizeError(f"error.error_class がない: {variant!r}")
        obj = variant.get("object")
        code = variant.get("code")
        if obj is not None and not isinstance(obj, str):
            raise NormalizeError(f"error.object が文字列でない: {variant!r}")
        if code is not None and not isinstance(code, str):
            raise NormalizeError(f"error.code が文字列でない: {variant!r}")
        return {"type": "error", "error_class": error_class, "object": obj, "code": code}

    raise NormalizeError(f"未知の結果バリアント type={vtype!r}: {variant!r}")


# ---------------------------------------------------------------------------
# 生レコード(surfaces)-> 正規化済みエントリ
# ---------------------------------------------------------------------------


def record_to_entry(
    index: int,
    record: Dict[str, Any],
    *,
    columns_override: Optional[Sequence[str]] = None,
) -> Dict[str, Any]:
    """surfaces の生レコード 1 件を正規化済みエントリへ変換する。

    入力(surfaces.StatementRecord 形):
        {
            "sql": str,
            "kind": "query" | "success" | "rows_affected" | "error",
            "columns": [str, ...] | None,    # kind == "query"(gRPC は None)
            "rows": [[...], ...] | None,     # kind == "query"(位置ベース)
            "affected_count": int | None,    # kind == "rows_affected"
            "error_message": str | None,     # kind == "error"
        }

    出力(正規化済みエントリ):
        {"index": int, "sql": str, "result": {...結果バリアント...}}
    """
    sql = record.get("sql")
    kind = record.get("kind")
    if not isinstance(sql, str) or kind not in RESULT_TYPES:
        raise NormalizeError(f"不正な生レコード: {record!r}")

    if kind == "success":
        variant: Dict[str, Any] = {"type": "success"}
    elif kind == "rows_affected":
        variant = {"type": "rows_affected", "count": record.get("affected_count")}
    elif kind == "error":
        message = record.get("error_message")
        if not message:
            raise NormalizeError(f"error レコードに error_message がない: {record!r}")
        variant = {"type": "error", **classify_error(message)}
    else:  # query
        columns = record.get("columns")
        if columns_override is not None:
            columns = list(columns_override)
        rows = record.get("rows")
        if rows is None:
            raise NormalizeError(f"query レコードに rows がない: {record!r}")
        if columns is None:
            # 空のみ許容(normalize_result_variant が検証する)
            named: List[Any] = list(rows)
        else:
            named = []
            for row_index, row in enumerate(rows):
                row = list(row)
                if len(row) != len(columns):
                    raise NormalizeError(
                        f"行 {row_index}: 列数 {len(row)} が列名数 {len(columns)} と"
                        f"不一致 (sql={sql!r})"
                    )
                named.append(dict(zip(columns, row)))
        variant = {"type": "query", "columns": columns, "rows": named}

    return {"index": index, "sql": sql, "result": normalize_result_variant(variant)}


def normalize_records(
    records: Iterable[Dict[str, Any]],
    *,
    columns_source: Optional[Sequence[Dict[str, Any]]] = None,
) -> List[Dict[str, Any]]:
    """生レコード列を正規化済みエントリ列へ変換する(index は 1 始まり)。

    ``columns_source`` に正規化済みエントリ列を渡すと、対応する位置の
    query 結果の列名を columns_override として使う(gRPC 経路の列名補完)。
    """
    entries: List[Dict[str, Any]] = []
    for position, record in enumerate(records):
        override: Optional[Sequence[str]] = None
        if columns_source is not None and position < len(columns_source):
            source_result = columns_source[position].get("result", {})
            if source_result.get("type") == "query":
                override = source_result.get("columns")
        entries.append(
            record_to_entry(position + 1, record, columns_override=override)
        )
    return entries


# ---------------------------------------------------------------------------
# ファイル入出力(期待値 / 実測の単一ローダー)
# ---------------------------------------------------------------------------


def parse_statements_document(
    document: Dict[str, Any], *, source: str = "<memory>"
) -> List[Dict[str, Any]]:
    """1 ドキュメント({"corpus","format","statements":[...]})をパースする。

    - per-statement の結果キーは ``expected`` / ``actual`` のどちらか一方
      (単一実装で両方を読む)。
    - トップレベルの余剰キー(``variant`` / ``precondition`` / ``status`` /
      ``known_gap_note`` 等)は拒否しない。
    - 戻り値: [{"index", "sql", "result", ("known_gap")}, ...]
    """
    if not isinstance(document, dict) or not isinstance(
        document.get("statements"), list
    ):
        raise NormalizeError(f"ドキュメントに statements リストがない: {source}")
    entries: List[Dict[str, Any]] = []
    for position, statement in enumerate(document["statements"]):
        if not isinstance(statement, dict):
            raise NormalizeError(f"{source}: statements[{position}] が dict でない")
        sql = statement.get("sql")
        if not isinstance(sql, str) or not sql:
            raise NormalizeError(f"{source}: statements[{position}] に sql がない")
        present = [key for key in STATEMENT_RESULT_KEYS if key in statement]
        if len(present) != 1:
            raise NormalizeError(
                f"{source}: statements[{position}] は expected / actual の"
                f" どちらか一方を持つこと(検出: {present})"
            )
        try:
            result = normalize_result_variant(statement[present[0]])
        except NormalizeError as exc:
            raise NormalizeError(f"{source}: statements[{position}]: {exc}") from exc
        entry: Dict[str, Any] = {
            "index": statement.get("index", position + 1),
            "sql": sql,
            "result": result,
        }
        if "known_gap" in statement:
            entry["known_gap"] = bool(statement["known_gap"])
        entries.append(entry)
    return entries


def load_statements_file(path: Path) -> List[Dict[str, Any]]:
    """期待値 / 実測出力ファイルを読み込み、正規化済みエントリ列を返す。

    ファイルは単一ドキュメント、または複数コーパスファイル実行時の
    ドキュメント配列(embedded writer の PARITY_OUTPUT)を許容する。
    配列の場合は statements を出現順に連結する。
    """
    with open(path, encoding="utf-8") as fh:
        data = json.load(fh)
    if isinstance(data, list):
        entries: List[Dict[str, Any]] = []
        for doc_index, document in enumerate(data):
            entries.extend(
                parse_statements_document(document, source=f"{path}[{doc_index}]")
            )
        return entries
    return parse_statements_document(data, source=str(path))


def canonical_json(obj: Any) -> str:
    """比較・差分表示用の正準 JSON 文字列。"""
    return json.dumps(obj, sort_keys=True, ensure_ascii=False, indent=2)
