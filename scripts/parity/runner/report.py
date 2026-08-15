"""合否・SKIP 集計、差分報告、exit code 決定。

docs-public/specs/alopex-mode-parity-spec.md「スクリプト共通要件」:
- exit code: 成功 0 / 検証不一致 1 / 環境・起動エラー 2
- SKIP したケースは明示的に集計・表示し、成功数に含めない。
"""

from __future__ import annotations

import difflib
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence, Tuple

from . import normalize

EXIT_OK = 0
EXIT_MISMATCH = 1
EXIT_ENV = 2

STATUS_PASS = "PASS"
STATUS_FAIL = "FAIL"
STATUS_SKIP = "SKIP"
STATUS_ERROR = "ERROR"

_STATUSES = (STATUS_PASS, STATUS_FAIL, STATUS_SKIP, STATUS_ERROR)


@dataclass
class CaseResult:
    """1 検証ケース(経路 or マトリクスセル)の結果。"""

    section: str  # 例: "s2a", "s2b", "s2c", "demo:act1"
    case_id: str  # 例: "embedded<->http", "writer=cli/reader=grpc"
    status: str
    detail: str = ""


def record_diff(
    left_label: str,
    left: Dict[str, Any],
    right_label: str,
    right: Dict[str, Any],
) -> str:
    """正規化済みレコード同士の unified diff を返す。"""
    left_lines = normalize.canonical_json(left).splitlines(keepends=True)
    right_lines = normalize.canonical_json(right).splitlines(keepends=True)
    return "".join(
        difflib.unified_diff(
            left_lines, right_lines, fromfile=left_label, tofile=right_label
        )
    )


def results_equal(
    left: Dict[str, Any], right: Dict[str, Any]
) -> Tuple[bool, Optional[str]]:
    """正規化済みエントリの result 部(結果バリアント)を比較する。

    バリアント: success / rows_affected / query / error
    (runner.normalize のスキーマ参照)。known_gap は情報キーであり
    比較には影響しない。

    戻り値は (一致か, 注記)。注記は一致時にも付き得る
    (例: 片側の列名が補完不能で行値のみ比較した場合)。
    """
    lr = left.get("result", {})
    rr = right.get("result", {})
    if lr.get("type") != rr.get("type"):
        return False, None
    if lr.get("type") == "query":
        if lr.get("rows") != rr.get("rows"):
            return False, None
        lcols = lr.get("columns")
        rcols = rr.get("columns")
        # 空結果集合の列名は出力形式によって取得できない(columns=None)。
        # 片側欠落時は列名を比較対象にできないため、行値一致 + 注記とする。
        if lcols is None or rcols is None:
            return True, "列名は片側欠落のため行値のみ比較(空結果集合など)"
        if lcols != rcols:
            return False, None
        return True, None
    # success: type のみ / rows_affected: count / error: error_class+object+code
    return (lr == rr), None


class Report:
    """経路 × コーパスの合否/SKIP 集計と exit code 決定。"""

    def __init__(self) -> None:
        self.results: List[CaseResult] = []

    # -- 記録 ---------------------------------------------------------------

    def add(self, section: str, case_id: str, status: str, detail: str = "") -> None:
        if status not in _STATUSES:
            raise ValueError(f"不正なステータス: {status}")
        self.results.append(CaseResult(section, case_id, status, detail))

    def pass_(self, section: str, case_id: str, detail: str = "") -> None:
        self.add(section, case_id, STATUS_PASS, detail)

    def fail(self, section: str, case_id: str, detail: str = "") -> None:
        self.add(section, case_id, STATUS_FAIL, detail)

    def skip(self, section: str, case_id: str, reason: str) -> None:
        # SKIP は必ず理由を持つ(正直な報告)
        if not reason:
            raise ValueError("SKIP には理由が必須")
        self.add(section, case_id, STATUS_SKIP, reason)

    def error(self, section: str, case_id: str, detail: str = "") -> None:
        self.add(section, case_id, STATUS_ERROR, detail)

    # -- 比較 ---------------------------------------------------------------

    def compare_record_lists(
        self,
        section: str,
        case_id: str,
        left_label: str,
        left: Sequence[Dict[str, Any]],
        right_label: str,
        right: Sequence[Dict[str, Any]],
    ) -> bool:
        """正規化済みレコード列同士を比較し、結果を記録する。

        1 文でも不一致があれば FAIL とし、不一致の文・差分を detail に残す。
        戻り値は一致したかどうか。
        """
        details: List[str] = []
        mismatched = False
        if len(left) != len(right):
            mismatched = True
            details.append(
                f"文数不一致: {left_label}={len(left)} / {right_label}={len(right)}"
            )
        for index, (lrec, rrec) in enumerate(zip(left, right)):
            equal, note = results_equal(lrec, rrec)
            if not equal:
                mismatched = True
                diff = record_diff(left_label, lrec, right_label, rrec)
                details.append(
                    f"statement[{index}] sql={lrec.get('sql', '')!r} 不一致:\n{diff}"
                )
            elif note:
                details.append(f"statement[{index}] 注記: {note}")

        if mismatched:
            self.fail(section, case_id, "\n".join(details))
            return False
        self.pass_(section, case_id, "\n".join(details))
        return True

    # -- 集計・出力 -----------------------------------------------------------

    def counts(self) -> Dict[str, int]:
        counts = {status: 0 for status in _STATUSES}
        for result in self.results:
            counts[result.status] += 1
        return counts

    def render(self, *, require_all: bool = False) -> str:
        """人間可読なサマリ(セクション別集計 + 非 PASS の詳細)。"""
        lines: List[str] = []
        lines.append("=" * 72)
        lines.append("mode-parity 検証結果")
        lines.append("=" * 72)

        sections: Dict[str, List[CaseResult]] = {}
        for result in self.results:
            sections.setdefault(result.section, []).append(result)

        for section, cases in sections.items():
            lines.append(f"\n[{section}]")
            for case in cases:
                lines.append(f"  {case.status:5s}  {case.case_id}")
            counts = {status: 0 for status in _STATUSES}
            for case in cases:
                counts[case.status] += 1
            lines.append(
                "  --- "
                + " / ".join(f"{status}={counts[status]}" for status in _STATUSES)
            )

        detail_cases = [r for r in self.results if r.status != STATUS_PASS and r.detail]
        if detail_cases:
            lines.append("\n" + "-" * 72)
            lines.append("非 PASS ケースの詳細")
            lines.append("-" * 72)
            for case in detail_cases:
                lines.append(f"\n### [{case.section}] {case.case_id} ({case.status})")
                lines.append(case.detail)

        total = self.counts()
        lines.append("\n" + "=" * 72)
        lines.append(
            "合計: "
            + " / ".join(f"{status}={total[status]}" for status in _STATUSES)
            + f"  -> exit {self.exit_code(require_all=require_all)}"
        )
        lines.append("=" * 72)
        return "\n".join(lines)

    def exit_code(self, *, require_all: bool = False) -> int:
        """ERROR(環境)> FAIL/SKIP(全件必須)> OK の優先で exit code を決める。"""
        counts = self.counts()
        if counts[STATUS_ERROR] > 0:
            return EXIT_ENV
        if counts[STATUS_FAIL] > 0 or (require_all and counts[STATUS_SKIP] > 0):
            return EXIT_MISMATCH
        return EXIT_OK
