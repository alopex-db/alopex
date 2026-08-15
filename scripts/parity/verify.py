#!/usr/bin/env python3
"""S2 エントリポイント: サーフェス等価性マトリクス検証。

docs-public/specs/alopex-mode-parity-spec.md「シナリオ S2」:

- S2-a: 同一コーパスを 4 経路(embedded / cli / http / grpc)で独立実行し、
        正規化 JSON を相互 diff する(INV-2)。
- S2-b: writer × reader マトリクス。writer がコーパスを実行 → プロセス終了 →
        reader が同一データディレクトリを開き 99_verify.sql を実行し、
        期待値ゴールデンと一致することを検証する(INV-1)。
- S2-c: 旧バージョンデータディレクトリの互換検証。

使い方:
    python verify.py --corpus corpus/ --expected expected/ [--filter s2a|s2b|s2c]

exit code: 成功 0 / 検証不一致 1 / 環境・起動エラー 2
"""

from __future__ import annotations

import argparse
import hashlib
import itertools
import json
import shutil
import signal
import sys
import tarfile
import tempfile
from pathlib import Path, PurePosixPath
from typing import Any, Dict, List, Optional, Sequence

PARITY_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(PARITY_DIR))

from runner import normalize, surfaces  # noqa: E402
from runner.report import EXIT_ENV, Report  # noqa: E402
from runner.surfaces import (  # noqa: E402
    CliSurface,
    EmbeddedSurface,
    GrpcSurface,
    HttpSurface,
    SurfaceError,
    SurfaceSkip,
    start_server,
)

SECTION_S2A = "s2a"
SECTION_S2B = "s2b"
SECTION_S2C = "s2c"
ALL_FILTERS = (SECTION_S2A, SECTION_S2B, SECTION_S2C)

#: 全経路(reader として使える経路)
ROUTES = ("embedded", "cli", "http", "grpc")
#: S2-b の writer 経路(仕様のマトリクス行: 組み込み API / CLI / サーバー HTTP)
WRITERS = ("embedded", "cli", "http")
#: S2-b の reader 経路(仕様のマトリクス列)。cluster は SF-CLUSTER
#: (cluster-aware モード・単一メンバーで起動した alopex-server の HTTP 経路。
#: v0.7.1 で有効化)。
S2B_READERS = (*ROUTES, "cluster")

#: S2-c 互換フィクスチャの置き場所。契約:
#:   <root>/<version>/data/          旧バージョンで生成したデータディレクトリ
#:   <root>/<version>/expected.json  99_verify.sql の期待値(normalize 形)
COMPAT_FIXTURES_DIR = PARITY_DIR / "fixtures" / "compat"


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def extract_compat_data(
    fixture: Path, destination: Path, expected_data: Dict[str, str]
) -> Path:
    archive = fixture / "data.tar.gz"
    with tarfile.open(archive, mode="r:gz") as bundle:
        members = bundle.getmembers()
        for member in members:
            path = PurePosixPath(member.name)
            if path.is_absolute() or ".." in path.parts or path.parts[:1] != ("data",):
                raise SurfaceError(f"unsafe compatibility archive member: {member.name}")
        bundle.extractall(destination, members=members, filter="data")

    data_dir = destination / "data"
    actual_data = {
        path.relative_to(data_dir).as_posix(): sha256_file(path)
        for path in sorted(data_dir.rglob("*"))
        if path.is_file()
    }
    if actual_data != expected_data:
        raise SurfaceError(
            f"compatibility fixture data digest mismatch: {fixture.name}"
        )
    return data_dir


def proto_path(repo: Path) -> Path:
    # released モードでは検証対象バージョンの proto を使う
    # (ExecuteSql の戻り値型が v0.7.5 で変更されたため)。
    return surfaces.server_proto_path(repo)


def install_signal_handlers() -> None:
    """SIGTERM / SIGINT 受信時に子プロセスと一時ディレクトリを確実に掃除する。

    1. 起動中の alopex-server(surfaces._ACTIVE_SERVERS)を停止する。
    2. sys.exit() で SystemExit を送出し、実行中フレームから
       with 文(tempfile.TemporaryDirectory / ServerHandle / GrpcSurface)を
       巻き戻して一時ディレクトリ・スタブ生成物を削除する。
       (本スクリプトの except 節は SurfaceError 系のみを捕捉するため
       SystemExit は握り潰されない。)
    """

    def _handler(signum: int, _frame: Any) -> None:
        surfaces.cleanup_active_servers()
        sys.exit(128 + signum)

    signal.signal(signal.SIGTERM, _handler)
    signal.signal(signal.SIGINT, _handler)


# ---------------------------------------------------------------------------
# 経路実行ヘルパ
# ---------------------------------------------------------------------------


def run_embedded(
    repo: Path,
    corpus_dir: Path,
    *,
    role: str,
    data_dir: Path,
    scratch: Path,
) -> surfaces.EmbeddedRunResult:
    output = scratch / f"embedded-{role}.json"
    return EmbeddedSurface(repo).run(
        corpus_dir, role=role, data_dir=data_dir, output_path=output
    )


def run_http_statements(
    repo: Path,
    binaries: Dict[str, Path],
    statements: Sequence[str],
    *,
    data_dir: Path,
    scratch: Path,
) -> List[Dict[str, Any]]:
    """サーバーを起動して HTTP で文を実行し、確実に停止する。"""
    work = scratch / "srv-http"
    work.mkdir(parents=True, exist_ok=True)
    with start_server(
        binaries[surfaces.PRODUCT_BIN_SERVER], repo=repo, data_dir=data_dir, work_dir=work
    ) as server:
        return HttpSurface(server.http_base).run_statements(statements)


def run_grpc_statements(
    repo: Path,
    binaries: Dict[str, Path],
    statements: Sequence[str],
    *,
    data_dir: Path,
    scratch: Path,
) -> List[Dict[str, Any]]:
    """サーバーを起動して gRPC で文を実行し、確実に停止する。"""
    work = scratch / "srv-grpc"
    work.mkdir(parents=True, exist_ok=True)
    with start_server(
        binaries[surfaces.PRODUCT_BIN_SERVER], repo=repo, data_dir=data_dir, work_dir=work
    ) as server:
        with GrpcSurface(server.grpc_target, proto_path=proto_path(repo)) as grpc_surface:
            return grpc_surface.run_statements(statements)


def run_cluster_statements(
    repo: Path,
    binaries: Dict[str, Path],
    statements: Sequence[str],
    *,
    data_dir: Path,
    scratch: Path,
) -> List[Dict[str, Any]]:
    """SF-CLUSTER: cluster-aware サーバー(単一メンバー)を起動して HTTP で実行する。

    実行前に GET /api/admin/status でサーフェスの同一性を確認する
    (mode が cluster_aware でなければ、検証しているのは SF-CLUSTER ではない
    ため環境エラー)。結果の正誤判定は呼び出し側の期待値比較が担う。
    """
    work = scratch / "srv-cluster"
    work.mkdir(parents=True, exist_ok=True)
    with start_server(
        binaries[surfaces.PRODUCT_BIN_SERVER],
        repo=repo,
        data_dir=data_dir,
        work_dir=work,
        cluster_aware=True,
    ) as server:
        cluster = surfaces.fetch_cluster_status(server.http_base)
        mode = cluster.get("mode")
        if mode != "cluster_aware":
            raise SurfaceError(
                f"SF-CLUSTER サーフェスの mode が cluster_aware でない: {mode!r}"
            )
        return HttpSurface(server.http_base).run_statements(statements)


# ---------------------------------------------------------------------------
# S2-a: 実行経路の等価性(INV-2)
# ---------------------------------------------------------------------------


def run_s2a(
    rep: Report,
    repo: Path,
    binaries: Dict[str, Path],
    corpus_dir: Path,
) -> None:
    statements = surfaces.load_statements(surfaces.corpus_files(corpus_dir))
    raw_by_route: Dict[str, List[Dict[str, Any]]] = {}
    normalized: Dict[str, List[Dict[str, Any]]] = {}

    with tempfile.TemporaryDirectory(prefix="parity-s2a-") as tmp:
        base = Path(tmp)
        for route in ROUTES:
            scratch = base / route
            data_dir = scratch / "data"
            data_dir.mkdir(parents=True)
            try:
                if route == "embedded":
                    result = run_embedded(
                        repo,
                        corpus_dir,
                        role=surfaces.ROLE_WRITER,
                        data_dir=data_dir,
                        scratch=scratch,
                    )
                    # PARITY_OUTPUT は正規化済み(actual スキーマ)
                    normalized[route] = result.entries
                    # 組み込みテストは期待値ゴールデンとの一致を自己アサート
                    # する。成功時も PASS 行を出して報告の分母を一定にする。
                    if result.cargo_ok:
                        rep.pass_(
                            SECTION_S2A,
                            "embedded(self-assert)",
                            "cargo test parity_corpus: 期待値ゴールデンと全文一致",
                        )
                    else:
                        # 出力は回収済みなので相互 diff は続行しつつ、
                        # 自己アサート失敗は不一致として記録する。
                        rep.fail(
                            SECTION_S2A,
                            "embedded(self-assert)",
                            result.stderr_tail,
                        )
                elif route == "cli":
                    raw_by_route[route] = CliSurface(
                        binaries[surfaces.PRODUCT_BIN_CLI], repo
                    ).run_statements(statements, data_dir=data_dir)
                elif route == "http":
                    raw_by_route[route] = run_http_statements(
                        repo, binaries, statements, data_dir=data_dir, scratch=scratch
                    )
                elif route == "grpc":
                    raw_by_route[route] = run_grpc_statements(
                        repo, binaries, statements, data_dir=data_dir, scratch=scratch
                    )
            except SurfaceSkip as skip:
                rep.skip(SECTION_S2A, route, skip.reason)
            except SurfaceError as exc:
                rep.error(SECTION_S2A, route, str(exc))

    # 正規化。gRPC は列名メタデータを持たないため、他経路の正規化結果から
    # 列名を補完する(http -> embedded -> cli の順で最初に得られた経路)。
    for route in ("cli", "http"):
        if route not in raw_by_route:
            continue
        try:
            normalized[route] = normalize.normalize_records(raw_by_route[route])
        except normalize.NormalizeError as exc:
            rep.error(SECTION_S2A, f"{route}(normalize)", str(exc))
    if "grpc" in raw_by_route:
        columns_source: Optional[List[Dict[str, Any]]] = None
        for source in ("http", "embedded", "cli"):
            if source in normalized:
                columns_source = normalized[source]
                break
        try:
            normalized["grpc"] = normalize.normalize_records(
                raw_by_route["grpc"], columns_source=columns_source
            )
        except normalize.NormalizeError as exc:
            rep.error(SECTION_S2A, "grpc(normalize)", str(exc))

    # 相互 diff(全ペア)
    for left, right in itertools.combinations(
        [r for r in ROUTES if r in normalized], 2
    ):
        rep.compare_record_lists(
            SECTION_S2A,
            f"{left}<->{right}",
            left,
            normalized[left],
            right,
            normalized[right],
        )


# ---------------------------------------------------------------------------
# S2-b: writer × reader マトリクス(INV-1)
# ---------------------------------------------------------------------------


def _s2b_write_phase(
    writer: str,
    repo: Path,
    binaries: Dict[str, Path],
    corpus_dir: Path,
    corpus_statements: Sequence[str],
    *,
    data_dir: Path,
    scratch: Path,
) -> None:
    """writer がコーパスを実行し、プロセスを終了する(順次アクセス)。"""
    if writer == "embedded":
        run_embedded(
            repo,
            corpus_dir,
            role=surfaces.ROLE_WRITER,
            data_dir=data_dir,
            scratch=scratch,
        )
    elif writer == "cli":
        CliSurface(binaries[surfaces.PRODUCT_BIN_CLI], repo).run_statements(
            corpus_statements, data_dir=data_dir
        )
    elif writer == "http":
        run_http_statements(
            repo, binaries, corpus_statements, data_dir=data_dir, scratch=scratch
        )
    else:
        raise SurfaceError(f"未知の writer: {writer}")


def _s2b_read_phase(
    reader: str,
    repo: Path,
    binaries: Dict[str, Path],
    corpus_dir: Path,
    verify_statements: Sequence[str],
    *,
    data_dir: Path,
    scratch: Path,
    columns_source: Optional[Sequence[Dict[str, Any]]] = None,
) -> List[Dict[str, Any]]:
    """reader が同一データディレクトリを開き 99_verify.sql を実行する。

    戻り値は正規化済みエントリ列({"index","sql","result"})。
    gRPC reader は列名メタデータを持たないため ``columns_source``
    (通常は期待値エントリ列)で列名を補完する。
    """
    if reader == "embedded":
        # PARITY_OUTPUT は正規化済み(actual スキーマ)
        return run_embedded(
            repo,
            corpus_dir,
            role=surfaces.ROLE_READER,
            data_dir=data_dir,
            scratch=scratch,
        ).entries
    if reader == "cli":
        raw = CliSurface(binaries[surfaces.PRODUCT_BIN_CLI], repo).run_statements(
            verify_statements, data_dir=data_dir
        )
        return normalize.normalize_records(raw)
    if reader == "http":
        raw = run_http_statements(
            repo, binaries, verify_statements, data_dir=data_dir, scratch=scratch
        )
        return normalize.normalize_records(raw)
    if reader == "grpc":
        raw = run_grpc_statements(
            repo, binaries, verify_statements, data_dir=data_dir, scratch=scratch
        )
        return normalize.normalize_records(raw, columns_source=columns_source)
    if reader == "cluster":
        raw = run_cluster_statements(
            repo, binaries, verify_statements, data_dir=data_dir, scratch=scratch
        )
        return normalize.normalize_records(raw)
    raise SurfaceError(f"未知の reader: {reader}")


def run_s2b(
    rep: Report,
    repo: Path,
    binaries: Dict[str, Path],
    corpus_dir: Path,
    expected_dir: Path,
) -> None:
    expected_path = expected_dir / "99_verify.json"
    if not expected_path.is_file():
        rep.error(
            SECTION_S2B,
            "expected/99_verify.json",
            f"期待値ゴールデン未整備: {expected_path}。"
            " コーパス整備タスクで正規化済み期待値を作成すること。",
        )
        return
    try:
        expected_entries = normalize.load_statements_file(expected_path)
        corpus_statements = surfaces.load_statements(surfaces.corpus_files(corpus_dir))
        verify_statements = surfaces.load_statements(
            [surfaces.verify_sql_path(corpus_dir)]
        )
    except (OSError, normalize.NormalizeError, SurfaceError) as exc:
        rep.error(SECTION_S2B, "setup", str(exc))
        return

    for writer in WRITERS:
        for reader in S2B_READERS:
            cell = f"writer={writer}/reader={reader}"
            with tempfile.TemporaryDirectory(prefix="parity-s2b-") as tmp:
                base = Path(tmp)
                data_dir = base / "data"
                data_dir.mkdir()
                try:
                    _s2b_write_phase(
                        writer,
                        repo,
                        binaries,
                        corpus_dir,
                        corpus_statements,
                        data_dir=data_dir,
                        scratch=base / "writer",
                    )
                    actual = _s2b_read_phase(
                        reader,
                        repo,
                        binaries,
                        corpus_dir,
                        verify_statements,
                        data_dir=data_dir,
                        scratch=base / "reader",
                        columns_source=expected_entries if reader == "grpc" else None,
                    )
                except SurfaceSkip as skip:
                    rep.skip(SECTION_S2B, cell, skip.reason)
                    continue
                except (SurfaceError, normalize.NormalizeError) as exc:
                    rep.error(SECTION_S2B, cell, str(exc))
                    continue
            rep.compare_record_lists(
                SECTION_S2B, cell, "expected", expected_entries, reader, actual
            )


# ---------------------------------------------------------------------------
# S2-c: データファイルのバージョン互換
# ---------------------------------------------------------------------------


def run_s2c(
    rep: Report,
    repo: Path,
    binaries: Dict[str, Path],
    corpus_dir: Path,
    fixtures_dir: Path,
) -> None:
    if not fixtures_dir.is_dir() or not any(fixtures_dir.iterdir()):
        rep.skip(
            SECTION_S2C,
            "compat-fixtures",
            f"互換フィクスチャ未整備: {fixtures_dir}。"
            " 現行の alopex-core フィクスチャ"
            " (crates/alopex-core/examples/generate_compat_v0_1.rs) は"
            " 単一ファイル形式(v0_1.alopex)であり、サーフェスが開ける"
            " データディレクトリ形式のフィクスチャが存在しない。"
            " フィクスチャ生成器のデータディレクトリ対応後に有効化する。",
        )
        return

    try:
        verify_statements = surfaces.load_statements(
            [surfaces.verify_sql_path(corpus_dir)]
        )
    except SurfaceError as exc:
        rep.error(SECTION_S2C, "setup", str(exc))
        return

    for fixture in sorted(p for p in fixtures_dir.iterdir() if p.is_dir()):
        fixture_archive = fixture / "data.tar.gz"
        fixture_expected = fixture / "expected.json"
        fixture_provenance = fixture / "provenance.json"
        if not all(
            path.is_file()
            for path in (fixture_archive, fixture_expected, fixture_provenance)
        ):
            rep.error(
                SECTION_S2C,
                fixture.name,
                f"フィクスチャ契約違反: {fixture} に data.tar.gz、"
                "expected.json、provenance.json が必要",
            )
            continue
        try:
            provenance = json.loads(fixture_provenance.read_text(encoding="utf-8"))
            if provenance.get("schema") != "alopex-compat-fixture/v1":
                raise SurfaceError(f"未知の fixture provenance: {fixture}")
            archive_record = provenance.get("archive", {})
            if archive_record.get("path") != fixture_archive.name or archive_record.get(
                "sha256"
            ) != sha256_file(fixture_archive):
                raise SurfaceError(f"fixture archive digest mismatch: {fixture.name}")
            if provenance.get("expected_sha256") != sha256_file(fixture_expected):
                raise SurfaceError(f"fixture expected digest mismatch: {fixture.name}")
            expected_data = {
                item["path"]: item["sha256"] for item in provenance.get("data", [])
            }
            if not expected_data:
                raise SurfaceError(f"fixture data digest list is empty: {fixture.name}")
            expected_entries = normalize.load_statements_file(fixture_expected)
        except (OSError, json.JSONDecodeError, KeyError, normalize.NormalizeError, SurfaceError) as exc:
            rep.error(SECTION_S2C, fixture.name, str(exc))
            continue

        for reader in ROUTES:
            case = f"{fixture.name}/reader={reader}"
            with tempfile.TemporaryDirectory(prefix="parity-s2c-") as tmp:
                base = Path(tmp)
                try:
                    # 開く際の回復処理等でフィクスチャを汚さないよう、sparse
                    # archive を reader ごとの一時領域へ展開する。
                    data_dir = extract_compat_data(fixture, base, expected_data)
                    actual = _s2b_read_phase(
                        reader,
                        repo,
                        binaries,
                        corpus_dir,
                        verify_statements,
                        data_dir=data_dir,
                        scratch=base / "reader",
                        columns_source=expected_entries if reader == "grpc" else None,
                    )
                except SurfaceSkip as skip:
                    rep.skip(SECTION_S2C, case, skip.reason)
                    continue
                except (SurfaceError, normalize.NormalizeError) as exc:
                    rep.error(SECTION_S2C, case, str(exc))
                    continue
            rep.compare_record_lists(
                SECTION_S2C, case, "expected", expected_entries, reader, actual
            )


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
    parser.add_argument(
        "--filter",
        action="append",
        choices=ALL_FILTERS,
        help="実行するシナリオ(省略時は全部)",
    )
    parser.add_argument(
        "--compat-fixtures",
        type=Path,
        default=COMPAT_FIXTURES_DIR,
        help="S2-c 互換フィクスチャのルート",
    )
    parser.add_argument(
        "--require-all",
        action="store_true",
        help="SKIP を許可せず、1件でも未実施なら検証不一致(exit 1)にする",
    )
    args = parser.parse_args(argv)

    # SIGTERM/SIGINT で子プロセス(alopex-server)と一時ディレクトリを掃除する
    install_signal_handlers()

    repo = surfaces.repo_root()
    if not args.corpus.is_dir():
        print(f"環境エラー: コーパスディレクトリがない: {args.corpus}", file=sys.stderr)
        return EXIT_ENV

    rep = Report()

    # ビルドは単発・逐次(nim parser -> 製品バイナリ)。以後はビルド済みを使う。
    try:
        binaries = surfaces.build_products(repo)
    except SurfaceError as exc:
        print(f"環境エラー: {exc}", file=sys.stderr)
        return EXIT_ENV

    filters = tuple(args.filter) if args.filter else ALL_FILTERS
    if SECTION_S2A in filters:
        run_s2a(rep, repo, binaries, args.corpus)
    if SECTION_S2B in filters:
        run_s2b(rep, repo, binaries, args.corpus, args.expected)
    if SECTION_S2C in filters:
        run_s2c(rep, repo, binaries, args.corpus, args.compat_fixtures)

    print(rep.render(require_all=args.require_all))
    return rep.exit_code(require_all=args.require_all)


if __name__ == "__main__":
    sys.exit(main())
