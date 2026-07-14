"""alopex mode-parity 検証ランナー。

仕様: docs-public/specs/alopex-mode-parity-spec.md

サブモジュール:
- ``surfaces``  : 4 経路(embedded / cli / http / grpc)の実行とサーバー起動管理
- ``normalize`` : 「結果の正規化規則」の実装
- ``report``    : 合否・SKIP 集計、差分報告、exit code 決定

設計メモ:
- 検証ロジックはすべて Python(標準ライブラリ + requirements.txt の依存のみ)。
  検証用の Rust バイナリは追加しない。ビルドするのは製品バイナリ
  (``alopex``, ``alopex-server``)のみ。
- 外部依存(requests / grpcio)は surfaces 内で遅延 import するため、
  本パッケージおよび normalize / report の import は標準ライブラリのみで
  完結する。
- コーパス・期待値・正規化ロジックは S1 (demo.py) / S2 (verify.py) で
  単一ソースを共有する(二重管理禁止)。
"""

__version__ = "0.1.0"

__all__ = ["normalize", "report", "surfaces"]
