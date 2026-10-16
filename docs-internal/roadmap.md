# AlopexDB ストレージロードマップ（v1.x → v2.0）

この文書は、GitHub Issue とマイルストーンで管理しているストレージ運用構想の実装順序と、v2.0 リリース時点で到達したい状態を整理したものです。

## 方針

v2.0 は「実装を開始する時期」ではなく、v1.x で段階的に実装した機能を統合し、リリース可能な状態として受け入れる最終ゴールです。

したがって、個別機能の実装 Issue は v1.x のマイルストーンへ割り当て、v2.0 には最終受け入れ・リリース判定を残します。現在の v0.8.9 に属する不具合修正は、現行リリースの品質確保として扱い、このロードマップの実装マイルストーンへ移動しません。

正本は GitHub のマイルストーンと Issue #184 です。この文書は、それらの関係と依存順を説明します。

## 全体像

```text
v0.8.9 現行リリース修正
  #192
       |
       v
v1.1 Storage Foundation
  #183, #186
       |
       v
v1.2 Format Migration
  #185, #188
       |
       v
v1.3 Distributed Coordination
  #187, #200
       |
       +----------------------+
       |                      |
       v                      v
v1.4 Portable Backends    v1.5 Runtime Residency
  #190, #191              #205 - #209
       |                      |
       +----------+-----------+
                  v
v1.6 Cluster Integration
  #189
                  |
                  v
v2.0 Release Goal / Acceptance
  #184
```

v1.4 と v1.5 は、v1.1〜v1.3 の契約が安定した後であれば、依存関係を分離できる範囲で並行して進められます。v1.6 は分散・クラスタ統合の受け皿として、各バックエンドとランタイム形態の検証結果を統合します。

## マイルストーン別の計画

| 段階 | マイルストーン | 目的 | 対象 Issue |
| --- | --- | --- | --- |
| 現行 | [v0.8.9](https://github.com/alopex-db/alopex/releases/tag/v0.8.9) | 現行リリースの不具合修正と品質確保 | [#192](https://github.com/alopex-db/alopex/issues/192) |
| SQL 継続 | [v1.0-SQL](https://github.com/alopex-db/alopex/milestone/13) | SQL 機能の既存ロードマップ | SQL 関連 Issue |
| SQL 継続 | [v0.10.0](https://github.com/alopex-db/alopex/milestone/15) | 既存の SQL／リリース計画 | 既存の v0.10.0 対象 |
| 1 | [v1.1-Storage-Foundation](https://github.com/alopex-db/alopex/milestone/16) | 永続化の基礎契約とローカルファイルバックエンド | [#183](https://github.com/alopex-db/alopex/issues/183), [#186](https://github.com/alopex-db/alopex/issues/186) |
| 2 | [v1.2-Format-Migration](https://github.com/alopex-db/alopex/milestone/17) | 論理フォーマットと物理フォーマットの分離、移行 | [#185](https://github.com/alopex-db/alopex/issues/185), [#188](https://github.com/alopex-db/alopex/issues/188) |
| 3 | [v1.3-Distributed-Coordination](https://github.com/alopex-db/alopex/milestone/18) | epoch・manifest・chirps による分散協調と共通トランザクション／可視性契約 | [#187](https://github.com/alopex-db/alopex/issues/187), [#200](https://github.com/alopex-db/alopex/issues/200) |
| 4 | [v1.4-Portable-Backends](https://github.com/alopex-db/alopex/milestone/19) | S3 互換バックエンドとバックエンド抽象化 | [#190](https://github.com/alopex-db/alopex/issues/190), [#191](https://github.com/alopex-db/alopex/issues/191) |
| 5 | [v1.5-Runtime-Residency](https://github.com/alopex-db/alopex/milestone/20) | embedded・server・cluster のランタイム形態とHTTP QUERY API | [#198](https://github.com/alopex-db/alopex/issues/198), [#199](https://github.com/alopex-db/alopex/issues/199), [#205](https://github.com/alopex-db/alopex/issues/205)〜[#209](https://github.com/alopex-db/alopex/issues/209) |
| 6 | [v1.6-Cluster-Integration](https://github.com/alopex-db/alopex/milestone/21) | Kubernetes 互換を含むクラスタ統合 | [#189](https://github.com/alopex-db/alopex/issues/189) |
| 最終 | [v2.0](https://github.com/alopex-db/alopex/milestone/14) | 最終受け入れ、互換性確認、リリース判定 | [#184](https://github.com/alopex-db/alopex/issues/184) |

## 各段階の完了条件

### v1.1 — Storage Foundation

- ローカルファイルバックエンドの要求仕様が確定している。
- #183 が定義するローカルファイルバックエンドと、#186 が定義する永続化ディスパッチャ／バックエンド境界が整合している。
- 論理的な成果物、保存先、読み書き、エラー境界を後続のバックエンドが再利用できる。

### v1.2 — Format Migration

- 論理フォーマットと物理フォーマットが分離されている。
- フォーマットのバージョン、メタデータ、移行方針が明示されている。
- 旧形式からの移行と、移行失敗時の安全な扱いを検証できる。

### v1.3 — Distributed Coordination

- manifest、epoch、chirps の責務と整合性が定義されている。
- 複数プロセス／複数ノードでの可視性、競合、再試行、ロールバック方針が検証されている。
- ローカルバックエンドに閉じない協調契約になっている。
- トランザクション、freshness、commit barrier、post-commit read の共通契約が定義され、各 transport に写像できる。
- 分散環境で commit 完了と後続 read の可視性境界が検証できる。

### v1.4 — Portable Backends

- ファイルシステムと S3 互換オブジェクトストレージを同じバックエンド契約で扱える。
- 成果物の identity、manifest、capability、エラー分類がバックエンド間で一貫している。
- 一方のバックエンド固有機能に依存せず、移送・切り替え・復旧を検証できる。

### v1.5 — Runtime Residency

- embedded、server、cluster の各実行形態で、ストレージ契約が維持される。
- プロセス内常駐、サーバー常駐、外部クラスタでの責務分担が明示されている。
- 各形態の起動、停止、再接続、障害復旧、観測性を検証できる。
- HTTP QUERY の設計・実装・POST fallback・cache/freshness semantics が確立している（#198, #199）。
- HTTP 以外の transport でも共通の transaction／freshness 契約（#200）が維持される。

### v1.6 — Cluster Integration

- Kubernetes 互換環境を含むクラスタで、配置・協調・永続化が統合されている。
- ノード障害、再スケジュール、ローリング更新、混在バージョンを検証できる。
- v1.4 のバックエンドと v1.5 のランタイム形態をクラスタ運用として接続できる。

## v2.0 のリリース判定

v2.0 では、少なくとも次の状態を満たしていることを最終受け入れ条件とします。

- v1.1〜v1.6 の実装 Issue と #198〜#200 が完了し、未解決の設計上のブロッカーがない。
- ローカルファイル、S3 互換、Kubernetes 互換クラスタを含む対象構成の責務と互換性が文書化されている。
- 論理フォーマット、物理フォーマット、manifest、artifact identity、capability の関係が一貫している。
- embedded → server → cluster、および filesystem → object storage の移行・切り替え経路が検証されている。
- epoch／chirps／manifest を含む分散協調で、競合、障害、再試行、ロールバック、混在バージョンを検証できる。
- transport-independent な transaction／freshness contract と、HTTP QUERY の安全性・cache identity・POST fallback が検証できる。
- 主要な障害・復旧シナリオのテスト結果と運用手順が残っている。
- #184 を最終リリース受け入れトラッカーとしてクローズできる。

## 運用ルール

1. 新しい実装タスクは、最終的に到達したい v2.0 の状態を参照しつつ、具体的な実装段階の v1.x マイルストーンへ登録する。
2. v2.0 へは、横断的な受け入れ、互換性確認、リリース準備を登録する。個別バックエンドや個別ランタイムの実装を v2.0 に戻さない。
3. SQL の既存ロードマップ（v1.0-SQL、v0.10.0）と、ストレージ運用ロードマップ（v1.1〜v2.0）は目的を分けて管理する。
4. マイルストーンの対象変更時は、この文書、#184 の本文、各 Issue の依存関係を同じ変更単位で更新する。
5. Issue が大きくなった場合は、要求仕様、設計、実装、テスト、運用検証へ分割し、親 Issue は受け入れ条件と依存関係の追跡に限定する。
