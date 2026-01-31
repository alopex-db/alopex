# ストレスレーン

本ドキュメントは、ストレステストのレーン定義、選択規則、実行方法、CI起動方法を説明します。

## レーン定義

| レーン | feature フラグ | 目的 | 想定スケジュール |
| --- | --- | --- | --- |
| smoke | `lane_smoke` | 最小の動作確認 | PR / ローカル |
| ci | `lane_ci` | 標準CIカバレッジ | PR / main |
| nightly | `lane_nightly` | 長めのストレスシナリオ | nightly |
| weekly | `lane_weekly` | 非常に重い/長時間 | weekly |
| soak | `lane_soak` | 長時間バーンイン/資源監視 | nightly / weekly |
| perf | `lane_perf` | 性能ベースライン/回帰検知 | nightly |
| fuzz | `lane_fuzz` | 長時間ファズ（24h） | nightly / weekly |
| sanitizer | `lane_sanitizer` | TSAN/ASAN/LSAN/MSAN | nightly / weekly（schedule/dispatch） |

## レーン選択規則

レーンは次の順序で解決される。

1) 環境変数 `STRESS_LANE`
2) feature フラグ（`--features lane_*`）
3) 既定: `ci`

例:

```bash
# nightly のみ
STRESS_LANE=nightly cargo test -p alopex-core --tests --features lane_nightly

# 複数レーン
STRESS_LANE=ci,nightly cargo test -p alopex-core --tests --features lane_ci,lane_nightly

# 全レーン
STRESS_LANE=all cargo test -p alopex-core --tests --features lane_smoke,lane_ci,lane_nightly,lane_weekly,lane_soak,lane_perf,lane_fuzz,lane_sanitizer
```

## 代表的な実行コマンド

```bash
# CI レーン
cargo test -p alopex-core --tests --features lane_ci

# Nightly レーン
cargo test -p alopex-core --tests --features lane_nightly

# Weekly レーン
cargo test -p alopex-core --tests --features lane_weekly

# Soak レーン
cargo test -p alopex-core --tests --features lane_soak

# Perf レーン
STRESS_BASELINE_REQUIRED=true \
STRESS_BASELINE_DIR=target/stress-baselines \
STRESS_REPORT_DIR=target/stress-reports/perf \
STRESS_ARTIFACTS_DIR=target/stress-artifacts/perf \
cargo test -p alopex-core --features lane_perf --test stress_perf_baseline

# Fuzz レーン（cargo fuzz）
cd crates/alopex-sql/fuzz
cargo fuzz run sql_parser -- -max_total_time=86400
cd ../../alopex-dataframe/fuzz
cargo fuzz run dataframe_conversion -- -max_total_time=86400
# SQL/DF の両ターゲットを 24h 実行する。

# Sanitizer レーン（nightly + script）
./scripts/run-sanitizer-lane.sh asan
```

## レーン固有の環境変数

Soak レーン:

- `STRESS_SOAK_DURATION_SECS`
- `STRESS_WEEKLY_DURATION_SECS`
- `STRESS_SOAK_MAX_RSS_MB`
- `STRESS_SOAK_MAX_DB_MB`
- `STRESS_SOAK_CHECK_INTERVAL_SECS`
- `STRESS_SOAK_BATCH_SIZE`
- `STRESS_SOAK_VALUE_SIZE`
- `STRESS_SOAK_KEY_SPACE`

Fuzz レーン:

Fuzz の制御は libFuzzer の引数（例: `-max_total_time=86400`, `-seed=...`）で行う。
現在のターゲットは `STRESS_FUZZ_DURATION_SECS` / `STRESS_SEED` / `STRESS_REPLAY_SEED` を使用しない。

Perf レーン:

- `STRESS_BASELINE_REQUIRED`
- `STRESS_BASELINE_DIR`
- `STRESS_BASELINE_UPDATE`
- `STRESS_BASELINE_MARGIN_PCT`

Sanitizer レーン:

- `STRESS_REPORT_DIR`
- `STRESS_ARTIFACTS_DIR`
- `RUSTFLAGS`

## 共通環境変数

- `STRESS_LANE`: レーン指定（例: `ci`, `nightly`, `all`）
- `STRESS_SEED` / `STRESS_REPLAY_SEED`: 乱数シード固定
- `STRESS_REPLAY`: 再現モード有効化（`1` / `true`）
- `STRESS_STORAGE_MODE`: `memory`, `disk`, `both`（既定）
- `STRESS_REPORT_DIR`: メトリクス出力先
- `STRESS_ARTIFACTS_DIR`: アーティファクト出力先

## CI 起動方法

ストレステストCIは `Stress Tests` ワークフロー（`.github/workflows/stress-tests.yml`）で実行する。

起動トリガー:

- `pull_request`: 短時間の `stress-tests` のみ
- `workflow_dispatch`: sanitizer/fuzz/perf を含むすべての対象レーン
- `schedule`: cron の定期実行（毎日 + 毎週）

ジョブ対応:

- `stress-tests`（短時間）: PR / schedule / dispatch で実行
- `sanitizer-lane`: schedule / dispatch のみ
- `fuzz-lane`: schedule / dispatch のみ（各ターゲット 24h）
- `perf-lane`: schedule / dispatch のみ（ベースライン比較）

手動実行:

- GitHub Actions → `Stress Tests` → `Run workflow`
- 対象ブランチ（例: `main`, `rc/<version>`）を選択して実行

