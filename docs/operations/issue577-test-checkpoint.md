# Issue #577: metadata consumption regression

Codex records the READ_PARQUET regression and remaining acceptance work here.
Codex has confirmed 95 focused tests on Mac arm64 for the local implementation.
Codex has not completed the performance acceptance comparison for this source.

## Source identity

Codex used base HEAD `52c00ff91040222dcc45174a22ca90c950dc0a38` on
`fix/v0816-parquet-row-budget`. The implementation change is in
`crates/alopex-sql/src/executor/bulk/parquet/preflight.rs`, SHA256
`b83d6f6ffc4cd45d306b4b58412db03e49e4a0a0158de2328ccf9cfb02188111`.
Codex transferred that exact source to Mac worktree
`alopex/.codex/worktrees/issue577-metadata-chunks-52c00ff9`.
Codex keeps the acceptance record in
[Issue #577](https://github.com/alopex-db/alopex/issues/577#issuecomment-6165324212).

## Contract and results

Codex changed capacity-zero metadata readers to consume opaque payloads in
256-byte stack chunks while preserving shared reservations, Take boundaries
and truncated-input errors. Codex did not change read-budget thresholds.

| Check | Observed result |
| --- | --- |
| Added regression before fix | FAIL: build passed, exit 101, 0 passed / 1 failed, 8196 reads |
| Initial post-fix attempt | Environment interruption during build, exit 125; product tests did not start |
| Mac Parquet units after fix | PASS: 11 passed / 0 failed / 0 ignored; new regression 36 reads |
| Mac allocation regressions after fix | PASS: 69 passed / 0 failed / 0 ignored |
| Mac public READ_PARQUET/shared COPY regressions after fix | PASS: 15 passed / 0 failed / 0 ignored |
| Format | PASS: cargo fmt --all -- --check |
| Scoped warnings-denied lint | PASS: exit 0; 0 compiler/clippy warnings or errors |
| New-source three-way performance comparison | Not measured; acceptance incomplete |

Codex's allocation target covers row-group, column, statistics and embedded
Arrow metadata reservations, shared page/group budgets, DELTA expansion and
codec allocation boundaries. Codex's public target covers policy propagation,
schema/type changes, nulls, value preservation and shared COPY callers.
Codex used one build job and one test thread. Codex verified native parser
exported contract 0.27.0 before the tests.

## Exact commands

Codex reproduced RED with:

```sh
rtk proxy cargo test --offline --locked -p alopex-sql --features tokio --lib --message-format=json-render-diagnostics executor::bulk::parquet::preflight::tests::issue577_unbuffered_metadata_payload_uses_bounded_reads -- --exact --nocapture --test-threads=1
```

Codex ran the following commands on Mac through local RTK/SSH:

```sh
cargo test --offline --locked -p alopex-sql --features tokio --lib --message-format=json-render-diagnostics executor::bulk::parquet:: -- --nocapture --test-threads=1
cargo test --offline --locked -p alopex-sql --features tokio --test read_parquet_allocation --message-format=json-render-diagnostics issue577_ -- --nocapture --test-threads=1
cargo test --offline --locked -p alopex-sql --features tokio --test read_parquet --message-format=json-render-diagnostics -- --nocapture --test-threads=1
```

Codex's focused lint command is:

```sh
cargo clippy --offline --locked -p alopex-sql --features tokio --lib --test read_parquet_allocation --test read_parquet --message-format=json-render-diagnostics -- -D warnings
```

## Evidence and cleanup

| Run | Log SHA256 |
| --- | --- |
| RED | 31fbcf2b8ae7861048a6ad16f47f38d9132fb156759753c80014a6099c6a309e |
| Initial interrupted build | 74646d9a96238d9eceaa63419b4037430712741988c667d038c44df1083934da |
| Mac units | d6418bf7d9447536d0a21ceb323e6ebc2acdcdf57e1b6f0813f2e2572e64900f |
| Mac allocation | cddbbe510cd969c6a30e93ee6979d8943ba2f03f3a855d9240c3eafe9314de23 |
| Mac public SQL | a0eb508c6cd42312c70999576ccd8cd3413152df3bb5aaa8c72e86984f84e1f7 |
| Mac scoped lint | 4a217e310a270c7fc95888b662b86658429d50c8d21bdb4dd2c1f9df502ec0e8 |

Codex preserves the [RED and diagnostic evidence](https://github.com/alopex-db/alopex/issues/577#issuecomment-6235226604).
Codex removed each target's generated test executable and depinfo before
starting the next target: 34,575,849 B, 32,090,122 B and 29,990,427 B,
respectively. The last post-target Mac workspace measurement was
2,981,795,840 B, with shared target 1,024,429,056 B. Codex retained the verified
parser and current shared dependencies/check metadata. Codex removed no source
or fixture. The final inventory of the Mac workspace and external
project directories totaled 28,210,368,512 B.

Codex preserves the [33a595c6 comparison](https://github.com/alopex-db/alopex/issues/577#issuecomment-6234867071):
30 measured cells, zero missing, all values passed, but two performance
failures. Finite-budget columns had after/before p50 ratio 1.482147 and
embedded Arrow had ratio 1.200656, both at 268435456 B. Those results precede
the local fix; Codex does not claim that the new source passed performance
acceptance. The comparison report SHA256 is
`f993bc00726eab803448a9723de587db028a632c8526c0e4bc2e6c08c4c65728`.

Codex's next action is to perform the required before/after/reference
performance comparison. #577 remains incomplete.
