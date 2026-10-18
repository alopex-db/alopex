# Issue #577: Parquet metadata and IO regressions

Codex preserves the [complete 054521b8 comparison](https://github.com/alopex-db/alopex/issues/577#issuecomment-6248196985):
30 measured cells, 630 timed samples, every row value verified, and two p50
failures against the previous Alopex baseline. The finite-budget statistics
ratio was 1.225903 and the finite-budget embedded Arrow ratio was 1.110182.
Codex retains both failures and every real sample.

## Page-header read-ahead regression

Codex reproduced a 21-byte header fetching 8192 bytes through the production
`InputFile::get_read` entrypoint, using a real Parquet writer and file. Codex
observed 12 passing Parquet units and one failing regression. Codex reduced the
header reader buffer to 512 bytes and passed the same 13 units with zero
failures or ignored tests. Codex also decoded a valid 32803-byte header by
refilling the buffer and verified the following payload bytes and independent
reader positions. The buffer controls read-ahead; it does not limit valid
header sizes or change the product memory-policy limit.

Codex verified input source SHA256
`c9dad457d29bd4458d1018f051452714635540821518bd0352ea352d44fb5b4d`.
Codex ran this identical focused target before and after the fix:

```sh
rtk cargo test --offline --locked -p alopex-sql --features tokio --lib --message-format=json-render-diagnostics executor::bulk::parquet:: -- --nocapture --test-threads=1
```

Codex preserves RED log SHA256
`8674f81262c644eb07d33d0bf53c88285c7f738a9b8a8cc43188d22681eab0d5`
and GREEN log SHA256
`4802e15144029dbbc43c16c01395a33a8810eff4949b773e8459f8c7d2e2895d`.
Codex removed each target's compiler-reported executable and matching depinfo,
143870165 bytes after RED and 143869501 bytes after GREEN. Codex used one CPU
and a 4 GiB process-memory maximum and observed zero OOM events.

Codex preserves the [separate process and syscall diagnosis](https://github.com/alopex-db/alopex/issues/577#issuecomment-6248874685).
Codex observed 1181499 versus 203040 syscalls, and 2.68 versus 1.69 process CPU
seconds, for the before and after executables at 054521b8. Codex does not use
traced latency or whole-process CPU cost as substitutes for the public SQL
latency acceptance comparison.

Codex additionally passed all 69 allocation/routing regressions on Linux and
removed the compiler-reported executable and depinfo, 129054638 bytes. Codex
preserves allocation log SHA256
`d13a6337997b1b9c0b69716e0c07d48161e07fab0632ec44d657cff7f4191660`.

Codex verified Mac public SQL and scoped warnings-denied lint against candidate
`af3908fdccf7a164209d59f5990b24c5b4cf282b` composed with the issue's changes,
tree `78ebb95f66b3aece9baf5ecc5d201584addd365a`. Codex confirmed byte-identical
input and comparison-controller sources. Mac passed 16 public cases with one
pre-existing ignored case, then explicitly ran that case in the same binary with
`--ignored --exact read_parquet_maps_large_binary_to_blob` and passed it: 17
unique cases executed, zero failures. Mac also passed format and scoped lint.
Codex received the raw logs and verified their SHA256 values:

- Public target: `643f70d172adc5b18a695382a3ff3a1695515a4be05ad78f0e3c532a05bb3fd7`.
- Explicit ignored case: `51b04b8797044741f920428376ad56cd4544b9f87f8fcc779f3c624619e1555f`.
- Scoped lint: `eba5a4a79f6a5d7a18d5e866d0658dff9b85e981e2b5faf7ad38531063648537`.

Codex preserves each invocation's result separately; Codex does not convert an
ignored case into a normal-run pass. Codex verified these exact Mac commands:

```sh
cargo test --offline --locked -p alopex-sql --features tokio --test read_parquet -- --test-threads=1
cargo clippy --offline --locked -p alopex-sql --features tokio --lib --test read_parquet_allocation --test read_parquet -- -D warnings
cargo fmt --all -- --check
```

## Host storage boundary

Codex reproduced acceptance of an invalid 42 GB operating limit, then changed
the comparison controller to reject limits above 40 GB. Codex passed nine
focused tests covering 42 GB rejection, stopping at 40 GB, comparison CPU
qualification, memory reserves, profile changes, capacity reasons and
interrupted-attempt retention. Codex retains the original 42 GB bundles and
measurements; subsequent bundles use the host's 40 GB stop boundary.

```sh
rtk proxy env PYTHONPATH=scripts/performance:scripts python3 -B -m unittest test_issue577_matrix
```

Codex preserves RED log SHA256
`52830d509e9e57fc827ac0f0787b47a7163f32e2b5158e172e932e79bd368a05`
and GREEN log SHA256
`47ba5070cb63e61a589b0af562b43eb1220f264b2409314e14022086f7195286`.
Codex uses constant-cost filesystem counters and labels project size as an
estimate. Codex removes temporary test fixtures and generates no Python bytecode.

## Earlier metadata consumption fix

### Original source identity

Codex used base HEAD `52c00ff91040222dcc45174a22ca90c950dc0a38` on
`fix/v0816-parquet-row-budget`. The implementation change is in
`crates/alopex-sql/src/executor/bulk/parquet/preflight.rs`, SHA256
`b83d6f6ffc4cd45d306b4b58412db03e49e4a0a0158de2328ccf9cfb02188111`.
Codex transferred that exact source to Mac worktree
`alopex/.codex/worktrees/issue577-metadata-chunks-52c00ff9`.
Codex keeps the acceptance record in
[Issue #577](https://github.com/alopex-db/alopex/issues/577#issuecomment-6165324212).

### Contract and results

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
| fa3d10bf three-way performance comparison | 30 measured cells / 630 timed samples / all row values verified; five p50 acceptance failures |

Codex's allocation target covers row-group, column, statistics and embedded
Arrow metadata reservations, shared page/group budgets, DELTA expansion and
codec allocation boundaries. Codex's public target covers policy propagation,
schema/type changes, nulls, value preservation and shared COPY callers.
Codex used one build job and one test thread. Codex verified native parser
exported contract 0.27.0 before the tests.

### Exact commands

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

### Evidence and cleanup

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

Codex completed the [fa3d10bf comparison](https://github.com/alopex-db/alopex/issues/577#issuecomment-6246354290) with the same API boundary, shapes, settings, one CPU, warm cache, three warmups and 21 measurements per cell. Codex observed five acceptance failures: rows without a budget exceeded the reference by 1.095480; finite-budget row groups and columns exceeded the previous Alopex baseline by 1.120678 and 1.070767; embedded Arrow exceeded the previous baseline by 1.245856 without a budget and 1.025972 with a budget. Codex preserves every actual sample in the linked report. Codex must repair these failed performance contracts; #577 remains open.

## Schema-footer IO regression

Codex reproduced five underlying reads for a writer-produced footer containing
12 KiB of opaque metadata. Codex changed the scanner buffer capacity to the
minimum of the footer size, the configured limit and 16 KiB. Codex preserves
the inner Take bound and drops the scanner before allocating the projected
schema. The same regression then used two reads with both a 16 KiB limit and
an unbounded policy. Codex preserves the schema-planning contract that avoids
materializing row groups.

Codex ran this exact regression before and after the implementation change:

```sh
cargo test --offline --locked -p alopex-sql --features tokio --lib --message-format=json-render-diagnostics executor::bulk::parquet::preflight::tests::issue577_schema_footer_buffers_metadata_reads -- --exact --nocapture --test-threads=1
```

Codex verified 12 Parquet units, 69 allocation/routing regressions and 15 public
SQL regressions, all passing with zero failures or ignored tests, using the
same focused commands above. Codex also passed the same warnings-denied
scoped lint command. Codex verified source SHA256
`2a96abdb20a6c52ab1f1fe28795dd5a679124ee5cb395111994e1fe67c42c4be`.

| Run | Log SHA256 |
| --- | --- |
| Footer IO RED, five reads | fa8518bd32508dfd41c5e26aa474865bc12785744372c967a3b62ca0e117ed3a |
| Footer IO GREEN, two reads | 1e8d4430df8fe2eff8b235df16870c5e5cb98e312c75dd1e562d8f744842a7e9 |
| Parquet units, 12 PASS | bc4d774ad49f78aade1fb20061d26c1c821eaf2bbe6b14e5bea70860cb0a8fea |
| Allocation/routing, 69 PASS | fdb6e62a7cf320591be1faef95d2e1f26c995f6c3e7fb9c1f2ccf41894f11104 |
| Public SQL, 15 PASS | a0ddb3959daa512699f8ec2388daff61ac4a8bb8e78e5c36a919ec34b5756487 |
| Scoped lint, exit 0 | f37773276736cb19152b20cb72063f69551ef25fb368ace10471379ffc892561 |

Codex removed each produced test executable and its matching depinfo before
starting the next target. The full-unit, allocation and public targets removed
34,792,265 B, 32,090,122 B and 29,990,427 B respectively. Codex measured
483,964,100,608 B of free filesystem space after lint. Codex used constant-cost
filesystem counters and retained current dependencies, parser and raw evidence;
Codex did not traverse the whole workspace for a fresh size audit.

## Completed a2c40ad0 comparison and measurement qualification

Codex completed [all 30 cells and 630 timed samples at a2c40ad0](https://github.com/alopex-db/alopex/issues/577#issuecomment-6247398882).
Codex verified every row value and the numeric p50 comparator returned PASS.
Codex rejected this run as acceptance evidence because background host CPU
medians differed: after 24.1%, before 60.7%, reference 55.8%. Codex classifies
measurement quality as invalid and keeps the performance acceptance gate blocked.
Codex preserves this complete raw run and earlier performance failures.

Codex reproduced missing comparison-load admission and runtime rejection with
two failing tests, then reproduced profile adoption and output-budget failures
with two more failing tests. Codex also reproduced the missing capacity-stop
reason record. Codex fixed these contracts and passed seven focused tests:

```sh
rtk proxy env PYTHONPATH=scripts/performance:scripts python3 -B -m unittest test_issue577_matrix
```

Codex verified GREEN log SHA256
`aae7a2e0a29330c865087ba9ecaf527de3b912b3a2cc8c00d4cbffb20a7b5dda`.
Codex keeps benchmark result validation in CaseRunner and uses the existing
PC-protection watchdog for each real case. Codex separates comparison admission
from PC resource protection and separates operational storage budgets from the
product performance profile. Codex records capacity with constant-cost filesystem
counters and labels project size as an estimate. Codex introduces no total
execution-time limit or attempt-count limit.

Codex removed the unit tests temporary directories automatically and generated
no executable or bytecode cache. Codex retains raw real measurements and current
shared dependencies. The lifecycle tests use valueless command artifacts and
injected host observations; Codex does not use them as performance measurements.
