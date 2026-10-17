# Issue #577: test checkpoint, 2026-10-18 JST

Codex records the latest observed results here for recovery. This checkpoint does
not establish completion of #577 or readiness of v0.8.16.

## Identity and status

Codex used base HEAD `33a595c6828320e757ff01ceb21f505f1cb85d10` on
`fix/v0816-parquet-row-budget` in worktree
`alopex-worktrees/v0816-cache-composed/.codex/worktrees/parquet-row-budget`.
Codex keeps the canonical continuation record in
[Issue #577](https://github.com/alopex-db/alopex/issues/577#issuecomment-6165324212).

| Check | Observed result | Scope |
| --- | --- | --- |
| Unbuffered metadata regression, before fix | FAIL: exit 101, build passed, 0 passed / 1 failed, 8196 reads | Added regression on base HEAD; source digest below |
| Parquet units, after local fix | INTERRUPTED: exit 125, host CPU protection during build | Product tests did not start; no GREEN result |
| Before / after / DuckDB comparison | FAIL: 30 measured cells, 0 missing, all result values passed, 2 performance failures | Committed base HEAD, not the local fix |
| Allocation 69 / public API 15 / final lint | UNVERIFIED for the local fix | Codex has not run these checks on the local fix |

Codex's local `preflight.rs` change consumes unbuffered metadata in 256-byte
stack chunks. Codex retains that code and its regression test as uncommitted
work; this documentation commit does not claim that the fix passed validation.
The local source SHA256 is
`b83d6f6ffc4cd45d306b4b58412db03e49e4a0a0158de2328ccf9cfb02188111`.

## Exact focused commands and preserved evidence

Codex reproduced RED with:

```sh
rtk proxy cargo test --offline --locked -p alopex-sql --features tokio --lib --message-format=json-render-diagnostics executor::bulk::parquet::preflight::tests::issue577_unbuffered_metadata_payload_uses_bounded_reads -- --exact --nocapture --test-threads=1
```

Codex's RED source SHA256 was
`10acb0323c82f859e0d82209ea3f13307bc63f23b8657e778ed8987b87b5719d`;
its log SHA256 was
`31fbcf2b8ae7861048a6ad16f47f38d9132fb156759753c80014a6099c6a309e`.
Codex published [the failure and diagnostic evidence](https://github.com/alopex-db/alopex/issues/577#issuecomment-6235226604).

Codex attempted the following command after the local fix:

```sh
rtk proxy cargo test --offline --locked -p alopex-sql --features tokio --lib --message-format=json-render-diagnostics executor::bulk::parquet:: -- --nocapture --test-threads=1
```

Codex's resource monitor stopped that build at
`2026-10-17T14:50:50.720946Z` for host CPU pressure. The observed runtime was
10m16.633s; this was not a fixed runtime deadline. The log SHA256 was
`74646d9a96238d9eceaa63419b4037430712741988c667d038c44df1083934da`.
Codex classifies this as an environment interruption, not a product test failure
or a passing test.

Codex's [full comparison evidence](https://github.com/alopex-db/alopex/issues/577#issuecomment-6234867071)
retains all 30 cells and raw measurements. The fixed comparator returned exit 1:
the finite-budget columns case had after/before p50 ratio `1.482147`, and the
finite-budget embedded Arrow case had ratio `1.200656`. Both used a
`268435456 B` read budget. Codex did not relax the comparator or replace those
failures with the unverified local fix. The report JSON SHA256 was
`f993bc00726eab803448a9723de587db028a632c8526c0e4bc2e6c08c4c65728`.

## Cleanup and continuation

Codex removed the RED executable and depinfo (`143176037 B`) and the interrupted
build's owned partial objects and depinfo (`19891813 B`). The target size after
that cleanup was `20034680558 B`. The earlier named workspace measurement was
`34665675101 B` including external outputs; it is historical, not a fresh size
measurement for this documentation commit. Codex preserves source, fixtures,
raw evidence, current shared dependencies and other tasks' outputs.

Codex has paused local and Mac builds while nucbox remains very slow. Codex has
paused the high-CPU history index; the user reports that the PC remains very
slow. Codex has not restarted WSL. Codex observed no Rust/test workers, and
Codex includes its own process and diagnostics in the load investigation.

Codex's candidate ancestry check at the recorded base HEAD was:

```sh
rtk proxy git merge-base --is-ancestor HEAD integration/v0.8.16-candidate
```

The check returned exit 1 against candidate
`3237d671156adb6bfa4ed18aa6f1d71f87358185`; the base fix was not integrated.
Codex has not pushed a candidate, tagged or published a release.

Codex's next action is to recover nucbox responsiveness before retrying the
interrupted focused validation under a new run identity. Parent Codex owns
that action and the PC responsiveness contract. #577 remains blocked by the
known performance failures and incomplete validation.
