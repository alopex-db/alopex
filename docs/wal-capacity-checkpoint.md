# WAL capacity checkpoints

Commits that individually fit the configured WAL ring can continue when earlier
commits consume its free space. Before appending a new batch, the LSM commit path
checks its encoded size. It performs at most one checkpoint and one capacity
recheck. It never retries a partially written batch or an I/O failure.

An LSM commit larger than the entire ring returns `Error::Io` with
`ErrorKind::StorageFull` and a message containing requested and available capacity.
Such entries are the separate responsibility of #682; this change does not split
an atomic batch.

## Ordering and recovery

The lock order remains snapshot writer gate, commit lock, then WAL lock.
The internal checkpoint helper requires the first two locks and acquires the WAL
lock only after persisting data. It does not recursively call the public method.
An open owned cursor retains its existing source-stability guard: a writer waits
until that cursor closes. Capacity retry is bounded; cursor lifetime is controlled
by the caller.

All MemTable versions, including tombstones, are written in SSTable order so older
read-only transactions can continue reading their snapshots after a checkpoint.
An incomplete SSTable has a `.sst.tmp` suffix and is not discovered on reopen.
Completed SSTables are renamed and their directory is synced before publishing
the checkpoint metadata. The metadata is synced, atomically renamed, and its
parent directory synced before reclaiming the WAL. Windows retains the existing
directory-sync limitation documented in `container::sync_dir`.

The directory store discovers SSTables directly; its recovery watermark is
`checkpoint.meta`, not a separate SSTable manifest. A live `.alopex.d` sidecar is
authoritative until explicit convergence/close publishes the container. Automatic
checkpoints preserve this rule and do not copy the entire container on each commit.
Container publication and its manifest remain the responsibility of convergence.

The timestamp of the incoming batch is allocated after checkpoint completion,
so the recovery watermark cannot hide that batch. Checkpoint or metadata I/O
errors leave the previous WAL available and the incoming batch unappended.

## Configuration and observation

`should_checkpoint()` is now used before append. Size and interval thresholds
control proactive checkpoints. Capacity exhaustion overrides those thresholds,
including `auto_checkpoint = false`; that switch disables proactive work only.

`LsmKV::wal_checkpoint_stats()` exposes current WAL bytes, successful checkpoint
count, reclaimed bytes, and checkpoint duration in microseconds. Errors preserve
the underlying I/O cause; capacity errors distinguish an oversized entry from
space still unavailable after one checkpoint. Existing metrics structs and
on-disk formats are unchanged.

## Verification and manual measurement

Ordinary CI uses bounded tests: a 1–2-entry ring, at most six one-byte values,
checkpoint persistence failures, a retained snapshot, cursor release, and a
three-row SQL table with one index restored from a `.alopex` container.

For a manual before/after comparison, run:

```sh
cargo run -p alopex-core --example wal_capacity_measure
```

This example is not a CI benchmark. It uses three transactions of two rows each,
records each commit's elapsed time and WAL occupancy, and separately measures an
explicit checkpoint under the same configuration. Retain the source revision,
command, and output for both runs. These tiny measurements establish capacity
behavior and separate checkpoint cost; they are not throughput or latency claims.
