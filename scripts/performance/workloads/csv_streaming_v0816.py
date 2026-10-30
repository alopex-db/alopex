def _streaming(
    engine: str, fixture: Path, rows: int, warmups: int, runs: int
) -> dict[str, float]:
    parquet = fixture.suffix == ".parquet"
    if engine == "alopex":
        from alopex import LazyFrame

        scan = LazyFrame.scan_parquet if parquet else LazyFrame.scan_csv
        build = lambda: scan(str(fixture))
        materialize = lambda: build().collect().height

        def consume():
            started = time.perf_counter()
            with build().collect_batches(chunk_size=8192) as stream:
                first = next(stream)
                first_elapsed = time.perf_counter() - started
                count = first.height + sum(batch.height for batch in stream)
                return first_elapsed, time.perf_counter() - started, count, first.height

    else:
        import polars as pl

        if pl.__version__ != "1.43.2":
            raise RuntimeError(f"expected Polars 1.43.2, got {pl.__version__}")

        scan = pl.scan_parquet if parquet else pl.scan_csv
        build = lambda: scan(fixture)
        materialize = lambda: build().collect().height

        def consume():
            started = time.perf_counter()
            batches = iter(build().collect_batches(chunk_size=8192))
            first = next(batches)
            first_elapsed = time.perf_counter() - started
            count = first.height + sum(batch.height for batch in batches)
            return first_elapsed, time.perf_counter() - started, count, first.height

    checksum = _result_checksum(build().collect().to_dict(as_series=False))
    plan = _samples(build, warmups, runs)
    collect_samples = _samples(materialize, warmups, runs)
    for _ in range(warmups):
        consume()
    first_samples = []
    total_samples = []
    steady_samples = []
    for sample in range(runs):
        first, total, count, first_rows = consume()
        if count != rows:
            raise RuntimeError(f"{engine} streaming returned {count} rows, expected {rows}")
        first_samples.append(first)
        total_samples.append(total)
        steady_samples.append((count - first_rows) / max(total - first, 1e-9))
        _record_raw({"phase": "streaming", "sample": sample, "first_seconds": first,
                     "total_seconds": total, "rows": count, "first_rows": first_rows})
    if _result_checksum(build().collect().to_dict(as_series=False)) != checksum:
        raise RuntimeError(f"{engine} streaming results changed during measurement")
    median_total = statistics.median(total_samples)
    return {
        "result_sha256": checksum,
        "plan_build_p50_ms": statistics.median(plan) * 1000,
        "collect_p50_ms": statistics.median(collect_samples) * 1000,
        "time_to_first_batch_p50_ms": statistics.median(first_samples) * 1000,
        "total_p50_ms": median_total * 1000,
        "steady_state_rows_per_second": statistics.median(steady_samples),
        "peak_rss_bytes": _rss_bytes(),
    }

def _write_csv(path: Path, rows: int) -> None:
    with path.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.writer(stream)
        writer.writerow(("id", "label"))
        writer.writerows(
            (
                index,
                "" if index % 10 == 0 else f"row-{index}",
            )
            for index in range(rows)
        )
