def _sql_streaming(engine: str, rows: int, warmups: int, runs: int) -> dict[str, float]:
    returned_rows = (rows + 1) // 2
    if engine == "alopex":
        from alopex import Database

        connection = Database.new()
        _populate_sql(connection, engine, rows)

        def open_stream():
            return connection.execute_sql_stream(
                SQL_STREAM_QUERY,
                resource_limit_bytes=1024 * 1024 * 1024,
            )

        plan = lambda: connection.execute_sql(f"EXPLAIN (FORMAT JSON) {SQL_STREAM_QUERY}")

        def consume():
            started = time.perf_counter()
            with open_stream() as stream:
                next(stream)
                first_elapsed = time.perf_counter() - started
                count = 1 + sum(1 for _ in stream)
                return first_elapsed, time.perf_counter() - started, count, 1

        def validate_stream():
            with open_stream() as stream:
                return [tuple(row.values()) for row in stream]

    else:
        import datafusion
        import pyarrow as pa
        from datafusion import SessionContext

        if datafusion.__version__ != "50.0.0":
            raise RuntimeError(f"expected DataFusion 50.0.0, got {datafusion.__version__}")
        context = SessionContext()
        batches = [
            pa.record_batch(
                {
                    "id": range(start, min(start + 8192, rows)),
                    "value": [
                        float(index) for index in range(start, min(start + 8192, rows))
                    ],
                    "enabled": [
                        index % 2 == 0
                        for index in range(start, min(start + 8192, rows))
                    ],
                }
            )
            for start in range(0, rows, 8192)
        ]
        context.register_record_batches("bench", [batches])
        build = lambda: context.sql(SQL_STREAM_QUERY)
        plan = lambda: build().logical_plan()

        def consume():
            started = time.perf_counter()
            stream = iter(build().execute_stream())
            first = next(stream)
            first_elapsed = time.perf_counter() - started
            first_rows = first.to_pyarrow().num_rows
            count = first_rows + sum(
                batch.to_pyarrow().num_rows for batch in stream
            )
            return first_elapsed, time.perf_counter() - started, count, first_rows

        def validate_stream():
            return [tuple(row.values()) for batch in build().execute_stream()
                    for row in batch.to_pyarrow().to_pylist()]

    def validate():
        result = sorted(validate_stream())
        if result != [(index, float(index)) for index in range(0, rows, 2)]:
            raise RuntimeError(f"{engine} SQL streaming values differ from fixture")
        return _result_checksum(result)

    checksum = validate()
    plan_samples = _samples(plan, warmups, runs)
    for _ in range(warmups):
        consume()
    first_samples = []
    total_samples = []
    steady_samples = []
    for sample in range(runs):
        first, total, count, first_rows = consume()
        if count != returned_rows:
            raise RuntimeError(
                f"{engine} SQL streaming returned {count} rows, expected {returned_rows}"
            )
        first_samples.append(first)
        total_samples.append(total)
        steady_samples.append((count - first_rows) / max(total - first, 1e-9))
        _record_raw({"phase": "sql-streaming", "sample": sample, "first_seconds": first,
                     "total_seconds": total, "rows": count, "first_rows": first_rows})
    median_total = statistics.median(total_samples)
    if validate() != checksum:
        raise RuntimeError(f"{engine} SQL streaming values changed during measurement")
    if engine == "alopex":
        connection.close()
    return {
        "result_sha256": checksum,
        "plan_build_p50_ms": statistics.median(plan_samples) * 1000,
        "time_to_first_batch_p50_ms": statistics.median(first_samples) * 1000,
        "total_p50_ms": median_total * 1000,
        "steady_state_rows_per_second": statistics.median(steady_samples),
        "peak_rss_bytes": _rss_bytes(),
    }
