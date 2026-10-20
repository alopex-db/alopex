# KV scan pagination in v0.8.17

Python `Transaction.scan_prefix_page` and `scan_range_page` return as many
entries as fit in both `limit` and `max_bytes`. `max_bytes` counts returned key
and value bytes, excluding hidden entries and cursor metadata. A page that
cannot fit the next entry returns `has_more=True` and `next_cursor`. Continue
with the same scan conditions; a partial page is not the end of the scan.

An individual entry larger than `max_bytes` fails when it is the first visible
entry in the page. `InvalidParameter` for `max_bytes` reports the key as byte values, the limit
and the requested key-plus-value size. Previously returned pages remain valid;
increase the page budget to resume at that entry. Hidden and deleted physical
candidates still count toward `scan_budget`. An empty page can therefore have a
continuation. `strict_budget=True` still makes insufficient physical work budget
an error; a byte-bound partial page is normal even in strict mode.

`scan_prefix` and `scan_range` collect the complete result (or the requested
`limit`) before returning an iterator. Their `max_total_bytes` defaults to
100 MiB; `None` disables that aggregate cap. The per-entry page limit still
applies. A total-budget failure returns no partial iterator. The native owned
collector retains one physical scan while applying page boundaries, avoiding
repeated range traversal. Native borrowed `Transaction.scan_prefix/scan_range`
remain low-level raw iterators with their existing unbounded contract; use the
owned bounded page API when resource bounds are needed.

Python `alopex-scan-v2:` cursors bind the scan kind, exact prefix or range bounds,
glob/regex pattern and `include_internal`. Passing one to different conditions
raises `alopex.InvalidCursor`, a `ValueError` subclass using the existing generic
error code `ALOPEX-PY999`. Page size, byte budget and work
budget may change. v1 cursors are rejected; restart those scans without a cursor.
Tokens are opaque continuation metadata, not authenticated capabilities or
persisted snapshot identifiers. Rust and HTTP key-search cursors retain their
existing raw-key contract. Glob/regex search response budgets retain their
existing fail-on-overflow behavior; the partial-byte-page change applies to
prefix/range scans.

Async complete scans collect through bounded pages on the handle's owner thread
and yield event-loop control between pages. Each individual native page call is
synchronous. Use `iter_prefix/iter_range` to consume pages incrementally without
collecting a full result, and do not modify the same transaction concurrently
with a scan. The async stubs match these arguments, including the optional total
budget; a focused signature test detects drift across runtime, async stub and
synchronous stub.

The regression script `scripts/repro/v0816-followups/f2_kv_large_values.py` uses
three two-byte values and a four-byte page budget. It exercises the original
size-boundary failure without large data or a performance workload.

Legacy HTTP key-search responses without `has_more` are accepted, inferring
continuation from `next_cursor`; an explicit flag takes precedence. CLI search
still uses raw cursors and its original search budget, so this Python prefix/range
change does not redefine the HTTP request or the CLI budget.
