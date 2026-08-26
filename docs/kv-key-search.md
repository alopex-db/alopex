# KV key search

KV key search is an additive, bounded API over opaque key bytes. Existing
`get`, `scan_prefix`, and `scan_range` behavior is unchanged, and `/` has no
separator, hierarchy, normalization, or canonicalization meaning.

## Contract summary

- `glob` and `regex` are explicit modes, so exact lookup still addresses keys
  containing literal `*` or `?` bytes without ambiguity.
- Glob patterns are raw bytes. `*` matches zero or more bytes, `?` matches one
  byte, and `\` escapes the following byte.
- Regex patterns use Rust `regex::bytes` syntax and match raw key bytes. A
  pattern such as `(?-u:\xFF)` can match non-UTF-8 bytes.
- Results always include keys and values in ascending raw-byte key order.
- `cursor` is the exclusive last key from a prior page. HTTP transports keys,
  glob patterns, and cursors as JSON byte arrays; the CLI uses hexadecimal for
  binary glob patterns and cursors.
- The implementation automatically constrains glob scans by their leading
  literal bytes and constrains anchored regex scans by a simple literal prefix.

## Resource and error behavior

Each request must set `limit` and `scan_budget`. One page returns at most
10,000 entries, inspects at most 1,000,000 candidate keys, and accepts at most
4 KiB of pattern input. The raw key-and-value response budget defaults to 16
MiB and cannot exceed 100 MiB; HTTP further caps it from the configured
serialized-response limit. Zero or excessive bounds and invalid patterns are
`InvalidParameter` errors. Exhausting the candidate budget is
`SearchBudgetExceeded`; exceeding the response budget is
`SearchResponseTooLarge`. HTTP maps invalid input to 400 and either resource
limit to 413.

Core, owned, async, and embedded callers can pass `KeySearchCancellation` for
cooperative cancellation. HTTP and CLI calls remain bounded by `scan_budget`;
abandoning a page does not create persistent server-side search state.

When a response fills `limit`, `next_cursor` is the last returned raw key. The
caller may submit it as the next exclusive cursor. A final full page can yield
an empty follow-up page; this avoids an unbounded look-ahead scan.

## Public surfaces

Rust callers use `KeySearchRequest` with `KVTransaction::search_keys`,
`OwnedKVTransaction::search_keys`, or `AsyncKVTransaction::async_search_keys`.
Embedded callers use `Transaction::search_keys`.

HTTP accepts `POST /kv/search`:

```json
{
  "pattern": { "mode": "glob", "pattern": [97, 112, 112, 47, 42] },
  "cursor": null,
  "limit": 100,
  "scan_budget": 10000
}
```

CLI callers select a mode explicitly. `--pattern-hex` is available only for
binary glob patterns, `--cursor-hex` resumes a prior page, and `--max-bytes`
sets the raw response budget:

```console
alopex kv search --mode glob 'app/*/config'
alopex kv search --mode glob --pattern-hex ff2f2a --cursor-hex ff2f61
alopex kv search --mode regex 'tenant/[0-9]+/events'
```

The CLI `next_cursor_hex` column reports the resume cursor.
