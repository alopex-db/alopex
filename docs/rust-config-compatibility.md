# Rust configuration source compatibility

This policy applies to v0.8.17 and tracks
[#696](https://github.com/alopex-db/alopex/issues/696). Runtime/database compatibility
and Rust source compatibility are separate contracts.

## Supported clients

| Client source | 0.8.15 | 0.8.16 | 0.8.17 |
| --- | --- | --- | --- |
| Existing `EmbeddedConfig` constructors and `DatabaseOptions` methods | Compiles | Compiles | Compiles |
| Four-field `EmbeddedConfig` literal from 0.8.15 | Compiles | E0063 | E0063; migration required |
| Six-field `EmbeddedConfig` literal from 0.8.16 | Not available | Compiles | Compiles |
| New `DatabaseOptions` segment/cache setters and `Database::open_with_options` | Not available | Not available | Compiles |

The six public fields of `EmbeddedConfig` published in 0.8.16 are frozen.
Removing its two HNSW fields would break clients of that already-published
version. A single Rust struct cannot accept both complete four-field and
six-field literals unchanged. Adding `Default`, constructors, or
`#[non_exhaustive]` would not restore the old literal.

Consequently, v0.8.17 retains the known four-field incompatibility and supplies
an extensible configuration path. This is a declared migration requirement,
not a claim that unchanged 0.8.15 literals have been repaired.

## Migrate a 0.8.15 literal

For code that must compile against all three versions, replace the complete
literal with an existing constructor:

```rust
use alopex_embedded::{Database, EmbeddedConfig};

let config = EmbeddedConfig::in_memory();
// Or EmbeddedConfig::in_memory_with_limit(bytes).
// Or EmbeddedConfig::disk(path).with_segment_config(segment_config).
let db = Database::open_with_config(config)?;
```

Keep custom path, memory limit, and segment settings when migrating. The
constructor chooses defaults for fields omitted from the old source. No data
export/import is required by this source-code change.

## Use the extensible options API in new code

`DatabaseOptions` keeps its fields private. Future settings must be introduced
through methods on this type, rather than mandatory fields on `EmbeddedConfig`.
Existing `DatabaseOptions` constructors and methods retain their signatures.

```rust
use alopex_embedded::{Database, DatabaseOptions};
use std::path::Path;

let options = DatabaseOptions::new()
    .with_segment_config(Default::default())
    .with_hnsw_cache_limits(1024 * 1024, Some(2));
let db = Database::open_with_options(Path::new("data.alopex"), options)?;
```

For memory storage, use `DatabaseOptions::in_memory()` and
`Database::open_in_memory_with_options(options)`. `with_memory_limit(bytes)`
continues to enable memory mode. Passing memory options to `open_with_options`
ignores its path and does not create disk storage.

Cache defaults remain 256 MiB and 128 entries. Zero bytes disables retention;
an omitted entry count uses 128; `Some(0)` is rejected before storage creation.
Both options entry points load the SQL catalog and preserve the existing
disk close/reopen behavior. `Database::open_with_config` remains available
with its existing behavior and signature.

## Verification

`scripts/compatibility/source_config.py` checks the same constructor client
against the frozen 0.8.15, 0.8.16, and candidate dependency graphs, the unchanged
six-field client against 0.8.16/candidate, and the new options client against
the candidate. These are compile-only clients: no database, load, or performance
measurement is involved.

The unchanged four-field witness is checked separately against all three
graphs. The report labels its 0.8.16/candidate outcomes `known_incompatible`,
including compiler exit status 101. Only E0063 for the two known fields at
that client source is recognized. A dependency failure, different compiler
error, crash, timeout, missing result, or unexpected success fails the declared
contract check and requires investigation. These failures are never counted
as successful runtime compatibility cases.
