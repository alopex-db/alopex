# Pinned Parquet wire declarations

`format.rs` is derived from Apache Arrow's `parquet` 57.3.2 generated
wire schema, under Apache-2.0:

- Source: https://github.com/apache/arrow-rs/blob/57.3.2/parquet/src/format.rs
- Original file SHA-256:
  `14fa0958710481a9616e902a32991baa3e998f69a5b0d07f4002a286fa59273f`
- Generator recorded by upstream: Thrift Compiler 0.21.0.
- The generic `TSerializable` interface in `mod.rs` comes from
  https://github.com/apache/arrow-rs/blob/57.3.2/parquet/src/thrift.rs.

The generated file is unchanged except for an Apache license header, the module
documentation link, and replacing `crate::thrift::TSerializable` with
`super::TSerializable`. Keep its generated formatting. When comparing with the
upstream source, reverse those transformations and compare the checksum above.
The `generated_wire_declarations_match_upstream_57_3_2` unit test and
`scripts/ci/test_parquet_wire_source_contract.py` enforce this comparison for
all 66 generated serializable types. The checksum pins the declarations; it is
not a runtime compatibility test for every type under the current Thrift crate.
`golden_tests.rs` adds compact-protocol decode/encode cases derived from the
upstream 57.3.2 declarations for every named struct, union variant and enum
discriminant. Regenerate it offline with
`python3 scripts/ci/generate_parquet_wire_goldens.py PATH_TO_UPSTREAM_FORMAT_RS`;
the generator rejects any upstream file without the pinned checksum. The
upstream Parquet crate and its older Thrift dependency are not test dependencies.

The old codec-free `parquet-wire` dependency still required vulnerable Thrift
0.17.0 (RUSTSEC-2026-0435). Newer Parquet releases removed these public wire
types. Keeping just the existing schema here permits Thrift 0.24.0, including
the binary length and non-strict message-name fixes, without retaining the old
Parquet implementation or adding a separately published crate. This private
module ships inside the existing alopex-sql crate archive.

All production uses remain behind the existing Parquet allocation preflight.
Compression, Arrow conversion and file metadata decoding still use Parquet
60.1. The malformed-input integration tests include this same schema to build
small fixtures; no public Alopex wire API is introduced.

Do not regenerate from a newer schema or change serialization implicitly while
updating Thrift. Preserve the wire declarations and run the bounded Parquet
allocation, planned-file and codec compatibility tests. Changes to the schema
require an explicit compatibility review and an updated source/hash record.
