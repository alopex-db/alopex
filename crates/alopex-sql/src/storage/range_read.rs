//! Fenced, range-bounded table scans for distributed-read workers.
//!
//! Local SQL retains its historical table-prefix scan entry points.  A remote
//! worker must instead carry a [`StorageRangeConstraint`] and a transaction
//! opened at the identical cluster-issued [`ReadAtPoint`].

use std::fmt;

use alopex_core::{EncodedRowKeyRange, ReadAtPoint, RowKeyRange};

use crate::catalog::TableMetadata;

use super::error::Result;
use super::{KeyEncoder, SqlValue, StorageError, TableScanIterator};

/// The catalog and storage identities pinned for one range-worker scan.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RangeReadSnapshot {
    read_at: ReadAtPoint,
    schema_manifest_id: String,
}

impl RangeReadSnapshot {
    /// Creates the immutable catalog/schema/index fence for a range read.
    pub fn new(
        read_at: ReadAtPoint,
        schema_manifest_id: impl Into<String>,
    ) -> std::result::Result<Self, StorageRangeConstraintError> {
        let schema_manifest_id = schema_manifest_id.into();
        if schema_manifest_id.trim().is_empty() {
            return Err(StorageRangeConstraintError::MissingSchemaManifest);
        }
        Ok(Self {
            read_at,
            schema_manifest_id,
        })
    }

    /// Returns the cluster-issued read point backing this snapshot.
    pub const fn read_at(&self) -> ReadAtPoint {
        self.read_at
    }

    /// Returns the immutable schema-manifest identity.
    pub fn schema_manifest_id(&self) -> &str {
        &self.schema_manifest_id
    }

    /// Returns the pinned committed catalog version.
    pub const fn catalog_version(&self) -> u64 {
        self.read_at.metadata_version
    }

    /// Returns the pinned schema epoch.
    pub const fn schema_epoch(&self) -> u64 {
        self.read_at.schema_epoch
    }

    /// Returns the pinned index epoch.
    pub const fn index_epoch(&self) -> u64 {
        self.read_at.index_epoch
    }
}

/// Mandatory storage bounds and immutable fence for one physical range.
///
/// The primary-key interval is always represented as concrete half-open KV
/// bounds.  Even a full logical table uses the canonical table-prefix end,
/// never `scan_prefix`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StorageRangeConstraint {
    range_id: String,
    generation: u64,
    row_key_range: RowKeyRange,
    encoded_bounds: EncodedRowKeyRange,
    snapshot: RangeReadSnapshot,
}

impl StorageRangeConstraint {
    /// Constructs a validated, range-bound worker constraint.
    pub fn new(
        range_id: impl Into<String>,
        generation: u64,
        row_key_range: RowKeyRange,
        snapshot: RangeReadSnapshot,
    ) -> std::result::Result<Self, StorageRangeConstraintError> {
        let range_id = range_id.into();
        if range_id.trim().is_empty() {
            return Err(StorageRangeConstraintError::MissingRangeIdentity);
        }
        Ok(Self {
            range_id,
            generation,
            encoded_bounds: row_key_range.encoded_bounds(),
            row_key_range,
            snapshot,
        })
    }

    /// Returns the physical range identity selected by the route planner.
    pub fn range_id(&self) -> &str {
        &self.range_id
    }

    /// Returns the immutable range-generation fence.
    pub const fn generation(&self) -> u64 {
        self.generation
    }

    /// Returns the sole table whose rows may be emitted.
    pub const fn table_id(&self) -> u32 {
        self.row_key_range.table_id()
    }

    /// Returns the canonical primary-key interval.
    pub const fn row_key_range(&self) -> RowKeyRange {
        self.row_key_range
    }

    /// Returns concrete KV scan bounds `[lower_inclusive, upper_exclusive)`.
    pub fn encoded_bounds(&self) -> (&[u8], &[u8]) {
        (
            &self.encoded_bounds.lower_inclusive,
            &self.encoded_bounds.upper_exclusive,
        )
    }

    /// Returns the immutable catalog/schema/index snapshot fence.
    pub fn snapshot(&self) -> &RangeReadSnapshot {
        &self.snapshot
    }

    /// Validates that a supplied SQL table matches the planned physical range.
    pub fn validate_table(&self, table_meta: &TableMetadata) -> Result<()> {
        if table_meta.table_id != self.table_id() {
            return Err(StorageError::CorruptedData {
                reason: format!(
                    "range {} is fenced for table {}, not table {}",
                    self.range_id,
                    self.table_id(),
                    table_meta.table_id
                ),
            });
        }
        Ok(())
    }

    /// Validates that the storage transaction is pinned at this exact fence.
    pub fn validate_read_at(&self, actual: Option<ReadAtPoint>) -> Result<()> {
        match actual {
            Some(actual) if actual == self.snapshot.read_at() => Ok(()),
            Some(actual) => Err(StorageError::CorruptedData {
                reason: format!(
                    "range {} read-at fence mismatch: expected {:?}, got {:?}",
                    self.range_id,
                    self.snapshot.read_at(),
                    actual
                ),
            }),
            None => Err(StorageError::CorruptedData {
                reason: format!(
                    "range {} requires a transaction opened at its read-at fence",
                    self.range_id
                ),
            }),
        }
    }

    /// Rechecks a canonical primary key after a secondary-index lookup.
    pub fn contains_primary_key(&self, encoded_primary_key: &[u8]) -> Result<bool> {
        KeyEncoder::primary_key_is_in_range(encoded_primary_key, self.row_key_range)
    }

    /// Rechecks every secondary-index candidate against the primary-key range.
    ///
    /// Index sort order is not a substitute for physical row bounds.  A
    /// candidate outside the range is omitted before its row can be fetched or
    /// emitted.
    pub fn recheck_secondary_index_hits<I>(&self, row_ids: I) -> Result<Vec<u64>>
    where
        I: IntoIterator<Item = u64>,
    {
        row_ids
            .into_iter()
            .filter_map(|row_id| {
                match self.contains_primary_key(&KeyEncoder::row_key(self.table_id(), row_id)) {
                    Ok(true) => Some(Ok(row_id)),
                    Ok(false) => None,
                    Err(error) => Some(Err(error)),
                }
            })
            .collect()
    }
}

/// A constrained table iterator which refuses a row that escapes its range.
pub struct RangeBoundedScanIterator<'a> {
    inner: TableScanIterator<'a>,
    constraint: StorageRangeConstraint,
}

impl<'a> RangeBoundedScanIterator<'a> {
    /// Wraps a storage scan with a mandatory primary-key recheck.
    pub fn new(inner: TableScanIterator<'a>, constraint: StorageRangeConstraint) -> Self {
        Self { inner, constraint }
    }
}

impl Iterator for RangeBoundedScanIterator<'_> {
    type Item = Result<(u64, Vec<SqlValue>)>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(|entry| {
            let (row_id, values) = entry?;
            let key = KeyEncoder::row_key(self.constraint.table_id(), row_id);
            if self.constraint.contains_primary_key(&key)? {
                Ok((row_id, values))
            } else {
                Err(StorageError::CorruptedData {
                    reason: format!(
                        "storage scan returned row {} outside fenced range {}",
                        row_id,
                        self.constraint.range_id()
                    ),
                })
            }
        })
    }
}

/// Validation failure while constructing a mandatory worker constraint.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StorageRangeConstraintError {
    /// A range worker cannot operate without a stable route identity.
    MissingRangeIdentity,
    /// A range worker cannot operate without the pinned schema manifest.
    MissingSchemaManifest,
}

impl fmt::Display for StorageRangeConstraintError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingRangeIdentity => f.write_str("range constraint has no range identity"),
            Self::MissingSchemaManifest => {
                f.write_str("range constraint has no schema-manifest identity")
            }
        }
    }
}

impl std::error::Error for StorageRangeConstraintError {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::{RowCodec, TableScanIterator};
    use alopex_core::CanonicalRowKey;

    fn constraint() -> StorageRangeConstraint {
        let snapshot =
            RangeReadSnapshot::new(ReadAtPoint::new(7, 11, 13, 17), "schema-13").unwrap();
        StorageRangeConstraint::new(
            "range-a",
            3,
            RowKeyRange::new(7, Some(10), Some(20)).unwrap(),
            snapshot,
        )
        .unwrap()
    }

    #[test]
    fn constraint_uses_concrete_canonical_bounds_not_a_prefix_scan() {
        let constraint = constraint();
        let (lower, upper) = constraint.encoded_bounds();

        assert_eq!(lower, KeyEncoder::row_key(7, 10));
        assert_eq!(upper, KeyEncoder::row_key(7, 20));
        assert_ne!(lower, KeyEncoder::table_prefix(7));
    }

    #[test]
    fn secondary_index_candidates_are_rechecked_against_primary_bounds() {
        let constraint = constraint();

        assert_eq!(
            constraint
                .recheck_secondary_index_hits([9, 10, 19, 20])
                .unwrap(),
            vec![10, 19]
        );
    }

    #[test]
    fn scan_wrapper_never_emits_an_out_of_range_primary_row() {
        let entries = vec![(
            KeyEncoder::row_key(7, 20),
            RowCodec::encode(&[SqlValue::Integer(20)]),
        )];
        let table_scan = TableScanIterator::new(Box::new(entries.into_iter()), 7);
        let mut scan = RangeBoundedScanIterator::new(table_scan, constraint());

        assert!(matches!(
            scan.next(),
            Some(Err(StorageError::CorruptedData { .. }))
        ));
    }

    #[test]
    fn constraint_requires_route_and_schema_identities() {
        let point = ReadAtPoint::new(1, 2, 3, 4);
        assert_eq!(
            RangeReadSnapshot::new(point, ""),
            Err(StorageRangeConstraintError::MissingSchemaManifest)
        );
        let snapshot = RangeReadSnapshot::new(point, "schema-3").unwrap();
        assert_eq!(
            StorageRangeConstraint::new("", 1, RowKeyRange::full_table(7), snapshot),
            Err(StorageRangeConstraintError::MissingRangeIdentity)
        );
    }

    #[test]
    fn constraint_rejects_a_different_read_at_fence() {
        let constraint = constraint();
        assert!(
            constraint
                .validate_read_at(Some(ReadAtPoint::new(7, 11, 13, 18)))
                .is_err()
        );
        assert!(constraint.validate_read_at(None).is_err());
        assert!(
            constraint
                .validate_read_at(Some(ReadAtPoint::new(7, 11, 13, 17)))
                .is_ok()
        );
    }

    #[test]
    fn primary_key_recheck_rejects_malformed_keys() {
        let constraint = constraint();
        assert!(matches!(
            constraint.contains_primary_key(&[0x01]),
            Err(StorageError::InvalidKeyFormat)
        ));
        assert!(
            !constraint
                .contains_primary_key(&CanonicalRowKey::new(8, 19).encode())
                .unwrap()
        );
    }
}
