//! Canonical primary row-key encoding and half-open range contracts.
//!
//! This module is storage-neutral: SQL and cluster control share these bytes,
//! while secondary-index key order remains a separate concern.

use std::{error::Error, fmt};

const ROW_KEY_TAG: u8 = 0x01;
const TABLE_PREFIX_LEN: usize = 1 + std::mem::size_of::<u32>();
const ROW_KEY_LEN: usize = TABLE_PREFIX_LEN + std::mem::size_of::<u64>();

/// Error returned for malformed canonical primary row keys or intervals.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RowKeyRangeError {
    /// The byte sequence is not the canonical primary row-key format.
    InvalidKeyEncoding,
    /// Lower and upper bounds do not form a non-empty half-open interval.
    InvalidBounds,
    /// A supplied key bound belongs to another table.
    CrossTableBounds,
}

impl fmt::Display for RowKeyRangeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidKeyEncoding => f.write_str("invalid canonical primary row-key encoding"),
            Self::InvalidBounds => {
                f.write_str("row-key range bounds are not a non-empty half-open interval")
            }
            Self::CrossTableBounds => f.write_str("row-key range bounds refer to different tables"),
        }
    }
}

impl Error for RowKeyRangeError {}

/// Canonical lexicographically ordered primary row key.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CanonicalRowKey {
    table_id: u32,
    row_id: u64,
}

impl CanonicalRowKey {
    /// Builds a canonical primary key from a table and row identity.
    pub const fn new(table_id: u32, row_id: u64) -> Self {
        Self { table_id, row_id }
    }

    /// Decodes exactly the canonical primary-row-key format.
    pub fn decode(encoded: &[u8]) -> Result<Self, RowKeyRangeError> {
        if encoded.len() != ROW_KEY_LEN || encoded[0] != ROW_KEY_TAG {
            return Err(RowKeyRangeError::InvalidKeyEncoding);
        }
        let table_id = u32::from_be_bytes(
            encoded[1..TABLE_PREFIX_LEN]
                .try_into()
                .map_err(|_| RowKeyRangeError::InvalidKeyEncoding)?,
        );
        let row_id = u64::from_be_bytes(
            encoded[TABLE_PREFIX_LEN..]
                .try_into()
                .map_err(|_| RowKeyRangeError::InvalidKeyEncoding)?,
        );
        Ok(Self::new(table_id, row_id))
    }

    /// Serializes to the canonical bytes shared by SQL and cluster storage.
    pub fn encode(self) -> Vec<u8> {
        let mut encoded = Vec::with_capacity(ROW_KEY_LEN);
        encoded.extend_from_slice(&Self::table_prefix(self.table_id));
        encoded.extend_from_slice(&self.row_id.to_be_bytes());
        encoded
    }

    /// Returns the stable table component of the key.
    pub const fn table_id(self) -> u32 {
        self.table_id
    }

    /// Returns the stable row component of the key.
    pub const fn row_id(self) -> u64 {
        self.row_id
    }

    /// Returns the lexicographic prefix of every primary row key for a table.
    pub fn table_prefix(table_id: u32) -> Vec<u8> {
        let mut prefix = Vec::with_capacity(TABLE_PREFIX_LEN);
        prefix.push(ROW_KEY_TAG);
        prefix.extend_from_slice(&table_id.to_be_bytes());
        prefix
    }

    /// Returns the first byte sequence lexicographically after every row key
    /// with this table prefix. It remains valid even for `u32::MAX`.
    pub fn table_prefix_end(table_id: u32) -> Vec<u8> {
        prefix_successor(&Self::table_prefix(table_id))
            .expect("a primary row-key prefix beginning with 0x01 always has a successor")
    }
}

/// Storage scan bounds encoded from a [`RowKeyRange`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EncodedRowKeyRange {
    /// Inclusive storage lower bound.
    pub lower_inclusive: Vec<u8>,
    /// Exclusive storage upper bound.
    pub upper_exclusive: Vec<u8>,
}

/// A canonical primary-row-key half-open interval for one table.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RowKeyRange {
    table_id: u32,
    lower_inclusive: Option<u64>,
    upper_exclusive: Option<u64>,
}

impl RowKeyRange {
    /// Creates a whole-table range.
    pub const fn full_table(table_id: u32) -> Self {
        Self {
            table_id,
            lower_inclusive: None,
            upper_exclusive: None,
        }
    }

    /// Creates a validated half-open row-id interval within one table.
    pub fn new(
        table_id: u32,
        lower_inclusive: Option<u64>,
        upper_exclusive: Option<u64>,
    ) -> Result<Self, RowKeyRangeError> {
        if let (Some(lower), Some(upper)) = (lower_inclusive, upper_exclusive) {
            if lower >= upper {
                return Err(RowKeyRangeError::InvalidBounds);
            }
        }
        Ok(Self {
            table_id,
            lower_inclusive,
            upper_exclusive,
        })
    }

    /// Creates a range from canonical key bounds, rejecting cross-table input.
    pub fn from_keys(
        lower_inclusive: Option<CanonicalRowKey>,
        upper_exclusive: Option<CanonicalRowKey>,
        table_id: u32,
    ) -> Result<Self, RowKeyRangeError> {
        for key in [lower_inclusive, upper_exclusive].into_iter().flatten() {
            if key.table_id() != table_id {
                return Err(RowKeyRangeError::CrossTableBounds);
            }
        }
        Self::new(
            table_id,
            lower_inclusive.map(CanonicalRowKey::row_id),
            upper_exclusive.map(CanonicalRowKey::row_id),
        )
    }

    /// Returns the table covered by this primary-key range.
    pub const fn table_id(self) -> u32 {
        self.table_id
    }

    /// Returns the lower row-id bound, if it narrows the table minimum.
    pub const fn lower_inclusive(self) -> Option<u64> {
        self.lower_inclusive
    }

    /// Returns the upper row-id bound, if it narrows the table end.
    pub const fn upper_exclusive(self) -> Option<u64> {
        self.upper_exclusive
    }

    /// Converts the logical interval into storage scan bytes.
    pub fn encoded_bounds(self) -> EncodedRowKeyRange {
        EncodedRowKeyRange {
            lower_inclusive: CanonicalRowKey::new(self.table_id, self.lower_inclusive.unwrap_or(0))
                .encode(),
            upper_exclusive: self.upper_exclusive.map_or_else(
                || CanonicalRowKey::table_prefix_end(self.table_id),
                |row_id| CanonicalRowKey::new(self.table_id, row_id).encode(),
            ),
        }
    }

    /// Tests a decoded primary row key against the logical half-open interval.
    pub fn contains(self, key: CanonicalRowKey) -> bool {
        key.table_id() == self.table_id
            && self
                .lower_inclusive
                .is_none_or(|lower| key.row_id() >= lower)
            && self
                .upper_exclusive
                .is_none_or(|upper| key.row_id() < upper)
    }
}

fn prefix_successor(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut successor = prefix.to_vec();
    for index in (0..successor.len()).rev() {
        if successor[index] != u8::MAX {
            successor[index] += 1;
            successor.truncate(index + 1);
            return Some(successor);
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn canonical_key_roundtrip_and_lexicographic_order_are_stable() {
        let first = CanonicalRowKey::new(7, 1);
        let later = CanonicalRowKey::new(7, u64::MAX);

        assert_eq!(CanonicalRowKey::decode(&first.encode()).unwrap(), first);
        assert!(first.encode() < later.encode());
    }

    #[test]
    fn full_table_range_uses_prefix_successor_for_the_upper_bound() {
        let range = RowKeyRange::full_table(u32::MAX);
        let encoded = range.encoded_bounds();

        assert_eq!(
            encoded.lower_inclusive,
            CanonicalRowKey::new(u32::MAX, 0).encode()
        );
        assert_eq!(encoded.upper_exclusive, vec![0x02]);
        assert!(range.contains(CanonicalRowKey::new(u32::MAX, u64::MAX)));
    }

    #[test]
    fn half_open_bounds_include_lower_and_exclude_upper() {
        let range = RowKeyRange::new(3, Some(10), Some(20)).unwrap();

        assert!(!range.contains(CanonicalRowKey::new(3, 9)));
        assert!(range.contains(CanonicalRowKey::new(3, 10)));
        assert!(range.contains(CanonicalRowKey::new(3, 19)));
        assert!(!range.contains(CanonicalRowKey::new(3, 20)));
        assert!(!range.contains(CanonicalRowKey::new(4, 10)));
    }

    #[test]
    fn invalid_or_cross_table_bounds_are_rejected() {
        assert_eq!(
            CanonicalRowKey::decode(&[0x02, 0, 0, 0, 1]),
            Err(RowKeyRangeError::InvalidKeyEncoding)
        );
        assert_eq!(
            RowKeyRange::new(3, Some(20), Some(10)),
            Err(RowKeyRangeError::InvalidBounds)
        );
        assert_eq!(
            RowKeyRange::from_keys(Some(CanonicalRowKey::new(4, 1)), None, 3),
            Err(RowKeyRangeError::CrossTableBounds)
        );
    }
}
