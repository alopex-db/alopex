//! Core data types used throughout the AlopexDB.

/// A key used for lookups in the key-value store.
pub type Key = Vec<u8>;

/// A value stored in the key-value store.
pub type Value = Vec<u8>;

/// A unique identifier for a transaction.
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
pub struct TxnId(pub u64);

/// The mode in which a transaction is operating.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TxnMode {
    /// The transaction can only read data.
    ReadOnly,
    /// The transaction can read and write data.
    ReadWrite,
}

/// The current state of a transaction.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TxnState {
    /// The transaction is currently active and can perform operations.
    Active,
    /// The transaction has been successfully committed.
    Committed,
    /// The transaction has been rolled back and is no longer valid.
    RolledBack,
}
