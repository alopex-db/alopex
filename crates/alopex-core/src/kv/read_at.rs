//! Fenced snapshot-read contract for distributed reads.
//!
//! This module intentionally distinguishes a globally issued data epoch from
//! the local timestamp used by an ordinary storage transaction.  A backend is
//! never eligible for strong or stale remote reads merely because it can open
//! a local read-only transaction.

use serde::{Deserialize, Serialize};

/// A cluster-authorized, fenced snapshot cut requested from a storage backend.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReadAtPoint {
    /// Monotonic user-data epoch issued by the cluster authority.
    pub data_epoch: u64,
    /// Committed metadata version that must be visible at this read.
    pub metadata_version: u64,
    /// Schema manifest identity required by the query plan.
    pub schema_epoch: u64,
    /// Index definition identity required by the query plan.
    pub index_epoch: u64,
}

impl ReadAtPoint {
    /// Creates a fenced read point from cluster-issued identities.
    pub const fn new(
        data_epoch: u64,
        metadata_version: u64,
        schema_epoch: u64,
        index_epoch: u64,
    ) -> Self {
        Self {
            data_epoch,
            metadata_version,
            schema_epoch,
            index_epoch,
        }
    }
}

/// Backend evidence required before remote strong/stale reads may be enabled.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReadAtCapability {
    /// The backend retains every epoch in this inclusive readable interval and
    /// can bind a read-only session to the complete supplied fence.
    Available {
        /// Oldest retained data epoch.
        readable_from_epoch: u64,
        /// Newest data epoch proven readable.
        readable_through_epoch: u64,
    },
    /// The backend must not be selected for remote strong/stale reads.
    Unavailable {
        /// Stable operator-visible reason.
        reason: String,
    },
}

impl ReadAtCapability {
    /// Creates an explicit unavailable capability result.
    pub fn unavailable(reason: impl Into<String>) -> Self {
        Self::Unavailable {
            reason: reason.into(),
        }
    }

    /// Checks whether `point` is inside the proven retained interval.
    pub fn validate(&self, point: &ReadAtPoint) -> ReadAtResult<()> {
        match self {
            Self::Available {
                readable_from_epoch,
                ..
            } if point.data_epoch < *readable_from_epoch => Err(ReadAtError::Expired {
                requested_epoch: point.data_epoch,
                readable_from_epoch: *readable_from_epoch,
            }),
            Self::Available {
                readable_through_epoch,
                ..
            } if point.data_epoch > *readable_through_epoch => Err(ReadAtError::NotYetReadable {
                requested_epoch: point.data_epoch,
                readable_through_epoch: *readable_through_epoch,
            }),
            Self::Available { .. } => Ok(()),
            Self::Unavailable { reason } => Err(ReadAtError::Unavailable {
                requested_epoch: point.data_epoch,
                reason: reason.clone(),
            }),
        }
    }

    /// Produces an unavailable result while retaining the requested epoch.
    pub fn unavailable_error(&self, point: &ReadAtPoint, fallback_reason: &str) -> ReadAtError {
        match self {
            Self::Unavailable { reason } => ReadAtError::Unavailable {
                requested_epoch: point.data_epoch,
                reason: reason.clone(),
            },
            Self::Available { .. } => ReadAtError::Unavailable {
                requested_epoch: point.data_epoch,
                reason: fallback_reason.to_string(),
            },
        }
    }
}

/// Classified failure to open a fenced storage snapshot.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ReadAtError {
    /// The requested epoch was compacted before a session could open.
    #[error(
        "read point expired: requested={requested_epoch}, readable_from={readable_from_epoch}"
    )]
    Expired {
        /// Requested cluster data epoch.
        requested_epoch: u64,
        /// Oldest retained data epoch.
        readable_from_epoch: u64,
    },
    /// The requested epoch has not been applied by the backend.
    #[error("read point unavailable: requested={requested_epoch}, readable_through={readable_through_epoch}")]
    NotYetReadable {
        /// Requested cluster data epoch.
        requested_epoch: u64,
        /// Newest locally readable data epoch.
        readable_through_epoch: u64,
    },
    /// The backend has no admissible read-at implementation.
    #[error("read point unavailable at epoch {requested_epoch}: {reason}")]
    Unavailable {
        /// Requested cluster data epoch.
        requested_epoch: u64,
        /// Stable reason for ineligibility.
        reason: String,
    },
}

/// Result returned by fenced snapshot-read capability operations.
pub type ReadAtResult<T> = std::result::Result<T, ReadAtError>;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kv::{AnyKV, KVStore};

    fn point(epoch: u64) -> ReadAtPoint {
        ReadAtPoint::new(epoch, 4, 5, 6)
    }

    #[test]
    fn retained_interval_classifies_expired_and_unapplied_epochs() {
        let capability = ReadAtCapability::Available {
            readable_from_epoch: 10,
            readable_through_epoch: 20,
        };
        assert!(matches!(
            capability.validate(&point(9)),
            Err(ReadAtError::Expired { .. })
        ));
        assert!(capability.validate(&point(10)).is_ok());
        assert!(capability.validate(&point(20)).is_ok());
        assert!(matches!(
            capability.validate(&point(21)),
            Err(ReadAtError::NotYetReadable { .. })
        ));
    }

    #[test]
    fn any_kv_does_not_treat_a_local_snapshot_as_a_cluster_read_point() {
        let store = AnyKV::Memory(crate::kv::memory::MemoryKV::new());
        assert!(matches!(
            store.read_at_capability(),
            ReadAtCapability::Unavailable { .. }
        ));
        assert!(matches!(
            store.begin_read_at(&point(1)),
            Err(ReadAtError::Unavailable { .. })
        ));
    }
}
