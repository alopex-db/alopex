//! Fail-closed capability boundary for the Durable-profile changefeed.
//!
//! The adapter receives evidence from the Chirps integration and converts it
//! to the coordinator's one explicit readiness value.  It never substitutes a
//! local journal or an in-memory queue for an unavailable Durable service.

use crate::{FailureClass, chirps_cluster_capability};

use super::FeedPreflight;

/// Version evidence for the Chirps Durable capability.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct DurableCapabilityVersion {
    /// Major component of the capability version.
    pub major: u16,
    /// Minor component of the capability version.
    pub minor: u16,
    /// Patch component of the capability version.
    pub patch: u16,
}

impl DurableCapabilityVersion {
    /// Creates version evidence from the three semantic-version components.
    #[must_use]
    pub const fn new(major: u16, minor: u16, patch: u16) -> Self {
        Self {
            major,
            minor,
            patch,
        }
    }

    fn supports_changefeed(self) -> bool {
        self.major > 0 || (self.major == 0 && self.minor >= 7)
    }
}

/// Authorization evidence for Durable feed operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DurableAuthorization {
    /// The caller has passed the Durable adapter's authorization check.
    Authorized,
    /// The caller is not authorized to open or consume the Durable feed.
    Unauthorized,
}

/// Evidence that must be independently established before a feed is opened.
///
/// All fields are intentionally explicit: an omitted or false capability is a
/// rejection, never a request to use a local fallback.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DurableProfileEvidence {
    /// Advertised Chirps Durable capability version, if the foundation
    /// supplied one.
    pub capability_version: Option<DurableCapabilityVersion>,
    /// Whether the configured Durable endpoint is currently reachable.
    pub service_available: bool,
    /// Whether frames are dispatched through one authenticated dispatcher.
    pub authenticated_dispatcher: bool,
    /// Whether events and checkpoints use durable storage.
    pub durable_storage: bool,
    /// Whether the adapter can route and fence the requested range.
    pub range_routing: bool,
    /// Whether retention position/deadline can be enforced by Durable.
    pub retention: bool,
    /// Authorization evidence for the requested feed operation.
    pub authorization: DurableAuthorization,
}

impl DurableProfileEvidence {
    /// Returns deliberately incomplete evidence for a process with no
    /// configured Durable integration.
    #[must_use]
    pub const fn unavailable() -> Self {
        Self {
            capability_version: None,
            service_available: false,
            authenticated_dispatcher: false,
            durable_storage: false,
            range_routing: false,
            retention: false,
            authorization: DurableAuthorization::Authorized,
        }
    }

    /// Returns complete evidence for deterministic adapter tests and for a
    /// production integration after it has independently proved every field.
    #[must_use]
    pub const fn complete(version: DurableCapabilityVersion) -> Self {
        Self {
            capability_version: Some(version),
            service_available: true,
            authenticated_dispatcher: true,
            durable_storage: true,
            range_routing: true,
            retention: true,
            authorization: DurableAuthorization::Authorized,
        }
    }
}

/// Converts Chirps Durable evidence into the changefeed coordinator preflight.
///
/// This type owns only the evidence-to-outcome boundary.  Transport, consumer
/// group, retry, and storage-key choices stay inside the Chirps integration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DurableProfileAdapter {
    evidence: DurableProfileEvidence,
}

impl DurableProfileAdapter {
    /// Creates an adapter from explicit, independently verified evidence.
    #[must_use]
    pub const fn new(evidence: DurableProfileEvidence) -> Self {
        Self { evidence }
    }

    /// Returns evidence for the currently compiled foundation.
    ///
    /// The vendored Chirps `0.5.1` source has no Durable implementation and
    /// the current cluster capability reports no authenticated dispatcher, so
    /// this result is intentionally rejected.  A future dependency upgrade
    /// must supply fresh runtime evidence instead of changing this adapter's
    /// fail-closed behavior.
    #[must_use]
    pub fn compiled() -> Self {
        let cluster_capability = chirps_cluster_capability();
        Self::new(DurableProfileEvidence {
            capability_version: compiled_chirps_version(),
            service_available: false,
            authenticated_dispatcher: cluster_capability.available,
            durable_storage: false,
            range_routing: false,
            retention: false,
            authorization: DurableAuthorization::Authorized,
        })
    }

    /// Returns the only feed readiness state accepted by `FeedCoordinator`.
    ///
    /// In particular, an unavailable endpoint is retryable while an absent or
    /// incompatible Durable prerequisite is terminal.  Authorization is
    /// checked before capability details are exposed to an unauthorized caller.
    #[must_use]
    pub fn preflight(&self) -> FeedPreflight {
        if self.evidence.authorization == DurableAuthorization::Unauthorized {
            return rejected(FailureClass::Unauthorized, "changefeed_unauthorized", false);
        }
        let Some(version) = self.evidence.capability_version else {
            return rejected(
                FailureClass::PrerequisiteMissing,
                "durable_capability_missing",
                false,
            );
        };
        if !version.supports_changefeed() {
            return rejected(
                FailureClass::PrerequisiteMissing,
                "durable_version_incompatible",
                false,
            );
        }
        if !self.evidence.authenticated_dispatcher {
            return rejected(
                FailureClass::PrerequisiteMissing,
                "durable_authenticated_dispatcher_missing",
                false,
            );
        }
        if !self.evidence.durable_storage {
            return rejected(
                FailureClass::PrerequisiteMissing,
                "durable_storage_missing",
                false,
            );
        }
        if !self.evidence.range_routing {
            return rejected(
                FailureClass::PrerequisiteMissing,
                "durable_range_routing_missing",
                false,
            );
        }
        if !self.evidence.retention {
            return rejected(
                FailureClass::PrerequisiteMissing,
                "durable_retention_missing",
                false,
            );
        }
        if !self.evidence.service_available {
            return rejected(FailureClass::NodeUnavailable, "durable_unavailable", true);
        }
        FeedPreflight::ready()
    }

    /// Returns the evidence retained by this adapter for diagnostics and
    /// verification; callers still must use [`Self::preflight`] before open.
    #[must_use]
    pub const fn evidence(&self) -> &DurableProfileEvidence {
        &self.evidence
    }
}

fn rejected(
    failure_class: FailureClass,
    reason_code: &'static str,
    retryable: bool,
) -> FeedPreflight {
    FeedPreflight::rejected(failure_class, reason_code, retryable)
}

#[cfg(feature = "chirps")]
const fn compiled_chirps_version() -> Option<DurableCapabilityVersion> {
    Some(DurableCapabilityVersion::new(0, 5, 1))
}

#[cfg(not(feature = "chirps"))]
const fn compiled_chirps_version() -> Option<DurableCapabilityVersion> {
    None
}
