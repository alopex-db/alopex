//! Authorization, tenant/range ownership, and redaction boundary for feeds.

use std::collections::BTreeSet;

use crate::{
    AuthenticatedSubject, FailureClass, IdempotencyResult, OperationState, RangeId, RequestId,
    RoutingOutcome,
};

use super::{ChangefeedModelError, ChangefeedOutcome, ChangefeedResult, FeedIdentity};

/// Minimum capability required for a changefeed action.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum ChangefeedScope {
    /// Read, subscribe, poll, stream, or resume a feed.
    Read,
    /// Acknowledge, cancel, or close a feed.
    Ack,
    /// Alter retention for a feed.
    RetentionAdmin,
}

/// One authorized lifecycle operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChangefeedAction {
    Create,
    Subscribe,
    Poll,
    Stream,
    Resume,
    Ack,
    Cancel,
    Close,
    ManageRetention,
}

impl ChangefeedAction {
    /// Returns the one scope required for this action.
    #[must_use]
    pub const fn required_scope(self) -> ChangefeedScope {
        match self {
            Self::Create | Self::Subscribe | Self::Poll | Self::Stream | Self::Resume => {
                ChangefeedScope::Read
            }
            Self::Ack | Self::Cancel | Self::Close => ChangefeedScope::Ack,
            Self::ManageRetention => ChangefeedScope::RetentionAdmin,
        }
    }
}

/// Server-established authorization facts for one authenticated subject.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChangefeedAuthorization {
    pub subject: AuthenticatedSubject,
    pub tenant: String,
    pub allowed_ranges: BTreeSet<RangeId>,
    pub allowed_scopes: BTreeSet<ChangefeedScope>,
}

impl ChangefeedAuthorization {
    /// Checks scope, tenant, and range ownership before an operation opens a
    /// feed or mutates checkpoint/retention state.
    #[must_use]
    pub fn authorize(&self, request: ChangefeedAccessRequest) -> ChangefeedAuthorizationDecision {
        let allowed = self.tenant == request.tenant
            && self.allowed_ranges.contains(&request.range_id)
            && self
                .allowed_scopes
                .contains(&request.action.required_scope());
        if allowed {
            ChangefeedAuthorizationDecision::Authorized
        } else {
            // Do not expose which policy fact failed: it could reveal another
            // tenant's feed, range placement, or retained payload.
            ChangefeedAuthorizationDecision::Denied
        }
    }
}

/// Caller target checked against server-established authorization facts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChangefeedAccessRequest {
    pub action: ChangefeedAction,
    pub tenant: String,
    pub range_id: RangeId,
}

/// Result of the fail-closed authorization boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChangefeedAuthorizationDecision {
    Authorized,
    Denied,
}

impl ChangefeedAuthorizationDecision {
    /// Returns whether an operation may touch feed, checkpoint, or retention
    /// state. Denied requests must return a redacted outcome instead.
    #[must_use]
    pub const fn permits(self) -> bool {
        matches!(self, Self::Authorized)
    }

    /// Creates the canonical redacted denial result. It never carries a
    /// `ChangeEventEnvelope` or `AckResult`, so retained payloads and
    /// checkpoint state cannot be observed through authorization failure.
    pub fn denied_outcome(
        self,
        mut feed: FeedIdentity,
        routing: RoutingOutcome,
        operation_id: impl Into<String>,
        request_id: impl Into<RequestId>,
    ) -> Result<ChangefeedOutcome, ChangefeedModelError> {
        debug_assert_eq!(self, Self::Denied);
        let operation_id = operation_id.into();
        let request_id = request_id.into();
        feed.status = OperationState::TerminalFailure;
        ChangefeedOutcome::new(
            feed,
            operation_id.clone(),
            request_id.clone(),
            OperationState::TerminalFailure,
            Some(FailureClass::Unauthorized),
            Some("changefeed_unauthorized".to_string()),
            routing,
            false,
            IdempotencyResult {
                operation_id,
                request_id,
                first_outcome: "authorization_denied".to_string(),
                state: OperationState::TerminalFailure,
                duplicate_count: 0,
            },
            ChangefeedResult::Feed,
        )
    }
}
