//! HTTP boundary helpers for changefeed authorization.
//!
//! Route registration remains in the lifecycle task.  This module binds the
//! server-authenticated request actor to the shared changefeed authorization
//! contract before any future handler opens a feed or reads retained payload.

use alopex_cluster::{
    changefeed::{
        ChangefeedAccessRequest, ChangefeedAuthorization, ChangefeedAuthorizationDecision,
    },
    AuthenticatedSubject,
};

use super::RequestContext;

/// Checks that the server-established authorization belongs to the
/// middleware-authenticated actor, then checks tenant/range/scope ownership.
///
/// All denials collapse to `Denied`; callers must use the shared redacted
/// outcome and must not expose the failing scope, tenant, or range fact.
#[must_use]
pub fn authorize_changefeed(
    context: &RequestContext,
    authorization: &ChangefeedAuthorization,
    request: ChangefeedAccessRequest,
) -> ChangefeedAuthorizationDecision {
    let subject = AuthenticatedSubject::new(context.actor.as_deref().unwrap_or("anonymous"));
    if authorization.subject != subject {
        ChangefeedAuthorizationDecision::Denied
    } else {
        authorization.authorize(request)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use alopex_cluster::{
        changefeed::{
            ChangefeedAccessRequest, ChangefeedAction, ChangefeedAuthorization, ChangefeedScope,
        },
        AuthenticatedSubject,
    };

    use super::{authorize_changefeed, RequestContext};

    fn authorization(subject: &str) -> ChangefeedAuthorization {
        ChangefeedAuthorization {
            subject: AuthenticatedSubject::new(subject),
            tenant: "tenant-a".to_string(),
            allowed_ranges: BTreeSet::from(["range-a".into()]),
            allowed_scopes: BTreeSet::from([ChangefeedScope::Read]),
        }
    }

    fn request() -> ChangefeedAccessRequest {
        ChangefeedAccessRequest {
            action: ChangefeedAction::Poll,
            tenant: "tenant-a".to_string(),
            range_id: "range-a".into(),
        }
    }

    #[test]
    fn server_actor_must_match_authorization_subject_before_range_check() {
        let context = RequestContext {
            correlation_id: "correlation-a".to_string(),
            actor: Some("dev".to_string()),
        };
        assert!(!authorize_changefeed(&context, &authorization("other"), request()).permits());
        assert!(authorize_changefeed(&context, &authorization("dev"), request()).permits());
    }
}
