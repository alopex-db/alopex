use std::sync::Arc;

use alopex_cluster::{
    AuthenticatedSubject, LocalReadAuthorizationRecheck, LocalReadAuthorizationRequest,
};
use axum::http::HeaderMap;
use serde::{Deserialize, Serialize};
use tonic::metadata::MetadataMap;

const ANONYMOUS_SUBJECT: &str = "anonymous";
const DEV_SUBJECT: &str = "dev";

/// Authentication mode for the server.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum AuthMode {
    /// No authentication.
    #[default]
    None,
    /// Dev API key authentication.
    Dev { api_key: String },
}

/// Authentication error for HTTP/gRPC.
#[derive(Debug, thiserror::Error)]
pub enum AuthError {
    #[error("missing credentials")]
    Missing,
    #[error("invalid credentials")]
    Invalid,
}

/// The server-local policy consulted for every local data read.
///
/// A range worker receives only this narrow interface through
/// [`ServerLocalReadAuthorizationRecheck`].  It must not infer user authority
/// from the authenticated cluster peer or from a delegation credential alone.
pub trait LocalReadAuthorizationPolicy: Send + Sync {
    /// Authorize the exact read that would be permitted on this server locally.
    fn authorize_local_read(&self, request: &LocalReadAuthorizationRequest) -> Result<(), String>;
}

/// Adapts the server's local data policy to the cluster worker boundary.
#[derive(Clone)]
pub struct ServerLocalReadAuthorizationRecheck {
    policy: Arc<dyn LocalReadAuthorizationPolicy>,
}

impl ServerLocalReadAuthorizationRecheck {
    /// Create an adapter around the same policy used for local data access.
    pub fn new(policy: Arc<dyn LocalReadAuthorizationPolicy>) -> Self {
        Self { policy }
    }
}

impl LocalReadAuthorizationRecheck for ServerLocalReadAuthorizationRecheck {
    fn authorize(&self, request: &LocalReadAuthorizationRequest) -> Result<(), String> {
        self.policy.authorize_local_read(request)
    }
}

/// Middleware helper for authentication.
#[derive(Clone)]
pub struct AuthMiddleware {
    mode: AuthMode,
}

impl AuthMiddleware {
    /// Create a new auth middleware.
    pub fn new(mode: AuthMode) -> Self {
        Self { mode }
    }

    /// Validate HTTP headers and return actor identity (if any).
    pub fn validate_http(&self, headers: &HeaderMap) -> Result<Option<String>, AuthError> {
        match &self.mode {
            AuthMode::None => Ok(None),
            AuthMode::Dev { api_key } => {
                let provided = extract_api_key(headers);
                if provided.as_deref() == Some(api_key.as_str()) {
                    Ok(Some("dev".to_string()))
                } else if provided.is_none() {
                    Err(AuthError::Missing)
                } else {
                    Err(AuthError::Invalid)
                }
            }
        }
    }

    /// Validate gRPC metadata and return actor identity (if any).
    pub fn validate_grpc(&self, metadata: &MetadataMap) -> Result<Option<String>, AuthError> {
        match &self.mode {
            AuthMode::None => Ok(None),
            AuthMode::Dev { api_key } => {
                let provided = extract_api_key_from_metadata(metadata);
                if provided.as_deref() == Some(api_key.as_str()) {
                    Ok(Some("dev".to_string()))
                } else if provided.is_none() {
                    Err(AuthError::Missing)
                } else {
                    Err(AuthError::Invalid)
                }
            }
        }
    }

    pub fn mode(&self) -> &AuthMode {
        &self.mode
    }

    /// Derive the only subject that the configured local authentication mode
    /// can authorize. Call this after the request credentials have been
    /// validated; callers cannot select an arbitrary subject string.
    pub fn authenticated_subject(
        &self,
        actor: Option<&str>,
    ) -> Result<AuthenticatedSubject, AuthError> {
        match (&self.mode, actor) {
            (AuthMode::None, None) => Ok(AuthenticatedSubject::new(ANONYMOUS_SUBJECT)),
            (AuthMode::Dev { .. }, Some(DEV_SUBJECT)) => Ok(AuthenticatedSubject::new(DEV_SUBJECT)),
            _ => Err(AuthError::Invalid),
        }
    }

    /// Authorize a CRDT request only after transport credentials have been
    /// validated.  F2 adapters pass this subject to the common fail-closed
    /// policy instead of treating a caller-supplied actor string as authority.
    pub fn authorize_crdt(&self, actor: Option<&str>) -> Result<AuthenticatedSubject, AuthError> {
        self.authenticated_subject(actor)
    }

    /// Return the worker-facing recheck backed by this server's local policy.
    pub fn local_read_authorization_recheck(&self) -> Arc<dyn LocalReadAuthorizationRecheck> {
        Arc::new(ServerLocalReadAuthorizationRecheck::new(Arc::new(
            self.clone(),
        )))
    }
}

impl LocalReadAuthorizationPolicy for AuthMiddleware {
    fn authorize_local_read(&self, request: &LocalReadAuthorizationRequest) -> Result<(), String> {
        let expected_subject = match self.mode() {
            AuthMode::None => ANONYMOUS_SUBJECT,
            AuthMode::Dev { .. } => DEV_SUBJECT,
        };
        if request.subject.as_str() == expected_subject {
            Ok(())
        } else {
            Err("delegated subject is not authorized for the corresponding local read".into())
        }
    }
}

fn extract_api_key(headers: &HeaderMap) -> Option<String> {
    if let Some(value) = headers.get("x-api-key").and_then(|v| v.to_str().ok()) {
        return Some(value.to_string());
    }
    headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|raw| raw.strip_prefix("Bearer "))
        .map(|v| v.to_string())
}

fn extract_api_key_from_metadata(metadata: &MetadataMap) -> Option<String> {
    if let Some(value) = metadata.get("x-api-key").and_then(|v| v.to_str().ok()) {
        return Some(value.to_string());
    }
    metadata
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|raw| raw.strip_prefix("Bearer "))
        .map(|v| v.to_string())
}

#[cfg(test)]
mod tests {
    use alopex_cluster::{RangeId, ReadOperationScope, RequestId};
    use alopex_core::ReadAtPoint;

    use super::*;

    fn local_request(subject: &str) -> LocalReadAuthorizationRequest {
        LocalReadAuthorizationRequest {
            subject: AuthenticatedSubject::new(subject),
            operation: ReadOperationScope::Select,
            table_id: 7,
            range_id: RangeId::new("range-a"),
            request_id: RequestId::new("request-a"),
            query_digest: "query-a".into(),
            read_at: ReadAtPoint::new(4, 3, 2, 1),
        }
    }

    #[test]
    fn local_recheck_admits_only_the_subject_allowed_by_local_auth_mode() {
        let auth = AuthMiddleware::new(AuthMode::Dev {
            api_key: "secret".into(),
        });
        let recheck = auth.local_read_authorization_recheck();
        assert!(recheck.authorize(&local_request(DEV_SUBJECT)).is_ok());
        assert!(recheck
            .authorize(&local_request(ANONYMOUS_SUBJECT))
            .is_err());
    }

    #[test]
    fn authenticated_subject_is_derived_from_a_validated_actor_not_caller_input() {
        let auth = AuthMiddleware::new(AuthMode::Dev {
            api_key: "secret".into(),
        });
        assert_eq!(
            auth.authenticated_subject(Some(DEV_SUBJECT))
                .unwrap()
                .as_str(),
            DEV_SUBJECT
        );
        assert!(auth.authenticated_subject(Some("other-user")).is_err());

        let anonymous = AuthMiddleware::new(AuthMode::None);
        assert_eq!(
            anonymous.authenticated_subject(None).unwrap().as_str(),
            ANONYMOUS_SUBJECT
        );
        assert!(anonymous.authenticated_subject(Some(DEV_SUBJECT)).is_err());
    }

    #[test]
    fn crdt_authorization_uses_the_same_validated_subject_boundary() {
        let auth = AuthMiddleware::new(AuthMode::Dev {
            api_key: "secret".into(),
        });
        assert_eq!(
            auth.authorize_crdt(Some(DEV_SUBJECT)).unwrap().as_str(),
            DEV_SUBJECT
        );
        assert!(auth.authorize_crdt(Some("untrusted")).is_err());
    }
}
