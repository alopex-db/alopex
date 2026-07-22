//! Delegated user-read authorization at the worker boundary.
//!
//! The cluster crate verifies a narrow, signed capability and asks an injected
//! local policy to recheck it.  It deliberately does not import server auth
//! internals or treat an authenticated cluster peer as user authorization.

use std::collections::BTreeSet;

use alopex_core::ReadAtPoint;
use serde::{Deserialize, Serialize};

use crate::{ClusterId, NodeId, RangeId, RequestId, VerifiedPeerIdentity};

/// Opaque, authenticated end-user identity supplied by the server boundary.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct AuthenticatedSubject(String);

impl AuthenticatedSubject {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// The sole user-data operation that may be delegated in v0.8.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReadOperationScope {
    Select,
}

/// Signed capability transported to exactly one range worker.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReadDelegationCredential {
    pub issuer: NodeId,
    pub cluster_id: ClusterId,
    pub subject: AuthenticatedSubject,
    pub operation: ReadOperationScope,
    pub table_id: u32,
    pub allowed_ranges: BTreeSet<RangeId>,
    pub query_digest: String,
    pub request_id: RequestId,
    pub read_fence_digest: String,
    pub audience: NodeId,
    pub read_at: ReadAtPoint,
    pub issued_at_ms: u64,
    pub expires_at_ms: u64,
    pub key_id: String,
    pub signature: Vec<u8>,
}

impl ReadDelegationCredential {
    /// Returns canonical signed bytes, excluding the signature itself.
    pub fn signed_payload(&self) -> Result<Vec<u8>, DelegationAuthorizationError> {
        #[derive(Serialize)]
        struct Payload<'a> {
            issuer: &'a NodeId,
            cluster_id: &'a ClusterId,
            subject: &'a AuthenticatedSubject,
            operation: ReadOperationScope,
            table_id: u32,
            allowed_ranges: &'a BTreeSet<RangeId>,
            query_digest: &'a str,
            request_id: &'a RequestId,
            read_fence_digest: &'a str,
            audience: &'a NodeId,
            read_at: ReadAtPoint,
            issued_at_ms: u64,
            expires_at_ms: u64,
            key_id: &'a str,
        }
        serde_json::to_vec(&Payload {
            issuer: &self.issuer,
            cluster_id: &self.cluster_id,
            subject: &self.subject,
            operation: self.operation,
            table_id: self.table_id,
            allowed_ranges: &self.allowed_ranges,
            query_digest: &self.query_digest,
            request_id: &self.request_id,
            read_fence_digest: &self.read_fence_digest,
            audience: &self.audience,
            read_at: self.read_at,
            issued_at_ms: self.issued_at_ms,
            expires_at_ms: self.expires_at_ms,
            key_id: &self.key_id,
        })
        .map_err(|error| DelegationAuthorizationError::Encoding(error.to_string()))
    }
}

/// Signature verifier supplied by the configured cluster trust boundary.
pub trait ReadDelegationVerifier: Send + Sync {
    fn verify(&self, key_id: &str, payload: &[u8], signature: &[u8]) -> bool;
}

/// Minimal server-injected local authorization check, evaluated by every worker.
pub trait LocalReadAuthorizationRecheck: Send + Sync {
    fn authorize(&self, request: &LocalReadAuthorizationRequest) -> Result<(), String>;
}

/// Exactly the local facts the worker asks its authorization adapter to check.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocalReadAuthorizationRequest {
    pub subject: AuthenticatedSubject,
    pub operation: ReadOperationScope,
    pub table_id: u32,
    pub range_id: RangeId,
    pub request_id: RequestId,
    pub query_digest: String,
    pub read_at: ReadAtPoint,
}

/// Untrusted payload facts which must agree with the credential before rows exist.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DelegationValidationContext {
    pub peer: VerifiedPeerIdentity,
    pub range_id: RangeId,
    pub table_id: u32,
    pub operation: ReadOperationScope,
    pub request_id: RequestId,
    pub query_digest: String,
    pub read_fence_digest: String,
    pub read_at: ReadAtPoint,
    pub now_ms: u64,
}

/// Classified worker-side delegation rejection.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum DelegationAuthorizationError {
    #[error("delegation credential belongs to another cluster")]
    ClusterMismatch,
    #[error("delegation credential issuer does not match authenticated peer")]
    IssuerMismatch,
    #[error("delegation credential is addressed to another worker")]
    AudienceMismatch,
    #[error("delegation credential has expired or is not yet valid")]
    Expired,
    #[error("delegation credential does not permit this range")]
    RangeMismatch,
    #[error("delegation credential table or operation scope mismatches")]
    ScopeMismatch,
    #[error("delegation credential query, request, fence, or read point mismatches")]
    RequestMismatch,
    #[error("delegation signature is invalid")]
    InvalidSignature,
    #[error("local user authorization was denied: {0}")]
    LocalAuthorizationDenied(String),
    #[error("could not encode signed delegation payload: {0}")]
    Encoding(String),
}

/// Verifies the capability and performs the non-bypassable local policy recheck.
pub fn verify_and_recheck(
    credential: &ReadDelegationCredential,
    context: &DelegationValidationContext,
    verifier: &dyn ReadDelegationVerifier,
    local_authorizer: &dyn LocalReadAuthorizationRecheck,
) -> Result<(), DelegationAuthorizationError> {
    if credential.cluster_id != *context.peer.cluster_id() {
        return Err(DelegationAuthorizationError::ClusterMismatch);
    }
    if credential.issuer != *context.peer.node_id() {
        return Err(DelegationAuthorizationError::IssuerMismatch);
    }
    if credential.audience != *context.peer.node_id() {
        return Err(DelegationAuthorizationError::AudienceMismatch);
    }
    if context.now_ms < credential.issued_at_ms || context.now_ms > credential.expires_at_ms {
        return Err(DelegationAuthorizationError::Expired);
    }
    if !credential.allowed_ranges.contains(&context.range_id) {
        return Err(DelegationAuthorizationError::RangeMismatch);
    }
    if credential.table_id != context.table_id || credential.operation != context.operation {
        return Err(DelegationAuthorizationError::ScopeMismatch);
    }
    if credential.request_id != context.request_id
        || credential.query_digest != context.query_digest
        || credential.read_fence_digest != context.read_fence_digest
        || credential.read_at != context.read_at
    {
        return Err(DelegationAuthorizationError::RequestMismatch);
    }
    let payload = credential.signed_payload()?;
    if !verifier.verify(&credential.key_id, &payload, &credential.signature) {
        return Err(DelegationAuthorizationError::InvalidSignature);
    }
    local_authorizer
        .authorize(&LocalReadAuthorizationRequest {
            subject: credential.subject.clone(),
            operation: credential.operation,
            table_id: credential.table_id,
            range_id: context.range_id.clone(),
            request_id: credential.request_id.clone(),
            query_digest: credential.query_digest.clone(),
            read_at: credential.read_at,
        })
        .map_err(DelegationAuthorizationError::LocalAuthorizationDenied)
}

#[cfg(test)]
mod tests {
    use super::*;
    use sha2::{Digest, Sha256};

    struct Accept;
    impl ReadDelegationVerifier for Accept {
        fn verify(&self, _key_id: &str, payload: &[u8], signature: &[u8]) -> bool {
            signature == Sha256::digest(payload).as_slice()
        }
    }
    impl LocalReadAuthorizationRecheck for Accept {
        fn authorize(&self, request: &LocalReadAuthorizationRequest) -> Result<(), String> {
            (request.subject.as_str() == "user-a")
                .then_some(())
                .ok_or_else(|| "denied".into())
        }
    }

    fn credential() -> ReadDelegationCredential {
        let mut credential = ReadDelegationCredential {
            issuer: NodeId::new("gateway-a"),
            cluster_id: ClusterId::new("cluster-a"),
            subject: AuthenticatedSubject::new("user-a"),
            operation: ReadOperationScope::Select,
            table_id: 7,
            allowed_ranges: [RangeId::new("range-a")].into(),
            query_digest: "query".into(),
            request_id: RequestId::new("read-1"),
            read_fence_digest: "fence".into(),
            audience: NodeId::new("gateway-a"),
            read_at: ReadAtPoint::new(9, 2, 3, 4),
            issued_at_ms: 10,
            expires_at_ms: 20,
            key_id: "key-1".into(),
            signature: Vec::new(),
        };
        credential.signature = Sha256::digest(credential.signed_payload().unwrap()).to_vec();
        credential
    }
    fn context() -> DelegationValidationContext {
        DelegationValidationContext {
            peer: VerifiedPeerIdentity::new("gateway-a", "cluster-a"),
            range_id: RangeId::new("range-a"),
            table_id: 7,
            operation: ReadOperationScope::Select,
            request_id: RequestId::new("read-1"),
            query_digest: "query".into(),
            read_fence_digest: "fence".into(),
            read_at: ReadAtPoint::new(9, 2, 3, 4),
            now_ms: 15,
        }
    }
    #[test]
    fn accepted_delegation_is_rechecked_against_local_user_policy() {
        assert!(verify_and_recheck(&credential(), &context(), &Accept, &Accept).is_ok());
    }
    #[test]
    fn altered_range_scope_audience_and_fence_are_rejected_before_payload() {
        let mut range = context();
        range.range_id = RangeId::new("range-b");
        assert_eq!(
            verify_and_recheck(&credential(), &range, &Accept, &Accept),
            Err(DelegationAuthorizationError::RangeMismatch)
        );
        let mut audience = credential();
        audience.audience = NodeId::new("other");
        assert_eq!(
            verify_and_recheck(&audience, &context(), &Accept, &Accept),
            Err(DelegationAuthorizationError::AudienceMismatch)
        );
        let mut fence = context();
        fence.read_fence_digest = "other".into();
        assert_eq!(
            verify_and_recheck(&credential(), &fence, &Accept, &Accept),
            Err(DelegationAuthorizationError::RequestMismatch)
        );
    }

    #[test]
    fn altered_signed_subject_or_signature_is_rejected() {
        let mut subject = credential();
        subject.subject = AuthenticatedSubject::new("user-b");
        assert_eq!(
            verify_and_recheck(&subject, &context(), &Accept, &Accept),
            Err(DelegationAuthorizationError::InvalidSignature)
        );
        let mut signature = credential();
        signature.signature[0] ^= 1;
        assert_eq!(
            verify_and_recheck(&signature, &context(), &Accept, &Accept),
            Err(DelegationAuthorizationError::InvalidSignature)
        );
    }
}
