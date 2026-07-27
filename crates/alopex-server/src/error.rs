use alopex_cluster::{CrdtCommonFields, FailureClass, RoutingOutcomeKind};
use axum::http::StatusCode;

/// Server-wide result type.
pub type Result<T> = std::result::Result<T, ServerError>;

/// Top-level error type for alopex-server.
#[derive(Debug, thiserror::Error)]
pub enum ServerError {
    #[error("invalid config: {0}")]
    InvalidConfig(String),
    #[error("bad request: {0}")]
    BadRequest(String),
    #[error("unauthorized: {0}")]
    Unauthorized(String),
    #[error("not found: {0}")]
    NotFound(String),
    #[error("conflict: {0}")]
    Conflict(String),
    #[error("payload too large: {0}")]
    PayloadTooLarge(String),
    #[error("timeout: {0}")]
    Timeout(String),
    #[error("session expired: {0}")]
    SessionExpired(String),
    #[error("future distributed execution required: {0}")]
    FutureDistributedExecutionRequired(String),
    #[error("capability unavailable: {0}")]
    CapabilityUnavailable(String),
    #[error("not implemented: {0}")]
    NotImplemented(String),
    #[error("restore integrity mismatch: {0}")]
    RestoreIntegrityMismatch(String),
    #[error("sql error: {0}")]
    Sql(#[from] alopex_sql::SqlError),
    #[error("core error: {0}")]
    Core(#[from] alopex_core::Error),
    #[error("catalog error: {0}")]
    Catalog(#[from] alopex_sql::catalog::CatalogError),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("internal error: {0}")]
    Internal(String),
}

impl ServerError {
    /// Converts a canonical CRDT failure into the existing server error
    /// vocabulary.  The response itself keeps `FailureClass` authoritative;
    /// this only selects the HTTP/gRPC-compatible server category.
    pub fn from_crdt_common_fields(
        fields: &CrdtCommonFields,
        message: impl Into<String>,
    ) -> Option<Self> {
        let message = message.into();
        if fields.routing.kind == RoutingOutcomeKind::Unsupported {
            return Some(Self::NotImplemented(message));
        }

        fields.failure_class.map(|failure| match failure {
            FailureClass::Unauthorized => Self::Unauthorized(message),
            FailureClass::StaleMetadata
            | FailureClass::Gap
            | FailureClass::Overlap
            | FailureClass::EpochMismatch
            | FailureClass::Conflict => Self::Conflict(message),
            FailureClass::NotLeader
            | FailureClass::NodeUnavailable
            | FailureClass::PrerequisiteMissing => Self::CapabilityUnavailable(message),
            FailureClass::Timeout => Self::Timeout(message),
            FailureClass::InvalidRequest => Self::BadRequest(message),
            FailureClass::Internal => Self::Internal(message),
        })
    }

    /// Map error to HTTP status code.
    pub fn status_code(&self) -> StatusCode {
        match self {
            Self::InvalidConfig(_) | Self::BadRequest(_) => StatusCode::BAD_REQUEST,
            Self::Sql(err) if err.code() == "ALOPEX-S001" => StatusCode::CONFLICT,
            Self::Sql(_) => StatusCode::BAD_REQUEST,
            Self::Unauthorized(_) => StatusCode::UNAUTHORIZED,
            Self::NotFound(_) => StatusCode::NOT_FOUND,
            Self::Conflict(_) => StatusCode::CONFLICT,
            Self::PayloadTooLarge(_) => StatusCode::PAYLOAD_TOO_LARGE,
            Self::Timeout(_) => StatusCode::REQUEST_TIMEOUT,
            Self::SessionExpired(_) => StatusCode::GONE,
            Self::FutureDistributedExecutionRequired(_) => StatusCode::NOT_IMPLEMENTED,
            Self::CapabilityUnavailable(_) => StatusCode::SERVICE_UNAVAILABLE,
            Self::NotImplemented(_) => StatusCode::NOT_IMPLEMENTED,
            Self::RestoreIntegrityMismatch(_) => StatusCode::CONFLICT,
            Self::Core(_) | Self::Catalog(_) | Self::Io(_) | Self::Internal(_) => {
                StatusCode::INTERNAL_SERVER_ERROR
            }
        }
    }

    /// Map error to a stable error code for clients.
    pub fn error_code(&self) -> String {
        match self {
            Self::Sql(err) => err.code().to_string(),
            Self::InvalidConfig(_) => "INVALID_CONFIG".to_string(),
            Self::BadRequest(_) => "INVALID_REQUEST".to_string(),
            Self::Unauthorized(_) => "UNAUTHORIZED".to_string(),
            Self::NotFound(_) => "NOT_FOUND".to_string(),
            Self::Conflict(_) => "CONFLICT".to_string(),
            Self::PayloadTooLarge(_) => "PAYLOAD_TOO_LARGE".to_string(),
            Self::Timeout(_) => "QUERY_TIMEOUT".to_string(),
            Self::SessionExpired(_) => "SESSION_EXPIRED".to_string(),
            Self::FutureDistributedExecutionRequired(_) => {
                "FUTURE_DISTRIBUTED_EXECUTION_REQUIRED".to_string()
            }
            Self::CapabilityUnavailable(_) => "CAPABILITY_UNAVAILABLE".to_string(),
            Self::NotImplemented(_) => "NOT_IMPLEMENTED".to_string(),
            Self::RestoreIntegrityMismatch(_) => "RESTORE_INTEGRITY_MISMATCH".to_string(),
            Self::Core(_) | Self::Catalog(_) | Self::Io(_) | Self::Internal(_) => {
                "INTERNAL".to_string()
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use alopex_cluster::{
        CrdtCommonFields, CrdtObjectType, IdempotencyResult, OperationState, RangeIdentity,
        RoutingOutcome,
    };

    use super::{FailureClass, RoutingOutcomeKind, ServerError};

    fn common(
        failure_class: Option<FailureClass>,
        routing_kind: RoutingOutcomeKind,
    ) -> CrdtCommonFields {
        let range = RangeIdentity::new("cluster-a", 7, "range-a", None, None, 1, 9);
        CrdtCommonFields {
            object_type: CrdtObjectType::Counter,
            object_id: "counter-a".to_string(),
            range: range.clone(),
            state_epoch: 9,
            actor: "node-a".into(),
            request_id: "request-a".into(),
            operation_id: "operation-a".to_string(),
            state: OperationState::Rejected,
            failure_class,
            routing: RoutingOutcome::new(routing_kind, Some(range), 4, "fixture"),
            retryable: false,
            idempotency: IdempotencyResult {
                operation_id: "operation-a".to_string(),
                request_id: "request-a".into(),
                first_outcome: "rejected".to_string(),
                state: OperationState::Rejected,
                duplicate_count: 0,
            },
        }
    }

    #[test]
    fn crdt_common_fields_use_existing_server_error_categories() {
        let unauthorized = ServerError::from_crdt_common_fields(
            &common(
                Some(FailureClass::Unauthorized),
                RoutingOutcomeKind::Blocked,
            ),
            "denied",
        );
        assert!(matches!(unauthorized, Some(ServerError::Unauthorized(_))));

        let unavailable = ServerError::from_crdt_common_fields(
            &common(
                Some(FailureClass::PrerequisiteMissing),
                RoutingOutcomeKind::Blocked,
            ),
            "prerequisite",
        );
        assert!(matches!(
            unavailable,
            Some(ServerError::CapabilityUnavailable(_))
        ));

        let unsupported = ServerError::from_crdt_common_fields(
            &common(
                Some(FailureClass::PrerequisiteMissing),
                RoutingOutcomeKind::Unsupported,
            ),
            "unsupported",
        );
        assert!(matches!(unsupported, Some(ServerError::NotImplemented(_))));

        assert!(ServerError::from_crdt_common_fields(
            &common(None, RoutingOutcomeKind::SingleRange),
            "not an error"
        )
        .is_none());
    }
}
