//! Closed, pre-execution rejection registry for changefeed requests at the
//! SQL boundary.
//!
//! Changefeeds are deliberately not SQL statements.  This module therefore
//! does not extend the parser or executor with a `CHANGEFEED` grammar.  It
//! gives callers and the v0.9 verifier a closed SQL-local intent register so a
//! feed request routed through a SQL/DataFrame entry point is rejected before
//! planning, execution, transport, checkpoint mutation, or implicit feed
//! creation can begin.
//!
//! The SQL crate must not depend on `alopex-cluster`; the literal canonical
//! mapping fields below are consumed by higher surface adapters that own the
//! common `FailureClass` and routing types.

use std::error::Error;
use std::fmt;

use serde::Serialize;

/// Version carried by every SQL-local changefeed boundary rejection.
pub const CHANGEFEED_SQL_BOUNDARY_VERSION: &str = "v0.9";

/// Stable SQL-local classification code for every changefeed boundary row.
pub const CHANGEFEED_SQL_UNSUPPORTED_CODE: &str = "changefeed_sql_unsupported";

/// Canonical surface error family used after an adapter maps this rejection.
pub const CHANGEFEED_SQL_SURFACE_ERROR_CODE: &str = "changefeed_unsupported";

/// Canonical routing kind selected by an adapter for this boundary.
pub const CHANGEFEED_SQL_CANONICAL_ROUTING_KIND: &str = "unsupported";

/// Canonical failure class retained by an adapter alongside the unsupported
/// routing kind.  This remains a string here to keep the SQL crate below the
/// cluster/changefeed model layer.
pub const CHANGEFEED_SQL_CANONICAL_FAILURE_CLASS: &str = "invalid_request";

/// A lifecycle operation that is available on registered changefeed surfaces
/// but intentionally unavailable as SQL syntax or a SQL executor operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ChangefeedSqlLifecycle {
    Create,
    Subscribe,
    Poll,
    Stream,
    Ack,
    Resume,
    Cancel,
    Close,
}

impl ChangefeedSqlLifecycle {
    /// Stable request identity for manifests and diagnostics.
    pub const fn id(self) -> &'static str {
        match self {
            Self::Create => "create",
            Self::Subscribe => "subscribe",
            Self::Poll => "poll",
            Self::Stream => "stream",
            Self::Ack => "ack",
            Self::Resume => "resume",
            Self::Cancel => "cancel",
            Self::Close => "close",
        }
    }
}

/// A change kind whose feed publication is explicitly outside the SQL
/// boundary until its own approved evidence and implementation exist.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ChangefeedSqlChangeKind {
    DdlSchema,
    CopyBulk,
    VectorHnswColumnar,
    CrdtUpdate,
    DistributedTransaction,
}

impl ChangefeedSqlChangeKind {
    /// Stable change-kind identity for manifests and diagnostics.
    pub const fn id(self) -> &'static str {
        match self {
            Self::DdlSchema => "ddl_schema",
            Self::CopyBulk => "copy_bulk",
            Self::VectorHnswColumnar => "vector_hnsw_columnar",
            Self::CrdtUpdate => "crdt_update",
            Self::DistributedTransaction => "distributed_transaction",
        }
    }

    const fn reason_code(self) -> &'static str {
        match self {
            Self::DdlSchema => "schema_unsupported",
            Self::CopyBulk => "bulk_changefeed_unsupported",
            Self::VectorHnswColumnar => "vector_changefeed_unsupported",
            Self::CrdtUpdate => "crdt_changefeed_unsupported",
            Self::DistributedTransaction => "distributed_transaction_changefeed_unsupported",
        }
    }
}

/// One request shape that must be rejected at the SQL boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ChangefeedSqlRequest {
    Lifecycle(ChangefeedSqlLifecycle),
    ChangeKind(ChangefeedSqlChangeKind),
}

impl ChangefeedSqlRequest {
    /// Stable manifest identifier without inventing SQL text or parser tokens.
    pub const fn id(self) -> &'static str {
        match self {
            Self::Lifecycle(lifecycle) => lifecycle.id(),
            Self::ChangeKind(change_kind) => change_kind.id(),
        }
    }

    const fn reason_code(self) -> &'static str {
        match self {
            Self::Lifecycle(_) => "sql_changefeed_surface_unsupported",
            Self::ChangeKind(change_kind) => change_kind.reason_code(),
        }
    }
}

/// Closed SQL-local support classification.  There is intentionally no
/// `Supported` variant because this registry is not a hidden SQL feed API.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ChangefeedSqlClassification {
    PreExecutionUnsupported,
}

/// Structured rejection emitted before any SQL plan, executor, transport, or
/// changefeed state can be created.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ChangefeedSqlRejection {
    pub boundary_version: &'static str,
    pub request: ChangefeedSqlRequest,
    pub classification: ChangefeedSqlClassification,
    pub code: &'static str,
    pub reason_code: &'static str,
    pub canonical_routing_kind: &'static str,
    pub canonical_failure_class: &'static str,
    pub surface_error_code: &'static str,
    pub retryable: bool,
    pub execution_started: bool,
}

impl fmt::Display for ChangefeedSqlRejection {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "changefeed SQL request '{}' is unsupported before execution: {}",
            self.request.id(),
            self.reason_code
        )
    }
}

impl Error for ChangefeedSqlRejection {}

/// Every SQL-local changefeed intent the v0.9 registry deliberately rejects.
///
/// The row order is stable for verifier inputs.  Adding a parser statement or
/// a new SQL entry point requires an explicit new row; it cannot become
/// supported by omission.
pub const SQL_CHANGEFEED_UNSUPPORTED_REQUESTS: &[ChangefeedSqlRequest] = &[
    ChangefeedSqlRequest::Lifecycle(ChangefeedSqlLifecycle::Create),
    ChangefeedSqlRequest::Lifecycle(ChangefeedSqlLifecycle::Subscribe),
    ChangefeedSqlRequest::Lifecycle(ChangefeedSqlLifecycle::Poll),
    ChangefeedSqlRequest::Lifecycle(ChangefeedSqlLifecycle::Stream),
    ChangefeedSqlRequest::Lifecycle(ChangefeedSqlLifecycle::Ack),
    ChangefeedSqlRequest::Lifecycle(ChangefeedSqlLifecycle::Resume),
    ChangefeedSqlRequest::Lifecycle(ChangefeedSqlLifecycle::Cancel),
    ChangefeedSqlRequest::Lifecycle(ChangefeedSqlLifecycle::Close),
    ChangefeedSqlRequest::ChangeKind(ChangefeedSqlChangeKind::DdlSchema),
    ChangefeedSqlRequest::ChangeKind(ChangefeedSqlChangeKind::CopyBulk),
    ChangefeedSqlRequest::ChangeKind(ChangefeedSqlChangeKind::VectorHnswColumnar),
    ChangefeedSqlRequest::ChangeKind(ChangefeedSqlChangeKind::CrdtUpdate),
    ChangefeedSqlRequest::ChangeKind(ChangefeedSqlChangeKind::DistributedTransaction),
];

/// Rejects a SQL-local changefeed request without parsing or executing SQL.
pub const fn reject_changefeed_sql_request(
    request: ChangefeedSqlRequest,
) -> ChangefeedSqlRejection {
    ChangefeedSqlRejection {
        boundary_version: CHANGEFEED_SQL_BOUNDARY_VERSION,
        request,
        classification: ChangefeedSqlClassification::PreExecutionUnsupported,
        code: CHANGEFEED_SQL_UNSUPPORTED_CODE,
        reason_code: request.reason_code(),
        canonical_routing_kind: CHANGEFEED_SQL_CANONICAL_ROUTING_KIND,
        canonical_failure_class: CHANGEFEED_SQL_CANONICAL_FAILURE_CLASS,
        surface_error_code: CHANGEFEED_SQL_SURFACE_ERROR_CODE,
        retryable: false,
        execution_started: false,
    }
}

/// Result-shaped preflight for callers that need to stop before SQL planning.
pub fn preflight_changefeed_sql_request(
    request: ChangefeedSqlRequest,
) -> Result<(), ChangefeedSqlRejection> {
    Err(reject_changefeed_sql_request(request))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::*;

    #[test]
    fn every_registered_lifecycle_is_rejected_before_sql_execution() {
        let lifecycle = SQL_CHANGEFEED_UNSUPPORTED_REQUESTS
            .iter()
            .copied()
            .filter(|request| matches!(request, ChangefeedSqlRequest::Lifecycle(_)));

        for request in lifecycle {
            let rejection = preflight_changefeed_sql_request(request).unwrap_err();
            assert_eq!(
                rejection.classification,
                ChangefeedSqlClassification::PreExecutionUnsupported
            );
            assert_eq!(rejection.code, CHANGEFEED_SQL_UNSUPPORTED_CODE);
            assert_eq!(rejection.reason_code, "sql_changefeed_surface_unsupported");
            assert!(!rejection.execution_started);
            assert!(!rejection.retryable);
        }
    }

    #[test]
    fn unsupported_change_kinds_keep_distinct_stable_reasons() {
        let requests = [
            (
                ChangefeedSqlRequest::ChangeKind(ChangefeedSqlChangeKind::DdlSchema),
                "schema_unsupported",
            ),
            (
                ChangefeedSqlRequest::ChangeKind(ChangefeedSqlChangeKind::CopyBulk),
                "bulk_changefeed_unsupported",
            ),
            (
                ChangefeedSqlRequest::ChangeKind(ChangefeedSqlChangeKind::VectorHnswColumnar),
                "vector_changefeed_unsupported",
            ),
            (
                ChangefeedSqlRequest::ChangeKind(ChangefeedSqlChangeKind::CrdtUpdate),
                "crdt_changefeed_unsupported",
            ),
            (
                ChangefeedSqlRequest::ChangeKind(ChangefeedSqlChangeKind::DistributedTransaction),
                "distributed_transaction_changefeed_unsupported",
            ),
        ];

        for (request, reason_code) in requests {
            let rejection = reject_changefeed_sql_request(request);
            assert_eq!(rejection.reason_code, reason_code);
            assert!(!rejection.execution_started);
        }
    }

    #[test]
    fn registry_is_closed_and_carries_the_adapter_mapping_without_cluster_dependency() {
        let ids: BTreeSet<_> = SQL_CHANGEFEED_UNSUPPORTED_REQUESTS
            .iter()
            .map(|request| request.id())
            .collect();
        assert_eq!(ids.len(), SQL_CHANGEFEED_UNSUPPORTED_REQUESTS.len());
        assert_eq!(ids.len(), 13);

        let rejection = reject_changefeed_sql_request(ChangefeedSqlRequest::Lifecycle(
            ChangefeedSqlLifecycle::Create,
        ));
        assert_eq!(rejection.boundary_version, "v0.9");
        assert_eq!(rejection.canonical_routing_kind, "unsupported");
        assert_eq!(rejection.canonical_failure_class, "invalid_request");
        assert_eq!(rejection.surface_error_code, "changefeed_unsupported");
        assert_eq!(
            serde_json::to_value(rejection).unwrap()["execution_started"],
            false
        );
    }
}
