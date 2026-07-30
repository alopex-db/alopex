//! Closed pre-execution changefeed boundary for DataFrame execution and
//! DataFrame sources.
//!
//! Alopex's local `DataFrame`, `LazyFrame`, and bounded `DataFrameStream`
//! remain local data-processing APIs.  They do not become a distributed feed
//! client merely because a caller asks for a changefeed lifecycle operation.
//! This registry makes that boundary verifier-visible and rejects every
//! request before source opening, lazy planning, stream construction, CRDT
//! projection, or transaction work begins.

use std::error::Error;
use std::fmt;

/// Version carried by every DataFrame-local changefeed rejection.
pub const DATAFRAME_CHANGEFEED_BOUNDARY_VERSION: &str = "v0.9";

/// Stable DataFrame-local code for a rejected changefeed request.
pub const DATAFRAME_CHANGEFEED_UNSUPPORTED_CODE: &str = "dataframe_changefeed_unsupported";

/// Canonical surface error family selected by an upper adapter.
pub const DATAFRAME_CHANGEFEED_SURFACE_ERROR_CODE: &str = "changefeed_unsupported";

/// Canonical routing-kind string retained without introducing a reverse
/// dependency from the DataFrame crate to `alopex-cluster`.
pub const DATAFRAME_CHANGEFEED_CANONICAL_ROUTING_KIND: &str = "unsupported";

/// Canonical failure-class string retained by an upper adapter alongside the
/// unsupported routing result.
pub const DATAFRAME_CHANGEFEED_CANONICAL_FAILURE_CLASS: &str = "invalid_request";

/// Lifecycle names that are intentionally not DataFrame operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DataFrameChangefeedLifecycle {
    Create,
    Subscribe,
    Poll,
    Stream,
    Ack,
    Resume,
    Cancel,
    Close,
}

impl DataFrameChangefeedLifecycle {
    /// Stable identity for verifier rows and diagnostics.
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

/// DataFrame execution or source boundary through which a caller might try to
/// request a feed.  None of these targets owns a feed lifecycle.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DataFrameChangefeedTarget {
    EagerDataFrame,
    LazyFrame,
    CsvSource,
    ParquetSource,
    ColumnarSegmentSource,
    DataFrameStream,
    CrdtProjection,
    Transaction,
}

impl DataFrameChangefeedTarget {
    /// Stable identity for verifier rows and diagnostics.
    pub const fn id(self) -> &'static str {
        match self {
            Self::EagerDataFrame => "dataframe",
            Self::LazyFrame => "lazyframe",
            Self::CsvSource => "csv_source",
            Self::ParquetSource => "parquet_source",
            Self::ColumnarSegmentSource => "columnar_segment_source",
            Self::DataFrameStream => "dataframe_stream",
            Self::CrdtProjection => "crdt_projection",
            Self::Transaction => "transaction",
        }
    }
}

/// One rejected attempt to invoke a changefeed lifecycle operation through a
/// DataFrame target.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct DataFrameChangefeedRequest {
    pub target: DataFrameChangefeedTarget,
    pub lifecycle: DataFrameChangefeedLifecycle,
}

impl DataFrameChangefeedRequest {
    /// A stable request identifier that is not SQL text or a feed identifier.
    pub const fn id(self) -> (&'static str, &'static str) {
        (self.target.id(), self.lifecycle.id())
    }
}

/// The only classification exposed by this boundary.  A `Supported` variant
/// would incorrectly create an implicit distributed DataFrame feed API.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DataFrameChangefeedClassification {
    PreExecutionUnsupported,
}

/// Structured rejection returned before a DataFrame source, lazy plan, or
/// stream is opened.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DataFrameChangefeedRejection {
    pub boundary_version: &'static str,
    pub request: DataFrameChangefeedRequest,
    pub classification: DataFrameChangefeedClassification,
    pub code: &'static str,
    pub reason_code: &'static str,
    pub canonical_routing_kind: &'static str,
    pub canonical_failure_class: &'static str,
    pub surface_error_code: &'static str,
    pub retryable: bool,
    pub execution_started: bool,
}

impl fmt::Display for DataFrameChangefeedRejection {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let (target, lifecycle) = self.request.id();
        write!(
            formatter,
            "DataFrame changefeed request '{lifecycle}' for '{target}' is unsupported before execution: {}",
            self.reason_code
        )
    }
}

impl Error for DataFrameChangefeedRejection {}

/// Closed lifecycle register for all DataFrame boundary rows.
pub const DATAFRAME_CHANGEFEED_LIFECYCLES: &[DataFrameChangefeedLifecycle] = &[
    DataFrameChangefeedLifecycle::Create,
    DataFrameChangefeedLifecycle::Subscribe,
    DataFrameChangefeedLifecycle::Poll,
    DataFrameChangefeedLifecycle::Stream,
    DataFrameChangefeedLifecycle::Ack,
    DataFrameChangefeedLifecycle::Resume,
    DataFrameChangefeedLifecycle::Cancel,
    DataFrameChangefeedLifecycle::Close,
];

/// Closed target register.  CRDT and transaction rows ensure neither is
/// accidentally reached by a DataFrame fallback.
pub const DATAFRAME_CHANGEFEED_TARGETS: &[DataFrameChangefeedTarget] = &[
    DataFrameChangefeedTarget::EagerDataFrame,
    DataFrameChangefeedTarget::LazyFrame,
    DataFrameChangefeedTarget::CsvSource,
    DataFrameChangefeedTarget::ParquetSource,
    DataFrameChangefeedTarget::ColumnarSegmentSource,
    DataFrameChangefeedTarget::DataFrameStream,
    DataFrameChangefeedTarget::CrdtProjection,
    DataFrameChangefeedTarget::Transaction,
];

/// Iterates every registered DataFrame changefeed request exactly once.
pub fn dataframe_changefeed_requests(
) -> impl Iterator<Item = DataFrameChangefeedRequest> + Clone + 'static {
    DATAFRAME_CHANGEFEED_TARGETS
        .iter()
        .copied()
        .flat_map(|target| {
            DATAFRAME_CHANGEFEED_LIFECYCLES
                .iter()
                .copied()
                .map(move |lifecycle| DataFrameChangefeedRequest { target, lifecycle })
        })
}

/// Builds the rejection without opening a source or constructing a lazy plan.
pub const fn reject_dataframe_changefeed_request(
    request: DataFrameChangefeedRequest,
) -> DataFrameChangefeedRejection {
    DataFrameChangefeedRejection {
        boundary_version: DATAFRAME_CHANGEFEED_BOUNDARY_VERSION,
        request,
        classification: DataFrameChangefeedClassification::PreExecutionUnsupported,
        code: DATAFRAME_CHANGEFEED_UNSUPPORTED_CODE,
        reason_code: "dataframe_changefeed_surface_unsupported",
        canonical_routing_kind: DATAFRAME_CHANGEFEED_CANONICAL_ROUTING_KIND,
        canonical_failure_class: DATAFRAME_CHANGEFEED_CANONICAL_FAILURE_CLASS,
        surface_error_code: DATAFRAME_CHANGEFEED_SURFACE_ERROR_CODE,
        retryable: false,
        execution_started: false,
    }
}

/// Result-shaped preflight for adapters before DataFrame execution begins.
pub fn preflight_dataframe_changefeed_request(
    request: DataFrameChangefeedRequest,
) -> std::result::Result<(), DataFrameChangefeedRejection> {
    Err(reject_dataframe_changefeed_request(request))
}
