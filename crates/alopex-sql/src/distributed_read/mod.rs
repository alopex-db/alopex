//! Closed SQL catalog used before distributed-read routing.
//!
//! This module deliberately exposes classification data rather than a general
//! purpose logical-plan codec.  A worker must receive a later, normalized
//! descriptor and never an arbitrary [`crate::planner::LogicalPlan`].

pub mod assembler;
pub mod catalog_v0_8;
pub mod coverage;

pub use assembler::{
    AssemblerRow, AssemblerTerminalStatus, AssemblyPlan, DistributedReadAssemblyError,
    DistributedReadBudget, ExactAggregatePartial, ExactAggregatePlan, GlobalOrder,
    GlobalResultAssembler, OrderedAggregateInput, OrderedAggregatePlan, PreparedResult,
    PreparedResultStream, RangeAssemblerInput, RangeAssemblerPayload, RangeTerminal,
    ResultPresentation, RowMergePlan,
};
pub use catalog_v0_8::{
    REMOTE_DETERMINISTIC_SCALAR_FUNCTIONS, REMOTE_LOCAL_ONLY_SCALAR_FUNCTIONS,
    REMOTE_READ_CATALOG_VERSION, RemoteAggregate, RemoteReadCatalogV0_8, RemoteReadClassification,
    RemoteReadCoverageEntry, RemoteReadCoverageStatus, RemoteReadDescriptor, RemoteReadOperators,
    RemoteReadRejection, RemoteReadShape, classify, coverage_entries,
};
pub use coverage::{render_json, render_markdown};
