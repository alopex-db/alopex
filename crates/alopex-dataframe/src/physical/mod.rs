/// Resource accounting and lifecycle contracts used by bounded execution.
pub mod budget;
mod executor;
mod expr_eval;
mod operators;
mod plan;
mod streaming;

/// Physical plan executor.
pub use executor::{record_batch_memory_size, BudgetedMaterializedExecutor, Executor};
/// Physical plan compiler and plan node types.
pub use plan::{
    analyze_streaming, compile, compile_streaming, PhysicalPlan, PlanSubject, ScanSource,
    SourceLimit, StreamingEligibility, StreamingOperator, StreamingPhysicalPlan, StreamingReason,
    StreamingSource,
};
pub(crate) use streaming::open_streaming_plan;
/// Incremental batch source and stream contracts.
pub use streaming::{
    BatchOpenContext, BatchSource, BatchSourceFactory, ColumnarSegmentBatchPlan,
    ColumnarSegmentCursor, ColumnarSegmentProvider, DataFrameStream, StreamBatch,
};
