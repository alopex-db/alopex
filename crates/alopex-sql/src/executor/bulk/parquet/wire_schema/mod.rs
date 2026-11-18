//! Pinned wire declarations used after the allocation preflight.
//!
//! Keep these types private: decoding and compression use the current Parquet
//! crate. See README.md for the upstream source and update procedure.

use thrift::protocol::{TInputProtocol, TOutputProtocol};

// Preserve the upstream generated declarations without formatting/lint churn.
#[allow(clippy::all, missing_docs)]
#[rustfmt::skip]
pub mod format;

/// The generic serialization interface used by the upstream wire declarations.
pub trait TSerializable: Sized {
    fn read_from_in_protocol<T: TInputProtocol>(protocol: &mut T) -> thrift::Result<Self>;
    fn write_to_out_protocol<T: TOutputProtocol>(&self, protocol: &mut T) -> thrift::Result<()>;
}
