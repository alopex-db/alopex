//! Async columnar reader traits and adapters.

use crate::async_runtime::{BoxFuture, BoxStream, MaybeSend};
use crate::columnar::error::Result;
use crate::columnar::kvs_bridge::ColumnarKvsBridge;
use crate::columnar::segment_v2::{ColumnSegmentV2, RecordBatch};
#[cfg(feature = "tokio")]
use core::pin::Pin;
#[cfg(feature = "tokio")]
use core::task::{Context, Poll};
#[cfg(feature = "tokio")]
use futures_core::stream::Stream;

/// Columnar segment identifier.
pub type SegmentId = u64;

/// Column identifier in a segment schema.
pub type ColumnId = usize;

/// Row batch produced by columnar scans.
pub type RowBatch = RecordBatch;

/// Columnar segment payload.
pub type Segment = ColumnSegmentV2;

/// Async reader for columnar storage.
pub trait AsyncColumnarReader: MaybeSend {
    /// Read a segment by id.
    fn async_read_segment<'a>(&'a self, segment_id: SegmentId) -> BoxFuture<'a, Result<Segment>>;

    /// Scan projected columns and yield row batches.
    fn async_scan<'a>(&'a self, projection: &'a [ColumnId]) -> BoxStream<'a, Result<RowBatch>>;
}

/// Async adapter for [`ColumnarKvsBridge`] (tokio feature).
#[cfg(feature = "tokio")]
#[derive(Clone)]
pub struct AsyncColumnarReaderAdapter {
    bridge: ColumnarKvsBridge,
    table_id: u32,
}

#[cfg(feature = "tokio")]
impl AsyncColumnarReaderAdapter {
    /// Create a reader for a specific table.
    pub fn new(bridge: ColumnarKvsBridge, table_id: u32) -> Self {
        Self { bridge, table_id }
    }
}

#[cfg(feature = "tokio")]
impl AsyncColumnarReader for AsyncColumnarReaderAdapter {
    fn async_read_segment<'a>(&'a self, segment_id: SegmentId) -> BoxFuture<'a, Result<Segment>> {
        let bridge = self.bridge.clone();
        let table_id = self.table_id;
        Box::pin(async move {
            tokio::task::spawn_blocking(move || bridge.read_segment_raw(table_id, segment_id))
                .await
                .map_err(|_| {
                    crate::columnar::error::ColumnarError::InvalidFormat(
                        "blocking task cancelled".into(),
                    )
                })?
        })
    }

    fn async_scan<'a>(&'a self, projection: &'a [ColumnId]) -> BoxStream<'a, Result<RowBatch>> {
        let (sender, receiver) = tokio::sync::mpsc::channel(32);
        let bridge = self.bridge.clone();
        let table_id = self.table_id;
        let projection = projection.to_vec();

        tokio::task::spawn_blocking(move || {
            let result = (|| {
                let segment_ids = bridge.segment_index(table_id)?;
                for segment_id in segment_ids {
                    let reader = bridge.open_segment_reader(table_id, segment_id)?;
                    let row_group_count = reader.row_group_count();
                    for rg_index in 0..row_group_count {
                        let batch = reader.read_row_group_by_index(&projection, rg_index)?;
                        if sender.blocking_send(Ok(batch)).is_err() {
                            return Ok(());
                        }
                    }
                }
                Ok(())
            })();

            if let Err(err) = result {
                let _ = sender.blocking_send(Err(err));
            }
        });

        Box::pin(ReceiverStream { receiver })
    }
}

#[cfg(feature = "tokio")]
struct ReceiverStream<T> {
    receiver: tokio::sync::mpsc::Receiver<T>,
}

#[cfg(feature = "tokio")]
impl<T> Stream for ReceiverStream<T> {
    type Item = T;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.receiver.poll_recv(cx)
    }
}
