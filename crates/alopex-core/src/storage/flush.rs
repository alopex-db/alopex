//! Helpers for flushing vector segments alongside KV data.
use std::path::Path;

use crate::columnar::encoding::{Column, Compression, Encoding, LogicalType};
use crate::columnar::segment::{write_segment, SegmentMeta};
use crate::Result;

/// Writes an empty vector segment file at the given path.
///
/// This is a placeholder to ensure the flush path produces a columnar artifact
/// even when no vectors are present yet. Future integration can write actual
/// vector columns.
pub fn write_empty_vector_segment(path: &Path) -> Result<()> {
    let meta = SegmentMeta {
        logical_type: LogicalType::Binary,
        encoding: Encoding::Plain,
        compression: Compression::None,
        chunk_rows: 1,
        chunk_checksum: true,
    };
    let col = Column::Binary(Vec::new());
    Ok(write_segment(path, &col, &meta)?)
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::*;
    use crate::columnar::segment::SegmentReader;
    use tempfile::tempdir;

    #[test]
    fn writes_empty_segment() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("empty.vec");
        write_empty_vector_segment(&path).unwrap();

        let mut reader = SegmentReader::open(&path).unwrap();
        let mut iter = reader.iter();
        assert!(iter.next().is_none());
    }
}
