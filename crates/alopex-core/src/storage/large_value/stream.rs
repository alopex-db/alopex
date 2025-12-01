//! Streaming helpers for chunked large values.
//!
//! These helpers bridge `LargeValueWriter`/`LargeValueReader` with standard
//! `Read`/`Write` traits while keeping memory bounded to a single chunk.

use std::io::{Read, Write};

use crate::error::{Error, Result};

use super::{LargeValueReader, LargeValueWriter};

/// Streams all bytes from `reader` into the chunked writer using its chunk_size.
/// Returns the number of bytes written. Errors if the source ends before the
/// declared total_len.
pub fn copy_from_reader<R: Read>(writer: &mut LargeValueWriter, mut reader: R) -> Result<u64> {
    let mut buf = vec![0u8; writer.chunk_size() as usize];
    let mut written = 0u64;

    loop {
        let n = reader.read(&mut buf)?;
        if n == 0 {
            if writer.remaining() != 0 {
                return Err(Error::InvalidFormat(
                    "source exhausted before total_len for large_value".into(),
                ));
            }
            break;
        }
        writer.write_chunk(&buf[..n])?;
        written += n as u64;
        if writer.remaining() == 0 {
            break;
        }
    }

    Ok(written)
}

/// Drains all chunks from the reader into `sink`, returning bytes copied.
/// Callers can stop early by dropping the reader to simulate partial reads.
pub fn drain_to_writer<W: Write>(reader: &mut LargeValueReader, mut sink: W) -> Result<u64> {
    let mut copied = 0u64;
    while let Some((_info, chunk)) = reader.next_chunk()? {
        sink.write_all(&chunk)?;
        copied += chunk.len() as u64;
    }
    Ok(copied)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::large_value::chunk::{LargeValueKind, LargeValueMeta, LargeValueWriter};
    use std::io::Cursor;
    use tempfile::tempdir;

    fn typed_meta(total: u64, chunk: u32) -> LargeValueMeta {
        LargeValueMeta {
            kind: LargeValueKind::Typed(7),
            total_len: total,
            chunk_size: chunk,
        }
    }

    #[test]
    fn copies_from_reader_in_chunks() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("blob.lv");
        let payload = b"hello chunk streaming";

        {
            let mut writer =
                LargeValueWriter::create(&path, typed_meta(payload.len() as u64, 5)).unwrap();
            let bytes = copy_from_reader(&mut writer, Cursor::new(payload)).unwrap();
            assert_eq!(bytes, payload.len() as u64);
            writer.finish().unwrap();
        }

        let mut reader = LargeValueReader::open(&path).unwrap();
        let mut out = Vec::new();
        let copied = drain_to_writer(&mut reader, &mut out).unwrap();
        assert_eq!(copied, payload.len() as u64);
        assert_eq!(out, payload);
    }

    #[test]
    fn source_exhaustion_is_reported() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("short.lv");
        let payload = b"short";
        let mut writer =
            LargeValueWriter::create(&path, typed_meta((payload.len() + 2) as u64, 8)).unwrap();
        let err = copy_from_reader(&mut writer, Cursor::new(payload)).unwrap_err();
        match err {
            Error::InvalidFormat(msg) => assert!(msg.contains("source exhausted")),
            other => panic!("unexpected error: {other:?}"),
        }
        writer.cancel().unwrap();
    }
}
