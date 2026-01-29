//! 大容量チャンクフォーマットの writer/reader 実装。
//! バックプレッシャは `chunk_size` で制御し、writer/reader ともに同時保持は 1 チャンク分に限定する。
//! crc32 でボディを保護し、Blob/Typed いずれも同じレイアウトを用いる。

use crate::error::{Error, Result};
use crc32fast::Hasher;
use std::fs::{remove_file, File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

const HEADER_MAGIC: &[u8; 4] = b"LVCH";
const FOOTER_MAGIC: &[u8; 4] = b"LVFT";
const VERSION: u16 = 1;
const HEADER_SIZE: u64 = 4 + 2 + 1 + 1 + 2 + 8 + 4 + 4; // magic + version + kind + reserved + type_id + total_len + chunk_size + chunk_count
const FOOTER_SIZE: u64 = 4 + 4 + 4; // magic + chunk_count + checksum
/// Default chunk size (1 MiB) used when callers want a reasonable starting point.
pub const DEFAULT_CHUNK_SIZE: u32 = 1024 * 1024; // 1 MiB

/// Identifies whether the large value is typed or opaque.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LargeValueKind {
    /// Opaque BLOB without a type identifier.
    Blob,
    /// Typed payload with a user-provided type identifier.
    Typed(u16),
}

/// Metadata describing a large value container.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LargeValueMeta {
    /// Kind of payload (typed vs blob).
    pub kind: LargeValueKind,
    /// Total length of the payload in bytes.
    pub total_len: u64,
    /// Maximum chunk size in bytes.
    pub chunk_size: u32,
}

/// Per-chunk information returned by the reader.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LargeValueChunkInfo {
    /// Zero-based chunk index.
    pub index: u32,
    /// Whether this chunk is the last one.
    pub is_last: bool,
}

fn write_header(file: &mut File, meta: &LargeValueMeta, chunk_count: u32) -> Result<()> {
    let mut buf = [0u8; HEADER_SIZE as usize];
    buf[0..4].copy_from_slice(HEADER_MAGIC);
    buf[4..6].copy_from_slice(&VERSION.to_le_bytes());
    buf[6] = match meta.kind {
        LargeValueKind::Blob => 0,
        LargeValueKind::Typed(_) => 1,
    };
    buf[7] = 0; // reserved
    let type_id = match meta.kind {
        LargeValueKind::Blob => 0,
        LargeValueKind::Typed(id) => id,
    };
    buf[8..10].copy_from_slice(&type_id.to_le_bytes());
    buf[10..18].copy_from_slice(&meta.total_len.to_le_bytes());
    buf[18..22].copy_from_slice(&meta.chunk_size.to_le_bytes());
    buf[22..26].copy_from_slice(&chunk_count.to_le_bytes());
    file.seek(SeekFrom::Start(0))?;
    file.write_all(&buf)?;
    Ok(())
}

fn read_header(file: &mut File) -> Result<(LargeValueMeta, u32)> {
    let mut buf = [0u8; HEADER_SIZE as usize];
    file.seek(SeekFrom::Start(0))?;
    file.read_exact(&mut buf)?;

    if &buf[0..4] != HEADER_MAGIC {
        return Err(Error::InvalidFormat(
            "invalid large_value header magic".into(),
        ));
    }
    let version = u16::from_le_bytes(buf[4..6].try_into().unwrap());
    if version != VERSION {
        return Err(Error::InvalidFormat(format!(
            "unsupported large_value version: {version}"
        )));
    }
    let kind = match buf[6] {
        0 => LargeValueKind::Blob,
        1 => {
            let id = u16::from_le_bytes(buf[8..10].try_into().unwrap());
            LargeValueKind::Typed(id)
        }
        other => {
            return Err(Error::InvalidFormat(format!(
                "unknown large_value kind: {other}"
            )))
        }
    };

    let total_len = u64::from_le_bytes(buf[10..18].try_into().unwrap());
    let chunk_size = u32::from_le_bytes(buf[18..22].try_into().unwrap());
    if chunk_size == 0 {
        return Err(Error::InvalidFormat("chunk_size must be > 0".into()));
    }
    let chunk_count = u32::from_le_bytes(buf[22..26].try_into().unwrap());

    Ok((
        LargeValueMeta {
            kind,
            total_len,
            chunk_size,
        },
        chunk_count,
    ))
}

fn read_footer(file: &mut File, footer_start: u64) -> Result<(u32, u32)> {
    file.seek(SeekFrom::Start(footer_start))?;
    let mut buf = [0u8; FOOTER_SIZE as usize];
    file.read_exact(&mut buf)?;
    if &buf[0..4] != FOOTER_MAGIC {
        return Err(Error::InvalidFormat(
            "invalid large_value footer magic".into(),
        ));
    }
    let chunk_count = u32::from_le_bytes(buf[4..8].try_into().unwrap());
    let checksum = u32::from_le_bytes(buf[8..12].try_into().unwrap());
    Ok((chunk_count, checksum))
}

fn build_footer(chunk_count: u32, checksum: u32) -> [u8; FOOTER_SIZE as usize] {
    let mut buf = [0u8; FOOTER_SIZE as usize];
    buf[0..4].copy_from_slice(FOOTER_MAGIC);
    buf[4..8].copy_from_slice(&chunk_count.to_le_bytes());
    buf[8..12].copy_from_slice(&checksum.to_le_bytes());
    buf
}

/// Writer for chunked large values.
pub struct LargeValueWriter {
    path: PathBuf,
    writer: BufWriter<File>,
    meta: LargeValueMeta,
    chunk_index: u32,
    written: u64,
    hasher: Hasher,
    finished: bool,
}

impl LargeValueWriter {
    /// Creates a new chunked writer at the given path.
    pub fn create(path: &Path, meta: LargeValueMeta) -> Result<Self> {
        if meta.chunk_size == 0 {
            return Err(Error::InvalidFormat("chunk_size must be > 0".into()));
        }
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let mut file = OpenOptions::new()
            .write(true)
            .read(true)
            .create(true)
            .truncate(true)
            .open(path)?;
        write_header(&mut file, &meta, 0)?;

        Ok(Self {
            path: path.to_path_buf(),
            writer: BufWriter::new(file),
            meta,
            chunk_index: 0,
            written: 0,
            hasher: Hasher::new(),
            finished: false,
        })
    }

    /// Returns the metadata describing this large value.
    pub fn meta(&self) -> LargeValueMeta {
        self.meta
    }

    /// Returns remaining bytes expected before the writer reaches total_len.
    pub fn remaining(&self) -> u64 {
        self.meta
            .total_len
            .saturating_sub(self.written.min(self.meta.total_len))
    }

    /// Maximum chunk size permitted for this writer.
    pub fn chunk_size(&self) -> u32 {
        self.meta.chunk_size
    }

    /// Writes a single chunk. Chunks must respect the configured chunk_size and total_len.
    pub fn write_chunk(&mut self, chunk: &[u8]) -> Result<()> {
        if self.finished {
            return Err(Error::InvalidFormat("writer already finished".into()));
        }
        if chunk.is_empty() {
            return Err(Error::InvalidFormat("chunk must not be empty".into()));
        }
        if chunk.len() as u32 > self.meta.chunk_size {
            return Err(Error::InvalidFormat("chunk exceeds chunk_size".into()));
        }
        if self.written + chunk.len() as u64 > self.meta.total_len {
            return Err(Error::InvalidFormat(
                "chunk writes exceed declared total_len".into(),
            ));
        }

        let mut len_buf = [0u8; 8];
        len_buf[..4].copy_from_slice(&self.chunk_index.to_le_bytes());
        len_buf[4..].copy_from_slice(&(chunk.len() as u32).to_le_bytes());

        self.writer.write_all(&len_buf)?;
        self.writer.write_all(chunk)?;

        self.hasher.update(&len_buf);
        self.hasher.update(chunk);
        self.written += chunk.len() as u64;
        self.chunk_index += 1;
        Ok(())
    }

    /// Finalizes the writer, writing footer and updating header metadata.
    pub fn finish(mut self) -> Result<()> {
        if self.finished {
            return Err(Error::InvalidFormat("writer already finished".into()));
        }
        if self.written != self.meta.total_len {
            return Err(Error::InvalidFormat(
                "written length does not match total_len".into(),
            ));
        }

        let checksum = self.hasher.finalize();
        let footer = build_footer(self.chunk_index, checksum);
        self.writer.write_all(&footer)?;
        self.writer.flush()?;

        {
            let file = self.writer.get_mut();
            file.sync_all()?;
            // Rewrite header chunk_count with the final count.
            write_header(file, &self.meta, self.chunk_index)?;
            file.sync_all()?;
        }

        self.finished = true;
        Ok(())
    }

    /// Cancels the writer and removes the partially written file.
    pub fn cancel(self) -> Result<()> {
        // Drop the writer to close the file handle.
        drop(self.writer);
        let _ = remove_file(&self.path);
        Ok(())
    }
}

/// Reader for chunked large values. Maintains O(chunk_size) memory.
pub struct LargeValueReader {
    reader: BufReader<File>,
    meta: LargeValueMeta,
    footer_chunk_count: u32,
    footer_checksum: u32,
    hasher: Option<Hasher>,
    next_index: u32,
    remaining: u64,
    footer_start: u64,
    done: bool,
}

impl LargeValueReader {
    /// Opens a chunked large value file and validates header/footer.
    pub fn open(path: &Path) -> Result<Self> {
        let mut file = OpenOptions::new().read(true).open(path)?;
        let file_len = file.metadata()?.len();
        if file_len < HEADER_SIZE + FOOTER_SIZE {
            return Err(Error::InvalidFormat(
                "large_value file too small for header/footer".into(),
            ));
        }

        let (meta, header_chunk_count) = read_header(&mut file)?;
        let footer_start = file_len
            .checked_sub(FOOTER_SIZE)
            .ok_or_else(|| Error::InvalidFormat("file shorter than footer".into()))?;
        let (footer_chunk_count, footer_checksum) = read_footer(&mut file, footer_start)?;
        if header_chunk_count != 0 && header_chunk_count != footer_chunk_count {
            return Err(Error::InvalidFormat(
                "header/footer chunk counts do not match".into(),
            ));
        }

        file.seek(SeekFrom::Start(HEADER_SIZE))?;

        Ok(Self {
            reader: BufReader::new(file),
            meta,
            footer_chunk_count,
            footer_checksum,
            hasher: Some(Hasher::new()),
            next_index: 0,
            remaining: meta.total_len,
            footer_start,
            done: false,
        })
    }

    /// Returns the metadata of the large value.
    pub fn meta(&self) -> LargeValueMeta {
        self.meta
    }

    fn finalize_checksum(&mut self) -> Result<()> {
        if self.done {
            return Ok(());
        }
        let hasher = self
            .hasher
            .take()
            .ok_or_else(|| Error::InvalidFormat("reader checksum already finalized".into()))?;
        let computed = hasher.finalize();
        if computed != self.footer_checksum {
            return Err(Error::ChecksumMismatch);
        }
        if self.next_index != self.footer_chunk_count {
            return Err(Error::InvalidFormat(
                "chunk count mismatch at end of stream".into(),
            ));
        }
        self.done = true;
        Ok(())
    }

    /// Reads the next chunk, returning `Ok(None)` when the stream ends.
    pub fn next_chunk(&mut self) -> Result<Option<(LargeValueChunkInfo, Vec<u8>)>> {
        if self.done {
            return Ok(None);
        }
        if self.remaining == 0 {
            self.finalize_checksum()?;
            return Ok(None);
        }

        let pos = self.reader.stream_position()?;
        if pos + 8 > self.footer_start {
            return Err(Error::InvalidFormat(
                "unexpected end before footer while reading chunk header".into(),
            ));
        }

        let mut len_buf = [0u8; 8];
        self.reader.read_exact(&mut len_buf)?;
        let chunk_index = u32::from_le_bytes(len_buf[..4].try_into().unwrap());
        let chunk_len = u32::from_le_bytes(len_buf[4..].try_into().unwrap());

        if chunk_index != self.next_index {
            return Err(Error::InvalidFormat(
                "chunk index out of sequence in large_value".into(),
            ));
        }
        if chunk_len as u64 > self.remaining {
            return Err(Error::InvalidFormat(
                "chunk length exceeds remaining payload".into(),
            ));
        }
        if chunk_len > self.meta.chunk_size {
            return Err(Error::InvalidFormat(
                "chunk length exceeds declared chunk_size".into(),
            ));
        }
        let after_chunk = self.reader.stream_position()? + chunk_len as u64;
        if after_chunk > self.footer_start {
            return Err(Error::InvalidFormat(
                "chunk overruns footer boundary".into(),
            ));
        }

        let mut data = vec![0u8; chunk_len as usize];
        self.reader.read_exact(&mut data)?;

        if let Some(hasher) = &mut self.hasher {
            hasher.update(&len_buf);
            hasher.update(&data);
        }

        self.remaining -= chunk_len as u64;
        self.next_index += 1;
        let is_last = self.remaining == 0;
        if is_last {
            self.finalize_checksum()?;
        }

        Ok(Some((
            LargeValueChunkInfo {
                index: chunk_index,
                is_last,
            },
            data,
        )))
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn blob_meta(total: u64, chunk_size: u32) -> LargeValueMeta {
        LargeValueMeta {
            kind: LargeValueKind::Blob,
            total_len: total,
            chunk_size,
        }
    }

    #[test]
    fn writes_and_reads_blob_chunks() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("blob.lv");
        let data = b"abcdefghi";

        {
            let mut writer =
                LargeValueWriter::create(&path, blob_meta(data.len() as u64, 4)).unwrap();
            writer.write_chunk(&data[..4]).unwrap();
            writer.write_chunk(&data[4..8]).unwrap();
            writer.write_chunk(&data[8..]).unwrap();
            writer.finish().unwrap();
        }

        let mut reader = LargeValueReader::open(&path).unwrap();
        let mut collected = Vec::new();
        while let Some((info, chunk)) = reader.next_chunk().unwrap() {
            collected.extend_from_slice(&chunk);
            if info.is_last {
                assert_eq!(info.index, 2);
            }
        }
        assert_eq!(collected, data);
        assert_eq!(reader.meta().total_len, data.len() as u64);
    }

    #[test]
    fn typed_payload_roundtrip_and_partial_read() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("typed.lv");
        let data = b"012345";
        let meta = LargeValueMeta {
            kind: LargeValueKind::Typed(42),
            total_len: data.len() as u64,
            chunk_size: 4,
        };

        {
            let mut writer = LargeValueWriter::create(&path, meta).unwrap();
            writer.write_chunk(&data[..4]).unwrap();
            writer.write_chunk(&data[4..]).unwrap();
            assert_eq!(writer.remaining(), 0);
            writer.finish().unwrap();
        }

        let mut reader = LargeValueReader::open(&path).unwrap();
        assert!(matches!(reader.meta().kind, LargeValueKind::Typed(42)));

        // Partial read: only consume first chunk, ensure iterator can continue.
        let first = reader.next_chunk().unwrap().unwrap();
        assert_eq!(first.0.index, 0);
        assert!(!first.0.is_last);
        assert_eq!(first.1, b"0123");

        let second = reader.next_chunk().unwrap().unwrap();
        assert_eq!(second.0.index, 1);
        assert!(second.0.is_last);
        assert_eq!(second.1, b"45");
        assert!(reader.next_chunk().unwrap().is_none());
    }

    #[test]
    fn detects_checksum_mismatch() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("blob.lv");
        {
            let mut writer = LargeValueWriter::create(&path, blob_meta(3, 4)).unwrap();
            writer.write_chunk(b"abc").unwrap();
            writer.finish().unwrap();
        }

        // Corrupt one byte in the body.
        {
            let mut file = OpenOptions::new()
                .read(true)
                .write(true)
                .open(&path)
                .unwrap();
            file.seek(SeekFrom::Start(HEADER_SIZE + 8 + 1)).unwrap(); // header + chunk header + 1 byte into payload
            let mut b = [0u8; 1];
            file.read_exact(&mut b).unwrap();
            file.seek(SeekFrom::Current(-1)).unwrap();
            file.write_all(&[b[0] ^ 0xAA]).unwrap();
            file.sync_all().unwrap();
        }

        let mut reader = LargeValueReader::open(&path).unwrap();
        let err = reader.next_chunk().unwrap_err();
        assert!(matches!(err, Error::ChecksumMismatch));
    }

    #[test]
    fn cancel_removes_file() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("blob.lv");
        {
            let writer = LargeValueWriter::create(&path, blob_meta(3, 4)).unwrap();
            writer.cancel().unwrap();
        }
        assert!(!path.exists());
    }
}
