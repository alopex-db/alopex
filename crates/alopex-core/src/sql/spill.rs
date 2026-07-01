//! Generic external-sort spill primitives.

use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::fs::{self, File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::sync::Arc;

use crate::sql::stream::MemoryPolicy;
use crate::{Error, Result};

type KeyDecoder<K> = dyn Fn(&[u8]) -> Result<K>;
type RowDecoder<T> = dyn Fn(u64, &[u8]) -> Result<T>;
type KeyComparator<K> = dyn Fn(&K, &K) -> Ordering;
type RowId<T> = dyn Fn(&T) -> u64;

static SPILL_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Write one sorted spill run to disk and clear the in-memory entries.
pub fn spill_run<T, K, C, R, EK, ER>(
    entries: &mut Vec<(T, K)>,
    policy: &MemoryPolicy,
    prefix: &str,
    mut compare_keys: C,
    row_id: R,
    encode_key: EK,
    encode_row: ER,
) -> Result<PathBuf>
where
    C: FnMut(&K, &K) -> Ordering,
    R: Fn(&T) -> u64,
    EK: Fn(&K) -> Vec<u8>,
    ER: Fn(&T) -> Vec<u8>,
{
    let directory = policy.spill_directory().ok_or_else(|| Error::SpillFailed {
        reason: "sort spill: spill directory not configured".into(),
    })?;
    ensure_spill_dir(directory)?;
    let (path, file) = create_spill_file(directory, prefix)?;
    let mut writer = BufWriter::new(file);

    entries.sort_by(|a, b| compare_keys(&a.1, &b.1));

    let mut bytes_written = 0u64;
    for (row, keys) in entries.iter() {
        let key_bytes = encode_key(keys);
        let row_bytes = encode_row(row);
        let key_len = u32::try_from(key_bytes.len()).map_err(|_| Error::SpillFailed {
            reason: "sort spill: sort key size exceeds u32::MAX".into(),
        })?;
        let row_len = u32::try_from(row_bytes.len()).map_err(|_| Error::SpillFailed {
            reason: "sort spill: row size exceeds u32::MAX".into(),
        })?;

        writer
            .write_all(&row_id(row).to_le_bytes())
            .map_err(|err| spill_io_error("sort spill", err))?;
        writer
            .write_all(&key_len.to_le_bytes())
            .map_err(|err| spill_io_error("sort spill", err))?;
        writer
            .write_all(&row_len.to_le_bytes())
            .map_err(|err| spill_io_error("sort spill", err))?;
        writer
            .write_all(&key_bytes)
            .map_err(|err| spill_io_error("sort spill", err))?;
        writer
            .write_all(&row_bytes)
            .map_err(|err| spill_io_error("sort spill", err))?;
        bytes_written = bytes_written
            .saturating_add(8)
            .saturating_add(4)
            .saturating_add(4)
            .saturating_add(key_bytes.len() as u64)
            .saturating_add(row_bytes.len() as u64);
    }

    writer
        .flush()
        .map_err(|err| spill_io_error("sort spill", err))?;
    policy.record_spill(bytes_written, 1);
    entries.clear();

    Ok(path)
}

/// Ensure the spill directory exists.
pub fn ensure_spill_dir(directory: &Path) -> Result<()> {
    fs::create_dir_all(directory).map_err(|err| spill_io_error("sort spill", err))?;
    Ok(())
}

/// Create a unique spill file in `directory` using `prefix`.
pub fn create_spill_file(directory: &Path, prefix: &str) -> Result<(PathBuf, File)> {
    for _ in 0..16 {
        let counter = SPILL_COUNTER.fetch_add(1, AtomicOrdering::Relaxed);
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let path = directory.join(format!("{prefix}-{timestamp}-{counter}.bin"));
        match OpenOptions::new().create_new(true).write(true).open(&path) {
            Ok(file) => return Ok((path, file)),
            Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(err) => return Err(spill_io_error("sort spill", err)),
        }
    }
    Err(Error::SpillFailed {
        reason: "sort spill: failed to allocate spill file".into(),
    })
}

/// Convert a spill I/O failure into the stable core spill error variant.
pub fn spill_io_error(operation: &str, err: impl std::fmt::Display) -> Error {
    Error::SpillFailed {
        reason: format!("{operation}: {err}"),
    }
}

/// One decoded entry from a spill run.
pub struct SpillEntry<T, K> {
    /// Decoded row payload.
    pub row: T,
    /// Decoded sort key payload.
    pub keys: K,
}

/// Reader for one spill run file.
pub struct SpillRunReader<T, K> {
    path: PathBuf,
    reader: BufReader<File>,
    decode_key: Arc<KeyDecoder<K>>,
    decode_row: Arc<RowDecoder<T>>,
}

impl<T, K> SpillRunReader<T, K> {
    /// Open a spill run and configure decoders for key and row payloads.
    pub fn open<DK, DR>(path: PathBuf, decode_key: DK, decode_row: DR) -> Result<Self>
    where
        DK: Fn(&[u8]) -> Result<K> + 'static,
        DR: Fn(u64, &[u8]) -> Result<T> + 'static,
    {
        Self::open_with_decoders(path, Arc::new(decode_key), Arc::new(decode_row))
    }

    fn open_with_decoders(
        path: PathBuf,
        decode_key: Arc<KeyDecoder<K>>,
        decode_row: Arc<RowDecoder<T>>,
    ) -> Result<Self> {
        let file = File::open(&path).map_err(|err| spill_io_error("sort spill", err))?;
        Ok(Self {
            path,
            reader: BufReader::new(file),
            decode_key,
            decode_row,
        })
    }

    /// Read the next spill entry, or `None` at end of run.
    pub fn next_entry(&mut self) -> Result<Option<SpillEntry<T, K>>> {
        let mut row_id_buf = [0u8; 8];
        match self.reader.read_exact(&mut row_id_buf) {
            Ok(()) => {}
            Err(err) if err.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
            Err(err) => return Err(spill_io_error("sort spill", err)),
        }
        let row_id = u64::from_le_bytes(row_id_buf);
        let key_len = self.read_u32()?;
        let row_len = self.read_u32()?;

        let mut key_bytes = vec![0u8; key_len as usize];
        self.reader
            .read_exact(&mut key_bytes)
            .map_err(|err| spill_io_error("sort spill", err))?;
        let mut row_bytes = vec![0u8; row_len as usize];
        self.reader
            .read_exact(&mut row_bytes)
            .map_err(|err| spill_io_error("sort spill", err))?;

        let keys = (self.decode_key)(&key_bytes)?;
        let row = (self.decode_row)(row_id, &row_bytes)?;

        Ok(Some(SpillEntry { row, keys }))
    }

    fn read_u32(&mut self) -> Result<u32> {
        let mut buf = [0u8; 4];
        self.reader
            .read_exact(&mut buf)
            .map_err(|err| spill_io_error("sort spill", err))?;
        Ok(u32::from_le_bytes(buf))
    }
}

impl<T, K> Drop for SpillRunReader<T, K> {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}

/// K-way merge iterator over sorted spill run files.
pub struct SpillMergeIterator<T, K> {
    compare: Arc<KeyComparator<K>>,
    row_id: Arc<RowId<T>>,
    readers: Vec<SpillRunReader<T, K>>,
    heap: BinaryHeap<SpillHeapItem<T, K>>,
}

impl<T, K> SpillMergeIterator<T, K> {
    /// Open spill runs and initialize the merge heap.
    pub fn new<C, R, DK, DR>(
        runs: Vec<PathBuf>,
        compare_keys: C,
        row_id: R,
        decode_key: DK,
        decode_row: DR,
    ) -> Result<Self>
    where
        C: Fn(&K, &K) -> Ordering + 'static,
        R: Fn(&T) -> u64 + 'static,
        DK: Fn(&[u8]) -> Result<K> + 'static,
        DR: Fn(u64, &[u8]) -> Result<T> + 'static,
    {
        let compare: Arc<KeyComparator<K>> = Arc::new(compare_keys);
        let row_id: Arc<RowId<T>> = Arc::new(row_id);
        let decode_key: Arc<KeyDecoder<K>> = Arc::new(decode_key);
        let decode_row: Arc<RowDecoder<T>> = Arc::new(decode_row);
        let mut readers = Vec::with_capacity(runs.len());
        let mut heap = BinaryHeap::new();

        for (idx, path) in runs.into_iter().enumerate() {
            let mut reader = SpillRunReader::open_with_decoders(
                path,
                Arc::clone(&decode_key),
                Arc::clone(&decode_row),
            )?;
            if let Some(entry) = reader.next_entry()? {
                heap.push(SpillHeapItem {
                    run_idx: idx,
                    row: entry.row,
                    keys: entry.keys,
                    compare: Arc::clone(&compare),
                    row_id: Arc::clone(&row_id),
                });
            }
            readers.push(reader);
        }

        Ok(Self {
            compare,
            row_id,
            readers,
            heap,
        })
    }

    /// Return the next globally sorted row from the spill runs.
    pub fn next_item(&mut self) -> Option<Result<T>> {
        let item = self.heap.pop()?;
        let row = item.row;
        let run_idx = item.run_idx;

        match self.readers[run_idx].next_entry() {
            Ok(Some(entry)) => {
                self.heap.push(SpillHeapItem {
                    run_idx,
                    row: entry.row,
                    keys: entry.keys,
                    compare: Arc::clone(&self.compare),
                    row_id: Arc::clone(&self.row_id),
                });
            }
            Ok(None) => {}
            Err(err) => return Some(Err(err)),
        }

        Some(Ok(row))
    }
}

impl<T, K> Iterator for SpillMergeIterator<T, K> {
    type Item = Result<T>;

    fn next(&mut self) -> Option<Self::Item> {
        self.next_item()
    }
}

struct SpillHeapItem<T, K> {
    run_idx: usize,
    row: T,
    keys: K,
    compare: Arc<KeyComparator<K>>,
    row_id: Arc<RowId<T>>,
}

impl<T, K> PartialEq for SpillHeapItem<T, K> {
    fn eq(&self, other: &Self) -> bool {
        (self.compare)(&self.keys, &other.keys) == Ordering::Equal
            && self.run_idx == other.run_idx
            && (self.row_id)(&self.row) == (self.row_id)(&other.row)
    }
}

impl<T, K> Eq for SpillHeapItem<T, K> {}

impl<T, K> PartialOrd for SpillHeapItem<T, K> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<T, K> Ord for SpillHeapItem<T, K> {
    fn cmp(&self, other: &Self) -> Ordering {
        let order = (self.compare)(&self.keys, &other.keys);
        let order = if order == Ordering::Equal {
            self.run_idx
                .cmp(&other.run_idx)
                .then_with(|| (self.row_id)(&self.row).cmp(&(self.row_id)(&other.row)))
        } else {
            order
        };
        order.reverse()
    }
}

#[cfg(test)]
mod tests {
    use super::{spill_io_error, spill_run, SpillMergeIterator, SpillRunReader};
    use crate::sql::stream::{MemoryPolicy, SpillPolicy};
    use crate::Error;

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct TestRow {
        row_id: u64,
        value: i64,
    }

    fn encode_i64(value: &i64) -> Vec<u8> {
        value.to_le_bytes().to_vec()
    }

    fn decode_i64(bytes: &[u8]) -> crate::Result<i64> {
        let data: [u8; 8] = bytes.try_into().map_err(|_| Error::SpillFailed {
            reason: "invalid i64 bytes".into(),
        })?;
        Ok(i64::from_le_bytes(data))
    }

    fn encode_row(row: &TestRow) -> Vec<u8> {
        row.value.to_le_bytes().to_vec()
    }

    fn decode_row(row_id: u64, bytes: &[u8]) -> crate::Result<TestRow> {
        Ok(TestRow {
            row_id,
            value: decode_i64(bytes)?,
        })
    }

    #[test]
    fn spill_run_writes_and_reader_reads_entries_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let policy = MemoryPolicy::new(
            Some(1),
            SpillPolicy::SpillToDisk {
                directory: dir.path().to_path_buf(),
            },
        );
        let mut entries = vec![
            (
                TestRow {
                    row_id: 2,
                    value: 20,
                },
                20,
            ),
            (
                TestRow {
                    row_id: 1,
                    value: 10,
                },
                10,
            ),
        ];

        let path = spill_run(
            &mut entries,
            &policy,
            "test-run",
            |left, right| left.cmp(right),
            |row| row.row_id,
            encode_i64,
            encode_row,
        )
        .unwrap();

        assert!(entries.is_empty());
        let mut reader = SpillRunReader::open(path.clone(), decode_i64, decode_row).unwrap();
        assert_eq!(
            reader
                .next_entry()
                .unwrap()
                .map(|entry| (entry.row, entry.keys)),
            Some((
                TestRow {
                    row_id: 1,
                    value: 10
                },
                10
            ))
        );
        assert_eq!(
            reader
                .next_entry()
                .unwrap()
                .map(|entry| (entry.row, entry.keys)),
            Some((
                TestRow {
                    row_id: 2,
                    value: 20
                },
                20
            ))
        );
        assert!(reader.next_entry().unwrap().is_none());
        drop(reader);
        assert!(!path.exists());
    }

    #[test]
    fn spill_merge_iterator_outputs_k_way_sorted_order() {
        let dir = tempfile::tempdir().unwrap();
        let policy = MemoryPolicy::new(
            Some(1),
            SpillPolicy::SpillToDisk {
                directory: dir.path().to_path_buf(),
            },
        );
        let mut left = vec![
            (
                TestRow {
                    row_id: 3,
                    value: 30,
                },
                30,
            ),
            (
                TestRow {
                    row_id: 1,
                    value: 10,
                },
                10,
            ),
        ];
        let mut right = vec![
            (
                TestRow {
                    row_id: 4,
                    value: 40,
                },
                40,
            ),
            (
                TestRow {
                    row_id: 2,
                    value: 20,
                },
                20,
            ),
        ];
        let run_a = spill_run(
            &mut left,
            &policy,
            "test-run",
            |left, right| left.cmp(right),
            |row| row.row_id,
            encode_i64,
            encode_row,
        )
        .unwrap();
        let run_b = spill_run(
            &mut right,
            &policy,
            "test-run",
            |left, right| left.cmp(right),
            |row| row.row_id,
            encode_i64,
            encode_row,
        )
        .unwrap();

        let mut iter = SpillMergeIterator::new(
            vec![run_a, run_b],
            |left, right| left.cmp(right),
            |row: &TestRow| row.row_id,
            decode_i64,
            decode_row,
        )
        .unwrap();

        let mut values = Vec::new();
        while let Some(row) = iter.next_item() {
            values.push(row.unwrap().value);
        }

        assert_eq!(values, vec![10, 20, 30, 40]);
    }

    #[test]
    fn spill_failure_returns_stable_error_variant() {
        let err = spill_io_error("sort spill", std::io::Error::other("disk full"));

        assert!(matches!(err, Error::SpillFailed { .. }));
        assert!(err.to_string().contains("sort spill"));
        assert!(err.to_string().contains("disk full"));
    }
}
