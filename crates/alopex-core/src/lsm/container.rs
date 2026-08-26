//! Convergence of a running LSM sidecar into one self-contained `.alopex` container.
//!
//! # なぜこのモジュールがあるか
//!
//! `docs-public/tech/file-format-comparison.md` の「Alopex 単一最終ファイル方針」は
//! *稼働中は WAL + 現行 `.alopex` の2本立て、**安定後は `.alopex` 単体で完全状態***
//! と約束している。v0.8.7 までは稼働用の作業ディレクトリ `X.alopex.d/`
//! （WAL リングバッファ + `sst/*.sst` + `checkpoint.meta`）しか存在せず、
//! `X.alopex` は 0 バイトの存在確認マーカーでしかなかった（issue #178）。
//!
//! このモジュールは「収束点（converge）」で作業領域の内容を **既に出荷済みの統一
//! コンテナ形式** [`crate::storage::format`] へ書き出し、逆に `.alopex` 単体しか
//! 無い状態から作業領域を再構築（rehydrate）する。
//!
//! # 物理レイアウト
//!
//! ```text
//! [FileHeader 64B  : "ALPX" | FileVersion::CURRENT | Crc32 | Snappy | flags]
//! [Section 0..N-1  : SectionType::SSTable,  CompressionAlgorithm::None, `<id>.sst` の生バイト]
//! [Section N       : SectionType::Metadata, CompressionAlgorithm::None, LsmManifest(bincode)]
//! [SectionIndex    : 4B count + SectionEntry(40B) * count]
//! [FileFooter 64B  : ... | wal_sequence_number = converged_lsn | footer_crc32 | "XPLA"]
//! ```
//!
//! SSTable セクションは **無圧縮でバイト同一** に格納する（設計裁定 D3）。SSTable は
//! 既にブロック単位で圧縮済みなので二重圧縮に意味がなく、バイト同一なら rehydrate が
//! 単純コピーで済み、`SSTableReader` / `SSTableCursor` / `BufferPool` を無改造で使える。
//!
//! WASM ターゲットではコンテナの書き出し/復元は行わない（`storage::format::writer` が
//! native 専用のため）。ポリシー型だけを共有し、収束処理は native に閉じる。

use std::path::{Path, PathBuf};

use bincode::Options;
use serde::{Deserialize, Serialize};

use crate::compaction::leveled::{KeyRange, SSTableMeta};
use crate::error::{Error, Result};
use crate::storage::format::bincode_config;
#[cfg(not(target_arch = "wasm32"))]
use crate::storage::format::{FOOTER_SIZE, HEADER_SIZE};

#[cfg(not(target_arch = "wasm32"))]
use std::fs::{self, File, OpenOptions};
#[cfg(not(target_arch = "wasm32"))]
use std::io::Write;
#[cfg(not(target_arch = "wasm32"))]
use std::time::{SystemTime, UNIX_EPOCH};
#[cfg(not(target_arch = "wasm32"))]
use tracing::warn;

#[cfg(not(target_arch = "wasm32"))]
use crate::lsm::checkpoint::{save_checkpoint_meta, CheckpointMeta};
#[cfg(not(target_arch = "wasm32"))]
use crate::lsm::sstable::{sstable_format_version, SSTableReader};
#[cfg(not(target_arch = "wasm32"))]
use crate::lsm::wal::{WalConfig, WalWriter, WAL_FORMAT_VERSION};
#[cfg(not(target_arch = "wasm32"))]
use crate::storage::compression::CompressionAlgorithm;
#[cfg(not(target_arch = "wasm32"))]
use crate::storage::format::{
    AlopexFileReader, AlopexFileWriter, FileFlags, FileReader, FileSource, FileVersion, SectionType,
};

/// Manifest 形式のバージョン。前方互換を明示的に拒否するために使う。
pub const LSM_MANIFEST_VERSION: u32 = 1;

/// The WAL file name inside a sidecar working directory.
pub const SIDECAR_WAL_FILE: &str = "lsm.wal";
/// The checkpoint metadata file name inside a sidecar working directory.
pub const SIDECAR_CHECKPOINT_FILE: &str = "checkpoint.meta";
/// The SSTable sub-directory inside a sidecar working directory.
pub const SIDECAR_SST_DIR: &str = "sst";

/// いつ `.alopex` コンテナへ収束するかのポリシー。
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum ConvergePolicy {
    /// 収束しない（サーバ運用や素のディレクトリ運用と完全に同じ挙動）。
    Never,
    /// データディレクトリが `X.alopex.d` 形式のときだけ `X.alopex` へ収束する（既定）。
    #[default]
    SidecarOnly,
    /// 常に指定パスのコンテナへ収束する。
    Always {
        /// 収束先のコンテナパス。
        container: PathBuf,
    },
}

/// `converge()` の実行結果。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConvergeResult {
    /// コンテナを書き出したか（対象外/無変更なら false）。
    pub container_written: bool,
    /// 収束時点の LSN（= `ts_oracle.current_timestamp()`）。
    pub converged_lsn: u64,
    /// 書き出したコンテナのバイト数。
    pub bytes_written: u64,
    /// コンテナに格納した SSTable の数。
    pub sstable_count: usize,
    /// 所要時間（ms）。
    pub duration_ms: u64,
}

impl ConvergeResult {
    /// コンテナを書かずに終わった収束の結果を作る。
    pub(crate) fn skipped(converged_lsn: u64) -> Self {
        Self {
            container_written: false,
            converged_lsn,
            bytes_written: 0,
            sstable_count: 0,
            duration_ms: 0,
        }
    }
}

/// コンテナ内の 1 SSTable を指す manifest エントリ。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LsmManifestTable {
    /// 展開先ファイル名 `sst/{file_id}.sst` の識別子。
    pub file_id: u64,
    /// LSM レベル。
    pub level: u32,
    /// コンテナ内 `SectionEntry.section_id`。
    pub section_id: u32,
    /// SSTable のバイト数（無圧縮なのでセクション長と一致する）。
    pub size_bytes: u64,
    /// エントリ数（診断用）。
    pub entry_count: u64,
    /// 最小キー。
    pub first_key: Vec<u8>,
    /// 最大キー。
    pub last_key: Vec<u8>,
}

/// `.alopex` コンテナの Metadata セクションに載る LSM manifest。
///
/// 契約 L69「MANIFEST 相当を `.alopex` footer に包含する設計」を、footer の
/// `wal_sequence_number` と本 manifest の組で満たす。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LsmManifest {
    /// Manifest 形式バージョン（= [`LSM_MANIFEST_VERSION`]）。
    pub manifest_version: u32,
    /// 収束時点の LSN。
    pub converged_lsn: u64,
    /// 復元後に払い出すべき次のタイムスタンプ。
    pub next_timestamp: u64,
    /// 収束時刻（epoch ms）。
    pub created_at_ms: u64,
    /// 収束時の SSTable 形式バージョン。
    pub sstable_format_version: u16,
    /// 収束時の WAL 形式バージョン。
    pub wal_format_version: u16,
    /// 格納した SSTable 一覧。
    pub tables: Vec<LsmManifestTable>,
}

impl LsmManifest {
    /// Manifest を決定的な bincode 表現へ符号化する。
    pub fn encode(&self) -> Result<Vec<u8>> {
        bincode_config()
            .serialize(self)
            .map_err(|err| Error::InvalidFormat(format!("lsm manifest encode failed: {err}")))
    }

    /// Manifest を復号し、未来のバージョンを明示的に拒否する。
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let manifest: Self = bincode_config()
            .deserialize(bytes)
            .map_err(|err| Error::InvalidFormat(format!("lsm manifest decode failed: {err}")))?;
        if manifest.manifest_version > LSM_MANIFEST_VERSION {
            return Err(Error::InvalidFormat(format!(
                "lsm manifest version {} is newer than supported version {LSM_MANIFEST_VERSION}",
                manifest.manifest_version
            )));
        }
        Ok(manifest)
    }
}

/// Resolve the container path a data directory converges into, if any.
///
/// `SidecarOnly` only matches the `X.alopex.d` sidecar shape produced by
/// `alopex_embedded::disk_data_dir_path`, so plain directories (the server, and
/// `Database::open("./mydb")`) never grow a container.
pub fn container_path_for(data_dir: &Path, policy: &ConvergePolicy) -> Option<PathBuf> {
    match policy {
        ConvergePolicy::Never => None,
        ConvergePolicy::Always { container } => Some(container.clone()),
        ConvergePolicy::SidecarOnly => {
            if data_dir.extension().and_then(|ext| ext.to_str()) != Some("d") {
                return None;
            }
            let candidate = data_dir.with_extension("");
            if candidate.extension().and_then(|ext| ext.to_str()) == Some("alopex") {
                Some(candidate)
            } else {
                None
            }
        }
    }
}

/// Reconstruct level metadata from a manifest without reopening the SSTables.
pub fn manifest_levels(manifest: &LsmManifest, max_levels: usize) -> Vec<Vec<SSTableMeta>> {
    let mut levels = vec![Vec::new(); max_levels];
    if max_levels == 0 {
        return levels;
    }
    for table in &manifest.tables {
        let level = (table.level as usize).min(max_levels - 1);
        levels[level].push(SSTableMeta {
            id: table.file_id,
            level,
            size_bytes: table.size_bytes,
            key_range: KeyRange {
                first_key: table.first_key.clone(),
                last_key: table.last_key.clone(),
            },
        });
    }
    levels
}

/// Whether a sidecar working directory is still the authoritative copy.
///
/// The WAL file is the single deciding artifact (裁定 D7): present means the
/// sidecar is live and wins over the container; absent means the sidecar is
/// either gone or a half-pruned leftover that must be discarded.
#[cfg(not(target_arch = "wasm32"))]
pub fn sidecar_is_live(data_dir: &Path) -> bool {
    data_dir.join(SIDECAR_WAL_FILE).exists()
}

/// Whether `path` is too small to be a real container.
///
/// v0.8.6 wrote a 0-byte `.alopex` marker next to the sidecar; anything shorter
/// than a header plus footer cannot be a container and is treated as "absent".
#[cfg(not(target_arch = "wasm32"))]
pub fn is_legacy_marker(path: &Path) -> bool {
    match fs::metadata(path) {
        Ok(meta) => meta.len() < (HEADER_SIZE + FOOTER_SIZE) as u64,
        Err(_) => true,
    }
}

/// fsync a directory so entry creations/removals inside it become durable.
#[cfg(all(not(target_arch = "wasm32"), not(windows)))]
pub(crate) fn sync_dir(dir: &Path) -> Result<()> {
    if dir.as_os_str().is_empty() || !dir.is_dir() {
        return Ok(());
    }
    File::open(dir)?.sync_all()?;
    Ok(())
}

/// `std::fs::File::open` cannot open a directory for syncing on Windows.
#[cfg(windows)]
pub(crate) fn sync_dir(_dir: &Path) -> Result<()> {
    // ponytail: add a narrow Win32 directory-flush wrapper if power-loss
    // durability of the directory entry becomes a Windows requirement.
    Ok(())
}

/// fsync the directory that contains `path`.
///
/// `AlopexFileWriter::finalize` fsyncs the file body before renaming but not the
/// parent directory, so a power cut could otherwise lose the rename itself.
#[cfg(not(target_arch = "wasm32"))]
pub(crate) fn sync_parent_dir(path: &Path) -> Result<()> {
    match path.parent() {
        Some(parent) => sync_dir(parent),
        None => Ok(()),
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(not(target_arch = "wasm32"))]
fn sstable_path(sst_dir: &Path, file_id: u64) -> PathBuf {
    sst_dir.join(format!("{file_id}.sst"))
}

/// Write every live SSTable plus a manifest into one atomic `.alopex` container.
///
/// The writer builds `<container>.alopex.tmp`, fsyncs it, then renames — so a
/// crash mid-write can only leave the previous container or a temp file, never a
/// half-written container.
#[cfg(not(target_arch = "wasm32"))]
pub(crate) fn write_container(
    container: &Path,
    sst_dir: &Path,
    tables: &[SSTableMeta],
    converged_lsn: u64,
) -> Result<ConvergeResult> {
    let started = std::time::Instant::now();
    let created_at_ms = now_ms();

    let mut writer =
        AlopexFileWriter::new(container.to_path_buf(), FileVersion::CURRENT, FileFlags(0))?;

    let mut manifest_tables = Vec::with_capacity(tables.len());
    let mut total_entries = 0u64;
    let mut total_bytes = 0u64;

    let build = (|| -> Result<()> {
        for meta in tables {
            let path = sstable_path(sst_dir, meta.id);
            let bytes = fs::read(&path)?;
            let entry_count = SSTableReader::open(&path)?.entry_count();
            // NOTE: key ranges are intentionally *not* registered with the writer.
            // L0 tables legitimately overlap, and `validate_key_range` would reject
            // them with `FormatError::KeyRangeOverlap` (裁定 D3 / risk log).
            let section_id = writer.add_section_with_compression(
                SectionType::SSTable,
                &bytes,
                CompressionAlgorithm::None,
            )?;
            total_entries = total_entries.saturating_add(entry_count);
            total_bytes = total_bytes.saturating_add(bytes.len() as u64);
            manifest_tables.push(LsmManifestTable {
                file_id: meta.id,
                level: meta.level as u32,
                section_id,
                size_bytes: bytes.len() as u64,
                entry_count,
                first_key: meta.key_range.first_key.clone(),
                last_key: meta.key_range.last_key.clone(),
            });
        }

        let manifest = LsmManifest {
            manifest_version: LSM_MANIFEST_VERSION,
            converged_lsn,
            next_timestamp: converged_lsn.saturating_add(1),
            created_at_ms,
            sstable_format_version: sstable_format_version(),
            wal_format_version: WAL_FORMAT_VERSION,
            tables: manifest_tables.clone(),
        };
        writer.add_section_with_compression(
            SectionType::Metadata,
            &manifest.encode()?,
            CompressionAlgorithm::None,
        )?;
        writer.update_stats(total_entries, total_bytes);
        writer.set_wal_sequence_number(converged_lsn);
        Ok(())
    })();

    if let Err(err) = build {
        let _ = writer.abort();
        return Err(err);
    }

    writer.finalize()?;
    sync_parent_dir(container)?;

    let bytes_written = fs::metadata(container).map(|meta| meta.len()).unwrap_or(0);
    Ok(ConvergeResult {
        container_written: true,
        converged_lsn,
        bytes_written,
        sstable_count: manifest_tables.len(),
        duration_ms: started.elapsed().as_millis() as u64,
    })
}

/// What [`rehydrate`] reconstructed from a container.
#[cfg(not(target_arch = "wasm32"))]
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RehydrateOutcome {
    /// Restored LSN; the timestamp oracle must resume above this value.
    pub converged_lsn: u64,
    /// Number of SSTables expanded into the sidecar.
    pub tables_restored: usize,
    /// Bytes copied out of the container.
    pub bytes_read: u64,
}

/// Rebuild a sidecar working directory from a self-contained container.
///
/// Validation is deliberately four-layered (裁定 D4): header magic + version,
/// footer reverse-magic + CRC, per-section CRC, and a **mandatory** manifest. A
/// container that holds SSTable sections but no readable manifest is an *error* —
/// silently opening it as an empty database would look exactly like total data
/// loss to the user.
#[cfg(not(target_arch = "wasm32"))]
pub(crate) fn rehydrate(
    container: &Path,
    data_dir: &Path,
    wal_config: &WalConfig,
) -> Result<RehydrateOutcome> {
    let reader = AlopexFileReader::open(FileSource::Path(container.to_path_buf()))?;
    reader.header().check_compatibility(&FileVersion::CURRENT)?;

    let entries = reader.section_index().entries.clone();
    let sstable_count = entries
        .iter()
        .filter(|entry| entry.section_type == SectionType::SSTable)
        .count();
    let metadata_sections: Vec<u32> = entries
        .iter()
        .filter(|entry| entry.section_type == SectionType::Metadata)
        .map(|entry| entry.section_id)
        .collect();

    let manifest = match read_manifest(&reader, &metadata_sections) {
        Some(result) => result?,
        None if sstable_count == 0 => {
            // No payload at all: a legitimately empty database. This is also the
            // shape the v0.1 compatibility fixtures use.
            fs::create_dir_all(data_dir.join(SIDECAR_SST_DIR))?;
            let converged_lsn = reader.footer().wal_sequence_number;
            finish_rehydrate(data_dir, wal_config, converged_lsn)?;
            return Ok(RehydrateOutcome {
                converged_lsn,
                tables_restored: 0,
                bytes_read: 0,
            });
        }
        None => {
            return Err(Error::InvalidFormat(format!(
                "container {} holds {sstable_count} SSTable section(s) but no LSM manifest; \
                 refusing to open it as an empty database",
                container.display()
            )));
        }
    };

    let sst_dir = data_dir.join(SIDECAR_SST_DIR);
    fs::create_dir_all(&sst_dir)?;

    let mut bytes_read = 0u64;
    for table in &manifest.tables {
        // Verify the stored bytes before trusting them.
        reader.validate_section(table.section_id)?;
        let bytes = reader.read_section(table.section_id)?;
        if bytes.len() as u64 != table.size_bytes {
            return Err(Error::InvalidFormat(format!(
                "container section {} length {} does not match manifest size {}",
                table.section_id,
                bytes.len(),
                table.size_bytes
            )));
        }
        bytes_read = bytes_read.saturating_add(bytes.len() as u64);

        let target = sstable_path(&sst_dir, table.file_id);
        let tmp = target.with_extension("sst.tmp");
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&tmp)?;
        file.write_all(&bytes)?;
        file.sync_data()?;
        drop(file);
        fs::rename(&tmp, &target)?;
    }
    sync_dir(&sst_dir)?;

    finish_rehydrate(data_dir, wal_config, manifest.converged_lsn)?;
    Ok(RehydrateOutcome {
        converged_lsn: manifest.converged_lsn,
        tables_restored: manifest.tables.len(),
        bytes_read,
    })
}

/// Read the manifest out of the first decodable Metadata section.
///
/// Returns `None` only when the container carries no Metadata section at all; a
/// present-but-broken manifest surfaces as `Some(Err(..))` so it can never be
/// mistaken for "empty database".
#[cfg(not(target_arch = "wasm32"))]
fn read_manifest(
    reader: &AlopexFileReader,
    metadata_sections: &[u32],
) -> Option<Result<LsmManifest>> {
    if metadata_sections.is_empty() {
        return None;
    }
    let mut last_error: Option<Error> = None;
    for section_id in metadata_sections {
        if let Err(err) = reader.validate_section(*section_id) {
            last_error = Some(err.into());
            continue;
        }
        match reader.read_section(*section_id) {
            Ok(bytes) => match LsmManifest::decode(&bytes) {
                Ok(manifest) => return Some(Ok(manifest)),
                Err(err) => last_error = Some(err),
            },
            Err(err) => last_error = Some(err.into()),
        }
    }
    Some(Err(last_error.unwrap_or_else(|| {
        Error::InvalidFormat("container metadata section is not an LSM manifest".to_owned())
    })))
}

/// Recreate `checkpoint.meta` and a fresh empty WAL so the normal recovery path
/// resumes the timestamp oracle above `converged_lsn` (裁定 D5).
#[cfg(not(target_arch = "wasm32"))]
fn finish_rehydrate(data_dir: &Path, wal_config: &WalConfig, converged_lsn: u64) -> Result<()> {
    let checkpoint_path = data_dir.join(SIDECAR_CHECKPOINT_FILE);
    save_checkpoint_meta(
        &checkpoint_path,
        &CheckpointMeta::new(converged_lsn, now_ms()),
    )?;

    let wal_path = data_dir.join(SIDECAR_WAL_FILE);
    let writer = WalWriter::create(
        &wal_path,
        wal_config.clone(),
        1,
        converged_lsn.saturating_add(1),
    )?;
    drop(writer);
    sync_parent_dir(&wal_path)?;
    Ok(())
}

/// Delete a sidecar working directory that a container has fully superseded.
///
/// Removal order is fixed (裁定 D8): the WAL goes first, so a crash mid-prune
/// leaves a directory that [`sidecar_is_live`] classifies as dead and the next
/// open safely discards and re-hydrates it.
#[cfg(not(target_arch = "wasm32"))]
pub(crate) fn prune_sidecar(data_dir: &Path) -> Result<()> {
    if !data_dir.exists() {
        return Ok(());
    }
    let wal_path = data_dir.join(SIDECAR_WAL_FILE);
    if wal_path.exists() {
        fs::remove_file(&wal_path)?;
        sync_parent_dir(&wal_path)?;
    }
    let checkpoint_path = data_dir.join(SIDECAR_CHECKPOINT_FILE);
    if checkpoint_path.exists() {
        fs::remove_file(&checkpoint_path)?;
    }
    let sst_dir = data_dir.join(SIDECAR_SST_DIR);
    if sst_dir.exists() {
        fs::remove_dir_all(&sst_dir)?;
    }
    fs::remove_dir_all(data_dir)?;
    sync_parent_dir(data_dir)?;
    Ok(())
}

/// Discard a dead sidecar left over from an interrupted prune.
#[cfg(not(target_arch = "wasm32"))]
pub(crate) fn discard_dead_sidecar(data_dir: &Path) {
    if let Err(err) = fs::remove_dir_all(data_dir) {
        warn!(error = %err, path = ?data_dir, "failed to discard a dead sidecar directory");
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::*;
    use crate::kv::{KVStore, KVTransaction};
    use crate::lsm::wal::SyncMode;
    use crate::lsm::{LsmKV, LsmKVConfig};
    use crate::types::TxnMode;
    use tempfile::tempdir;

    fn test_config() -> LsmKVConfig {
        LsmKVConfig {
            wal: WalConfig {
                segment_size: 4096,
                max_segments: 2,
                sync_mode: SyncMode::NoSync,
            },
            ..Default::default()
        }
    }

    #[test]
    fn sidecar_only_policy_matches_only_the_alopex_sidecar_shape() {
        let policy = ConvergePolicy::SidecarOnly;
        assert_eq!(
            container_path_for(Path::new("/tmp/mydb.alopex.d"), &policy),
            Some(PathBuf::from("/tmp/mydb.alopex"))
        );
        assert_eq!(
            container_path_for(Path::new("/tmp/plaindir"), &policy),
            None
        );
        assert_eq!(container_path_for(Path::new("/tmp/other.d"), &policy), None);
        assert_eq!(
            container_path_for(Path::new("/tmp/mydb.alopex.d"), &ConvergePolicy::Never),
            None
        );
    }

    #[test]
    fn legacy_zero_byte_marker_is_not_a_container() {
        let dir = tempdir().unwrap();
        let marker = dir.path().join("db.alopex");
        fs::write(&marker, b"").unwrap();
        assert!(is_legacy_marker(&marker));
        assert!(is_legacy_marker(&dir.path().join("missing.alopex")));
    }

    /// A plain directory is what `alopex-server` passes to `open_with_config`.
    /// Opening and dropping one must not create, move, or delete anything.
    #[test]
    fn plain_directory_open_and_drop_leaves_the_layout_untouched() {
        let dir = tempdir().unwrap();
        let data_dir = dir.path().join("server-data");

        let before = {
            let (store, _recovery) =
                LsmKV::open_with_config(&data_dir, test_config()).expect("open");
            assert_eq!(store.container_path(), None);
            let mut txn = store.begin(TxnMode::ReadWrite).unwrap();
            txn.put(b"k".to_vec(), b"v".to_vec()).unwrap();
            txn.commit_self().unwrap();
            drop(store);
            let mut names: Vec<String> = fs::read_dir(&data_dir)
                .unwrap()
                .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
                .collect();
            names.sort();
            names
        };

        assert!(before.contains(&SIDECAR_WAL_FILE.to_owned()));
        assert!(!dir.path().join("server-data.alopex").exists());

        let (store, _recovery) = LsmKV::open_with_config(&data_dir, test_config()).expect("reopen");
        drop(store);

        let mut after: Vec<String> = fs::read_dir(&data_dir)
            .unwrap()
            .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
            .collect();
        after.sort();
        assert_eq!(before, after, "plain-directory layout must not change");
    }

    #[test]
    fn manifest_rejects_a_newer_version() {
        let manifest = LsmManifest {
            manifest_version: LSM_MANIFEST_VERSION + 1,
            converged_lsn: 7,
            next_timestamp: 8,
            created_at_ms: 1,
            sstable_format_version: sstable_format_version(),
            wal_format_version: WAL_FORMAT_VERSION,
            tables: Vec::new(),
        };
        let bytes = manifest.encode().unwrap();
        let err = LsmManifest::decode(&bytes).unwrap_err();
        assert!(matches!(err, Error::InvalidFormat(_)), "got {err:?}");
    }

    #[test]
    fn manifest_round_trips() {
        let manifest = LsmManifest {
            manifest_version: LSM_MANIFEST_VERSION,
            converged_lsn: 42,
            next_timestamp: 43,
            created_at_ms: 99,
            sstable_format_version: sstable_format_version(),
            wal_format_version: WAL_FORMAT_VERSION,
            tables: vec![LsmManifestTable {
                file_id: 3,
                level: 0,
                section_id: 1,
                size_bytes: 128,
                entry_count: 4,
                first_key: b"a".to_vec(),
                last_key: b"z".to_vec(),
            }],
        };
        let decoded = LsmManifest::decode(&manifest.encode().unwrap()).unwrap();
        assert_eq!(decoded, manifest);
    }
}
