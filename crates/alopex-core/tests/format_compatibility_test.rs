#[cfg(not(target_arch = "wasm32"))]
use alopex_core::storage::compression::CompressionAlgorithm;
use alopex_core::storage::format::{
    AlopexFileReader, FileReader, FileSource, FileVersion, FormatError, HEADER_SIZE,
};
#[cfg(not(target_arch = "wasm32"))]
use alopex_core::storage::format::{AlopexFileWriter, FileFlags, SectionType};
#[cfg(not(target_arch = "wasm32"))]
use std::env;

#[cfg(not(target_arch = "wasm32"))]
use tempfile::tempdir;

const V0_1_GOLDEN: &[u8] = include_bytes!("data/compatibility/v0_1.alopex");

#[cfg(not(target_arch = "wasm32"))]
fn generate_v0_1_test_file() -> (tempfile::TempDir, std::path::PathBuf) {
    let dir = tempdir().expect("create tempdir");
    let path = dir.path().join("compat_v0_1.alopex");
    std::fs::write(&path, V0_1_GOLDEN).expect("write golden file");
    (dir, path)
}

#[cfg(target_arch = "wasm32")]
fn generate_v0_1_test_bytes() -> Vec<u8> {
    V0_1_GOLDEN.to_vec()
}

#[cfg(not(target_arch = "wasm32"))]
fn mutate_version(path: &std::path::Path, file_version: FileVersion) -> std::path::PathBuf {
    let mut bytes = std::fs::read(path).expect("read file");
    let mut header = [0u8; HEADER_SIZE];
    header.copy_from_slice(&bytes[..HEADER_SIZE]);
    let mut header =
        alopex_core::storage::format::FileHeader::from_bytes(&header).expect("parse header");
    header.version = file_version;
    let serialized = header.to_bytes();
    bytes[..HEADER_SIZE].copy_from_slice(&serialized);

    let bumped_path = path.with_file_name("newer_version.alopex");
    std::fs::write(&bumped_path, bytes).expect("write bumped file");
    bumped_path
}

#[cfg(target_arch = "wasm32")]
fn mutate_version_bytes(mut bytes: Vec<u8>, file_version: FileVersion) -> Vec<u8> {
    let mut header_bytes = [0u8; HEADER_SIZE];
    header_bytes.copy_from_slice(&bytes[..HEADER_SIZE]);
    let mut header =
        alopex_core::storage::format::FileHeader::from_bytes(&header_bytes).expect("parse header");
    header.version = file_version;
    let serialized = header.to_bytes();
    bytes[..HEADER_SIZE].copy_from_slice(&serialized);
    bytes
}

#[cfg(not(target_arch = "wasm32"))]
fn compression_from_env() -> CompressionAlgorithm {
    match env::var("ALOPEX_COMPAT_COMPRESSION")
        .unwrap_or_else(|_| "snappy".to_string())
        .as_str()
    {
        "none" => CompressionAlgorithm::None,
        "zstd" => CompressionAlgorithm::Zstd,
        "lz4" => CompressionAlgorithm::Lz4,
        _ => CompressionAlgorithm::Snappy,
    }
}

#[cfg(not(target_arch = "wasm32"))]
#[allow(clippy::match_like_matches_macro)]
fn compression_available(alg: CompressionAlgorithm) -> bool {
    match alg {
        CompressionAlgorithm::Zstd => cfg!(feature = "compression-zstd"),
        CompressionAlgorithm::Lz4 => cfg!(feature = "compression-lz4"),
        _ => true,
    }
}

#[cfg(not(target_arch = "wasm32"))]
#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn reads_v0_1_golden_file() {
    let (_dir, path) = generate_v0_1_test_file();
    let reader = AlopexFileReader::open(FileSource::Path(path.clone())).expect("open v0.1 file");
    assert_eq!(reader.header().version, FileVersion::new(0, 1, 0));
    reader.validate_all().expect("validate sections");
    assert_eq!(reader.section_index().entries.len(), 2);
    assert_eq!(reader.read_section(0).unwrap(), b"compat-meta-v0.1");
    assert_eq!(reader.read_section(1).unwrap(), b"compat-data-v0.1");
    // ensure footer layout aligns with index offsets
    assert!(
        reader.footer().section_index_offset >= alopex_core::storage::format::HEADER_SIZE as u64
    );
}

#[cfg(not(target_arch = "wasm32"))]
#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn newer_version_is_rejected() {
    let (_dir, path) = generate_v0_1_test_file();
    let bumped = mutate_version(
        &path,
        FileVersion::new(FileVersion::CURRENT.major + 1, 0, 0),
    );
    let err = match AlopexFileReader::open(FileSource::Path(bumped)) {
        Ok(_) => panic!("should reject newer"),
        Err(e) => e,
    };
    match err {
        FormatError::IncompatibleVersion { file, reader } => {
            assert!(
                file > reader,
                "file {:?} should be newer than reader {:?}",
                file,
                reader
            );
        }
        other => panic!("unexpected error: {:?}", other),
    }
}

#[cfg(not(target_arch = "wasm32"))]
#[test]
fn overflow_fenced_version_is_rejected_by_generic_reader() {
    let (_dir, path) = generate_v0_1_test_file();
    let fenced = mutate_version(&path, FileVersion::WAL_OVERFLOW);
    let err = match AlopexFileReader::open(FileSource::Path(fenced)) {
        Ok(_) => panic!("generic reader must reject overflow fence"),
        Err(error) => error,
    };
    assert_eq!(
        err,
        FormatError::IncompatibleVersion {
            file: FileVersion::WAL_OVERFLOW,
            reader: FileVersion::CURRENT,
        }
    );
}

#[cfg(not(target_arch = "wasm32"))]
#[test]
fn newer_header_is_rejected_before_truncated_footer() {
    for version in [FileVersion::WAL_OVERFLOW, FileVersion::new(0, 3, 0)] {
        let (_dir, path) = generate_v0_1_test_file();
        let changed = mutate_version(&path, version);
        let mut bytes = std::fs::read(&changed).unwrap();
        bytes.truncate(HEADER_SIZE);
        std::fs::write(&changed, bytes).unwrap();
        let err = match AlopexFileReader::open(FileSource::Path(changed)) {
            Ok(_) => panic!("unsupported header must be rejected before footer"),
            Err(error) => error,
        };
        assert_eq!(
            err,
            FormatError::IncompatibleVersion {
                file: version,
                reader: FileVersion::CURRENT,
            }
        );
    }
}

#[cfg(not(target_arch = "wasm32"))]
#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn version_0_3_is_rejected_by_generic_reader() {
    let (_dir, path) = generate_v0_1_test_file();
    let bumped = mutate_version(&path, FileVersion::new(0, 3, 0));
    let err = match AlopexFileReader::open(FileSource::Path(bumped)) {
        Ok(_) => panic!("0.3.0 must be rejected"),
        Err(error) => error,
    };
    assert_eq!(
        err,
        FormatError::IncompatibleVersion {
            file: FileVersion::new(0, 3, 0),
            reader: FileVersion::CURRENT,
        }
    );
}

#[cfg(not(target_arch = "wasm32"))]
#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn roundtrip_with_requested_compression() {
    let alg = compression_from_env();
    if !compression_available(alg) {
        eprintln!("skip compression {alg:?} because feature is disabled");
        return;
    }
    let dir = tempdir().expect("tempdir");
    let path = dir.path().join("compat_env.alopex");
    let mut writer =
        AlopexFileWriter::new(path.clone(), FileVersion::CURRENT, FileFlags(0)).expect("writer");
    writer
        .add_section(SectionType::Metadata, b"env-meta", false)
        .expect("write metadata");
    writer
        .add_section_with_compression(SectionType::SSTable, b"env-data", alg)
        .expect("write data");
    writer.finalize().expect("finalize");

    let reader = AlopexFileReader::open(FileSource::Path(path)).expect("open generated file");
    reader.validate_all().expect("validate generated file");
    assert_eq!(reader.header().version, FileVersion::CURRENT);
    assert_eq!(reader.read_section(1).unwrap(), b"env-data");
}

#[cfg(target_arch = "wasm32")]
mod wasm {
    use super::*;
    use alopex_core::storage::format::{RangeLoader, WasmReaderConfig};
    use std::sync::{Arc, Mutex};
    use wasm_bindgen_test::*;

    struct RecordingLoader {
        bytes: Vec<u8>,
        reads: Arc<Mutex<Vec<(u64, u64)>>>,
    }

    impl RangeLoader for RecordingLoader {
        fn load_range(&self, offset: u64, length: u64) -> Result<Vec<u8>, FormatError> {
            self.reads.lock().unwrap().push((offset, length));
            let start = usize::try_from(offset).map_err(|_| FormatError::IncompleteWrite)?;
            let count = usize::try_from(length).map_err(|_| FormatError::IncompleteWrite)?;
            let end = start
                .checked_add(count)
                .ok_or(FormatError::IncompleteWrite)?;
            Ok(self
                .bytes
                .get(start..end)
                .ok_or(FormatError::IncompleteWrite)?
                .to_vec())
        }
    }

    #[wasm_bindgen_test]
    fn reads_v0_1_golden_file() {
        let buffer = generate_v0_1_test_bytes();
        let reader = AlopexFileReader::open(FileSource::Buffer(buffer)).expect("open v0.1 file");
        assert_eq!(reader.header().version, FileVersion::new(0, 1, 0));
        reader.validate_all().expect("validate sections");
        assert_eq!(reader.section_index().entries.len(), 2);
        assert_eq!(reader.read_section(0).unwrap(), b"compat-meta-v0.1");
        assert_eq!(reader.read_section(1).unwrap(), b"compat-data-v0.1");
    }

    #[wasm_bindgen_test]
    fn newer_version_is_rejected() {
        let buffer = generate_v0_1_test_bytes();
        let bumped = mutate_version_bytes(
            buffer,
            FileVersion::new(FileVersion::CURRENT.major + 1, 0, 0),
        );
        let err = match AlopexFileReader::open(FileSource::Buffer(bumped)) {
            Ok(_) => panic!("should reject newer"),
            Err(e) => e,
        };
        match err {
            FormatError::IncompatibleVersion { file, reader } => assert!(file > reader),
            other => panic!("unexpected error: {:?}", other),
        }
    }

    #[wasm_bindgen_test]
    fn overflow_fenced_version_is_rejected_by_generic_reader() {
        let fenced = mutate_version_bytes(generate_v0_1_test_bytes(), FileVersion::WAL_OVERFLOW);
        let err = match AlopexFileReader::open(FileSource::Buffer(fenced)) {
            Ok(_) => panic!("generic reader must reject overflow fence"),
            Err(error) => error,
        };
        assert_eq!(
            err,
            FormatError::IncompatibleVersion {
                file: FileVersion::WAL_OVERFLOW,
                reader: FileVersion::CURRENT,
            }
        );
    }

    #[wasm_bindgen_test]
    fn overflow_fenced_version_is_rejected_with_wasm_config() {
        let fenced = mutate_version_bytes(generate_v0_1_test_bytes(), FileVersion::WAL_OVERFLOW);
        let err = match AlopexFileReader::open_with_config(
            FileSource::Buffer(fenced),
            alopex_core::storage::format::WasmReaderConfig::default(),
        ) {
            Ok(_) => panic!("configured generic reader must reject overflow fence"),
            Err(error) => error,
        };
        assert_eq!(
            err,
            FormatError::IncompatibleVersion {
                file: FileVersion::WAL_OVERFLOW,
                reader: FileVersion::CURRENT,
            }
        );
    }

    #[wasm_bindgen_test]
    fn oversized_fenced_buffer_rejects_version_without_a_loader() {
        let fenced = mutate_version_bytes(generate_v0_1_test_bytes(), FileVersion::WAL_OVERFLOW);
        let err = match AlopexFileReader::open_with_config(
            FileSource::Buffer(fenced),
            WasmReaderConfig {
                full_load_threshold_bytes: 1,
                range_loader: None,
            },
        ) {
            Ok(_) => panic!("unsupported header must be rejected before load threshold"),
            Err(error) => error,
        };
        assert_eq!(
            err,
            FormatError::IncompatibleVersion {
                file: FileVersion::WAL_OVERFLOW,
                reader: FileVersion::CURRENT,
            }
        );
    }

    #[wasm_bindgen_test]
    fn newer_header_is_rejected_before_truncated_footer() {
        for version in [FileVersion::WAL_OVERFLOW, FileVersion::new(0, 3, 0)] {
            let mut bytes = mutate_version_bytes(generate_v0_1_test_bytes(), version);
            bytes.truncate(HEADER_SIZE);
            let err = match AlopexFileReader::open(FileSource::Buffer(bytes)) {
                Ok(_) => panic!("unsupported header must be rejected before footer"),
                Err(error) => error,
            };
            assert_eq!(
                err,
                FormatError::IncompatibleVersion {
                    file: version,
                    reader: FileVersion::CURRENT,
                }
            );
        }
    }

    #[wasm_bindgen_test]
    fn buffer_rejects_unsupported_version_before_range_loader() {
        let bytes = mutate_version_bytes(generate_v0_1_test_bytes(), FileVersion::WAL_OVERFLOW);
        let reads = Arc::new(Mutex::new(Vec::new()));
        let loader = RecordingLoader {
            bytes: bytes.clone(),
            reads: Arc::clone(&reads),
        };
        let err = match AlopexFileReader::open_with_config(
            FileSource::Buffer(bytes),
            WasmReaderConfig {
                full_load_threshold_bytes: 1,
                range_loader: Some(Box::new(loader)),
            },
        ) {
            Ok(_) => panic!("unsupported header must be rejected before index fetch"),
            Err(error) => error,
        };
        assert_eq!(
            err,
            FormatError::IncompatibleVersion {
                file: FileVersion::WAL_OVERFLOW,
                reader: FileVersion::CURRENT,
            }
        );
        assert!(reads.lock().unwrap().is_empty());
    }

    #[cfg(feature = "wasm-indexeddb")]
    #[wasm_bindgen_test]
    fn indexed_db_reads_only_header_for_unsupported_version() {
        let bytes = mutate_version_bytes(generate_v0_1_test_bytes(), FileVersion::WAL_OVERFLOW);
        let reads = Arc::new(Mutex::new(Vec::new()));
        let loader = RecordingLoader {
            bytes: bytes.clone(),
            reads: Arc::clone(&reads),
        };
        let err = match AlopexFileReader::open_with_config(
            FileSource::IndexedDb {
                db_name: "test".into(),
                key: "fenced".into(),
                length: bytes.len() as u64,
                loader: Box::new(loader),
            },
            WasmReaderConfig::default(),
        ) {
            Ok(_) => panic!("unsupported header must be rejected before full fetch"),
            Err(error) => error,
        };
        assert_eq!(
            err,
            FormatError::IncompatibleVersion {
                file: FileVersion::WAL_OVERFLOW,
                reader: FileVersion::CURRENT,
            }
        );
        assert_eq!(*reads.lock().unwrap(), vec![(0, HEADER_SIZE as u64)]);
    }

    #[wasm_bindgen_test]
    fn version_0_3_is_rejected_by_generic_reader() {
        let bumped = mutate_version_bytes(generate_v0_1_test_bytes(), FileVersion::new(0, 3, 0));
        let err = match AlopexFileReader::open(FileSource::Buffer(bumped)) {
            Ok(_) => panic!("0.3.0 must be rejected"),
            Err(error) => error,
        };
        assert_eq!(
            err,
            FormatError::IncompatibleVersion {
                file: FileVersion::new(0, 3, 0),
                reader: FileVersion::CURRENT,
            }
        );
    }
}
