#![cfg(not(target_arch = "wasm32"))]

use alopex_core::storage::format::{
    AlopexFileReader, AlopexFileWriter, FileFlags, FileReader, FileSource, FileVersion,
    SectionType, FOOTER_SIZE,
};
use tempfile::tempdir;

fn sample_writer(path: &std::path::Path) -> AlopexFileWriter {
    AlopexFileWriter::new(path.to_path_buf(), FileVersion::CURRENT, FileFlags(0)).unwrap()
}

#[test]
fn abort_removes_temp_file() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("recover.alopex");
    let tmp = path.with_extension("alopex.tmp");

    let writer = sample_writer(&path);
    // temp file should exist before abort
    assert!(tmp.exists());
    writer.abort().unwrap();
    assert!(!tmp.exists());
    assert!(!path.exists());
}

#[test]
fn truncated_file_detected_as_incomplete() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("truncated.alopex");

    {
        let mut writer = sample_writer(&path);
        writer
            .add_section(SectionType::Metadata, b"meta", false)
            .unwrap();
        writer.finalize().unwrap();
    }

    // truncate footer bytes to simulate crash before footer fully written
    let meta = std::fs::metadata(&path).unwrap();
    let new_len = meta.len().saturating_sub((FOOTER_SIZE / 2) as u64);
    let f = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
    f.set_len(new_len).unwrap();
    f.sync_all().unwrap();

    let err = AlopexFileReader::open(FileSource::Path(path))
        .err()
        .expect("should be incomplete");
    assert!(matches!(
        err,
        alopex_core::storage::format::FormatError::IncompleteWrite
    ));
}

#[test]
fn finalize_performs_atomic_rename() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("atomic.alopex");
    let tmp = path.with_extension("alopex.tmp");

    {
        let mut writer = sample_writer(&path);
        writer
            .add_section(SectionType::Metadata, b"meta", false)
            .unwrap();
        writer.finalize().unwrap();
    }

    assert!(path.exists());
    assert!(
        !tmp.exists(),
        "temporary file should be removed after finalize"
    );
}
