#![cfg(not(target_arch = "wasm32"))]

use alopex_core::storage::format::value_separator::{
    ValueRef, ValueSeparationConfig, ValueSeparator,
};
use alopex_core::storage::format::{
    AlopexFileReader, AlopexFileWriter, FileFlags, FileReader, FileSource, FileVersion, SectionType,
};
use tempfile::tempdir;

fn large_value(len: usize, seed: u8) -> Vec<u8> {
    (0..len).map(|i| seed.wrapping_add(i as u8)).collect()
}

#[test]
fn inline_and_separated_values_roundtrip() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("value_sep.alopex");
    let config = ValueSeparationConfig {
        threshold_bytes: 4,
        compress_large_values: false,
    };

    // build large value section (uncompressed raw)
    let mut sep = ValueSeparator::default();
    let v_small = b"cat".to_vec();
    let v_large = large_value(16, 5);
    let mut refs = vec![
        sep.process_value(v_small.clone(), &config),
        sep.process_value(v_large.clone(), &config),
    ];
    let (_, raw, mut pointers) = sep
        .build_large_value_section(1, &config)
        .expect("build raw section");

    // write file with LargeValue section (compress=true -> Snappy)
    {
        let mut writer =
            AlopexFileWriter::new(path.clone(), FileVersion::CURRENT, FileFlags(0)).unwrap();
        writer
            .add_section(SectionType::Metadata, b"meta", false)
            .unwrap();
        let section_id = writer
            .add_section(SectionType::LargeValue, &raw, false)
            .unwrap();
        for p in &mut pointers {
            p.section_id = section_id;
        }
        writer.finalize().unwrap();
    }

    // read and validate
    let reader = AlopexFileReader::open(FileSource::Path(path)).unwrap();
    reader.validate_all().unwrap();
    let large_section_raw = reader.read_section(1).unwrap();
    sep.hydrate_pointers(&pointers, &mut refs).expect("hydrate");

    // inline stays inline
    match &refs[0] {
        ValueRef::Inline(v) => assert_eq!(v, &v_small),
        other => panic!("expected inline, got {:?}", other),
    }
    // separated resolves to original
    match &refs[1] {
        ValueRef::Separated(ptr) => {
            let resolved = sep.resolve_pointer(ptr, &large_section_raw).unwrap();
            assert_eq!(resolved, v_large);
        }
        other => panic!("expected separated, got {:?}", other),
    }
}

#[test]
fn pending_pointer_errors_and_resolved_succeeds() {
    let config = ValueSeparationConfig {
        threshold_bytes: 1,
        compress_large_values: false,
    };
    let mut sep = ValueSeparator::default();
    let value = large_value(12, 9);
    let mut refs = vec![sep.process_value(value.clone(), &config)];
    let (_, raw, mut pointers) = sep.build_large_value_section(1, &config).expect("build");

    // write compressed section
    let dir = tempdir().unwrap();
    let path = dir.path().join("pending.alopex");
    {
        let mut writer =
            AlopexFileWriter::new(path.clone(), FileVersion::CURRENT, FileFlags(0)).unwrap();
        writer
            .add_section(SectionType::Metadata, b"meta", false)
            .unwrap();
        let section_id = writer
            .add_section(SectionType::LargeValue, &raw, true)
            .unwrap();
        for p in &mut pointers {
            p.section_id = section_id;
        }
        writer.finalize().unwrap();
    }

    // pending should error before hydrate
    if let ValueRef::Pending(ptr) = refs[0].clone() {
        let err = sep
            .resolve_pointer(&ptr, &raw)
            .expect_err("pending should fail");
        assert!(matches!(
            err,
            alopex_core::storage::format::FormatError::InvalidPointer { .. }
        ));
    } else {
        panic!("expected pending");
    }

    // hydrate and resolve
    let reader = AlopexFileReader::open(FileSource::Path(path)).unwrap();
    let raw_decompressed = reader.read_section(1).unwrap();
    sep.hydrate_pointers(&pointers, &mut refs).unwrap();
    match &refs[0] {
        ValueRef::Separated(ptr) => {
            let resolved = sep.resolve_pointer(ptr, &raw_decompressed).unwrap();
            assert_eq!(resolved, value);
        }
        _ => panic!("expected separated"),
    }
}
