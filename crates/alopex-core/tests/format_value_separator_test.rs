use alopex_core::storage::compression;
use alopex_core::storage::format::value_separator::{
    ValueRef, ValueSeparationConfig, ValueSeparator,
};
use alopex_core::storage::format::{FormatError, SectionType};

fn large_value(len: usize, seed: u8) -> Vec<u8> {
    (0..len).map(|i| seed.wrapping_add(i as u8)).collect()
}

#[test]
fn pending_pointer_is_error_when_unresolved() {
    let config = ValueSeparationConfig {
        threshold_bytes: 1,
        compress_large_values: false,
    };
    let mut sep = ValueSeparator::default();
    let value = large_value(10, 1);
    let mut refs = vec![sep.process_value(value.clone(), &config)];
    // build section but do not hydrate
    let (entry, raw, pointers) = sep
        .build_large_value_section(2, &config)
        .expect("build section");
    assert_eq!(entry.section_type as u8, SectionType::LargeValue as u8);
    // pending pointer should error
    match refs[0] {
        ValueRef::Pending(ptr) => {
            let err = sep
                .resolve_pointer(&ptr, &raw)
                .expect_err("should be invalid pointer");
            assert!(matches!(err, FormatError::InvalidPointer { .. }));
        }
        _ => panic!("expected pending"),
    }
    // hydrate and ensure resolved
    sep.hydrate_pointers(&pointers, &mut refs).expect("hydrate");
    match &refs[0] {
        ValueRef::Separated(ptr) => {
            let resolved = sep.resolve_pointer(ptr, &raw).expect("resolve");
            assert_eq!(resolved, value);
        }
        other => panic!("expected separated, got {:?}", other),
    }
}

#[test]
fn duplicate_length_values_do_not_mismatch() {
    let config = ValueSeparationConfig {
        threshold_bytes: 1,
        compress_large_values: false,
    };
    let mut sep = ValueSeparator::default();
    let v1 = large_value(8, 3);
    let v2 = large_value(8, 7); // same length, different content/checksum
    let mut refs = vec![
        sep.process_value(v1.clone(), &config),
        sep.process_value(v2.clone(), &config),
    ];
    let (entry, raw, pointers) = sep
        .build_large_value_section(5, &config)
        .expect("build section");
    assert_eq!(entry.section_type as u8, SectionType::LargeValue as u8);
    sep.hydrate_pointers(&pointers, &mut refs).expect("hydrate");

    for (idx, value_ref, original) in [(0usize, &refs[0], &v1), (1usize, &refs[1], &v2)].into_iter()
    {
        match value_ref {
            ValueRef::Separated(ptr) => {
                let resolved = sep.resolve_pointer(ptr, &raw).expect("resolve");
                assert_eq!(&resolved, original, "entry {} resolved to wrong value", idx);
            }
            _ => panic!("expected separated"),
        }
    }
}

#[test]
fn compressed_large_value_roundtrip() {
    let config = ValueSeparationConfig {
        threshold_bytes: 1,
        compress_large_values: true, // uses Snappy (always available)
    };
    let mut sep = ValueSeparator::default();
    let value = large_value(32, 9);
    let mut refs = vec![sep.process_value(value.clone(), &config)];
    let (entry, compressed, pointers) = sep.build_large_value_section(9, &config).expect("build");
    assert_eq!(entry.section_type as u8, SectionType::LargeValue as u8);
    // decompress to raw for resolve_pointer
    let raw = compression::decompress(&compressed, entry.compression).expect("decompress");
    sep.hydrate_pointers(&pointers, &mut refs).expect("hydrate");
    match &refs[0] {
        ValueRef::Separated(ptr) => {
            let resolved = sep.resolve_pointer(ptr, &raw).expect("resolve");
            assert_eq!(resolved, value);
        }
        _ => panic!("expected separated"),
    }
}
