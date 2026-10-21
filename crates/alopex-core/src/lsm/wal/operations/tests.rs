use super::*;
use proptest::prelude::*;

fn limits() -> OperationLimits {
    OperationLimits::new(512, 16, 32).unwrap()
}
fn writes() -> BTreeMap<Vec<u8>, Option<Vec<u8>>> {
    BTreeMap::from([(b"a".to_vec(), Some(b"bc".to_vec())), (b"d".to_vec(), None)])
}
fn encode(writes: &BTreeMap<Vec<u8>, Option<Vec<u8>>>) -> Vec<u8> {
    let mut stream = OperationStream::new(writes, limits()).unwrap();
    let expected = stream.payload_bytes();
    let mut bytes = Vec::new();
    // A one-byte consumer crosses every header/key/value boundary.
    let mut byte = [0];
    while stream.read(&mut byte).unwrap() != 0 {
        bytes.push(byte[0]);
    }
    assert_eq!(stream.read(&mut byte).unwrap(), 0);
    assert_eq!(bytes.len() as u64, expected);
    bytes
}
fn decode(bytes: &[u8], width: usize) -> Result<BTreeMap<Vec<u8>, Option<Vec<u8>>>> {
    let mut decoder = OperationDecoder::new(bytes.len() as u64, limits())?;
    let mut result = BTreeMap::new();
    for chunk in bytes.chunks(width) {
        decoder.push(chunk, |op| {
            assert_eq!(
                op.op_type,
                if op.value.is_some() {
                    WalOpType::Put
                } else {
                    WalOpType::Delete
                }
            );
            result.insert(op.key, op.value);
            Ok(())
        })?;
    }
    decoder.finish()?;
    Ok(result)
}

#[test]
fn encoding_matches_independent_fixed_width_wire_fixture() {
    let mut expected = b"ALPXOPS1".to_vec();
    expected.extend_from_slice(&2u64.to_le_bytes());
    expected.push(0);
    expected.extend_from_slice(&1u64.to_le_bytes());
    expected.extend_from_slice(&2u64.to_le_bytes());
    expected.extend_from_slice(b"abc");
    expected.push(1);
    expected.extend_from_slice(&1u64.to_le_bytes());
    expected.extend_from_slice(&0u64.to_le_bytes());
    expected.extend_from_slice(b"d");
    assert_eq!(encode(&writes()), expected);
    assert_eq!(decode(&expected, 3).unwrap(), writes());
}

#[test]
fn empty_stream_and_empty_put_are_distinct_from_delete() {
    for map in [
        BTreeMap::new(),
        BTreeMap::from([(vec![], Some(vec![]))]),
        BTreeMap::from([(vec![], None)]),
    ] {
        assert_eq!(decode(&encode(&map), 1).unwrap(), map);
    }
}

#[test]
fn size_budgets_are_checked_before_stream_construction() {
    let map = writes();
    let n = encode(&map).len() as u64;
    assert!(OperationStream::new(&map, OperationLimits::new(n, 1, 2).unwrap()).is_ok());
    for bound in [
        OperationLimits::new(n - 1, 1, 2).unwrap(),
        OperationLimits::new(n, 0, 2).unwrap(),
        OperationLimits::new(n, 1, 1).unwrap(),
    ] {
        assert!(matches!(
            OperationStream::new(&map, bound),
            Err(Error::WalCapacityExceeded { .. })
        ));
    }
    assert!(OperationLimits::new(15, 1, 1).is_err());
    assert!(OperationLimits::new(16, usize::MAX, 1).is_err());
    assert!(OperationDecoder::new(15, limits()).is_err());
    assert!(matches!(
        OperationDecoder::new(513, limits()),
        Err(Error::WalCapacityExceeded { .. })
    ));
}

#[test]
fn hostile_count_and_lengths_are_rejected_without_payload_allocation() {
    let baseline = encode(&writes());
    for offset in [8, 17, 25] {
        let mut bytes = baseline.clone();
        bytes[offset..offset + 8].copy_from_slice(&u64::MAX.to_le_bytes());
        assert!(decode(&bytes, 1).is_err());
    }
    // Length fits independent limits but leaves too little space for the next op.
    let mut bytes = baseline.clone();
    bytes[17..25].copy_from_slice(&16u64.to_le_bytes());
    assert!(decode(&bytes, 1).is_err());
    // A count below the encoded operations must reject trailing records.
    let mut bytes = baseline;
    bytes[8..16].copy_from_slice(&1u64.to_le_bytes());
    assert!(decode(&bytes, 1).is_err());
}

#[test]
fn rejects_unknown_version_tag_and_delete_value() {
    let baseline = encode(&writes());
    for (offset, byte) in [(7, b'2'), (16, 2), (16, 1)] {
        let mut bytes = baseline.clone();
        bytes[offset] = byte;
        assert!(decode(&bytes, 2).is_err());
    }
}

#[test]
fn every_truncation_is_incomplete_and_extra_bytes_poison_decoder() {
    let bytes = encode(&writes());
    for end in 0..bytes.len() {
        let mut decoder = OperationDecoder::new(bytes.len() as u64, limits()).unwrap();
        decoder.push(&bytes[..end], |_| Ok(())).unwrap();
        assert!(decoder.finish().is_err());
    }
    let mut decoder = OperationDecoder::new(bytes.len() as u64, limits()).unwrap();
    decoder.push(&bytes, |_| Ok(())).unwrap();
    assert!(decoder.push(b"x", |_| Ok(())).is_err());
    assert!(decoder.push(&[], |_| Ok(())).is_err());
    assert!(decoder.finish().is_err());
}

#[test]
fn sink_error_is_preserved_and_decoder_cannot_resume() {
    let bytes = encode(&writes());
    let mut decoder = OperationDecoder::new(bytes.len() as u64, limits()).unwrap();
    let error = decoder
        .push(&bytes, |_| Err(io::Error::other("sink failed").into()))
        .unwrap_err();
    assert!(matches!(error, Error::Io(e) if e.to_string() == "sink failed"));
    assert!(decoder
        .push(&[], |_| panic!("failed decoder must not emit"))
        .is_err());
    assert!(decoder.finish().is_err());
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]
    #[test]
    fn fragmented_roundtrip_matches_write_set(
        map in proptest::collection::btree_map(proptest::collection::vec(any::<u8>(), 0..5), proptest::option::of(proptest::collection::vec(any::<u8>(),0..7)), 0..4),
        width in 1usize..20,
        reader_width in 1usize..20,
    ) {
        let mut stream = OperationStream::new(&map, limits()).unwrap();
        prop_assert_eq!(stream.read(&mut []).unwrap(), 0);
        let mut encoded = Vec::new(); let mut buf = vec![0; reader_width];
        loop { let n = stream.read(&mut buf).unwrap(); if n == 0 { break; } encoded.extend_from_slice(&buf[..n]); }
        prop_assert_eq!(encoded.len() as u64, stream.payload_bytes());
        prop_assert_eq!(decode(&encoded, width).unwrap(), map);
    }
}

#[cfg(not(windows))]
#[test]
fn operation_stream_replays_through_spool_larger_than_tiny_ring() {
    use super::super::{
        overflow, spool, SyncMode, WalConfig, WalReader, WalWriter, WAL_SEGMENT_HEADER_SIZE,
    };
    let dir = tempfile::tempdir().unwrap();
    let wal = dir.path().join("wal");
    let spools = dir.path().join("spools");
    std::fs::create_dir(&spools).unwrap();
    std::fs::File::open(dir.path()).unwrap().sync_all().unwrap();
    let config = WalConfig {
        segment_size: WAL_SEGMENT_HEADER_SIZE + 64,
        max_segments: 1,
        sync_mode: SyncMode::NoSync,
    };
    let mut map = writes();
    map.insert(b"e".to_vec(), Some(vec![]));
    let mut stream = OperationStream::new(&map, limits()).unwrap();
    let length = stream.payload_bytes();
    assert!(length > 64);
    let spool_limits =
        overflow::OverflowLimits::new(spool::SpoolLimits::new(128, 512, 3).unwrap(), 512, 64)
            .unwrap();
    let mut writer = WalWriter::create_overflow(&wal, config.clone(), 1, 1).unwrap();
    writer
        .append_spool(
            &mut stream,
            &spools,
            spool::SpoolIdentity::new([1; 16], 1).unwrap(),
            length,
            3,
            spool_limits,
        )
        .unwrap();
    drop(writer);
    let mut decoder = None;
    let mut result = BTreeMap::new();
    let recovered = WalReader::open_overflow(&wal, config)
        .unwrap()
        .recover_overflow(&spools, 0, spool_limits, |event| {
            match event {
                overflow::RecoveryEvent::Begin(descriptor) => {
                    decoder = Some(OperationDecoder::new(descriptor.payload_bytes(), limits())?);
                }
                overflow::RecoveryEvent::Fragment(bytes) => {
                    decoder.as_mut().unwrap().push(bytes, |op| {
                        result.insert(op.key, op.value);
                        Ok(())
                    })?
                }
                overflow::RecoveryEvent::End => decoder.take().unwrap().finish()?,
                overflow::RecoveryEvent::Inline(_) => panic!("only spool reference expected"),
            }
            Ok(())
        })
        .unwrap();
    assert_eq!(recovered.records, 1);
    assert_eq!(recovered.last_lsn, 1);
    assert!(recovered.stopped_at.is_none());
    assert_eq!(result, map);
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(16))]
    #[test]
    fn impossible_count_fails_at_header_before_waiting_for_body(count in 1u64..4, remaining in 1u64..17) {
        let declared = 16 + remaining;
        // Independent fixed-width model: even empty operations need 17 bytes.
        prop_assert!(16 + count * 17 > declared);
        let mut header = b"ALPXOPS1".to_vec();
        header.extend_from_slice(&count.to_le_bytes());
        let mut decoder = OperationDecoder::new(declared, limits()).unwrap();
        prop_assert!(decoder.push(&header, |_| panic!("invalid header must not reach sink")).is_err());
        prop_assert!(decoder.finish().is_err());
    }

    #[test]
    fn completion_requires_every_declared_byte(extra in 1u64..4) {
        let bytes = encode(&BTreeMap::new());
        let mut decoder = OperationDecoder::new(bytes.len() as u64 + extra, limits()).unwrap();
        decoder.push(&bytes, |_| panic!("empty stream")).unwrap();
        prop_assert!(decoder.finish().is_err());
    }
}
