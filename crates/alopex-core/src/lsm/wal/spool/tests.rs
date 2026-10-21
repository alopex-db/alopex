use super::*;
use std::io::{self, Cursor};

fn limits() -> SpoolLimits {
    SpoolLimits::new(16, 256, 4).unwrap()
}
fn identity() -> SpoolIdentity {
    SpoolIdentity::new([7; 16], 42).unwrap()
}
fn fixture() -> (Vec<u8>, SpoolDescriptor) {
    let mut out = Vec::new();
    let descriptor = write_spool(
        &mut &[1, 2, 3, 4, 5][..],
        &mut out,
        identity(),
        5,
        2,
        limits(),
    )
    .unwrap();
    (out, descriptor)
}
fn rejects(bytes: Vec<u8>, expected: SpoolDescriptor) {
    let mut delivered = 0;
    assert!(
        read_verified_spool(&mut Cursor::new(bytes), &expected, limits(), |b| {
            delivered += b.len();
            Ok(())
        })
        .is_err()
    );
    assert_eq!(delivered, 0, "validation must finish before replay begins");
}

// Keep every checksum valid so structural rejection cannot be masked by CRC.
fn rebind_fragment(bytes: &mut [u8], frame: usize, payload_len: usize) -> SpoolDescriptor {
    let crc = crc32fast::hash(
        &[
            &bytes[frame..frame + 8],
            &bytes[frame + FRAME_LEN..frame + FRAME_LEN + payload_len],
        ]
        .concat(),
    );
    bytes[frame + 8..frame + 12].copy_from_slice(&crc.to_le_bytes());
    let seal = bytes.len() - SEAL_LEN;
    let whole = crc32fast::hash(&bytes[..seal]);
    bytes[seal + 8..seal + 12].copy_from_slice(&whole.to_le_bytes());
    let crc = crc32fast::hash(&bytes[seal..seal + 12]);
    bytes[seal + 12..].copy_from_slice(&crc.to_le_bytes());
    SpoolDescriptor::from_parts(identity(), 5, 2, whole).unwrap()
}

#[test]
fn matches_independently_encoded_one_byte_fixture() {
    // Generated from the documented LE layout using Python struct + zlib.crc32,
    // independently of this Rust encoder and decoder.
    let golden = [
        0x41, 0x4c, 0x50, 0x58, 0x4f, 0x56, 0x46, 0x4c, 0x01, 0x00, 0x00, 0x00, 0x07, 0x07, 0x07,
        0x07, 0x07, 0x07, 0x07, 0x07, 0x07, 0x07, 0x07, 0x07, 0x07, 0x07, 0x07, 0x07, 0x2a, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02,
        0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0xf2, 0x88, 0x8c, 0xdc, 0x00, 0x00, 0x00, 0x00,
        0x01, 0x00, 0x00, 0x00, 0xd0, 0x6c, 0xdc, 0xe1, 0x61, 0x41, 0x4c, 0x50, 0x58, 0x53, 0x45,
        0x41, 0x4c, 0x2b, 0x8d, 0xd2, 0xdf, 0xb0, 0xd9, 0xfe, 0xd3,
    ];
    let expected = SpoolDescriptor::from_parts(identity(), 1, 2, 3_755_117_867).unwrap();
    let mut encoded = Vec::new();
    assert_eq!(
        write_spool(&mut &b"a"[..], &mut encoded, identity(), 1, 2, limits()).unwrap(),
        expected
    );
    assert_eq!(encoded, golden);
    let mut decoded = Vec::new();
    read_verified_spool(&mut Cursor::new(golden), &expected, limits(), |b| {
        decoded.extend_from_slice(b);
        Ok(())
    })
    .unwrap();
    assert_eq!(decoded, b"a");
}

#[test]
fn three_fragments_round_trip_at_exact_disk_and_buffer_limits() {
    let (bytes, descriptor) = fixture();
    assert_eq!(bytes.len(), HEADER_LEN + 3 * FRAME_LEN + 5 + SEAL_LEN);
    assert_eq!(descriptor.file_bytes().unwrap(), bytes.len() as u64);
    assert_eq!(descriptor.identity().transaction(), [7; 16]);
    assert_eq!(descriptor.identity().lsn(), 42);
    assert_eq!(descriptor.payload_bytes(), 5);
    assert_eq!(descriptor.fragment_bytes(), 2);
    let exact = SpoolLimits::new(5, bytes.len() as u64, 2).unwrap();
    let mut chunks = Vec::new();
    read_verified_spool(&mut Cursor::new(bytes), &descriptor, exact, |b| {
        chunks.push(b.to_vec());
        Ok(())
    })
    .unwrap();
    assert_eq!(chunks, vec![vec![1, 2], vec![3, 4], vec![5]]);
}

#[test]
fn rejects_each_budget_before_output() {
    let (bytes, descriptor) = fixture();
    for limited in [
        SpoolLimits::new(4, 256, 4).unwrap(),
        SpoolLimits::new(16, bytes.len() as u64 - 1, 4).unwrap(),
        SpoolLimits::new(16, 256, 1).unwrap(),
    ] {
        let mut output = Vec::new();
        let err = write_spool(
            &mut &[1, 2, 3, 4, 5][..],
            &mut output,
            identity(),
            5,
            2,
            limited,
        )
        .unwrap_err();
        assert!(matches!(err, Error::Io(e) if e.kind() == io::ErrorKind::StorageFull));
        assert!(output.is_empty());
        let mut input = Cursor::new(bytes.clone());
        assert!(verify_spool(&mut input, &descriptor, limited).is_err());
        assert_eq!(input.position(), 0);
    }
}

#[test]
fn rejects_invalid_shape_without_allocating() {
    for (payload, fragment) in [
        (0, 1),
        (1, 0),
        (1, MAX_FRAGMENT_BYTES + 1),
        (u64::MAX, 1),
        (u64::MAX, MAX_FRAGMENT_BYTES),
    ] {
        assert!(layout(payload, fragment).is_err());
    }
    assert_eq!(
        layout(4, 2).unwrap(),
        (2, (HEADER_LEN + SEAL_LEN + 2 * FRAME_LEN + 4) as u64)
    );
    assert_eq!(layout(5, 2).unwrap().0, 3);
    assert_eq!(layout(1, MAX_FRAGMENT_BYTES).unwrap().0, 1);
    assert!(layout(u64::from(u32::MAX), 1).is_ok());
    assert!(SpoolLimits::new(0, 1, 1).is_err());
    assert!(SpoolLimits::new(1, 0, 1).is_err());
    assert!(SpoolLimits::new(1, 1, 0).is_err());
    assert!(SpoolLimits::new(1, 1, MAX_FRAGMENT_BYTES + 1).is_err());
    assert!(SpoolLimits::new(1, 1, MAX_FRAGMENT_BYTES).is_ok());
    assert!(SpoolIdentity::new([0; 16], 1).is_err());
    assert!(SpoolIdentity::new([1; 16], 0).is_err());
}

#[test]
fn rejects_unknown_version_and_reserved_flags() {
    for offset in [8, 10] {
        let (mut bytes, descriptor) = fixture();
        bytes[offset] = 2;
        let crc = crc32fast::hash(&bytes[..52]);
        bytes[52..56].copy_from_slice(&crc.to_le_bytes());
        let err = verify_spool(&mut bytes.as_slice(), &descriptor, limits()).unwrap_err();
        let expected = if offset == 8 {
            "magic or version"
        } else {
            "flags"
        };
        assert!(matches!(err, Error::InvalidFormat(message) if message.contains(expected)));
        rejects(bytes, descriptor);
    }
}

#[test]
fn rejects_header_crc_and_hostile_length() {
    let (mut bytes, descriptor) = fixture();
    bytes[12] ^= 1;
    rejects(bytes, descriptor);
    let (mut bytes, descriptor) = fixture();
    bytes[36..44].copy_from_slice(&u64::MAX.to_le_bytes());
    let crc = crc32fast::hash(&bytes[..52]);
    bytes[52..56].copy_from_slice(&crc.to_le_bytes());
    rejects(bytes, descriptor);
}

#[test]
fn rejects_wrong_reference_identity_and_checksum() {
    let (bytes, descriptor) = fixture();
    let wrong = SpoolDescriptor::from_parts(
        SpoolIdentity::new([8; 16], 42).unwrap(),
        5,
        2,
        descriptor.checksum(),
    )
    .unwrap();
    rejects(bytes.clone(), wrong);
    let wrong = SpoolDescriptor::from_parts(identity(), 5, 2, descriptor.checksum() ^ 1).unwrap();
    rejects(bytes, wrong);
}

#[test]
fn rejects_truncated_header() {
    let (bytes, descriptor) = fixture();
    rejects(bytes[..HEADER_LEN - 1].to_vec(), descriptor);
}

#[test]
fn rejects_truncated_fragment() {
    let (bytes, descriptor) = fixture();
    rejects(bytes[..HEADER_LEN + FRAME_LEN + 1].to_vec(), descriptor);
}

#[test]
fn rejects_truncated_seal() {
    let (bytes, descriptor) = fixture();
    rejects(bytes[..bytes.len() - 1].to_vec(), descriptor);
}

#[test]
fn rejects_duplicate_or_out_of_order_fragment() {
    for ordinal in [0u32, 2] {
        let (mut bytes, _) = fixture();
        let second = HEADER_LEN + FRAME_LEN + 2;
        bytes[second..second + 4].copy_from_slice(&ordinal.to_le_bytes());
        let descriptor = rebind_fragment(&mut bytes, second, 2);
        rejects(bytes, descriptor);
    }
}

#[test]
fn rejects_fragment_length_and_payload_corruption() {
    let (mut bytes, _) = fixture();
    bytes[HEADER_LEN + 4..HEADER_LEN + 8].copy_from_slice(&u32::MAX.to_le_bytes());
    let descriptor = rebind_fragment(&mut bytes, HEADER_LEN, 2);
    rejects(bytes, descriptor);
    let (mut bytes, descriptor) = fixture();
    bytes[HEADER_LEN + FRAME_LEN] ^= 1;
    rejects(bytes, descriptor);
}

#[test]
fn whole_checksum_binds_all_fragments_even_when_local_crc_matches() {
    let (mut bytes, descriptor) = fixture();
    bytes[HEADER_LEN + FRAME_LEN] ^= 1;
    let crc = frame_crc(
        &bytes[HEADER_LEN..HEADER_LEN + 8],
        &bytes[HEADER_LEN + FRAME_LEN..HEADER_LEN + FRAME_LEN + 2],
    );
    bytes[HEADER_LEN + 8..HEADER_LEN + 12].copy_from_slice(&crc.to_le_bytes());
    rejects(bytes, descriptor);
}

#[test]
fn rejects_missing_seal_and_trailing_bytes() {
    let (mut bytes, descriptor) = fixture();
    let seal = bytes.len() - SEAL_LEN;
    bytes[seal] ^= 1;
    rejects(bytes, descriptor);
    let (mut bytes, descriptor) = fixture();
    bytes.push(0);
    rejects(bytes, descriptor);
}

#[test]
fn rejects_seal_checksum_corruption() {
    let (mut bytes, descriptor) = fixture();
    let last = bytes.len() - 1;
    bytes[last] ^= 1;
    rejects(bytes, descriptor);
}

#[test]
fn rejects_wrong_sealed_stream_checksum_even_with_valid_seal_crc() {
    let (mut bytes, descriptor) = fixture();
    let seal = bytes.len() - SEAL_LEN;
    bytes[seal + 8] ^= 1;
    let crc = crc32fast::hash(&bytes[seal..seal + 12]);
    bytes[seal + 12..].copy_from_slice(&crc.to_le_bytes());
    rejects(bytes, descriptor);
}

#[test]
fn input_length_must_match_declared_length() {
    for mut input in [&[1, 2, 3, 4][..], &[1, 2, 3, 4, 5, 6][..]] {
        let mut output = Vec::new();
        assert!(write_spool(&mut input, &mut output, identity(), 5, 2, limits()).is_err());
        assert!(!output.windows(SEAL_MAGIC.len()).any(|b| b == SEAL_MAGIC));
    }
}

struct ShortWriter {
    bytes: Vec<u8>,
    fail_at: Option<usize>,
}
impl Write for ShortWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if self.fail_at == Some(self.bytes.len()) {
            return Err(io::Error::other("injected write failure"));
        }
        self.bytes.push(bytes[0]);
        Ok(1)
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[test]
fn handles_short_writes_and_propagates_write_errors() {
    let (expected, _) = fixture();
    let mut output = ShortWriter {
        bytes: Vec::new(),
        fail_at: None,
    };
    write_spool(
        &mut &[1, 2, 3, 4, 5][..],
        &mut output,
        identity(),
        5,
        2,
        limits(),
    )
    .unwrap();
    assert_eq!(output.bytes, expected);
    for position in [
        0,
        HEADER_LEN,
        HEADER_LEN + FRAME_LEN + 1,
        expected.len() - 1,
    ] {
        let mut output = ShortWriter {
            bytes: Vec::new(),
            fail_at: Some(position),
        };
        assert!(matches!(
            write_spool(
                &mut &[1, 2, 3, 4, 5][..],
                &mut output,
                identity(),
                5,
                2,
                limits()
            ),
            Err(Error::Io(_))
        ));
        assert_eq!(output.bytes.len(), position);
    }
}

#[test]
fn visitor_error_stops_private_replay() {
    let (bytes, descriptor) = fixture();
    let mut callbacks = 0;
    let result = read_verified_spool(&mut Cursor::new(bytes), &descriptor, limits(), |_| {
        callbacks += 1;
        Err(io::Error::other("private replay failed").into())
    });
    assert!(matches!(result, Err(Error::Io(_))));
    assert_eq!(callbacks, 1);
}

struct FailingReader {
    cursor: Cursor<Vec<u8>>,
    second_pass: bool,
    fail_second_pass: bool,
    fail_at: u64,
}
impl Read for FailingReader {
    fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
        if self.second_pass == self.fail_second_pass && self.cursor.position() == self.fail_at {
            return Err(io::Error::other("injected read failure"));
        }
        let length = bytes.len().min(1);
        self.cursor.read(&mut bytes[..length])
    }
}
impl Seek for FailingReader {
    fn seek(&mut self, position: SeekFrom) -> io::Result<u64> {
        if matches!(position, SeekFrom::Start(_)) {
            self.second_pass = true;
        }
        self.cursor.seek(position)
    }
}

#[test]
fn first_pass_io_failure_delivers_nothing() {
    let (bytes, descriptor) = fixture();
    let mut reader = FailingReader {
        cursor: Cursor::new(bytes),
        second_pass: false,
        fail_second_pass: false,
        fail_at: (HEADER_LEN + FRAME_LEN + 1) as u64,
    };
    let mut calls = 0;
    let result = read_verified_spool(&mut reader, &descriptor, limits(), |_| {
        calls += 1;
        Ok(())
    });
    assert!(matches!(result, Err(Error::Io(_))));
    assert_eq!(calls, 0);
}

#[test]
fn second_pass_io_failure_requires_discarding_private_state() {
    let (bytes, descriptor) = fixture();
    let mut reader = FailingReader {
        cursor: Cursor::new(bytes.clone()),
        second_pass: false,
        fail_second_pass: true,
        fail_at: (HEADER_LEN + FRAME_LEN + 2) as u64,
    };
    let mut private = Vec::new();
    let result = read_verified_spool(&mut reader, &descriptor, limits(), |b| {
        private.extend_from_slice(b);
        Ok(())
    });
    assert!(matches!(result, Err(Error::Io(_))));
    assert_eq!(private, [1, 2]);
    // Recovery ownership belongs to the caller: discard rather than publish.
    private.clear();
    read_verified_spool(&mut Cursor::new(bytes), &descriptor, limits(), |b| {
        private.extend_from_slice(b);
        Ok(())
    })
    .unwrap();
    assert_eq!(private, [1, 2, 3, 4, 5]);
}

proptest::proptest! {
    #![proptest_config(proptest::test_runner::Config::with_cases(12))]
    #[test]
    fn rejects_one_invalid_frame_field_with_all_checksums_valid(order in proptest::prelude::any::<bool>(), bad in 0u32..4) {
        let (mut bytes, _) = fixture();
        let second = HEADER_LEN + FRAME_LEN + 2;
        let (offset, valid) = if order { (second, 1) } else { (second + 4, 2) };
        let bad = if bad == valid { bad + 1 } else { bad };
        bytes[offset..offset + 4].copy_from_slice(&bad.to_le_bytes());
        let expected = rebind_fragment(&mut bytes, second, 2);
        let error = verify_spool(&mut bytes.as_slice(), &expected, limits()).unwrap_err();
        proptest::prop_assert!(matches!(error, Error::InvalidFormat(message) if message.contains("fragment order or length")));
        rejects(bytes, expected);
    }

    #[test]
    fn bounded_stream_matches_reference(payload in proptest::collection::vec(proptest::prelude::any::<u8>(), 1..9), fragment in 1u32..5) {
        let mut bytes = Vec::new();
        let descriptor = write_spool(&mut payload.as_slice(), &mut bytes,
            identity(), payload.len() as u64, fragment, limits()).unwrap();
        let mut actual = Vec::new();
        read_verified_spool(&mut Cursor::new(bytes), &descriptor, limits(), |b| {
            assert!(b.len() <= fragment as usize);
            actual.extend_from_slice(b); Ok(())
        }).unwrap();
        proptest::prop_assert_eq!(actual, payload);
    }
}
