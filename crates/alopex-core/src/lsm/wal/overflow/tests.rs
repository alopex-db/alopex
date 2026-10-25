use super::*;
use tempfile::TempDir;

#[test]
#[cfg(not(windows))]
fn directory_sync_requires_an_existing_directory() {
    let dir = tempfile::tempdir().unwrap();
    assert!(sync_dir(dir.path()).is_ok());
    assert!(sync_dir(&dir.path().join("missing")).is_err());
    let file = dir.path().join("file");
    fs::write(&file, b"x").unwrap();
    assert!(sync_dir(&file).is_err());
}

struct Fixture {
    dir: TempDir,
    wal: PathBuf,
    spools: PathBuf,
    config: WalConfig,
}
impl Fixture {
    fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let wal = dir.path().join("wal");
        let spools = dir.path().join("spools");
        fs::create_dir(&spools).unwrap();
        #[cfg(not(windows))]
        sync_dir(dir.path()).unwrap();
        Self {
            dir,
            wal,
            spools,
            config: WalConfig {
                segment_size: WAL_SEGMENT_HEADER_SIZE + 64,
                max_segments: 2,
                sync_mode: SyncMode::NoSync,
            },
        }
    }
    fn writer(&self) -> WalWriter {
        WalWriter::create_overflow(&self.wal, self.config.clone(), 1, 1).unwrap()
    }
    fn recover(&self, checkpoint: u64) -> Result<(RecoveryOutcome, Vec<u8>, Vec<u64>)> {
        let mut reader = WalReader::open_overflow(&self.wal, self.config.clone())?;
        let mut payload = Vec::new();
        let mut lsns = Vec::new();
        let outcome = reader.recover_overflow(&self.spools, checkpoint, limits(), |event| {
            match event {
                RecoveryEvent::Inline(entry) => lsns.push(entry.lsn),
                RecoveryEvent::Begin(d) => lsns.push(d.identity().lsn()),
                RecoveryEvent::Fragment(bytes) => payload.extend_from_slice(bytes),
                RecoveryEvent::End => {}
            }
            Ok(())
        })?;
        Ok((outcome, payload, lsns))
    }
}
fn limits() -> OverflowLimits {
    OverflowLimits::new(SpoolLimits::new(256, 512, 32).unwrap(), 1024, 128).unwrap()
}

#[test]
fn inline_synced_forces_durability_for_every_sync_mode() {
    for mode in [
        SyncMode::EveryWrite,
        SyncMode::NoSync,
        SyncMode::BatchSync {
            max_batch_size: usize::MAX,
            max_wait_ms: u64::MAX,
        },
    ] {
        let mut f = Fixture::new();
        f.config.sync_mode = mode;
        let mut writer = f.writer();
        reset_sync_calls();
        let stats = writer
            .append_inline_synced(&WalEntry::put(1, vec![1], vec![2]))
            .unwrap();
        assert_eq!(sync_calls(), 1);
        assert_eq!(stats.bytes_written, writer.used_bytes());
        assert_eq!(f.recover(0).unwrap().2, vec![1]);
    }
}

#[test]
fn inline_synced_uncertain_outcome_poisoning_preserves_replay() {
    for boundary in [9, 10] {
        let f = Fixture::new();
        let mut writer = f.writer();
        let entry = WalEntry::put(1, vec![1], vec![2]);
        assert!(stage(boundary, || writer.append_inline_synced(&entry)).is_err());
        assert!(writer.append_inline_synced(&entry).is_err());
        assert!(append(&mut writer, &f, 2).is_err());
        assert!(writer.force_sync().is_err());
        assert_eq!(f.recover(0).unwrap().2, vec![1]);
    }
}

#[test]
fn inline_synced_known_capacity_refusal_keeps_writer_usable() {
    let f = Fixture::new();
    let mut writer = f.writer();
    assert!(matches!(
        writer.append_inline_synced(&WalEntry::put(1, vec![1], vec![2; 129])),
        Err(Error::WalCapacityExceeded { .. })
    ));
    writer
        .append_inline_synced(&WalEntry::put(1, vec![1], vec![2]))
        .unwrap();
    assert_eq!(f.recover(0).unwrap().2, vec![1]);
}

#[test]
fn inline_synced_exact_ring_fill_then_refusal_keeps_writer_healthy() {
    let f = Fixture::new();
    let mut writer = f.writer();
    let entry = WalEntry::put(1, vec![1], vec![2; 108]);
    assert_eq!(entry.encode().unwrap().len() as u64, writer.ring_len());
    writer.append_inline_synced(&entry).unwrap();
    assert_eq!(writer.used_bytes(), writer.ring_len());
    assert!(matches!(
        writer.append_inline_synced(&WalEntry::put(2, vec![3], vec![4])),
        Err(Error::WalCapacityExceeded { .. })
    ));
    assert!(writer.ensure_healthy().is_ok());
    assert_eq!(f.recover(0).unwrap().2, vec![1]);
}

#[test]
fn inline_synced_real_write_error_poisons_writer() {
    let f = Fixture::new();
    let mut writer = f.writer();
    writer
        .append_inline_synced(&WalEntry::put(1, vec![1], vec![2]))
        .unwrap();
    writer.file = File::open(&f.wal).unwrap(); // The OS rejects writes on this read-only handle.
    assert!(writer
        .append_inline_synced(&WalEntry::put(2, vec![3], vec![4]))
        .is_err());
    assert!(writer.ensure_healthy().is_err());
    assert_eq!(f.recover(0).unwrap().2, vec![1]);
}

#[test]
fn inline_synced_rejects_v2_without_changing_bytes() {
    let f = Fixture::new();
    let mut writer = WalWriter::create(&f.wal, f.config.clone(), 1, 1).unwrap();
    let before = fs::read(&f.wal).unwrap();
    assert!(writer
        .append_inline_synced(&WalEntry::put(1, vec![1], vec![2]))
        .is_err());
    assert_eq!(fs::read(&f.wal).unwrap(), before);
    writer.append(&WalEntry::put(1, vec![1], vec![2])).unwrap();
}
fn id(lsn: u64) -> SpoolIdentity {
    SpoolIdentity::new([7; 16], lsn).unwrap()
}
fn append(writer: &mut WalWriter, f: &Fixture, lsn: u64) -> Result<SpoolDescriptor> {
    writer.append_spool(&mut &b"abc"[..], &f.spools, id(lsn), 3, 2, limits())
}
fn stage<T>(number: u8, f: impl FnOnce() -> T) -> T {
    struct Reset;
    impl Drop for Reset {
        fn drop(&mut self) {
            FAIL_STAGE.set(0);
        }
    }
    FAIL_STAGE.set(number);
    let _reset = Reset;
    f()
}
fn rewrite_reference(
    writer: &mut WalWriter,
    descriptor: SpoolDescriptor,
    edit: impl FnOnce(&mut [u8]),
) {
    let mut bytes = reference(descriptor);
    edit(&mut bytes);
    let crc = crc32fast::hash(&bytes[..REFERENCE_BYTES - 4]);
    bytes[REFERENCE_BYTES - 4..].copy_from_slice(&crc.to_le_bytes());
    writer
        .write_ring(0, &bytes, descriptor.identity().lsn())
        .unwrap();
    writer.force_sync().unwrap();
}

#[test]
fn larger_than_ring_payload_commits_as_one_synced_reference() {
    let mut f = Fixture::new();
    f.config.segment_size = WAL_SEGMENT_HEADER_SIZE + 32; // 64-byte ring
    let mut writer = f.writer();
    reset_sync_calls();
    let payload = [9u8; 65]; // minimal ring overflow, three fragments
    writer
        .append_spool(&mut payload.as_slice(), &f.spools, id(1), 65, 32, limits())
        .unwrap();
    assert_eq!(writer.used_bytes(), REFERENCE_BYTES as u64);
    assert!(
        sync_calls() >= 1,
        "NoSync configuration cannot weaken overflow commit"
    );
    drop(writer);
    let (outcome, actual, lsns) = f.recover(0).unwrap();
    assert_eq!(actual, payload);
    assert_eq!(lsns, [1]);
    assert_eq!(
        outcome,
        RecoveryOutcome {
            last_lsn: 1,
            records: 1,
            stopped_at: None
        }
    );
}

#[test]
fn v2_cannot_write_references_and_default_readers_reject_v3() {
    let f = Fixture::new();
    let mut writer = WalWriter::create(&f.wal, f.config.clone(), 1, 1).unwrap();
    assert!(append(&mut writer, &f, 1).is_err());
    assert_eq!(directory_usage(&f.spools).unwrap(), 0);
    assert_eq!(writer.used_bytes(), 0);
    drop(writer);
    drop(activate(&f.wal, f.config.clone(), 1024).unwrap());
    assert!(WalReader::open(&f.wal, f.config.clone()).is_err());
    assert!(WalWriter::open(&f.wal, f.config.clone()).is_err());
    assert!(detect_wal_format_version(&f.wal, &f.config).is_err());
    assert!(WalReader::open_overflow(&f.wal, f.config.clone())
        .unwrap()
        .replay()
        .is_err());
}

#[test]
fn activation_preserves_checkpoint_and_existing_ring_records() {
    let f = Fixture::new();
    let checkpoint = f.dir.path().join("checkpoint.meta");
    let meta = crate::lsm::checkpoint::CheckpointMeta::new(9, 123);
    crate::lsm::checkpoint::save_checkpoint_meta(&checkpoint, &meta).unwrap();
    let before = fs::read(&checkpoint).unwrap();
    let mut writer = WalWriter::create(&f.wal, f.config.clone(), 13, 9).unwrap();
    writer.append(&WalEntry::put(9, vec![1], vec![2])).unwrap();
    writer.append(&WalEntry::delete(10, vec![1])).unwrap();
    writer.force_sync().unwrap();
    let used = writer.used_bytes();
    drop(writer);
    let mut writer = activate(&f.wal, f.config.clone(), 1024).unwrap();
    assert_eq!(writer.used_bytes(), used);
    assert_eq!(fs::read(checkpoint).unwrap(), before);
    append(&mut writer, &f, 11).unwrap();
    drop(writer);
    let (outcome, payload, lsns) = f.recover(9).unwrap();
    assert_eq!(lsns, [10, 11]);
    assert_eq!(payload, b"abc");
    assert_eq!(outcome.records, 2);
}

#[test]
fn activation_budget_and_existing_temp_leave_old_wal_intact() {
    let f = Fixture::new();
    drop(WalWriter::create(&f.wal, f.config.clone(), 1, 1).unwrap());
    let before = fs::read(&f.wal).unwrap();
    assert!(activate(&f.wal, f.config.clone(), before.len() as u64 - 1).is_err());
    assert_eq!(fs::read(&f.wal).unwrap(), before);
    let temporary = f.wal.with_extension("overflow-upgrade");
    fs::write(&temporary, b"owned by recovery").unwrap();
    assert!(activate(&f.wal, f.config.clone(), 1024).is_err());
    assert_eq!(fs::read(&temporary).unwrap(), b"owned by recovery");
    assert_eq!(fs::read(&f.wal).unwrap(), before);
}

#[test]
fn interrupted_activation_leaves_readable_old_or_explicit_new_format() {
    for fault in [6, 7] {
        let f = Fixture::new();
        let mut writer = WalWriter::create(&f.wal, f.config.clone(), 1, 1).unwrap();
        writer.append(&WalEntry::put(1, vec![1], vec![2])).unwrap();
        writer.force_sync().unwrap();
        drop(writer);
        assert!(stage(fault, || activate(&f.wal, f.config.clone(), 1024)).is_err());
        if fault == 6 {
            let replay = WalReader::open(&f.wal, f.config.clone())
                .unwrap()
                .replay()
                .unwrap();
            assert_eq!(replay.entries, [WalEntry::put(1, vec![1], vec![2])]);
        } else {
            assert!(WalReader::open(&f.wal, f.config.clone()).is_err());
            assert_eq!(f.recover(0).unwrap().2, [1]);
        }
    }
}

#[test]
fn prepared_spool_failures_do_not_commit_or_poison_prior_records() {
    for fault in [1, 2, 3] {
        let f = Fixture::new();
        let mut writer = f.writer();
        writer.append(&WalEntry::put(1, vec![1], vec![2])).unwrap();
        writer.force_sync().unwrap();
        let before = writer.used_bytes();
        assert!(stage(fault, || append(&mut writer, &f, 2)).is_err());
        assert_eq!(writer.used_bytes(), before);
        assert_eq!(directory_usage(&f.spools).unwrap(), 0);
        assert_eq!(f.recover(0).unwrap().2, [1]);
        append(&mut writer, &f, 2).unwrap();
    }
}

#[test]
fn uncertain_reference_retains_spool_and_blocks_all_writer_mutations() {
    for fault in [4, 5] {
        let f = Fixture::new();
        let mut writer = f.writer();
        writer.append(&WalEntry::put(1, vec![1], vec![2])).unwrap();
        writer.force_sync().unwrap();
        assert!(stage(fault, || append(&mut writer, &f, 2)).is_err());
        assert!(spool_path(&f.spools, id(2)).exists());
        assert!(append(&mut writer, &f, 3).is_err());
        assert!(writer.append(&WalEntry::delete(3, vec![1])).is_err());
        assert!(writer.advance_start(0).is_err());
        assert!(writer.truncate_tail_to(0).is_err());
        assert!(writer.force_sync().is_err());
        drop(writer);
        let (_, payload, lsns) = f.recover(0).unwrap();
        if fault == 4 {
            assert_eq!(lsns, [1]);
            assert!(payload.is_empty());
        } else {
            assert_eq!(lsns, [1, 2]);
            assert_eq!(payload, b"abc");
        }
    }
}

#[test]
fn missing_referenced_spool_is_hard_error() {
    let f = Fixture::new();
    let mut writer = f.writer();
    append(&mut writer, &f, 1).unwrap();
    drop(writer);
    fs::remove_file(spool_path(&f.spools, id(1))).unwrap();
    assert!(f.recover(0).is_err());
    // Only a durable checkpoint authorizes missing older spool data.
    assert_eq!(f.recover(1).unwrap().0.records, 0);
}

#[test]
fn corrupt_referenced_spool_is_hard_error_before_callbacks() {
    let f = Fixture::new();
    let mut writer = f.writer();
    append(&mut writer, &f, 1).unwrap();
    drop(writer);
    let path = spool_path(&f.spools, id(1));
    let mut bytes = fs::read(&path).unwrap();
    bytes[68] ^= 1;
    fs::write(path, bytes).unwrap();
    let mut calls = 0;
    let mut reader = WalReader::open_overflow(&f.wal, f.config.clone()).unwrap();
    assert!(reader
        .recover_overflow(&f.spools, 0, limits(), |_| {
            calls += 1;
            Ok(())
        })
        .is_err());
    assert_eq!(calls, 0);
}

#[test]
fn truncated_referenced_spool_is_not_a_recoverable_ring_tail() {
    let f = Fixture::new();
    let mut writer = f.writer();
    append(&mut writer, &f, 1).unwrap();
    drop(writer);
    OpenOptions::new()
        .write(true)
        .open(spool_path(&f.spools, id(1)))
        .unwrap()
        .set_len(3)
        .unwrap();
    assert!(f.recover(0).is_err());
}

#[test]
fn unknown_reference_version_and_flags_fail_even_with_valid_crc() {
    for offset in [13, 15] {
        let f = Fixture::new();
        let mut writer = f.writer();
        let d = append(&mut writer, &f, 1).unwrap();
        rewrite_reference(&mut writer, d, |b| b[offset] = 2);
        assert!(
            matches!(f.recover(0), Err(Error::InvalidFormat(s)) if s.contains("reference version"))
        );
    }
}

#[test]
fn hostile_reference_length_is_rejected_without_payload_allocation() {
    let f = Fixture::new();
    let mut writer = f.writer();
    let d = append(&mut writer, &f, 1).unwrap();
    rewrite_reference(&mut writer, d, |b| {
        b[33..41].copy_from_slice(&u64::MAX.to_le_bytes())
    });
    assert!(f.recover(0).is_err());
}

#[test]
fn corrupt_reference_crc_is_an_uncommitted_ring_tail() {
    let f = Fixture::new();
    let mut writer = f.writer();
    let d = append(&mut writer, &f, 1).unwrap();
    let mut bytes = reference(d);
    bytes[49] ^= 1;
    writer.write_ring(0, &bytes, 1).unwrap();
    writer.force_sync().unwrap();
    assert_eq!(
        f.recover(0).unwrap().0,
        RecoveryOutcome {
            last_lsn: 0,
            records: 0,
            stopped_at: Some(0)
        }
    );
    assert!(spool_path(&f.spools, id(1)).exists());
}

#[test]
fn truncated_ring_header_returns_explicit_tail_position() {
    let f = Fixture::new();
    let mut writer = f.writer();
    append(&mut writer, &f, 1).unwrap();
    writer.truncate_tail_to(13).unwrap();
    writer.force_sync().unwrap();
    assert_eq!(f.recover(0).unwrap().0.stopped_at, Some(0));
}

#[test]
fn directory_budget_counts_orphans_and_temporary_files() {
    let f = Fixture::new();
    let mut writer = f.writer();
    fs::write(f.spools.join("orphan.tmp"), b"x").unwrap();
    let reserved = SpoolDescriptor::from_parts(id(1), 3, 2, 0)
        .unwrap()
        .file_bytes()
        .unwrap();
    let budget = OverflowLimits::new(limits().spool, reserved, 128).unwrap();
    assert!(matches!(
        writer.append_spool(&mut &b"abc"[..], &f.spools, id(1), 3, 2, budget),
        Err(Error::WalCapacityExceeded { .. })
    ));
    assert_eq!(writer.used_bytes(), 0);
    assert_eq!(directory_usage(&f.spools).unwrap(), 1);
    assert_eq!(f.recover(0).unwrap().0.records, 0);
    let exact = OverflowLimits::new(limits().spool, reserved + 1, 128).unwrap();
    writer
        .append_spool(&mut &b"abc"[..], &f.spools, id(1), 3, 2, exact)
        .unwrap();
}

#[test]
fn full_ring_rejects_before_consuming_input_or_creating_spool() {
    struct NoRead;
    impl Read for NoRead {
        fn read(&mut self, _: &mut [u8]) -> std::io::Result<usize> {
            panic!("preflight must not read input")
        }
    }
    let f = Fixture::new();
    let mut writer = f.writer();
    writer
        .append(&WalEntry::put(1, vec![1], vec![0; 70]))
        .unwrap();
    assert!(writer
        .append_spool(&mut NoRead, &f.spools, id(2), 3, 2, limits())
        .is_err());
    assert_eq!(directory_usage(&f.spools).unwrap(), 0);
}

#[test]
fn duplicate_identity_never_overwrites_a_sealed_spool() {
    let f = Fixture::new();
    let mut writer = f.writer();
    append(&mut writer, &f, 1).unwrap();
    let path = spool_path(&f.spools, id(1));
    let before = fs::read(&path).unwrap();
    assert!(append(&mut writer, &f, 1).is_err());
    assert_eq!(fs::read(path).unwrap(), before);
}

#[test]
fn repeated_lsn_is_a_hard_error_not_duplicate_replay() {
    let f = Fixture::new();
    let mut writer = f.writer();
    append(&mut writer, &f, 1).unwrap();
    let other = SpoolIdentity::new([8; 16], 1).unwrap();
    writer
        .append_spool(&mut &b"abc"[..], &f.spools, other, 3, 2, limits())
        .unwrap();
    assert!(
        matches!(f.recover(0), Err(Error::InvalidFormat(s)) if s.contains("strictly increase"))
    );
}

#[test]
fn inline_and_directory_recovery_budgets_are_independent() {
    let f = Fixture::new();
    let mut writer = f.writer();
    writer.append(&WalEntry::put(1, vec![1], vec![2])).unwrap();
    writer.force_sync().unwrap();
    let small_inline =
        OverflowLimits::new(limits().spool, 1024, WAL_ENTRY_FIXED_HEADER + 5).unwrap();
    let mut reader = WalReader::open_overflow(&f.wal, f.config.clone()).unwrap();
    assert!(reader
        .recover_overflow(&f.spools, 0, small_inline, |_| Ok(()))
        .is_err());
    fs::write(f.spools.join("orphan.tmp"), b"ab").unwrap();
    let small_disk = OverflowLimits::new(limits().spool, 1, 128).unwrap();
    assert!(reader
        .recover_overflow(&f.spools, 0, small_disk, |_| Ok(()))
        .is_err());
}

#[test]
fn visitor_failure_aborts_recovery_without_end_event() {
    let f = Fixture::new();
    let mut writer = f.writer();
    append(&mut writer, &f, 1).unwrap();
    let mut reader = WalReader::open_overflow(&f.wal, f.config.clone()).unwrap();
    let mut calls = 0;
    assert!(reader
        .recover_overflow(&f.spools, 0, limits(), |_| {
            calls += 1;
            Err(std::io::Error::other("sink failure").into())
        })
        .is_err());
    assert_eq!(calls, 1);
}

#[cfg(unix)]
#[test]
fn symlinks_in_owned_spool_directory_are_rejected() {
    let f = Fixture::new();
    let mut writer = f.writer();
    fs::write(f.dir.path().join("outside"), b"x").unwrap();
    std::os::unix::fs::symlink(f.dir.path().join("outside"), f.spools.join("alias")).unwrap();
    assert!(append(&mut writer, &f, 1).is_err());
    assert!(f.recover(0).is_err());
}

proptest::proptest! {
    #![proptest_config(proptest::test_runner::Config::with_cases(8))]
    #[test]
    fn reference_round_trip_binds_every_descriptor_field(payload in 1u64..9, fragment in 1u32..5, lsn in 1u64..8, crc in proptest::prelude::any::<u32>()) {
        let expected = SpoolDescriptor::from_parts(id(lsn), payload, fragment, crc).unwrap();
        let encoded = reference(expected);
        proptest::prop_assert_eq!(encoded.len(), REFERENCE_BYTES);
        proptest::prop_assert_eq!(descriptor(&encoded).unwrap(), expected);
        proptest::prop_assert_eq!(crc32fast::hash(&encoded[..49]), u32::from_le_bytes(encoded[49..].try_into().unwrap()));
    }
}

#[test]
fn reference_matches_independent_wire_fixture() {
    // Python struct.pack('<QIBHH16sQII', 1, 41, 3, 1, 0,
    // bytes([7])*16, 3, 2, 0x12345678), followed by zlib.crc32.
    let hex = "010000000000000029000000030100000007070707070707070707070707070707030000000000000002000000785634128505e5b1";
    let expected: Vec<u8> = (0..hex.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).unwrap())
        .collect();
    let d = SpoolDescriptor::from_parts(id(1), 3, 2, 0x12345678).unwrap();
    assert_eq!(reference(d).as_slice(), expected);
}

#[test]
fn exactly_full_ring_reference_wraps_and_reopens() {
    let mut f = Fixture::new();
    f.config.segment_size = WAL_SEGMENT_HEADER_SIZE + REFERENCE_BYTES;
    f.config.max_segments = 1;
    let mut writer = f.writer();
    // Start the reference after a checkpoint, so it crosses physical ring end.
    writer.append(&WalEntry::delete(1, vec![1])).unwrap();
    let end = writer.end_offset();
    writer.advance_start(end).unwrap();
    append(&mut writer, &f, 2).unwrap();
    assert_eq!(writer.used_bytes(), REFERENCE_BYTES as u64);
    drop(writer);
    assert_eq!(f.recover(1).unwrap().2, [2]);
}

#[test]
fn exact_activation_and_recovery_budgets_are_inclusive() {
    let f = Fixture::new();
    let mut writer = WalWriter::create(&f.wal, f.config.clone(), 1, 1).unwrap();
    let entry = WalEntry::put(1, vec![1], vec![2]);
    writer.append(&entry).unwrap();
    writer.force_sync().unwrap();
    drop(writer);
    let exact_file = fs::metadata(&f.wal).unwrap().len();
    drop(activate(&f.wal, f.config.clone(), exact_file).unwrap());
    fs::write(f.spools.join("orphan.tmp"), b"x").unwrap();
    let exact = OverflowLimits::new(limits().spool, 1, entry.encode().unwrap().len()).unwrap();
    let mut reader = WalReader::open_overflow(&f.wal, f.config.clone()).unwrap();
    assert_eq!(
        reader
            .recover_overflow(&f.spools, 1, exact, |_| panic!("covered inline record"))
            .unwrap()
            .records,
        0
    );
    assert_eq!(
        reader
            .recover_overflow(&f.spools, 0, exact, |_| Ok(()))
            .unwrap()
            .records,
        1
    );
}

#[test]
fn input_failure_cleans_unreferenced_temp_and_allows_retry() {
    let f = Fixture::new();
    let mut writer = f.writer();
    // A truncated source is independent of a spool or reference sync failure.
    assert!(writer
        .append_spool(&mut &b"a"[..], &f.spools, id(1), 3, 2, limits())
        .is_err());
    assert_eq!(writer.used_bytes(), 0);
    assert_eq!(directory_usage(&f.spools).unwrap(), 0);
    append(&mut writer, &f, 1).unwrap();
    assert_eq!(f.recover(0).unwrap().1, b"abc");
}

#[test]
fn hostile_inline_batch_count_fails_before_callbacks_or_large_allocation() {
    let f = Fixture::new();
    let mut writer = f.writer();
    let mut bytes = 1u64.to_le_bytes().to_vec();
    bytes.extend_from_slice(&15u32.to_le_bytes());
    bytes.push(WalEntryType::Batch as u8);
    encode_varint(u64::MAX, &mut bytes);
    bytes.extend_from_slice(&crc32fast::hash(&bytes).to_le_bytes());
    assert_eq!(bytes.len(), 27);
    writer.append_encoded(&bytes, 1).unwrap();
    writer.force_sync().unwrap();
    let mut reader = WalReader::open_overflow(&f.wal, f.config.clone()).unwrap();
    assert!(reader
        .recover_overflow(&f.spools, 0, limits(), |_| panic!("invalid inline batch"))
        .is_err());
}

#[test]
fn create_overflow_does_not_replace_an_existing_wal() {
    let f = Fixture::new();
    drop(WalWriter::create(&f.wal, f.config.clone(), 1, 1).unwrap());
    let before = fs::read(&f.wal).unwrap();
    assert!(WalWriter::create_overflow(&f.wal, f.config.clone(), 1, 1).is_err());
    assert_eq!(fs::read(&f.wal).unwrap(), before);
}
