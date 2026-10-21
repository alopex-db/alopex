use super::*;

fn framed(body: &[u8]) -> Vec<u8> {
    let mut bytes = 1u64.to_le_bytes().to_vec();
    bytes.extend_from_slice(&((body.len() + 4) as u32).to_le_bytes());
    bytes.extend_from_slice(body);
    bytes.extend_from_slice(&crc32fast::hash(&bytes).to_le_bytes());
    bytes
}

#[cfg_attr(target_arch = "wasm32", wasm_bindgen_test::wasm_bindgen_test)]
#[cfg_attr(not(target_arch = "wasm32"), test)]
fn hostile_batch_count_is_rejected_before_reservation() {
    // 27-byte record: no large allocation or workload is needed.
    let mut body = vec![WalEntryType::Batch as u8];
    encode_varint(u64::MAX, &mut body);
    assert!(matches!(
        WalEntry::decode(&framed(&body)),
        Err(Error::InvalidFormat(_))
    ));
}

#[cfg_attr(target_arch = "wasm32", wasm_bindgen_test::wasm_bindgen_test)]
#[cfg_attr(not(target_arch = "wasm32"), test)]
fn oversized_key_and_value_lengths_are_errors_not_panics() {
    for tag in [WalEntryType::Put, WalEntryType::Delete, WalEntryType::Batch] {
        let mut prefix = vec![tag as u8];
        if tag == WalEntryType::Batch {
            prefix.extend_from_slice(&[1, WalOpType::Put as u8]);
        }
        let mut key = prefix.clone();
        encode_varint(u64::MAX, &mut key);
        assert!(WalEntry::decode(&framed(&key)).is_err());
        let mut value = prefix;
        value.push(0); // empty key
        encode_varint(u64::MAX, &mut value);
        assert!(WalEntry::decode(&framed(&value)).is_err());
    }
}

#[cfg_attr(target_arch = "wasm32", wasm_bindgen_test::wasm_bindgen_test)]
#[cfg_attr(not(target_arch = "wasm32"), test)]
fn varint_rejects_bits_outside_u64() {
    let mut encoded = vec![0xff; 9];
    encoded.push(1);
    assert_eq!(decode_varint(&encoded).unwrap(), (u64::MAX, 10));
    encoded[9] = 2;
    assert!(decode_varint(&encoded).is_err());
}

#[cfg(not(target_arch = "wasm32"))]
proptest::proptest! {
    #![proptest_config(proptest::test_runner::Config::with_cases(8))]
    #[test]
    fn batch_count_is_bounded_by_minimum_operation_size(count in 0usize..9, actual in 0usize..9) {
        let mut body = vec![WalEntryType::Batch as u8];
        encode_varint(count as u64, &mut body);
        for _ in 0..actual { body.extend_from_slice(&[WalOpType::Delete as u8, 0, 0]); }
        let result = WalEntry::decode(&framed(&body));
        proptest::prop_assert_eq!(result.is_ok(), count == actual);
        if let Ok((entry, _)) = result {
            let WalEntryPayload::Batch(ops) = entry.payload else { unreachable!() };
            proptest::prop_assert_eq!(ops.len(), actual);
            proptest::prop_assert!(ops.iter().all(|op| op.key.is_empty() && op.value.is_none()));
        }
    }
}

#[cfg_attr(target_arch = "wasm32", wasm_bindgen_test::wasm_bindgen_test)]
#[cfg_attr(not(target_arch = "wasm32"), test)]
fn empty_batch_and_minimum_sized_operations_remain_valid() {
    for count in [0u8, 1] {
        let mut body = vec![WalEntryType::Batch as u8, count];
        if count == 1 {
            body.extend_from_slice(&[WalOpType::Delete as u8, 0, 0]);
        }
        let (entry, _) = WalEntry::decode(&framed(&body)).unwrap();
        let WalEntryPayload::Batch(ops) = entry.payload else {
            unreachable!()
        };
        assert_eq!(ops.len(), usize::from(count));
    }
}

#[cfg(not(target_arch = "wasm32"))]
proptest::proptest! {
    #![proptest_config(proptest::test_runner::Config::with_cases(8))]
    #[test]
    fn inline_put_matches_independent_length_model(key_len in 0usize..9, value_len in 0usize..9) {
        let key = vec![b'k'; key_len];
        let value = vec![b'v'; value_len];
        let mut body = vec![WalEntryType::Put as u8, key_len as u8];
        body.extend_from_slice(&key);
        body.push(value_len as u8);
        body.extend_from_slice(&value);
        let (decoded, consumed) = WalEntry::decode(&framed(&body)).unwrap();
        proptest::prop_assert_eq!(decoded, WalEntry::put(1, key, value));
        proptest::prop_assert_eq!(consumed, 12 + body.len() + 4);
    }

    #[test]
    fn small_overclaimed_key_lengths_are_errors_without_panics(actual in 0usize..9, excess in 1usize..4) {
        let mut body = vec![WalEntryType::Put as u8, (actual + excess) as u8];
        body.extend(std::iter::repeat_n(b'k', actual));
        proptest::prop_assert!(WalEntry::decode(&framed(&body)).is_err());
    }
}

#[cfg_attr(target_arch = "wasm32", wasm_bindgen_test::wasm_bindgen_test)]
#[cfg_attr(not(target_arch = "wasm32"), test)]
fn accepts_four_byte_key_with_empty_value() {
    let bytes = framed(&[WalEntryType::Put as u8, 4, 1, 2, 3, 4, 0]);
    assert_eq!(
        WalEntry::decode(&bytes).unwrap().0,
        WalEntry::put(1, vec![1, 2, 3, 4], vec![])
    );
}
