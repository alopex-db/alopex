//! Versioned continuation tokens bound to one logical scan specification.
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};

const VERSION: &[u8] = b"alopex-scan-v2:";

pub(super) fn scope(kind: u8, include_internal: bool, parts: &[&[u8]]) -> Vec<u8> {
    let mut scope = vec![kind, u8::from(include_internal)];
    for part in parts {
        scope.extend_from_slice(&(part.len() as u64).to_be_bytes());
        scope.extend_from_slice(part);
    }
    scope
}

pub(super) fn encode(scope: &[u8], key: &[u8]) -> Vec<u8> {
    let mut payload = scope.to_vec();
    payload.extend_from_slice(key);
    let mut cursor = VERSION.to_vec();
    cursor.extend_from_slice(URL_SAFE_NO_PAD.encode(payload).as_bytes());
    cursor
}

pub(super) fn decode(cursor: &[u8], scope: &[u8]) -> Result<Vec<u8>, ()> {
    let payload = URL_SAFE_NO_PAD
        .decode(cursor.strip_prefix(VERSION).ok_or(())?)
        .map_err(|_| ())?;
    Ok(payload.strip_prefix(scope).ok_or(())?.to_vec())
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        #[test]
        fn binary_cursor_round_trip_and_scope_rejection(
            prefix in proptest::collection::vec(any::<u8>(), 0..16),
            key in proptest::collection::vec(any::<u8>(), 0..16),
            internal in any::<bool>(),
        ) {
            let original = scope(0, internal, &[&prefix]);
            let token = encode(&original, &key);
            prop_assert_eq!(decode(&token, &original), Ok(key));
            prop_assert!(decode(&token, &scope(0, !internal, &[&prefix])).is_err());
            prop_assert!(decode(&token, &scope(1, internal, &[&prefix, b""])).is_err());
            let mut different = prefix.clone();
            different.push(0);
            prop_assert!(decode(&token, &scope(0, internal, &[&different])).is_err());
        }
    }

    #[test]
    fn rejects_old_malformed_and_ambiguous_range_tokens() {
        let a = scope(1, false, &[b"a", b"bc"]);
        let b = scope(1, false, &[b"ab", b"c"]);
        assert!(decode(&encode(&a, b""), &b).is_err());
        for token in [
            b"alopex-scan-v1:YQ".as_slice(),
            b"alopex-scan-v2:!",
            b"alopex-scan-v2:",
            b"raw",
        ] {
            assert!(decode(token, &a).is_err());
        }
    }
}
