//! Bridge the codec-free Thrift schema to the current Parquet decoder.
//! Callers validate the entire header and its allocation budget before decoding it.
#![allow(
    deprecated,
    reason = "Only the pinned, codec-free wire schema uses deprecated types"
)]

use parquet::basic::{Encoding, Type};
use parquet::column::page::PageMetadata;
use parquet::data_type::{ByteArray, FixedLenByteArray, Int96};
use parquet::errors::{ParquetError, Result};
use parquet::file::statistics::{Statistics, ValueStatistics};
use parquet::util::bit_util::FromBytes;
use parquet_wire::format;

fn invalid() -> ParquetError {
    ParquetError::General("invalid parquet page metadata".into())
}

pub(super) fn encoding(value: format::Encoding) -> Result<Encoding> {
    Ok(match value {
        format::Encoding::PLAIN => Encoding::PLAIN,
        format::Encoding::PLAIN_DICTIONARY => Encoding::PLAIN_DICTIONARY,
        format::Encoding::RLE => Encoding::RLE,
        format::Encoding::BIT_PACKED => Encoding::BIT_PACKED,
        format::Encoding::DELTA_BINARY_PACKED => Encoding::DELTA_BINARY_PACKED,
        format::Encoding::DELTA_LENGTH_BYTE_ARRAY => Encoding::DELTA_LENGTH_BYTE_ARRAY,
        format::Encoding::DELTA_BYTE_ARRAY => Encoding::DELTA_BYTE_ARRAY,
        format::Encoding::RLE_DICTIONARY => Encoding::RLE_DICTIONARY,
        format::Encoding::BYTE_STREAM_SPLIT => Encoding::BYTE_STREAM_SPLIT,
        _ => return Err(invalid()),
    })
}

pub(super) fn page_metadata(header: &format::PageHeader) -> Result<PageMetadata> {
    let (rows, levels, is_dict) = match header.type_ {
        format::PageType::DATA_PAGE => {
            let h = header.data_page_header.as_ref().ok_or_else(invalid)?;
            (
                None,
                Some(usize::try_from(h.num_values).map_err(|_| invalid())?),
                false,
            )
        }
        format::PageType::DATA_PAGE_V2 => {
            let h = header.data_page_header_v2.as_ref().ok_or_else(invalid)?;
            (
                Some(usize::try_from(h.num_rows).map_err(|_| invalid())?),
                Some(usize::try_from(h.num_values).map_err(|_| invalid())?),
                false,
            )
        }
        format::PageType::DICTIONARY_PAGE => (None, None, true),
        _ => return Err(invalid()),
    };
    Ok(PageMetadata {
        num_rows: rows,
        num_levels: levels,
        is_dict,
    })
}

fn fixed<const N: usize>(value: Vec<u8>) -> Result<[u8; N]> {
    value.try_into().map_err(|_| invalid())
}

/// Move binary statistics into the decoder without making another payload copy.
pub(super) fn statistics(
    physical: Type,
    value: Option<format::Statistics>,
) -> Result<Option<Statistics>> {
    let Some(value) = value else {
        return Ok(None);
    };
    let nulls = value
        .null_count
        .map(u64::try_from)
        .transpose()
        .map_err(|_| invalid())?;
    let distinct = value
        .distinct_count
        .map(u64::try_from)
        .transpose()
        .map_err(|_| invalid())?;
    let old = value.min_value.is_none() && value.max_value.is_none();
    let min = if old { value.min } else { value.min_value };
    let max = if old { value.max } else { value.max_value };
    macro_rules! scalar {
        ($variant:ident, $decode:expr) => {
            Statistics::$variant(
                min.map($decode).transpose()?,
                max.map($decode).transpose()?,
                distinct,
                nulls,
                old,
            )
        };
    }
    let stats = match physical {
        Type::BOOLEAN => scalar!(boolean, |v| Ok::<_, ParquetError>(fixed::<1>(v)?[0] != 0)),
        Type::INT32 => scalar!(int32, |v| Ok::<_, ParquetError>(i32::from_le_bytes(fixed(
            v
        )?))),
        Type::INT64 => scalar!(int64, |v| Ok::<_, ParquetError>(i64::from_le_bytes(fixed(
            v
        )?))),
        Type::FLOAT => scalar!(float, |v| Ok::<_, ParquetError>(f32::from_le_bytes(fixed(
            v
        )?))),
        Type::DOUBLE => scalar!(double, |v| Ok::<_, ParquetError>(f64::from_le_bytes(
            fixed(v)?
        ))),
        Type::INT96 => scalar!(int96, |v| Int96::try_from_le_slice(&fixed::<12>(v)?)),
        Type::BYTE_ARRAY => Statistics::ByteArray(
            ValueStatistics::new(
                min.map(ByteArray::from),
                max.map(ByteArray::from),
                distinct,
                nulls,
                old,
            )
            .with_min_is_exact(value.is_min_value_exact.unwrap_or(false))
            .with_max_is_exact(value.is_max_value_exact.unwrap_or(false)),
        ),
        Type::FIXED_LEN_BYTE_ARRAY => Statistics::FixedLenByteArray(
            ValueStatistics::new(
                min.map(ByteArray::from).map(FixedLenByteArray::from),
                max.map(ByteArray::from).map(FixedLenByteArray::from),
                distinct,
                nulls,
                old,
            )
            .with_min_is_exact(value.is_min_value_exact.unwrap_or(false))
            .with_max_is_exact(value.is_max_value_exact.unwrap_or(false)),
        ),
    };
    Ok(Some(stats))
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn stats(min: Vec<u8>, max: Vec<u8>) -> format::Statistics {
        format::Statistics {
            min: None,
            max: None,
            null_count: Some(1),
            distinct_count: Some(2),
            min_value: Some(min),
            max_value: Some(max),
            is_min_value_exact: Some(true),
            is_max_value_exact: Some(false),
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(32))]
        #[test]
        fn integer_statistics_preserve_values_and_counts(a in any::<i64>(), b in any::<i64>()) {
            let result = statistics(Type::INT64, Some(stats(a.to_le_bytes().to_vec(), b.to_le_bytes().to_vec()))).unwrap().unwrap();
            let min = a.to_le_bytes();
            let max = b.to_le_bytes();
            prop_assert_eq!(result.min_bytes_opt(), Some(min.as_slice()));
            prop_assert_eq!(result.max_bytes_opt(), Some(max.as_slice()));
            prop_assert_eq!(result.null_count_opt(), Some(1));
            prop_assert_eq!(result.distinct_count_opt(), Some(2));
            prop_assert!(!result.is_min_max_deprecated());
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(32))]
        #[test]
        fn partial_modern_bounds_never_fall_back_to_legacy(a in any::<i64>(), b in any::<i64>()) {
            for keep_min in [false, true] {
                let mut value = stats(a.to_le_bytes().to_vec(), b.to_le_bytes().to_vec());
                value.min = Some((-1_i64).to_le_bytes().to_vec());
                value.max = Some(1_i64.to_le_bytes().to_vec());
                if keep_min { value.max_value = None; } else { value.min_value = None; }
                let expected_min = value.min_value.clone();
                let expected_max = value.max_value.clone();
                let result = statistics(Type::INT64, Some(value)).unwrap().unwrap();
                prop_assert!(!result.is_min_max_deprecated());
                prop_assert_eq!(result.min_bytes_opt(), expected_min.as_deref());
                prop_assert_eq!(result.max_bytes_opt(), expected_max.as_deref());
            }
        }
    }

    #[test]
    fn binary_statistics_move_buffers_and_preserve_exactness() {
        let value = stats(vec![1, 2], vec![3, 4]);
        let address = value.min_value.as_ref().unwrap().as_ptr();
        for physical in [Type::BYTE_ARRAY, Type::FIXED_LEN_BYTE_ARRAY] {
            let result = statistics(physical, Some(value.clone())).unwrap().unwrap();
            assert_eq!(result.min_bytes_opt(), Some([1, 2].as_slice()));
            assert_eq!(result.max_bytes_opt(), Some([3, 4].as_slice()));
            assert!(result.min_is_exact());
            assert!(!result.max_is_exact());
        }
        let result = statistics(Type::BYTE_ARRAY, Some(value)).unwrap().unwrap();
        assert_eq!(result.min_bytes_opt().unwrap().as_ptr(), address);
    }

    #[test]
    fn malformed_statistics_return_errors_without_panicking() {
        for (physical, width) in [
            (Type::BOOLEAN, 1),
            (Type::INT32, 4),
            (Type::INT64, 8),
            (Type::INT96, 12),
            (Type::FLOAT, 4),
            (Type::DOUBLE, 8),
        ] {
            for length in [0, width - 1, width + 1] {
                assert!(
                    statistics(physical, Some(stats(vec![0; length], vec![0; width]))).is_err()
                );
                assert!(
                    statistics(physical, Some(stats(vec![0; width], vec![0; length]))).is_err()
                );
            }
            assert!(statistics(physical, Some(stats(vec![0; width], vec![0; width]))).is_ok());
        }
        let mut value = stats(vec![0; 8], vec![0; 8]);
        value.null_count = Some(-1);
        assert!(statistics(Type::INT64, Some(value.clone())).is_err());
        value.null_count = None;
        value.distinct_count = Some(-1);
        assert!(statistics(Type::INT64, Some(value)).is_err());
        assert!(statistics(Type::INT64, None).unwrap().is_none());
    }

    #[test]
    fn legacy_statistics_and_unknown_counts_remain_distinct() {
        let mut value = stats(vec![7; 4], vec![8; 4]);
        value.min = value.min_value.take();
        value.max = value.max_value.take();
        value.null_count = None;
        value.distinct_count = None;
        let result = statistics(Type::INT32, Some(value)).unwrap().unwrap();
        assert!(result.is_min_max_deprecated());
        assert_eq!(result.min_bytes_opt(), Some([7; 4].as_slice()));
        assert_eq!(result.max_bytes_opt(), Some([8; 4].as_slice()));
        assert_eq!(result.null_count_opt(), None);
        assert_eq!(result.distinct_count_opt(), None);
    }

    #[test]
    fn encoding_bridge_rejects_unknown_discriminants() {
        for (wire, current) in [
            (0, Encoding::PLAIN),
            (2, Encoding::PLAIN_DICTIONARY),
            (3, Encoding::RLE),
            (4, Encoding::BIT_PACKED),
            (5, Encoding::DELTA_BINARY_PACKED),
            (6, Encoding::DELTA_LENGTH_BYTE_ARRAY),
            (7, Encoding::DELTA_BYTE_ARRAY),
            (8, Encoding::RLE_DICTIONARY),
            (9, Encoding::BYTE_STREAM_SPLIT),
        ] {
            assert_eq!(encoding(format::Encoding(wire)).unwrap(), current);
        }
        for wire in [-1, 1, 10, i32::MAX] {
            assert!(encoding(format::Encoding(wire)).is_err());
        }
    }

    #[test]
    fn page_metadata_preserves_v1_v2_and_dictionary_boundaries() {
        let mut header = format::PageHeader::new(
            format::PageType::DATA_PAGE,
            0,
            0,
            None,
            None,
            None,
            None,
            None,
        );
        assert!(page_metadata(&header).is_err());
        header.data_page_header = Some(format::DataPageHeader::new(
            3,
            format::Encoding::PLAIN,
            format::Encoding::RLE,
            format::Encoding::RLE,
            None,
        ));
        let meta = page_metadata(&header).unwrap();
        assert_eq!(meta.num_rows, None);
        assert_eq!(meta.num_levels, Some(3));
        assert!(!meta.is_dict);
        header.data_page_header.as_mut().unwrap().num_values = -1;
        assert!(page_metadata(&header).is_err());
        header.type_ = format::PageType::DATA_PAGE_V2;
        assert!(page_metadata(&header).is_err());
        header.data_page_header_v2 = Some(format::DataPageHeaderV2::new(
            3,
            1,
            2,
            format::Encoding::PLAIN,
            0,
            0,
            None,
            None,
        ));
        let meta = page_metadata(&header).unwrap();
        assert_eq!(meta.num_rows, Some(2));
        assert_eq!(meta.num_levels, Some(3));
        assert!(!meta.is_dict);
        header.data_page_header_v2.as_mut().unwrap().num_rows = -1;
        assert!(page_metadata(&header).is_err());
        header.data_page_header_v2.as_mut().unwrap().num_rows = 2;
        header.data_page_header_v2.as_mut().unwrap().num_values = -1;
        assert!(page_metadata(&header).is_err());
        header.type_ = format::PageType::DICTIONARY_PAGE;
        let meta = page_metadata(&header).unwrap();
        assert!(meta.is_dict);
        assert_eq!(meta.num_rows, None);
        assert_eq!(meta.num_levels, None);
        header.type_ = format::PageType(123);
        assert!(page_metadata(&header).is_err());
    }
}
