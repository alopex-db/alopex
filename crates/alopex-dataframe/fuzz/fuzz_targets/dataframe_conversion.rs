#![cfg_attr(not(feature = "honggfuzz"), no_main)]

use alopex_dataframe::{DataFrame, Series};
use arrow::array::{ArrayRef, BooleanArray, Float64Array, Int32Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use std::sync::Arc;

fn run(data: &[u8]) {
    let (name, array) = decode_array(data);
    if let Ok(series) = Series::from_arrow(&name, vec![array.clone()]) {
        let _ = DataFrame::new(vec![series]);
    }
    let schema = Arc::new(Schema::new(vec![Field::new(
        name,
        array.data_type().clone(),
        true,
    )]));
    if let Ok(batch) = RecordBatch::try_new(schema, vec![array]) {
        let _ = DataFrame::from_batches(vec![batch]);
    }
}

fn decode_array(data: &[u8]) -> (String, ArrayRef) {
    if data.is_empty() {
        return ("i32".to_string(), Arc::new(Int32Array::from(vec![0])));
    }
    let kind = data[0] % 4;
    let payload = &data[1..];
    match kind {
        0 => {
            let mut values = Vec::new();
            for chunk in payload.chunks(5) {
                if chunk.is_empty() {
                    continue;
                }
                let present = chunk[0] % 2 == 0;
                if !present {
                    values.push(None);
                    continue;
                }
                let mut bytes = [0u8; 4];
                for (i, b) in chunk.iter().skip(1).take(4).enumerate() {
                    bytes[i] = *b;
                }
                values.push(Some(i32::from_le_bytes(bytes)));
            }
            ("i32".to_string(), Arc::new(Int32Array::from(values)))
        }
        1 => {
            let mut values = Vec::new();
            for chunk in payload.chunks(9) {
                if chunk.is_empty() {
                    continue;
                }
                let present = chunk[0] % 2 == 0;
                if !present {
                    values.push(None);
                    continue;
                }
                let mut bytes = [0u8; 8];
                for (i, b) in chunk.iter().skip(1).take(8).enumerate() {
                    bytes[i] = *b;
                }
                values.push(Some(f64::from_bits(u64::from_le_bytes(bytes))));
            }
            ("f64".to_string(), Arc::new(Float64Array::from(values)))
        }
        2 => {
            let values: Vec<Option<bool>> = payload
                .iter()
                .map(|b| if b % 3 == 0 { None } else { Some(b % 2 == 0) })
                .collect();
            ("bool".to_string(), Arc::new(BooleanArray::from(values)))
        }
        _ => {
            let mut values = Vec::new();
            let mut idx = 0usize;
            while idx < payload.len() {
                let len = (payload[idx] as usize).min(64);
                idx += 1;
                if idx + len > payload.len() {
                    break;
                }
                let slice = &payload[idx..idx + len];
                idx += len;
                let text = String::from_utf8_lossy(slice).to_string();
                values.push(Some(text));
            }
            if values.is_empty() {
                values.push(Some(String::new()));
            }
            ("utf8".to_string(), Arc::new(StringArray::from(values)))
        }
    }
}

#[cfg(feature = "honggfuzz")]
fn main() {
    loop {
        honggfuzz::fuzz!(|data: &[u8]| {
            run(data);
        });
    }
}

#[cfg(not(feature = "honggfuzz"))]
libfuzzer_sys::fuzz_target!(|data: &[u8]| {
    run(data);
});
