#![cfg(not(target_arch = "wasm32"))]

mod common;

use std::time::Duration;

use alopex_core::columnar::encoding::{Column, LogicalType};
use alopex_core::columnar::segment_v2::{ColumnSchema, RecordBatch, Schema};
use alopex_core::dataframe::cast::{cast_value, DataType, DataValue};
use alopex_core::dataframe::partition_scan::{PartitionScanner, VecBatchSource};
use alopex_core::{Error, Result};
use common::{
    compare_v05_to_current, format_comparison_line, run_with_warmup_and_median, V06Comparison,
};

const STABILITY_GATE_RATIO: f64 = 0.30;
const CAST_ROWS: usize = 16_384;
const PARTITION_BATCHES: usize = 16;
const PARTITION_ROWS_PER_BATCH: usize = 257;
const PARTITION_SIZE: usize = 64;
const PARTITION_SCAN_REPEATS: usize = 64;

fn within_stability_gate(comparison: &V06Comparison) -> bool {
    comparison.degradation_ratio.abs() <= STABILITY_GATE_RATIO
}

fn schema() -> Schema {
    Schema {
        columns: vec![
            ColumnSchema {
                name: "partition_key".into(),
                logical_type: LogicalType::Int64,
                nullable: false,
                fixed_len: None,
            },
            ColumnSchema {
                name: "value".into(),
                logical_type: LogicalType::Int64,
                nullable: false,
                fixed_len: None,
            },
        ],
    }
}

fn batch(keys: Vec<i64>, values: Vec<i64>) -> RecordBatch {
    RecordBatch::new(
        schema(),
        vec![Column::Int64(keys), Column::Int64(values)],
        vec![None, None],
    )
}

fn build_partition_batches() -> Vec<RecordBatch> {
    (0..PARTITION_BATCHES)
        .map(|batch_idx| {
            let start = batch_idx * PARTITION_ROWS_PER_BATCH;
            let end = start + PARTITION_ROWS_PER_BATCH;
            let keys = (start..end)
                .map(|row| (row / PARTITION_SIZE) as i64)
                .collect();
            let values = (start..end).map(|row| row as i64).collect();
            batch(keys, values)
        })
        .collect()
}

fn checksum_int64(value: DataValue) -> Result<i64> {
    match value {
        DataValue::Int64(value) => Ok(value),
        other => Err(Error::InvalidFormat(format!(
            "expected Int64 cast result, got {other:?}"
        ))),
    }
}

fn checksum_float64(value: DataValue) -> Result<i64> {
    match value {
        DataValue::Float64(value) => Ok(value as i64),
        other => Err(Error::InvalidFormat(format!(
            "expected Float64 cast result, got {other:?}"
        ))),
    }
}

fn run_cast_workload() -> Result<()> {
    let mut checksum = 0i64;
    for row in 0..CAST_ROWS {
        let int = DataValue::Int32((row % 100_000) as i32);
        checksum = checksum.wrapping_add(checksum_int64(cast_value(&int, DataType::Int64)?)?);

        let text = DataValue::Utf8((row % 10_000).to_string());
        checksum = checksum.wrapping_add(checksum_int64(cast_value(&text, DataType::Int64)?)?);
        checksum = checksum.wrapping_add(checksum_float64(cast_value(&text, DataType::Float64)?)?);

        let timestamp = DataValue::TimestampMicros((row as i64) * 1_000_000);
        checksum = checksum.wrapping_add(checksum_int64(cast_value(&timestamp, DataType::Int64)?)?);
    }
    std::hint::black_box(checksum);
    Ok(())
}

fn run_partition_scan_workload() -> Result<()> {
    let expected_rows = PARTITION_BATCHES * PARTITION_ROWS_PER_BATCH;
    for _ in 0..PARTITION_SCAN_REPEATS {
        let source = VecBatchSource::new(build_partition_batches());
        let mut scanner = PartitionScanner::new(source, vec![0])?;
        let mut row_count = 0usize;
        let mut partition_count = 0usize;

        scanner.for_each_partition(|partition| {
            row_count += partition
                .chunks()
                .iter()
                .map(|chunk| chunk.row_count())
                .sum::<usize>();
            partition_count += 1;
            Ok(())
        })?;

        if row_count != expected_rows {
            return Err(Error::InvalidFormat(format!(
                "expected {expected_rows} partition rows, got {row_count}"
            )));
        }
        std::hint::black_box((row_count, partition_count));
    }
    Ok(())
}

fn run_dataframe_workload() -> Result<()> {
    run_cast_workload()?;
    run_partition_scan_workload()
}

#[test]
fn dataframe_cast_and_partition_scan_stability_within_30_percent() {
    let baseline_median = run_with_warmup_and_median(run_dataframe_workload)
        .expect("baseline workload should succeed");
    let current_median = run_with_warmup_and_median(run_dataframe_workload)
        .expect("current workload should succeed");

    let comparison = compare_v05_to_current(baseline_median, current_median);
    let line = format_comparison_line(&comparison);
    eprintln!("dataframe_stability {line}");
    assert!(
        within_stability_gate(&comparison),
        "expected degradation_ratio.abs() <= {STABILITY_GATE_RATIO:.2}, got {line}"
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stability_gate_accepts_clear_inside_ratios() {
        let positive =
            compare_v05_to_current(Duration::from_millis(100), Duration::from_millis(125));
        let negative =
            compare_v05_to_current(Duration::from_millis(100), Duration::from_millis(75));

        assert!(within_stability_gate(&positive));
        assert!(within_stability_gate(&negative));
    }

    #[test]
    fn stability_gate_rejects_clear_outside_ratios() {
        let positive =
            compare_v05_to_current(Duration::from_millis(100), Duration::from_millis(135));
        let negative =
            compare_v05_to_current(Duration::from_millis(100), Duration::from_millis(65));

        assert!(!within_stability_gate(&positive));
        assert!(!within_stability_gate(&negative));
    }
}
