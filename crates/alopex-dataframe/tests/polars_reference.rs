use std::process::Command;
use std::sync::Arc;

use alopex_dataframe::{col, lit, DataFrame, DataFrameError, Series};
use arrow::array::{Array, ArrayRef, Int64Array};
use serde_json::{json, Value};

const POLARS_REFERENCE: &str = r#"
import json
import polars as pl

assert pl.__version__ == "1.43.2", pl.__version__
df = pl.DataFrame({"a": [1, 2, 3, None], "b": [10, 20, 30, 40]})
out = (
    df.lazy()
    .filter(pl.col("a") > 1)
    .select("b", "a")
    .with_columns((pl.col("b") + 1).alias("b"))
    .collect()
)
try:
    df.select("missing")
except pl.exceptions.ColumnNotFoundError:
    missing_column = True
else:
    missing_column = False
print(json.dumps({
    "schema": [[name, str(dtype)] for name, dtype in out.schema.items()],
    "rows": [list(row) for row in out.rows()],
    "missing_column": missing_column,
}, separators=(",", ":")))
"#;

fn alopex_result() -> Value {
    let a: ArrayRef = Arc::new(Int64Array::from(vec![Some(1), Some(2), Some(3), None]));
    let b: ArrayRef = Arc::new(Int64Array::from(vec![10_i64, 20, 30, 40]));
    let df = DataFrame::new(vec![
        Series::from_arrow("a", vec![a]).unwrap(),
        Series::from_arrow("b", vec![b]).unwrap(),
    ])
    .unwrap();
    let out = df
        .lazy()
        .filter(col("a").gt(lit(1_i64)))
        .select(vec![col("b"), col("a")])
        .with_columns(vec![col("b").add(lit(1_i64)).alias("b")])
        .collect()
        .unwrap();

    let batches = out.to_arrow();
    let batch = &batches[0];
    let rows = (0..batch.num_rows())
        .map(|row| {
            batch
                .columns()
                .iter()
                .map(|column| {
                    let values = column.as_any().downcast_ref::<Int64Array>().unwrap();
                    if values.is_null(row) {
                        Value::Null
                    } else {
                        json!(values.value(row))
                    }
                })
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    let missing_column = matches!(
        df.select(vec![col("missing")]).unwrap_err(),
        DataFrameError::ColumnNotFound { .. }
    );

    json!({
        "schema": out.schema().fields().iter().map(|field| {
            json!([field.name(), field.data_type().to_string()])
        }).collect::<Vec<_>>(),
        "rows": rows,
        "missing_column": missing_column,
    })
}

#[test]
#[ignore = "the dedicated exact-reference CI lane installs Polars 1.43.2"]
fn live_polars_1_43_2_matches_dataframe_contract() {
    let python = std::env::var("ALOPEX_POLARS_PYTHON").unwrap_or_else(|_| "python3".into());
    let output = Command::new(python)
        .args(["-c", POLARS_REFERENCE])
        .output()
        .expect("run the exact Polars reference");
    assert!(
        output.status.success(),
        "Polars reference failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let polars: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(alopex_result(), polars);
}
