use std::sync::Arc;

use alopex_dataframe::{all, col, concat_str, lit, ConcatStrNullBehavior, DataFrame, Series};
use arrow::array::{Array, ArrayRef, BooleanArray, Int64Array, StringArray};

const I10A_REGISTER: [&str; 18] = [
    "add",
    "sub",
    "mul",
    "div",
    "eq",
    "neq",
    "gt",
    "lt",
    "ge",
    "le",
    "and",
    "or",
    "not",
    "alias",
    "col",
    "lit",
    "all",
    "concat_str",
];

#[derive(Debug)]
struct StatusRow {
    operation: &'static str,
    passed: bool,
}

fn dataframe() -> DataFrame {
    let a: ArrayRef = Arc::new(Int64Array::from(vec![4_i64, 2]));
    let b: ArrayRef = Arc::new(Int64Array::from(vec![2_i64, 4]));
    let predicate: ArrayRef = Arc::new(BooleanArray::from(vec![true, false]));
    let other_predicate: ArrayRef = Arc::new(BooleanArray::from(vec![true, true]));
    let text: ArrayRef = Arc::new(StringArray::from(vec!["x", "y"]));
    DataFrame::new(vec![
        Series::from_arrow("a", vec![a]).unwrap(),
        Series::from_arrow("b", vec![b]).unwrap(),
        Series::from_arrow("predicate", vec![predicate]).unwrap(),
        Series::from_arrow("other_predicate", vec![other_predicate]).unwrap(),
        Series::from_arrow("text", vec![text]).unwrap(),
    ])
    .unwrap()
}

fn int_values(df: &DataFrame, column: &str) -> Vec<i64> {
    let values = df.column(column).unwrap().to_arrow();
    let values = values[0].as_any().downcast_ref::<Int64Array>().unwrap();
    (0..values.len()).map(|index| values.value(index)).collect()
}

fn bool_values(df: &DataFrame, column: &str) -> Vec<bool> {
    let values = df.column(column).unwrap().to_arrow();
    let values = values[0].as_any().downcast_ref::<BooleanArray>().unwrap();
    (0..values.len()).map(|index| values.value(index)).collect()
}

fn string_values(df: &DataFrame, column: &str) -> Vec<String> {
    let values = df.column(column).unwrap().to_arrow();
    let values = values[0].as_any().downcast_ref::<StringArray>().unwrap();
    (0..values.len())
        .map(|index| values.value(index).to_owned())
        .collect()
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn i10a_every_expression_builder_and_operator_has_a_status_row() {
    let input = dataframe();
    let projected = input
        .select(vec![
            col("a").add(col("b")).alias("add"),
            col("a").sub(col("b")).alias("sub"),
            col("a").mul(col("b")).alias("mul"),
            col("a").div(col("b")).alias("div"),
            col("a").eq(col("b")).alias("eq"),
            col("a").neq(col("b")).alias("neq"),
            col("a").gt(col("b")).alias("gt"),
            col("a").lt(col("b")).alias("lt"),
            col("a").ge(col("b")).alias("ge"),
            col("a").le(col("b")).alias("le"),
            col("predicate").and_(col("other_predicate")).alias("and"),
            col("predicate").or_(col("other_predicate")).alias("or"),
            col("predicate").not_().alias("not"),
            col("a").alias("renamed"),
            col("a").alias("column"),
            lit(7_i64).alias("literal"),
            concat_str(
                vec![col("text"), col("text")],
                "/",
                ConcatStrNullBehavior::Propagate,
            )
            .unwrap()
            .alias("joined"),
        ])
        .unwrap();
    let wildcard = input.select(vec![all()]).unwrap();

    let rows = [
        StatusRow {
            operation: "add",
            passed: int_values(&projected, "add") == [6, 6],
        },
        StatusRow {
            operation: "sub",
            passed: int_values(&projected, "sub") == [2, -2],
        },
        StatusRow {
            operation: "mul",
            passed: int_values(&projected, "mul") == [8, 8],
        },
        StatusRow {
            operation: "div",
            passed: int_values(&projected, "div") == [2, 0],
        },
        StatusRow {
            operation: "eq",
            passed: bool_values(&projected, "eq") == [false, false],
        },
        StatusRow {
            operation: "neq",
            passed: bool_values(&projected, "neq") == [true, true],
        },
        StatusRow {
            operation: "gt",
            passed: bool_values(&projected, "gt") == [true, false],
        },
        StatusRow {
            operation: "lt",
            passed: bool_values(&projected, "lt") == [false, true],
        },
        StatusRow {
            operation: "ge",
            passed: bool_values(&projected, "ge") == [true, false],
        },
        StatusRow {
            operation: "le",
            passed: bool_values(&projected, "le") == [false, true],
        },
        StatusRow {
            operation: "and",
            passed: bool_values(&projected, "and") == [true, false],
        },
        StatusRow {
            operation: "or",
            passed: bool_values(&projected, "or") == [true, true],
        },
        StatusRow {
            operation: "not",
            passed: bool_values(&projected, "not") == [false, true],
        },
        StatusRow {
            operation: "alias",
            passed: projected.column("renamed").is_ok(),
        },
        StatusRow {
            operation: "col",
            passed: int_values(&projected, "column") == int_values(&input, "a"),
        },
        StatusRow {
            operation: "lit",
            passed: int_values(&projected, "literal") == [7, 7],
        },
        StatusRow {
            operation: "all",
            passed: wildcard.to_arrow() == input.to_arrow(),
        },
        StatusRow {
            operation: "concat_str",
            passed: string_values(&projected, "joined") == ["x/x", "y/y"],
        },
    ];

    let names: Vec<_> = rows.iter().map(|row| row.operation).collect();
    assert_eq!(
        names, I10A_REGISTER,
        "the I-10a expression register drifted"
    );
    for row in rows {
        assert!(
            row.passed,
            "{} must retain its verified behavior",
            row.operation
        );
    }
}
