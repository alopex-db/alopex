use std::sync::Arc;

use alopex_dataframe::{DataFrame, JoinType, Series};
use arrow::array::{
    Array, ArrayRef, Int64Array, ListBuilder, StringArray, StringBuilder, UInt64Array,
};

const I09A_REGISTER: [&str; 15] = [
    "join.inner",
    "join.left",
    "join.right",
    "join.full",
    "join.semi",
    "join.anti",
    "sort",
    "head",
    "tail",
    "unique",
    "fill_null",
    "drop_nulls",
    "null_count",
    "explode",
    "implode",
];

#[derive(Debug)]
struct StatusRow {
    operation: &'static str,
    passed: bool,
}

fn int_series(name: &str, values: Vec<Option<i64>>) -> Series {
    Series::from_arrow(name, vec![Arc::new(Int64Array::from(values)) as ArrayRef]).unwrap()
}

fn string_series(name: &str, values: Vec<Option<&str>>) -> Series {
    Series::from_arrow(name, vec![Arc::new(StringArray::from(values)) as ArrayRef]).unwrap()
}

fn join_inputs() -> (DataFrame, DataFrame) {
    let left = DataFrame::new(vec![
        int_series("id", vec![Some(1), Some(2), Some(3)]),
        int_series("left_value", vec![Some(10), Some(20), Some(30)]),
    ])
    .unwrap();
    let right = DataFrame::new(vec![
        int_series("id", vec![Some(2), Some(3), Some(4)]),
        int_series("right_value", vec![Some(200), Some(300), Some(400)]),
    ])
    .unwrap();
    (left, right)
}

fn assert_status_register(rows: &[StatusRow]) {
    let names: Vec<_> = rows.iter().map(|row| row.operation).collect();
    assert_eq!(names, I09A_REGISTER, "the I-09a operation register drifted");
    for row in rows {
        assert!(
            row.passed,
            "{} must retain its verified behavior",
            row.operation
        );
    }
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn i09a_every_join_and_dataframe_operation_has_a_semantic_status_row() {
    let (left, right) = join_inputs();
    let inner = left
        .join(&right, vec!["id".to_owned()], JoinType::Inner)
        .unwrap();
    let left_join = left
        .join(&right, vec!["id".to_owned()], JoinType::Left)
        .unwrap();
    let right_join = left
        .join(&right, vec!["id".to_owned()], JoinType::Right)
        .unwrap();
    let full_join = left
        .join(&right, vec!["id".to_owned()], JoinType::Full)
        .unwrap();
    let semi_join = left
        .join(&right, vec!["id".to_owned()], JoinType::Semi)
        .unwrap();
    let anti_join = left
        .join(&right, vec!["id".to_owned()], JoinType::Anti)
        .unwrap();

    let operations = DataFrame::new(vec![
        int_series("id", vec![Some(3), Some(1), Some(2), Some(2)]),
        int_series("value", vec![Some(30), None, Some(20), Some(20)]),
    ])
    .unwrap();
    let sorted = operations.sort(vec!["id".to_owned()], vec![false]).unwrap();
    let head = operations.head(2).unwrap();
    let tail = operations.tail(2).unwrap();
    let unique = operations.unique(Some(vec!["id".to_owned()])).unwrap();
    let filled = operations.fill_null(0_i64).unwrap();
    let dropped = operations
        .drop_nulls(Some(vec!["value".to_owned()]))
        .unwrap();
    let nulls = operations.null_count().unwrap();

    let words = DataFrame::new(vec![string_series(
        "word",
        vec![Some("a"), None, Some("c")],
    )])
    .unwrap();
    let imploded = words.implode().unwrap();
    let exploded = list_dataframe().explode("items").unwrap();

    let sorted_id = sorted.column("id").unwrap().to_arrow();
    let sorted_id = sorted_id[0].as_any().downcast_ref::<Int64Array>().unwrap();
    let filled_value = filled.column("value").unwrap().to_arrow();
    let filled_value = filled_value[0]
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let null_count = nulls.column("value").unwrap().to_arrow();
    let null_count = null_count[0]
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap();
    let exploded_items = exploded.column("items").unwrap().to_arrow();
    let exploded_items = exploded_items[0]
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();

    let rows = [
        StatusRow {
            operation: "join.inner",
            passed: inner.height() == 2 && inner.width() == 3,
        },
        StatusRow {
            operation: "join.left",
            passed: left_join.height() == 3 && left_join.column("right_value").is_ok(),
        },
        StatusRow {
            operation: "join.right",
            passed: right_join.height() == 3 && right_join.column("left_value").is_ok(),
        },
        StatusRow {
            operation: "join.full",
            passed: full_join.height() == 4 && full_join.width() == 3,
        },
        StatusRow {
            operation: "join.semi",
            passed: semi_join.height() == 2 && semi_join.width() == left.width(),
        },
        StatusRow {
            operation: "join.anti",
            passed: anti_join.height() == 1 && anti_join.width() == left.width(),
        },
        StatusRow {
            operation: "sort",
            passed: sorted.height() == 4 && sorted_id.value(0) == 1 && sorted_id.value(3) == 3,
        },
        StatusRow {
            operation: "head",
            passed: head.height() == 2 && head.width() == operations.width(),
        },
        StatusRow {
            operation: "tail",
            passed: tail.height() == 2 && tail.width() == operations.width(),
        },
        StatusRow {
            operation: "unique",
            passed: unique.height() == 3,
        },
        StatusRow {
            operation: "fill_null",
            passed: filled_value.value(1) == 0 && !filled_value.is_null(1),
        },
        StatusRow {
            operation: "drop_nulls",
            passed: dropped.height() == 3,
        },
        StatusRow {
            operation: "null_count",
            passed: nulls.height() == 1 && null_count.value(0) == 1,
        },
        StatusRow {
            operation: "explode",
            passed: exploded.height() == 4
                && exploded_items.value(0) == "a"
                && exploded_items.value(1) == "b"
                && exploded_items.is_null(2)
                && exploded_items.is_null(3),
        },
        StatusRow {
            operation: "implode",
            passed: imploded.height() == 1 && imploded.width() == words.width(),
        },
    ];
    assert_status_register(&rows);
}

fn list_dataframe() -> DataFrame {
    let mut items = ListBuilder::new(StringBuilder::new());
    for row in [Some(vec![Some("a"), Some("b")]), Some(Vec::new()), None] {
        match row {
            Some(values) => {
                for value in values {
                    match value {
                        Some(value) => items.values().append_value(value),
                        None => items.values().append_null(),
                    }
                }
                items.append(true);
            }
            None => items.append(false),
        }
    }
    DataFrame::new(vec![
        string_series("id", vec![Some("x"), Some("y"), Some("z")]),
        Series::from_arrow("items", vec![Arc::new(items.finish()) as ArrayRef]).unwrap(),
    ])
    .unwrap()
}
