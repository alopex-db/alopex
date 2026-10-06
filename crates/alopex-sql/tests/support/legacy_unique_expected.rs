//! Fixed result oracle, independent of either engine's decoded metadata.
pub fn rows(case: &str) -> Vec<Vec<Option<i32>>> {
    match case {
        "column" | "named" | "composite" => vec![vec![Some(1), Some(10), Some(20)]],
        "nullable" => vec![
            vec![Some(1), None, Some(20)],
            vec![Some(2), None, Some(20)],
            vec![Some(3), Some(10), None],
        ],
        "duplicate" | "named_duplicate" => vec![
            vec![Some(1), Some(10), Some(20)],
            vec![Some(2), Some(10), Some(21)],
        ],
        "composite_duplicate" => vec![
            vec![Some(1), Some(10), Some(20)],
            vec![Some(2), Some(10), Some(20)],
        ],
        _ => panic!("unknown fixed fixture case"),
    }
}
