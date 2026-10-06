//! Fixed oracle for the six single-column, row-storage legacy PK cases.
pub const CASES: [&str; 6] = [
    "pk_first_valid",
    "pk_later_valid",
    "pk_first_null_attempt",
    "pk_later_null_attempt",
    "pk_first_duplicate_attempt",
    "pk_later_duplicate_attempt",
];

pub fn setup(name: &str) -> String {
    assert!(CASES.contains(&name));
    let constraints = if name.starts_with("pk_first_") {
        "PRIMARY KEY(id), UNIQUE(value)"
    } else {
        "UNIQUE(value), PRIMARY KEY(id)"
    };
    format!(
        "CREATE TABLE items (id INTEGER, value INTEGER, {constraints}); \
             INSERT INTO items VALUES (1,10),(2,20); \
             CREATE TABLE neighbor (id INTEGER PRIMARY KEY); INSERT INTO neighbor VALUES (7);"
    )
}

pub fn attempt(name: &str) -> Option<&'static str> {
    assert!(CASES.contains(&name));
    if name.ends_with("null_attempt") {
        Some("INSERT INTO items VALUES (NULL,30)")
    } else if name.ends_with("duplicate_attempt") {
        Some("INSERT INTO items VALUES (1,30)")
    } else {
        None
    }
}

pub fn rows(name: &str, outcome: &str) -> Vec<Vec<Option<i32>>> {
    assert!(CASES.contains(&name));
    assert!(matches!(outcome, "valid" | "accepted" | "rejected"));
    let mut rows = vec![vec![Some(1), Some(10)], vec![Some(2), Some(20)]];
    if outcome == "accepted" {
        let id = if name.ends_with("null_attempt") {
            None
        } else {
            Some(1)
        };
        rows.push(vec![id, Some(30)]);
    }
    rows
}
