use super::Row;

/// Apply LIMIT/OFFSET to rows.
pub fn execute_limit(mut rows: Vec<Row>, limit: Option<u64>, offset: Option<u64>) -> Vec<Row> {
    let start = offset.unwrap_or(0) as usize;
    let end = limit
        .map(|l| start.saturating_add(l as usize))
        .unwrap_or_else(|| rows.len());
    if start >= rows.len() {
        return Vec::new();
    }
    let end = end.min(rows.len());
    rows.drain(0..start);
    rows.truncate(end - start);
    rows
}
