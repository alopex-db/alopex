//! Flat planner: filter → score → top-k using vector metrics.
use crate::types::Key;
use crate::vector::{score, Metric};
use crate::Result;

/// A candidate item for flat search.
#[derive(Debug)]
pub struct Candidate<'a> {
    /// Key identifying the item.
    pub key: &'a Key,
    /// Vector embedding.
    pub vector: &'a [f32],
}

/// Scored output item.
#[derive(Debug, Clone, PartialEq)]
pub struct ScoredItem {
    /// Key identifying the item.
    pub key: Key,
    /// Similarity score (higher is better).
    pub score: f32,
}

/// Executes a flat search:
/// 1) Optionally filters candidates
/// 2) Scores using the selected metric
/// 3) Returns the top-k results sorted by descending score, then key for stability
pub fn search_flat<'a, F>(
    query: &[f32],
    metric: Metric,
    top_k: usize,
    candidates: impl IntoIterator<Item = Candidate<'a>>,
    mut filter: Option<F>,
) -> Result<Vec<ScoredItem>>
where
    F: FnMut(&Candidate<'a>) -> bool,
{
    if top_k == 0 {
        return Ok(Vec::new());
    }

    let mut results = Vec::new();
    for cand in candidates {
        if let Some(ref mut pred) = filter {
            if !pred(&cand) {
                continue;
            }
        }

        // score() will validate dimension equality and return typed errors.
        let s = score(metric, query, cand.vector)?;
        results.push(ScoredItem {
            key: cand.key.clone(),
            score: s,
        });
    }

    results.sort_by(|a, b| {
        b.score
            .total_cmp(&a.score)
            .then_with(|| a.key.cmp(&b.key))
    });
    if results.len() > top_k {
        results.truncate(top_k);
    }
    Ok(results)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(bytes: &[u8]) -> Key {
        bytes.to_vec()
    }

    #[test]
    fn respects_filter_before_scoring() {
        let query = [1.0, 0.0];
        let ka = key(b"a");
        let kb = key(b"b");
        let items = vec![
            Candidate {
                key: &ka,
                vector: &[1.0, 0.0],
            },
            Candidate {
                key: &kb,
                vector: &[0.0, 1.0],
            },
        ];
        let res = search_flat(&query, Metric::Cosine, 10, items, Some(|c: &Candidate| c.key != b"b"))
            .unwrap();
        assert_eq!(res.len(), 1);
        assert_eq!(res[0].key, b"a");
    }

    #[test]
    fn orders_by_score_then_key() {
        let query = [1.0, 0.0];
        let kb = key(b"b");
        let ka = key(b"a");
        let items = vec![
            Candidate {
                key: &kb,
                vector: &[1.0, 0.0],
            },
            Candidate {
                key: &ka,
                vector: &[1.0, 0.0],
            },
        ];
        let res = search_flat(&query, Metric::Cosine, 10, items, None::<fn(&Candidate) -> bool>).unwrap();
        // same score, sorted by key for determinism
        assert_eq!(res[0].key, b"a");
        assert_eq!(res[1].key, b"b");
    }

    #[test]
    fn switches_metric() {
        let query = [1.0, 0.0];
        let ka = key(b"a");
        let kb = key(b"b");
        let items = vec![
            Candidate {
                key: &ka,
                vector: &[2.0, 0.0],
            },
            Candidate {
                key: &kb,
                vector: &[0.0, 2.0],
            },
        ];
        let res = search_flat(&query, Metric::L2, 1, items, None::<fn(&Candidate) -> bool>).unwrap();
        // L2 negative distance: closer is higher (less negative), so "a" should win.
        assert_eq!(res[0].key, b"a");
    }

    #[test]
    fn enforces_dimension_match() {
        let query = [1.0, 0.0];
        let ka = key(b"a");
        let items = vec![Candidate {
            key: &ka,
            vector: &[1.0, 0.0, 1.0],
        }];
        use crate::Error;
        let err = search_flat(&query, Metric::Cosine, 1, items, None::<fn(&Candidate) -> bool>).unwrap_err();
        assert!(matches!(err, Error::DimensionMismatch { .. }));
    }
}
