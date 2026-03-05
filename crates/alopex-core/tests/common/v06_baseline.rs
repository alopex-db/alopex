use serde::Serialize;
use std::time::{Duration, Instant};

pub const V06_TOTAL_RUNS: usize = 10;
pub const V06_WARMUP_RUNS: usize = 2;

#[derive(Clone, Copy, Debug, Serialize, PartialEq)]
pub struct V06Comparison {
    pub baseline_median_ms: f64,
    pub current_median_ms: f64,
    pub degradation_ratio: f64,
    pub measured_runs: usize,
    pub warmup_runs: usize,
}

pub fn run_with_warmup_and_median<E, F>(mut run_once: F) -> Result<Duration, E>
where
    F: FnMut() -> Result<(), E>,
{
    let mut samples = Vec::with_capacity(V06_TOTAL_RUNS);
    for _ in 0..V06_TOTAL_RUNS {
        let started = Instant::now();
        run_once()?;
        samples.push(started.elapsed());
    }
    Ok(median_excluding_warmup(&samples, V06_WARMUP_RUNS))
}

pub fn compare_v05_to_current(
    baseline_median: Duration,
    current_median: Duration,
) -> V06Comparison {
    let baseline_median_ms = baseline_median.as_secs_f64() * 1000.0;
    let current_median_ms = current_median.as_secs_f64() * 1000.0;
    let degradation_ratio = if baseline_median_ms <= f64::EPSILON {
        0.0
    } else {
        (current_median_ms - baseline_median_ms) / baseline_median_ms
    };

    V06Comparison {
        baseline_median_ms,
        current_median_ms,
        degradation_ratio,
        measured_runs: V06_TOTAL_RUNS - V06_WARMUP_RUNS,
        warmup_runs: V06_WARMUP_RUNS,
    }
}

pub fn format_comparison_line(c: &V06Comparison) -> String {
    format!(
        "baseline_ms={:.3} current_ms={:.3} degradation_ratio={:+.4}",
        c.baseline_median_ms, c.current_median_ms, c.degradation_ratio
    )
}

pub fn median_excluding_warmup(samples: &[Duration], warmup_runs: usize) -> Duration {
    if samples.is_empty() {
        return Duration::ZERO;
    }

    let slice = if warmup_runs >= samples.len() {
        &samples[samples.len().saturating_sub(1)..]
    } else {
        &samples[warmup_runs..]
    };

    let mut ordered = slice.to_vec();
    ordered.sort_unstable();
    ordered[ordered.len() / 2]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn median_with_warmup_is_deterministic() {
        let samples = vec![
            Duration::from_millis(20),
            Duration::from_millis(18),
            Duration::from_millis(7),
            Duration::from_millis(9),
            Duration::from_millis(8),
        ];
        let median = median_excluding_warmup(&samples, 2);
        assert_eq!(median, Duration::from_millis(8));
    }

    #[test]
    fn comparison_has_expected_ratio_sign() {
        let baseline = Duration::from_millis(10);
        let current = Duration::from_millis(12);
        let report = compare_v05_to_current(baseline, current);
        assert!(report.degradation_ratio > 0.0);
    }

    #[test]
    fn median_empty_input_is_safe() {
        assert_eq!(median_excluding_warmup(&[], 2), Duration::ZERO);
    }
}
