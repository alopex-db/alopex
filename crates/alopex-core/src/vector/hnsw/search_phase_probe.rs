//! Comparison-only search instrumentation; never a release performance result.
use std::io::{self, Write};
use std::time::Instant;

#[derive(Clone, Copy)]
pub(super) enum Phase {
    Setup = 0,
    ResolveScore,
    NodeRead,
    Frontier,
    ResultHeap,
    Ordering,
    Materialize,
}

const NAMES: [&str; 7] = [
    "setup",
    "resolve_score",
    "node_read",
    "frontier",
    "result_heap",
    "ordering",
    "materialize",
];

pub(super) struct Probe {
    start: Option<Instant>,
    elapsed: [u128; 7],
    calls: [u64; 7],
}

impl Probe {
    pub(super) fn from_env() -> Self {
        Self::new(std::env::var("ALOPEX_HNSW_PHASE_PROBE").as_deref() == Ok("1"))
    }

    fn new(enabled: bool) -> Self {
        Self {
            start: enabled.then(Instant::now),
            elapsed: [0; 7],
            calls: [0; 7],
        }
    }

    #[cfg(test)]
    pub(super) fn enabled_for_test(enabled: bool) -> Self {
        Self::new(enabled)
    }

    #[inline]
    pub(super) fn measure<T>(&mut self, phase: Phase, operation: impl FnOnce() -> T) -> T {
        if self.start.is_none() {
            return operation();
        }
        let start = Instant::now();
        let result = operation();
        let index = phase as usize;
        self.elapsed[index] += start.elapsed().as_nanos();
        self.calls[index] += 1;
        result
    }

    pub(super) fn emit(self, level: usize, ef: usize, before: (u64, u64), after: (u64, u64)) {
        self.write(&mut io::stderr().lock(), level, ef, before, after)
            .expect("search phase diagnostic output failed");
    }

    fn write(
        self,
        output: &mut impl Write,
        level: usize,
        ef: usize,
        before: (u64, u64),
        after: (u64, u64),
    ) -> io::Result<()> {
        let Some(start) = self.start else {
            return Ok(());
        };
        let total = start.elapsed().as_nanos();
        let classified: u128 = self.elapsed.iter().sum();
        let residual = total
            .checked_sub(classified)
            .expect("overlapping search phase timers");
        let visited = after
            .0
            .checked_sub(before.0)
            .expect("search visited counter decreased");
        let distances = after
            .1
            .checked_sub(before.1)
            .expect("search distance counter decreased");
        write!(output, "{{\"schema\":\"alopex.hnsw-search-phase/v1\",\"scope\":\"layer\",\"level\":{level},\"ef\":{ef},\"total_ns\":{total},\"residual_ns\":{residual},\"nodes_visited\":{visited},\"distance_computations\":{distances},\"phases\":{{")?;
        for (index, name) in NAMES.iter().enumerate() {
            if index != 0 {
                write!(output, ",")?;
            }
            write!(
                output,
                "\"{name}\":{{\"ns\":{},\"calls\":{}}}",
                self.elapsed[index], self.calls[index]
            )?;
        }
        writeln!(output, "}}}}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn disabled_probe_executes_once_and_emits_nothing() {
        let mut probe = Probe::new(false);
        let mut called = 0;
        let result = probe.measure(Phase::Frontier, || {
            called += 1;
            Err::<(), _>("original")
        });
        assert_eq!(result, Err("original"));
        assert_eq!(called, 1);
        assert_eq!(probe.elapsed, [0; 7]);
        assert_eq!(probe.calls, [0; 7]);
        let mut bytes = Vec::new();
        probe.write(&mut bytes, 0, 2, (9, 9), (0, 0)).unwrap();
        assert!(bytes.is_empty());
    }

    #[test]
    fn active_probe_preserves_borrows_and_counts_each_phase_separately() {
        let mut probe = Probe::new(true);
        let values = [1u8, 2];
        let borrowed = probe.measure(Phase::NodeRead, || &values[..]);
        assert_eq!(borrowed.as_ptr(), values.as_ptr());
        assert_eq!(probe.measure(Phase::ResultHeap, || 7), 7);
        assert_eq!(probe.measure(Phase::ResultHeap, || 9), 9);
        assert_eq!(probe.calls, [0, 0, 1, 0, 2, 0, 0]);
        let mut bytes = Vec::new();
        probe.write(&mut bytes, 0, 16, (3, 2), (8, 6)).unwrap();
        let text = String::from_utf8(bytes).unwrap();
        assert!(text.starts_with("{\"schema\":\"alopex.hnsw-search-phase/v1\""));
        assert!(text.contains("\"nodes_visited\":5,\"distance_computations\":4"));
        assert!(text.ends_with("}}}\n"));
        let value: serde_json::Value = serde_json::from_str(&text).unwrap();
        assert_eq!(value["level"], 0);
        assert_eq!(value["ef"], 16);
        assert_eq!(value["phases"]["node_read"]["calls"], 1);
        assert_eq!(value["phases"]["result_heap"]["calls"], 2);
        let classified: u64 = value["phases"]
            .as_object()
            .unwrap()
            .values()
            .map(|phase| phase["ns"].as_u64().unwrap())
            .sum();
        assert_eq!(
            value["total_ns"].as_u64().unwrap(),
            classified + value["residual_ns"].as_u64().unwrap()
        );
        for name in NAMES {
            assert_eq!(text.matches(&format!("\"{name}\":")).count(), 1);
        }
    }

    #[test]
    fn failing_sink_remains_an_error() {
        struct Broken;
        impl Write for Broken {
            fn write(&mut self, _: &[u8]) -> io::Result<usize> {
                Err(io::ErrorKind::BrokenPipe.into())
            }
            fn flush(&mut self) -> io::Result<()> {
                Ok(())
            }
        }
        let error = Probe::new(true)
            .write(&mut Broken, 0, 1, (0, 0), (1, 1))
            .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::BrokenPipe);
    }

    #[test]
    #[should_panic(expected = "search visited counter decreased")]
    fn decreasing_counter_is_not_replaced_by_zero() {
        Probe::new(true)
            .write(&mut Vec::new(), 0, 1, (2, 1), (1, 1))
            .unwrap();
    }

    #[test]
    fn elapsed_time_accumulates_the_measured_operations() {
        let mut probe = Probe::new(true);
        let mut minimum_ns = 0;
        for _ in 0..2 {
            probe.measure(Phase::ResolveScore, || {
                let start = Instant::now();
                std::thread::sleep(std::time::Duration::from_millis(1));
                minimum_ns += start.elapsed().as_nanos();
            });
            assert!(minimum_ns > 0);
            assert!(probe.elapsed[Phase::ResolveScore as usize] >= minimum_ns);
        }
        assert_eq!(probe.calls[Phase::ResolveScore as usize], 2);
    }

    #[test]
    fn environment_switch_and_emission_are_checked_in_subprocesses() {
        if let Ok(expected) = std::env::var("ALOPEX_TEST_EXPECT_PHASE_PROBE") {
            let probe = Probe::from_env();
            assert_eq!(probe.start.is_some(), expected == "enabled");
            probe.emit(2, 16, (1, 1), (3, 4));
            return;
        }
        for setting in [Some("1"), Some("0"), Some("2"), Some(""), None] {
            let enabled = setting == Some("1");
            let mut command = std::process::Command::new(std::env::current_exe().unwrap());
            command.args(["--exact",
                "vector::hnsw::search_phase_probe::tests::environment_switch_and_emission_are_checked_in_subprocesses",
                "--nocapture"]);
            command.env(
                "ALOPEX_TEST_EXPECT_PHASE_PROBE",
                if enabled { "enabled" } else { "disabled" },
            );
            if let Some(value) = setting {
                command.env("ALOPEX_HNSW_PHASE_PROBE", value);
            } else {
                command.env_remove("ALOPEX_HNSW_PHASE_PROBE");
            }
            let output = command.output().unwrap();
            assert!(output.status.success(), "child probe failed");
            if enabled {
                let row: serde_json::Value = serde_json::from_slice(&output.stderr).unwrap();
                assert_eq!(row["schema"], "alopex.hnsw-search-phase/v1");
                assert_eq!(row["level"], 2);
                assert_eq!(row["ef"], 16);
                assert_eq!(row["nodes_visited"], 2);
                assert_eq!(row["distance_computations"], 3);
            } else {
                assert!(output.stderr.is_empty());
            }
        }
    }
}
