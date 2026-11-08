//! Thread-local probes applied only to a manual comparison source overlay.
use std::cell::Cell;
use std::time::Instant;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Snapshot {
    pub fork_calls: u64,
    pub fork_ns: u64,
    pub stage_calls: u64,
    pub stage_ns: u64,
}

#[derive(Clone, Copy)]
pub enum Kind {
    Fork,
    Stage,
}

thread_local! {
    static ACTIVE: Cell<Option<Snapshot>> = const { Cell::new(None) };
}

pub fn begin() {
    ACTIVE.with(|slot| {
        assert!(slot.get().is_none(), "nested writer diagnostic");
        slot.set(Some(Snapshot::default()));
    });
}

pub fn finish() -> Snapshot {
    ACTIVE.with(|slot| slot.take().expect("writer diagnostic not active"))
}

fn record(kind: Kind, elapsed_ns: u64) {
    ACTIVE.with(|slot| {
        let mut value = slot.get().expect("writer diagnostic not active");
        let (calls, elapsed) = match kind {
            Kind::Fork => (&mut value.fork_calls, &mut value.fork_ns),
            Kind::Stage => (&mut value.stage_calls, &mut value.stage_ns),
        };
        *calls = calls.checked_add(1).expect("diagnostic count overflow");
        *elapsed = elapsed
            .checked_add(elapsed_ns)
            .expect("diagnostic timer overflow");
        slot.set(Some(value));
    });
}

pub fn measure<T>(kind: Kind, operation: impl FnOnce() -> T) -> T {
    if !ACTIVE.with(|slot| slot.get().is_some()) {
        return operation();
    }
    let started = Instant::now();
    let result = operation();
    let elapsed_ns =
        u64::try_from(started.elapsed().as_nanos()).expect("diagnostic duration overflow");
    record(kind, elapsed_ns);
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accumulates_distinct_operations_and_clears_between_statements() {
        begin();
        record(Kind::Fork, 13);
        record(Kind::Stage, 29);
        record(Kind::Stage, 31);
        assert_eq!(
            finish(),
            Snapshot {
                fork_calls: 1,
                fork_ns: 13,
                stage_calls: 2,
                stage_ns: 60
            }
        );
        begin();
        assert_eq!(finish(), Snapshot::default());
    }

    #[test]
    fn inactive_measure_executes_exactly_once_without_recording() {
        let mut calls = 0;
        let result = measure(Kind::Fork, || {
            calls += 1;
            42
        });
        assert_eq!((result, calls), (42, 1));
        assert_eq!(ACTIVE.with(Cell::get), None);
    }

    #[test]
    fn active_measure_preserves_success_and_failure_results() {
        begin();
        let mut calls = 0;
        let ok: Result<u8, u8> = measure(Kind::Fork, || {
            calls += 1;
            Ok(7)
        });
        let error: Result<u8, u8> = measure(Kind::Stage, || {
            calls += 1;
            Err(9)
        });
        let snapshot = finish();
        assert_eq!((ok, error, calls), (Ok(7), Err(9), 2));
        assert_eq!((snapshot.fork_calls, snapshot.stage_calls), (1, 1));
    }

    #[test]
    fn separate_threads_cannot_mix_measurements() {
        begin();
        record(Kind::Fork, 11);
        let other = std::thread::spawn(|| {
            assert_eq!(ACTIVE.with(Cell::get), None);
            begin();
            record(Kind::Stage, 23);
            finish()
        })
        .join()
        .unwrap();
        assert_eq!(
            other,
            Snapshot {
                stage_calls: 1,
                stage_ns: 23,
                ..Snapshot::default()
            }
        );
        assert_eq!(
            finish(),
            Snapshot {
                fork_calls: 1,
                fork_ns: 11,
                ..Snapshot::default()
            }
        );
    }

    #[test]
    fn rejects_nested_and_missing_intervals() {
        assert!(std::panic::catch_unwind(finish).is_err());
        begin();
        record(Kind::Fork, 5);
        assert!(std::panic::catch_unwind(begin).is_err());
        assert_eq!(finish().fork_ns, 5);
    }

    #[test]
    fn rejects_overflow_instead_of_wrapping() {
        for kind in [Kind::Fork, Kind::Stage] {
            begin();
            record(kind, u64::MAX);
            assert!(std::panic::catch_unwind(|| record(kind, 1)).is_err());
            finish();
            ACTIVE.with(|slot| {
                slot.set(Some(Snapshot {
                    fork_calls: u64::MAX,
                    stage_calls: u64::MAX,
                    ..Snapshot::default()
                }))
            });
            assert!(std::panic::catch_unwind(|| record(kind, 0)).is_err());
            finish();
        }
    }
}
