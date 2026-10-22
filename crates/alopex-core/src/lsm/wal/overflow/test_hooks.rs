//! Explicitly instrumented development builds only; never a Cargo feature.
//!
//! Use RUSTFLAGS="--cfg alopex_wal_test_hooks" for the manual public API tests.
//! Normal builds (including --all-features) do not expose or execute these hooks.
#[cfg(not(debug_assertions))]
compile_error!("alopex_wal_test_hooks must not be enabled in release builds");

use crate::error::{Error, Result};
use std::cell::Cell;

thread_local! {
    static ARMED: Cell<Option<(u8, bool)>> = const { Cell::new(None) };
}

/// Arm one persistence boundary on the calling thread. Consumed on first hit.
/// Stages 1..=5 and 8 match the cfg(test) public LSM acceptance matrix.
pub fn arm(stage: u8, exit: bool) -> Result<()> {
    if !matches!(stage, 1..=5 | 8) {
        return Err(Error::InvalidFormat(
            "invalid test persistence stage".into(),
        ));
    }
    ARMED.set(Some((stage, exit)));
    Ok(())
}

pub(super) fn hit(stage: u8) -> Result<()> {
    let action = ARMED.with(|armed| match armed.get() {
        Some((target, exit)) if target == stage => {
            armed.set(None);
            Some(exit)
        }
        _ => None,
    });
    match action {
        Some(true) => std::process::exit(80 + i32::from(stage)),
        Some(false) => Err(std::io::Error::other("injected overflow persistence failure").into()),
        None => Ok(()),
    }
}
