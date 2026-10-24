//! Explicitly instrumented development builds only; never a Cargo feature.
//!
//! Use RUSTFLAGS="--cfg alopex_wal_test_hooks" for the manual public API tests.
//! Normal builds (including --all-features) do not expose or execute these hooks.
#[cfg(not(debug_assertions))]
compile_error!("alopex_wal_test_hooks must not be enabled in release builds");

use crate::error::{Error, Result};
use std::cell::Cell;
use std::io::{Read, Write};

thread_local! {
    static ARMED: Cell<Option<(u8, bool)>> = const { Cell::new(None) };
    static PAUSE_REFERENCE: Cell<bool> = const { Cell::new(false) };
}

/// Arm one persistence boundary on the calling thread. Consumed on first hit.
/// Stages 1..=5 and 8 match the cfg(test) public LSM acceptance matrix.
pub fn arm(stage: u8, exit: bool) -> Result<()> {
    if !matches!(stage, 1..=5 | 8) {
        return Err(Error::InvalidFormat(
            "invalid test persistence stage".into(),
        ));
    }
    PAUSE_REFERENCE.set(false);
    ARMED.set(Some((stage, exit)));
    Ok(())
}

/// Pause once after spool publication, before writing the WAL reference.
/// A manual parent process can apply an OS limit, then send `G` on stdin.
/// This hook controls timing only: it does not substitute an I/O error.
pub fn pause_before_reference() {
    ARMED.set(None);
    PAUSE_REFERENCE.set(true);
}

fn wait_for_resume(input: &mut impl Read, output: &mut impl Write) -> std::io::Result<()> {
    output.write_all(b"ALOPEX_BEFORE_REFERENCE\n")?;
    output.flush()?;
    let mut token = [0];
    input.read_exact(&mut token)?;
    if token != *b"G" {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "invalid manual WAL resume token",
        ));
    }
    Ok(())
}

pub(super) fn hit(stage: u8) -> Result<()> {
    if stage == 4 && PAUSE_REFERENCE.replace(false) {
        wait_for_resume(&mut std::io::stdin().lock(), &mut std::io::stdout().lock())?;
    }
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

#[cfg(test)]
mod tests {
    use super::wait_for_resume;
    use std::io::ErrorKind;

    #[test]
    fn resumes_only_after_parent_acknowledgement() {
        let mut output = Vec::new();
        wait_for_resume(&mut &b"G"[..], &mut output).unwrap();
        assert_eq!(output, b"ALOPEX_BEFORE_REFERENCE\n");
    }

    #[test]
    fn refuses_closed_parent_pipe() {
        let error = wait_for_resume(&mut &b""[..], &mut Vec::new()).unwrap_err();
        assert_eq!(error.kind(), ErrorKind::UnexpectedEof);
    }

    #[test]
    fn refuses_wrong_parent_acknowledgement() {
        let error = wait_for_resume(&mut &b"X"[..], &mut Vec::new()).unwrap_err();
        assert_eq!(error.kind(), ErrorKind::InvalidData);
    }
}
