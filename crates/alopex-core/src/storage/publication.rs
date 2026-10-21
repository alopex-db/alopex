//! File publication within existing directories owned by the caller.
//!
//! The source must be fully synced and closed before publication. Unix callers
//! still sync the parent directory after rename. Windows uses a write-through
//! move, never a cross-volume copy/delete. This does not persist new ancestors
//! or replace a directory-sync protocol. On error, do not acknowledge a commit.

#[cfg(windows)]
pub(crate) mod windows;

/// Replace a destination with an already synced and closed temporary file.
pub(crate) fn replace_file(
    temporary: &std::path::Path,
    path: &std::path::Path,
) -> std::io::Result<()> {
    #[cfg(windows)]
    {
        windows::publish(temporary, path, true)
    }
    #[cfg(not(windows))]
    {
        std::fs::rename(temporary, path)
    }
}
