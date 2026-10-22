//! File publication within existing directories owned by the caller.
//!
//! The source must be fully synced and closed before publication. Unix callers
//! still sync the parent directory after rename. Windows uses a write-through
//! move, never a cross-volume copy/delete. New Windows directories are published
//! separately, parent first. On error, do not acknowledge a commit.

#[cfg(all(not(target_arch = "wasm32"), any(windows, test)))]
mod directories;

#[cfg(windows)]
pub(crate) mod windows;

#[cfg(not(target_arch = "wasm32"))]
pub(crate) mod staging;

/// Create missing directories. Windows publishes each empty directory through
/// a non-replacing write-through move before creating anything inside it.
/// An existing ancestor is assumed durable; this is not a general dir flush.
pub(crate) fn create_dir_all(path: &std::path::Path) -> std::io::Result<()> {
    #[cfg(windows)]
    {
        directories::create_dir_all_with(path, &|source, target| {
            windows::publish(source, target, false)
        })
    }
    #[cfg(not(windows))]
    std::fs::create_dir_all(path)
}

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
