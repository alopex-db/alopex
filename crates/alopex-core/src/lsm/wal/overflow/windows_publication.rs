//! Windows file publication, not a general directory flush substitute.
use super::*;
use std::os::windows::ffi::OsStrExt;
use windows_sys::Win32::Storage::FileSystem::{
    MoveFileExW, MOVEFILE_REPLACE_EXISTING, MOVEFILE_WRITE_THROUGH,
};

fn wide_path(path: &Path) -> Result<Vec<u16>> {
    // Canonicalize only the existing parent: the destination need not exist.
    // This also yields a verbatim absolute path for Unicode and long names.
    let name = path
        .file_name()
        .ok_or_else(|| invalid("publication needs a file name"))?;
    let parent = path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    let absolute = fs::canonicalize(parent)?.join(name);
    let mut wide: Vec<u16> = absolute.as_os_str().encode_wide().collect();
    if wide.contains(&0) {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "NUL in publication path",
        )
        .into());
    }
    wide.push(0);
    Ok(wide)
}

/// Publish a synced, closed regular file on the same volume. Both directories
/// must already exist durably and be exclusively owned. Do not add COPY_ALLOWED:
/// a cross-volume copy/delete is not an atomic WAL publication.
pub(super) fn publish(temporary: &Path, path: &Path, replace: bool) -> Result<()> {
    let source = wide_path(temporary)?;
    let destination = wide_path(path)?;
    let flags = MOVEFILE_WRITE_THROUGH
        | if replace {
            MOVEFILE_REPLACE_EXISTING
        } else {
            0
        };
    // SAFETY: these owned UTF-16 buffers have exactly one trailing NUL, contain
    // no interior NUL, and stay alive through this synchronous Win32 call.
    // MoveFileExW neither retains the pointers nor writes into the buffers.
    let moved = unsafe { MoveFileExW(source.as_ptr(), destination.as_ptr(), flags) };
    if moved == 0 {
        return Err(std::io::Error::last_os_error().into());
    }
    Ok(())
}
