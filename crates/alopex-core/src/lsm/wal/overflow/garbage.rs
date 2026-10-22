//! Recoverable retirement of unreferenced spool files. Only the exclusive owner
//! may invoke this after checkpoint/recovery proves that no WAL needs the file.
//! A reserved length in the name keeps a failed truncate/flush charged. Capacity
//! is released only after zero length is synced and published with reservation 0.
//! A subsequently resurrected deleted entry is therefore empty, not new capacity.

#[cfg(any(windows, test))]
use std::path::Path;

pub(crate) type Reference = ([u8; 16], u64);

pub(crate) fn identity(name: &str) -> Option<(Reference, &str)> {
    let (stem, extension) = name.rsplit_once('.')?;
    if stem.len() != 49 || stem.as_bytes()[32] != b'-' {
        return None;
    }
    let transaction = &stem[..32];
    let lsn = &stem[33..];
    if !transaction
        .bytes()
        .chain(lsn.bytes())
        .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        return None;
    }
    let mut transaction_id = [0; 16];
    for (index, byte) in transaction_id.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&transaction[index * 2..index * 2 + 2], 16).ok()?;
    }
    let lsn = u64::from_str_radix(lsn, 16).ok()?;
    if transaction_id == [0; 16] || lsn == 0 {
        return None;
    }
    Some(((transaction_id, lsn), extension))
}

pub(crate) fn retired(name: &str) -> Option<(u64, &str)> {
    let rest = name.strip_prefix(".gc-")?;
    let (length, original) = rest.split_once('-')?;
    if length.len() != 16
        || !length
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        || !identity(original).is_some_and(|(_, extension)| matches!(extension, "spool" | "tmp"))
    {
        return None;
    }
    Some((u64::from_str_radix(length, 16).ok()?, original))
}

pub(crate) fn charge(name: Option<&str>, actual: u64) -> u64 {
    actual.max(name.and_then(retired).map_or(0, |(reserved, _)| reserved))
}

#[cfg(any(windows, test))]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Phase {
    Retired,
    Truncated,
    Synced,
    Released,
}

// A previous failed deletion can leave the zero name behind before an
// uncommitted LSN is reused. Only that canonical, empty retirement is disposable;
// an occupied/non-regular destination must still block publication.
#[cfg(any(windows, test))]
fn clear_empty_destination(path: &Path) -> std::io::Result<()> {
    match std::fs::symlink_metadata(path) {
        Ok(metadata) if metadata.is_file() && metadata.len() == 0 => std::fs::remove_file(path),
        Ok(_) => Err(std::io::ErrorKind::AlreadyExists.into()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error),
    }
}

#[cfg(windows)]
pub(crate) fn remove(path: &Path) -> std::io::Result<()> {
    remove_with(
        path,
        &|from, to| crate::storage::publication::windows::publish(from, to, false),
        &|_| Ok(()),
    )
}

// The publisher must persist a same-directory, non-replacing move. Fault hooks
// let tiny tests stop at each boundary without simulating file system durability.
#[cfg(any(windows, test))]
fn remove_with(
    path: &Path,
    publish: &impl Fn(&Path, &Path) -> std::io::Result<()>,
    phase: &impl Fn(Phase) -> std::io::Result<()>,
) -> std::io::Result<()> {
    use std::{fs, io};
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| io::Error::other("invalid spool name"))?;
    let metadata = fs::symlink_metadata(path)?;
    if !metadata.is_file() {
        return Err(io::Error::other("garbage is not a regular file"));
    }
    let (reserved, original, staged) = if let Some((reserved, original)) = retired(name) {
        if metadata.len() > reserved {
            return Err(io::Error::other("retired spool exceeds its reservation"));
        }
        (reserved, original, path.to_path_buf())
    } else {
        if !identity(name).is_some_and(|(_, extension)| matches!(extension, "spool" | "tmp")) {
            return Err(io::Error::other("refusing to retire an unknown spool name"));
        }
        let reserved = metadata.len();
        let staged = path.with_file_name(format!(".gc-{reserved:016x}-{name}"));
        publish(path, &staged)?;
        (reserved, name, staged)
    };
    phase(Phase::Retired)?;
    let file = fs::OpenOptions::new().write(true).open(&staged)?;
    file.set_len(0)?;
    phase(Phase::Truncated)?;
    file.sync_all()?;
    phase(Phase::Synced)?;
    drop(file);
    let empty = if reserved == 0 {
        staged
    } else {
        let empty = path.with_file_name(format!(".gc-0000000000000000-{original}"));
        clear_empty_destination(&empty)?;
        publish(&staged, &empty)?;
        empty
    };
    phase(Phase::Released)?;
    fs::remove_file(empty)
}

#[cfg(test)]
mod tests;
