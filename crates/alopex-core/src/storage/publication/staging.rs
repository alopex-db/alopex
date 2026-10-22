//! Reclaim only a redundant prefix of an authoritative file, under the caller's
//! exclusive database lock. Never promote a stale file. The caller validates the
//! source format and supplies the only permitted header transformation.

use crate::error::{Error, Result};
use std::fs::{self, File, OpenOptions};
use std::io::Read;
use std::path::Path;

pub(crate) fn recover_prefix(
    source: &Path,
    temporary: &Path,
    budget: u64,
    mut alternate: impl FnMut(u64, &mut [u8]) -> Result<()>,
) -> Result<()> {
    let metadata = match fs::symlink_metadata(temporary) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error.into()),
    };
    if !metadata.is_file() || !fs::symlink_metadata(source)?.is_file() {
        return Err(Error::InvalidFormat(
            "staging recovery requires regular files".into(),
        ));
    }
    let mut input = File::open(source)?;
    let mut staged = OpenOptions::new().read(true).write(true).open(temporary)?;
    let source_bytes = input.metadata()?.len();
    let staged_bytes = staged.metadata()?.len();
    if source_bytes > budget || staged_bytes > budget {
        return Err(Error::WalCapacityExceeded {
            reason: "staging recovery exceeds copy budget".into(),
        });
    }
    if staged_bytes > source_bytes {
        return Err(Error::InvalidFormat(
            "staging exceeds authoritative source".into(),
        ));
    }
    // Compare identities of the opened handles, not path strings. This also
    // refuses hard links and case/short-name aliases before any truncation.
    if same_file::Handle::from_file(input.try_clone()?)?
        == same_file::Handle::from_file(staged.try_clone()?)?
    {
        return Err(Error::InvalidFormat(
            "staging aliases authoritative source".into(),
        ));
    }
    let mut original = [0u8; 1024];
    let mut changed = [0u8; 1024];
    let mut actual = [0u8; 1024];
    let mut offset = 0;
    while offset < staged_bytes {
        let length = (staged_bytes - offset).min(original.len() as u64) as usize;
        input.read_exact(&mut original[..length])?;
        staged.read_exact(&mut actual[..length])?;
        changed[..length].copy_from_slice(&original[..length]);
        alternate(offset, &mut changed[..length])?;
        if (0..length).any(|i| actual[i] != original[i] && actual[i] != changed[i]) {
            return Err(Error::InvalidFormat(
                "unrecognized staging contents; file preserved".into(),
            ));
        }
        offset += length as u64;
    }
    // Empty data is synced before unlink. If deletion is interrupted, an empty
    // prefix remains recoverable and does not consume another copy's budget.
    staged.set_len(0)?;
    staged.sync_all()?;
    drop(staged);
    fs::remove_file(temporary)?;
    #[cfg(not(windows))]
    File::open(
        temporary
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or(Path::new(".")),
    )?
    .sync_all()?;
    Ok(())
}

#[cfg(test)]
mod tests;
