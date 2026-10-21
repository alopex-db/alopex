//! Only the exclusive LSM owner may collect after durable checkpoint/WAL
//! reclamation or complete recovery reconciliation. Unknown names remain charged.

use super::*;

pub(super) type Reference = ([u8; 16], u64);

fn identity(name: &str) -> Option<(Reference, &str)> {
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

fn collectible(name: &str, checkpoint: u64) -> bool {
    identity(name).is_some_and(|((_, lsn), extension)| {
        extension == "tmp" || (extension == "spool" && lsn <= checkpoint)
    })
}

pub(super) fn collect(directory: &Path, checkpoint: u64) -> Result<()> {
    collect_matching(directory, |name| collectible(name, checkpoint))
}

// Only called after complete v3 replay, activation, and SST discovery succeed.
// The sorted reference index is bounded by the configured replay budget; a
// directory scan needs no file list or payload allocation.
pub(super) fn collect_recovered(directory: &Path, references: &[Reference]) -> Result<()> {
    collect_matching(directory, |name| {
        identity(name).is_some_and(|(reference, extension)| {
            extension == "tmp"
                || (extension == "spool" && references.binary_search(&reference).is_err())
        })
    })
}

fn collect_matching(directory: &Path, predicate: impl Fn(&str) -> bool) -> Result<()> {
    // Validate the entire directory before deleting anything, including symlinks.
    disk::directory_usage(directory)?;
    for entry in fs::read_dir(directory)? {
        let entry = entry?;
        if entry.file_name().to_str().is_some_and(&predicate) {
            fault(5)?;
            fs::remove_file(entry.path())?;
        }
    }
    fault(6)?;
    disk::sync_dir(directory)
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(16))]
        #[test]
        fn sealed_gc_obeys_inclusive_checkpoint(lsn in 1u64..20, checkpoint in 0u64..20) {
            let name = format!("{}-{lsn:016x}.spool", "01".repeat(16));
            prop_assert_eq!(collectible(&name, checkpoint), lsn <= checkpoint);
            prop_assert!(collectible(&name.replace(".spool", ".tmp"), checkpoint));
        }
    }

    #[test]
    fn foreign_or_malformed_names_are_never_collected() {
        for name in [
            "other.tmp",
            "../../foo.spool",
            "ééééééééééééééééééééééééé.spool",
            "00000000000000000000000000000000-0000000000000001.spool",
            "01010101010101010101010101010101-0000000000000000.spool",
            "01010101010101010101010101010101-0000000000000001.other",
        ] {
            assert!(!collectible(name, u64::MAX));
        }
    }
}
