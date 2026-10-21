//! Only the exclusive LSM owner may invoke collection, after durable checkpoint
//! publication and successful WAL reclamation. Unknown names remain charged.

use super::*;

fn collectible(name: &str, checkpoint: u64) -> bool {
    let Some((stem, extension)) = name.rsplit_once('.') else {
        return false;
    };
    if stem.len() != 49 || stem.as_bytes()[32] != b'-' {
        return false;
    }
    let transaction = &stem[..32];
    let lsn = &stem[33..];
    if !transaction
        .bytes()
        .chain(lsn.bytes())
        .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        || transaction.bytes().all(|b| b == b'0')
    {
        return false;
    }
    let Ok(lsn) = u64::from_str_radix(lsn, 16) else {
        return false;
    };
    lsn != 0 && (extension == "tmp" || (extension == "spool" && lsn <= checkpoint))
}

pub(super) fn collect(directory: &Path, checkpoint: u64) -> Result<()> {
    // Validate the owned directory, including unexpected symlinks, before any
    // deletion. No recursive deletion or payload-size-dependent allocation.
    disk::directory_usage(directory)?;
    for entry in fs::read_dir(directory)? {
        let entry = entry?;
        let name = entry.file_name();
        if name
            .to_str()
            .is_some_and(|name| collectible(name, checkpoint))
        {
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
