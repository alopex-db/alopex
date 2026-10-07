use super::{CopySecurityConfig, validate_file_path};
use crate::executor::{ExecutorError, Result};
use cap_fs_ext::{FollowSymlinks, OpenOptionsFollowExt};
use cap_primitives::fs::{OpenOptions, open, open_ambient_dir};
use std::fs::File;
use std::path::Path;

pub(super) fn open_input_file(path: &str, config: &CopySecurityConfig) -> Result<File> {
    open_input_file_with_after_validation(path, config, || {})
}

pub(super) fn open_input_file_with_after_validation(
    path: &str,
    config: &CopySecurityConfig,
    after_validation: impl FnOnce(),
) -> Result<File> {
    let Some(base_dirs) = &config.allowed_base_dirs else {
        // Trusted unrestricted callers retain their existing diagnostics.
        validate_file_path(path, config)?;
        after_validation();
        return File::open(path)
            .map_err(|error| ExecutorError::BulkLoad(format!("failed to open parquet: {error}")));
    };
    let denied = || ExecutorError::PathValidationFailed {
        path: path.into(),
        reason: "path not in allowed directories".into(),
    };

    // Pin roots at this operation's authorization start, not after validation.
    // The DTO does not promise identity continuity from configuration time.
    // Keep configured path identities; never canonicalize roots to new targets.
    let roots: Vec<_> = base_dirs
        .iter()
        .filter_map(|base| {
            open_ambient_dir(base, cap_primitives::ambient_authority())
                .ok()
                .map(|directory| (base, directory))
        })
        .collect();

    // Preserve original-leaf symlink checks and existing file policy errors.
    validate_file_path(path, config)?;
    let input = Path::new(path);
    let target = if config.allow_symlinks {
        input.canonicalize().map_err(|_| denied())?
    } else {
        // Resolve ancestors, but retain the original leaf for cap's nofollow.
        let parent = input
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."));
        parent
            .canonicalize()
            .map_err(|_| denied())?
            .join(input.file_name().ok_or_else(denied)?)
    };
    let (base, directory) = roots
        .iter()
        .find(|(base, _)| target.starts_with(base))
        .ok_or_else(denied)?;
    let relative = target.strip_prefix(base).map_err(|_| denied())?;
    after_validation();

    let mut options = OpenOptions::new();
    options.read(true).follow(if config.allow_symlinks {
        FollowSymlinks::Yes
    } else {
        FollowSymlinks::No
    });
    let file = open(directory, relative, &options).map_err(|_| denied())?;
    let metadata = file.metadata().map_err(|_| denied())?;
    if !metadata.is_file() {
        return Err(denied());
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if metadata.permissions().mode() & 0o444 == 0 {
            return Err(denied());
        }
    }
    Ok(file)
}
