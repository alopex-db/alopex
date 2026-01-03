use super::{cli_version, supported_format_max, supported_format_min, Version};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VersionCheckResult {
    Compatible,
    CliOlderThanFile { cli: Version, file: Version },
    Incompatible { cli: Version, file: Version },
}

pub struct VersionChecker {
    cli_version: Version,
    supported_format_min: Version,
    supported_format_max: Version,
}

impl VersionChecker {
    pub fn new() -> Self {
        Self {
            cli_version: cli_version(),
            supported_format_min: supported_format_min(),
            supported_format_max: supported_format_max(),
        }
    }

    #[cfg(test)]
    pub(crate) fn new_with(cli_version: Version, min: Version, max: Version) -> Self {
        Self {
            cli_version,
            supported_format_min: min,
            supported_format_max: max,
        }
    }

    pub fn check_compatibility(&self, file_format_version: Version) -> VersionCheckResult {
        if file_format_version < self.supported_format_min
            || file_format_version > self.supported_format_max
        {
            return VersionCheckResult::Incompatible {
                cli: self.cli_version,
                file: file_format_version,
            };
        }

        if file_format_version > self.cli_version {
            return VersionCheckResult::CliOlderThanFile {
                cli: self.cli_version,
                file: file_format_version,
            };
        }

        VersionCheckResult::Compatible
    }
}

impl Default for VersionChecker {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn checker(cli: Version, min: Version, max: Version) -> VersionChecker {
        VersionChecker::new_with(cli, min, max)
    }

    #[test]
    fn compatible_when_file_in_range_and_not_newer() {
        let checker = checker(
            Version::new(1, 2, 0),
            Version::new(1, 0, 0),
            Version::new(2, 0, 0),
        );
        let result = checker.check_compatibility(Version::new(1, 1, 0));
        assert!(matches!(result, VersionCheckResult::Compatible));
    }

    #[test]
    fn warns_when_file_is_newer_but_supported() {
        let checker = checker(
            Version::new(1, 0, 0),
            Version::new(1, 0, 0),
            Version::new(2, 0, 0),
        );
        let result = checker.check_compatibility(Version::new(1, 5, 0));
        assert!(matches!(
            result,
            VersionCheckResult::CliOlderThanFile { .. }
        ));
    }

    #[test]
    fn incompatible_when_file_too_new() {
        let checker = checker(
            Version::new(1, 0, 0),
            Version::new(1, 0, 0),
            Version::new(1, 2, 0),
        );
        let result = checker.check_compatibility(Version::new(2, 0, 0));
        assert!(matches!(result, VersionCheckResult::Incompatible { .. }));
    }

    #[test]
    fn incompatible_when_file_too_old() {
        let checker = checker(
            Version::new(1, 0, 0),
            Version::new(1, 0, 0),
            Version::new(2, 0, 0),
        );
        let result = checker.check_compatibility(Version::new(0, 9, 0));
        assert!(matches!(result, VersionCheckResult::Incompatible { .. }));
    }
}
