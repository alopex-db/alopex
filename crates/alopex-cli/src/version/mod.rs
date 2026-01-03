use std::fmt;

use alopex_core::storage::format::FileVersion;

pub mod compatibility;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Version {
    pub major: u16,
    pub minor: u16,
    pub patch: u16,
}

impl Version {
    pub const fn new(major: u16, minor: u16, patch: u16) -> Self {
        Self {
            major,
            minor,
            patch,
        }
    }

    pub fn parse(raw: &str) -> Self {
        let mut parts = raw.split('.');
        let major = parse_part(parts.next());
        let minor = parse_part(parts.next());
        let patch = parse_part(parts.next());
        Self {
            major,
            minor,
            patch,
        }
    }
}

impl From<FileVersion> for Version {
    fn from(value: FileVersion) -> Self {
        Self::new(value.major, value.minor, value.patch)
    }
}

impl fmt::Display for Version {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}.{}.{}", self.major, self.minor, self.patch)
    }
}

pub fn cli_version() -> Version {
    Version::parse(env!("CARGO_PKG_VERSION"))
}

pub fn supported_format_min() -> Version {
    Version::new(0, 1, 0)
}

pub fn supported_format_max() -> Version {
    Version::from(FileVersion::CURRENT)
}

fn parse_part(part: Option<&str>) -> u16 {
    part.and_then(|value| {
        let digits: String = value.chars().take_while(|c| c.is_ascii_digit()).collect();
        if digits.is_empty() {
            None
        } else {
            digits.parse::<u16>().ok()
        }
    })
    .unwrap_or(0)
}
