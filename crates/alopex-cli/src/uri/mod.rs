//! URI Parser
//!
//! Parses storage URIs and validates S3 credentials.
//! Only handles URI parsing; storage construction is delegated to alopex-embedded.

use std::path::PathBuf;

use crate::error::{CliError, Result};

/// Storage URI representing either local filesystem or S3.
///
/// CLI only parses the URI; actual storage construction is delegated to alopex-embedded.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StorageUri {
    /// Local filesystem path
    Local(PathBuf),
    /// S3 bucket and prefix
    S3 {
        /// S3 bucket name
        bucket: String,
        /// Object prefix (path within bucket)
        prefix: String,
    },
}

impl StorageUri {
    /// Parse a URI string and return the appropriate StorageUri variant.
    ///
    /// # URI Schemes
    /// - No scheme or `file://` → Local filesystem
    /// - `s3://` → S3 storage
    ///
    /// # Examples
    /// ```ignore
    /// // Local paths
    /// StorageUri::parse("/path/to/db")?;           // Local
    /// StorageUri::parse("file:///path/to/db")?;    // Local
    /// StorageUri::parse("./relative/path")?;       // Local
    ///
    /// // S3 paths
    /// StorageUri::parse("s3://bucket/prefix")?;    // S3
    /// StorageUri::parse("s3://my-bucket/data/db")? // S3
    /// ```
    pub fn parse(uri: &str) -> Result<Self> {
        let uri = uri.trim();

        if uri.is_empty() {
            return Err(CliError::InvalidArgument("URI cannot be empty".to_string()));
        }

        if let Some(path) = uri.strip_prefix("file://") {
            // file:// scheme
            Ok(StorageUri::Local(PathBuf::from(path)))
        } else if let Some(rest) = uri.strip_prefix("s3://") {
            // s3:// scheme
            Self::parse_s3_uri(rest)
        } else if uri.contains("://") {
            // Unknown scheme
            let scheme = uri.split("://").next().unwrap_or("");
            Err(CliError::InvalidArgument(format!(
                "Unsupported URI scheme: '{}'. Use 's3://' or 'file://' or a local path",
                scheme
            )))
        } else {
            // No scheme - treat as local path
            Ok(StorageUri::Local(PathBuf::from(uri)))
        }
    }

    /// Parse the bucket and prefix from an S3 URI (after s3:// prefix).
    fn parse_s3_uri(rest: &str) -> Result<Self> {
        if rest.is_empty() {
            return Err(CliError::InvalidArgument(
                "S3 URI must specify a bucket: s3://bucket[/prefix]".to_string(),
            ));
        }

        let (bucket, prefix) = match rest.find('/') {
            Some(idx) => {
                let bucket = &rest[..idx];
                let prefix = &rest[idx + 1..];
                (bucket.to_string(), prefix.to_string())
            }
            None => (rest.to_string(), String::new()),
        };

        if bucket.is_empty() {
            return Err(CliError::InvalidArgument(
                "S3 bucket name cannot be empty".to_string(),
            ));
        }

        Ok(StorageUri::S3 { bucket, prefix })
    }

    /// Return the normalized URI string to pass to alopex-embedded.
    ///
    /// For local paths, returns the absolute path string.
    /// For S3, returns the full s3:// URI.
    pub fn to_embedded_uri(&self) -> String {
        match self {
            StorageUri::Local(path) => {
                // Return the path as-is; alopex-embedded handles path normalization
                path.display().to_string()
            }
            StorageUri::S3 { bucket, prefix } => {
                if prefix.is_empty() {
                    format!("s3://{}", bucket)
                } else {
                    format!("s3://{}/{}", bucket, prefix)
                }
            }
        }
    }

    /// Check if this is an S3 URI.
    pub fn is_s3(&self) -> bool {
        matches!(self, StorageUri::S3 { .. })
    }

    /// Check if this is a local path.
    #[allow(dead_code)]
    pub fn is_local(&self) -> bool {
        matches!(self, StorageUri::Local(_))
    }
}

/// Validate that S3 credentials are available in environment variables.
///
/// Checks for the presence of:
/// - `AWS_ACCESS_KEY_ID`
/// - `AWS_SECRET_ACCESS_KEY`
/// - `AWS_REGION`
///
/// This is a CLI-side check to provide better error messages before
/// attempting to connect to S3. The actual S3 connection is handled
/// by alopex-embedded.
pub fn validate_s3_credentials() -> Result<()> {
    let mut missing = Vec::new();

    if std::env::var("AWS_ACCESS_KEY_ID").is_err() {
        missing.push("AWS_ACCESS_KEY_ID");
    }
    if std::env::var("AWS_SECRET_ACCESS_KEY").is_err() {
        missing.push("AWS_SECRET_ACCESS_KEY");
    }
    if std::env::var("AWS_REGION").is_err() {
        missing.push("AWS_REGION");
    }

    if missing.is_empty() {
        Ok(())
    } else {
        Err(CliError::Credentials(format!(
            "Missing S3 credentials. Please set the following environment variables: {}",
            missing.join(", ")
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ========== Local URI Tests ==========

    #[test]
    fn test_parse_local_absolute_path() {
        let uri = StorageUri::parse("/path/to/db").unwrap();
        assert!(uri.is_local());
        assert!(!uri.is_s3());
        assert_eq!(uri, StorageUri::Local(PathBuf::from("/path/to/db")));
    }

    #[test]
    fn test_parse_local_relative_path() {
        let uri = StorageUri::parse("./relative/path").unwrap();
        assert!(uri.is_local());
        assert_eq!(uri, StorageUri::Local(PathBuf::from("./relative/path")));
    }

    #[test]
    fn test_parse_file_scheme() {
        let uri = StorageUri::parse("file:///path/to/db").unwrap();
        assert!(uri.is_local());
        assert_eq!(uri, StorageUri::Local(PathBuf::from("/path/to/db")));
    }

    #[test]
    fn test_parse_local_to_embedded_uri() {
        let uri = StorageUri::parse("/path/to/db").unwrap();
        assert_eq!(uri.to_embedded_uri(), "/path/to/db");
    }

    // ========== S3 URI Tests ==========

    #[test]
    fn test_parse_s3_with_prefix() {
        let uri = StorageUri::parse("s3://my-bucket/data/db").unwrap();
        assert!(uri.is_s3());
        assert!(!uri.is_local());
        assert_eq!(
            uri,
            StorageUri::S3 {
                bucket: "my-bucket".to_string(),
                prefix: "data/db".to_string(),
            }
        );
    }

    #[test]
    fn test_parse_s3_bucket_only() {
        let uri = StorageUri::parse("s3://my-bucket").unwrap();
        assert_eq!(
            uri,
            StorageUri::S3 {
                bucket: "my-bucket".to_string(),
                prefix: String::new(),
            }
        );
    }

    #[test]
    fn test_parse_s3_bucket_with_trailing_slash() {
        let uri = StorageUri::parse("s3://my-bucket/").unwrap();
        assert_eq!(
            uri,
            StorageUri::S3 {
                bucket: "my-bucket".to_string(),
                prefix: String::new(),
            }
        );
    }

    #[test]
    fn test_s3_to_embedded_uri_with_prefix() {
        let uri = StorageUri::S3 {
            bucket: "my-bucket".to_string(),
            prefix: "data/db".to_string(),
        };
        assert_eq!(uri.to_embedded_uri(), "s3://my-bucket/data/db");
    }

    #[test]
    fn test_s3_to_embedded_uri_bucket_only() {
        let uri = StorageUri::S3 {
            bucket: "my-bucket".to_string(),
            prefix: String::new(),
        };
        assert_eq!(uri.to_embedded_uri(), "s3://my-bucket");
    }

    // ========== Error Cases ==========

    #[test]
    fn test_parse_empty_uri() {
        let err = StorageUri::parse("").unwrap_err();
        assert!(matches!(err, CliError::InvalidArgument(_)));
    }

    #[test]
    fn test_parse_unknown_scheme() {
        let err = StorageUri::parse("http://example.com").unwrap_err();
        assert!(matches!(err, CliError::InvalidArgument(_)));
    }

    #[test]
    fn test_parse_s3_empty_bucket() {
        let err = StorageUri::parse("s3://").unwrap_err();
        assert!(matches!(err, CliError::InvalidArgument(_)));
    }

    #[test]
    fn test_parse_whitespace_trimmed() {
        let uri = StorageUri::parse("  /path/to/db  ").unwrap();
        assert_eq!(uri, StorageUri::Local(PathBuf::from("/path/to/db")));
    }

    // ========== Credential Validation Tests ==========

    #[test]
    fn test_validate_s3_credentials_missing() {
        // Save current values
        let access_key = std::env::var("AWS_ACCESS_KEY_ID").ok();
        let secret_key = std::env::var("AWS_SECRET_ACCESS_KEY").ok();
        let region = std::env::var("AWS_REGION").ok();

        // Remove all credentials
        std::env::remove_var("AWS_ACCESS_KEY_ID");
        std::env::remove_var("AWS_SECRET_ACCESS_KEY");
        std::env::remove_var("AWS_REGION");

        let result = validate_s3_credentials();
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, CliError::Credentials(_)));

        // Restore original values
        if let Some(v) = access_key {
            std::env::set_var("AWS_ACCESS_KEY_ID", v);
        }
        if let Some(v) = secret_key {
            std::env::set_var("AWS_SECRET_ACCESS_KEY", v);
        }
        if let Some(v) = region {
            std::env::set_var("AWS_REGION", v);
        }
    }

    #[test]
    fn test_validate_s3_credentials_present() {
        // Save current values
        let access_key = std::env::var("AWS_ACCESS_KEY_ID").ok();
        let secret_key = std::env::var("AWS_SECRET_ACCESS_KEY").ok();
        let region = std::env::var("AWS_REGION").ok();

        // Set all credentials
        std::env::set_var("AWS_ACCESS_KEY_ID", "test-key");
        std::env::set_var("AWS_SECRET_ACCESS_KEY", "test-secret");
        std::env::set_var("AWS_REGION", "us-east-1");

        let result = validate_s3_credentials();
        assert!(result.is_ok());

        // Restore original values
        match access_key {
            Some(v) => std::env::set_var("AWS_ACCESS_KEY_ID", v),
            None => std::env::remove_var("AWS_ACCESS_KEY_ID"),
        }
        match secret_key {
            Some(v) => std::env::set_var("AWS_SECRET_ACCESS_KEY", v),
            None => std::env::remove_var("AWS_SECRET_ACCESS_KEY"),
        }
        match region {
            Some(v) => std::env::set_var("AWS_REGION", v),
            None => std::env::remove_var("AWS_REGION"),
        }
    }
}
