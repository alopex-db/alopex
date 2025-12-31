//! S3-backed key-value storage using object_store.
//!
//! This module provides S3 storage support by:
//! 1. Downloading data from S3 to a local temp directory
//! 2. Using LsmKV for local operations
//! 3. Syncing changes back to S3 on flush/close
//!
//! # Safety
//!
//! When no prefix is specified (e.g., `s3://bucket`), the storage automatically
//! uses `_alopex/` as the prefix to prevent accidental bucket-wide operations.
//! This ensures deletion sync only affects alopex-managed objects.

/// Default namespace prefix used when no user prefix is specified.
/// This prevents accidental deletion of unrelated S3 objects.
const ALOPEX_S3_NAMESPACE: &str = "_alopex";

use std::path::{Path, PathBuf};
use std::sync::Arc;

use bytes::Bytes;
use futures::stream::StreamExt;
use object_store::aws::AmazonS3Builder;
use object_store::path::Path as ObjectPath;
use object_store::{ObjectStore, PutPayload};
use tokio::runtime::Runtime;

use crate::error::{Error, Result};
use crate::kv::KVStore;
use crate::lsm::{LsmKV, LsmKVConfig};
use crate::types::TxnMode;

/// Map S3 error to user-friendly message with Japanese support.
///
/// This function detects common S3 errors and returns more helpful messages.
fn map_s3_error<E: std::fmt::Display>(e: E) -> Error {
    let msg = e.to_string();

    // AccessDenied errors (FR-9.4)
    if msg.contains("AccessDenied") || msg.contains("Access Denied") {
        return Error::S3(format!(
            "アクセスが拒否されました (AccessDenied): バケット/プレフィックスへの書き込み権限がありません。\n\
             AWS 認証情報と IAM ポリシーを確認してください。\n\
             詳細: {}",
            msg
        ));
    }

    // NoSuchBucket errors
    if msg.contains("NoSuchBucket") {
        return Error::S3(format!(
            "バケットが見つかりません (NoSuchBucket): 指定された S3 バケットは存在しません。\n\
             詳細: {}",
            msg
        ));
    }

    // NoSuchKey errors
    if msg.contains("NoSuchKey") {
        return Error::S3(format!(
            "オブジェクトが見つかりません (NoSuchKey): 指定されたキーは存在しません。\n\
             詳細: {}",
            msg
        ));
    }

    // InvalidAccessKeyId errors
    if msg.contains("InvalidAccessKeyId") {
        return Error::S3(format!(
            "無効なアクセスキー (InvalidAccessKeyId): AWS_ACCESS_KEY_ID が正しくありません。\n\
             詳細: {}",
            msg
        ));
    }

    // SignatureDoesNotMatch errors
    if msg.contains("SignatureDoesNotMatch") {
        return Error::S3(format!(
            "署名が一致しません (SignatureDoesNotMatch): AWS_SECRET_ACCESS_KEY が正しくありません。\n\
             詳細: {}",
            msg
        ));
    }

    // Default: return original message
    Error::S3(msg)
}

/// S3-backed KV store configuration.
#[derive(Debug, Clone)]
pub struct S3Config {
    /// S3 bucket name.
    pub bucket: String,
    /// S3 prefix (path within bucket).
    pub prefix: String,
    /// AWS region.
    pub region: Option<String>,
    /// Local cache directory.
    pub cache_dir: Option<PathBuf>,
    /// LSM configuration.
    pub lsm_config: Option<LsmKVConfig>,
}

impl S3Config {
    /// Create a new S3 configuration from a URI.
    ///
    /// URI format: s3://bucket/prefix
    pub fn from_uri(uri: &str) -> Result<Self> {
        let url = url::Url::parse(uri).map_err(|e| Error::InvalidFormat(e.to_string()))?;

        if url.scheme() != "s3" {
            return Err(Error::InvalidFormat(format!(
                "Invalid S3 URI scheme: {}",
                url.scheme()
            )));
        }

        let bucket = url
            .host_str()
            .ok_or_else(|| Error::InvalidFormat("Missing bucket in S3 URI".into()))?
            .to_string();

        let prefix = url.path().trim_start_matches('/').to_string();

        let region = std::env::var("AWS_REGION").ok();

        Ok(Self {
            bucket,
            prefix,
            region,
            cache_dir: None,
            lsm_config: None,
        })
    }

    /// Set the local cache directory.
    pub fn with_cache_dir(mut self, path: PathBuf) -> Self {
        self.cache_dir = Some(path);
        self
    }
}

/// S3-backed KV store.
///
/// This wraps an LsmKV with S3 synchronization capabilities.
pub struct S3KV {
    /// Underlying LsmKV for local operations.
    inner: LsmKV,
    /// Object store client.
    store: Arc<dyn ObjectStore>,
    /// Effective prefix (uses ALOPEX_S3_NAMESPACE when config.prefix is empty).
    effective_prefix: String,
    /// Local cache directory (may be temp or user-specified).
    cache_dir: PathBuf,
    /// Tokio runtime for async operations.
    runtime: Runtime,
    /// Whether to clean up cache on drop.
    cleanup_cache: bool,
}

impl S3KV {
    /// Get the effective prefix, using the safety namespace when no prefix is specified.
    ///
    /// This also normalizes the prefix by removing trailing slashes to prevent
    /// double-slash issues in S3 keys (e.g., `prefix//file` when prefix ends with `/`).
    fn compute_effective_prefix(prefix: &str) -> String {
        let normalized = prefix.trim_end_matches('/');
        if normalized.is_empty() {
            ALOPEX_S3_NAMESPACE.to_string()
        } else {
            normalized.to_string()
        }
    }
}

impl S3KV {
    /// Open an S3-backed KV store.
    ///
    /// This will:
    /// 1. Create an object_store client
    /// 2. Download existing data from S3 to a local cache
    /// 3. Open LsmKV on the local cache
    ///
    /// # Safety
    ///
    /// When no prefix is specified, the storage automatically uses `_alopex/`
    /// as the prefix to prevent accidental bucket-wide operations.
    pub fn open(config: S3Config) -> Result<Self> {
        // Create tokio runtime for async operations
        let runtime = Runtime::new().map_err(Error::Io)?;

        // Create object store client
        let store = runtime.block_on(Self::create_object_store(&config))?;

        // Compute effective prefix (uses safety namespace when empty)
        let effective_prefix = Self::compute_effective_prefix(&config.prefix);

        // Determine cache directory
        let (cache_dir, cleanup_cache) = if let Some(ref dir) = config.cache_dir {
            (dir.clone(), false)
        } else {
            let temp_dir = std::env::temp_dir().join(format!(
                "alopex-s3-cache-{}-{}",
                config.bucket,
                effective_prefix.replace('/', "_")
            ));
            (temp_dir, true)
        };

        // Create cache directory if it doesn't exist
        std::fs::create_dir_all(&cache_dir).map_err(Error::Io)?;

        // Download data from S3 using effective prefix
        runtime.block_on(async {
            Self::download_from_s3(&store, &effective_prefix, &cache_dir).await
        })?;

        // Open LsmKV on the cache directory
        let inner = match &config.lsm_config {
            Some(cfg) => LsmKV::open_with_config(&cache_dir, cfg.clone())?,
            None => LsmKV::open(&cache_dir)?,
        };

        Ok(Self {
            inner,
            store,
            effective_prefix,
            cache_dir,
            runtime,
            cleanup_cache,
        })
    }

    /// Create the object store client.
    async fn create_object_store(config: &S3Config) -> Result<Arc<dyn ObjectStore>> {
        let mut builder = AmazonS3Builder::new()
            .with_bucket_name(&config.bucket)
            .with_access_key_id(
                std::env::var("AWS_ACCESS_KEY_ID")
                    .map_err(|_| Error::MissingCredentials("AWS_ACCESS_KEY_ID".into()))?,
            )
            .with_secret_access_key(
                std::env::var("AWS_SECRET_ACCESS_KEY")
                    .map_err(|_| Error::MissingCredentials("AWS_SECRET_ACCESS_KEY".into()))?,
            );

        if let Some(ref region) = config.region {
            builder = builder.with_region(region);
        } else if let Ok(region) = std::env::var("AWS_REGION") {
            builder = builder.with_region(&region);
        }

        // Support for custom endpoint (e.g., MinIO, LocalStack)
        if let Ok(endpoint) = std::env::var("AWS_ENDPOINT_URL") {
            builder = builder.with_endpoint(&endpoint).with_allow_http(true);
        }

        let store = builder.build().map_err(map_s3_error)?;

        Ok(Arc::new(store))
    }

    /// Download data from S3 to local cache.
    async fn download_from_s3(
        store: &Arc<dyn ObjectStore>,
        prefix: &str,
        cache_dir: &Path,
    ) -> Result<()> {
        let prefix_path = if prefix.is_empty() {
            None
        } else {
            Some(ObjectPath::from(prefix))
        };

        let mut stream = store.list(prefix_path.as_ref());

        while let Some(item) = stream.next().await {
            let meta = item.map_err(map_s3_error)?;
            let location = meta.location;

            // Calculate relative path
            let relative_path = if prefix.is_empty() {
                location.to_string()
            } else {
                location
                    .as_ref()
                    .strip_prefix(prefix)
                    .unwrap_or(location.as_ref())
                    .trim_start_matches('/')
                    .to_string()
            };

            if relative_path.is_empty() {
                continue;
            }

            // Create local file path
            let local_path = cache_dir.join(&relative_path);

            // Create parent directories
            if let Some(parent) = local_path.parent() {
                std::fs::create_dir_all(parent).map_err(Error::Io)?;
            }

            // Download file
            let data = store
                .get(&location)
                .await
                .map_err(map_s3_error)?
                .bytes()
                .await
                .map_err(map_s3_error)?;

            std::fs::write(&local_path, &data).map_err(Error::Io)?;
        }

        Ok(())
    }

    /// Sync local cache to S3 with full synchronization.
    ///
    /// This performs a complete sync:
    /// 1. Upload all local files to S3
    /// 2. Delete S3 objects that no longer exist locally
    async fn sync_to_s3(
        store: &Arc<dyn ObjectStore>,
        prefix: &str,
        cache_dir: &Path,
    ) -> Result<()> {
        // Collect local files (relative paths with forward slashes)
        let local_files = Self::collect_local_files(cache_dir)?;

        // Upload all local files
        for relative_path in &local_files {
            let local_path =
                cache_dir.join(relative_path.replace('/', std::path::MAIN_SEPARATOR_STR));
            let s3_path = if prefix.is_empty() {
                relative_path.clone()
            } else {
                format!("{}/{}", prefix, relative_path)
            };

            let location = ObjectPath::from(s3_path);
            let data = std::fs::read(&local_path).map_err(Error::Io)?;
            let payload = PutPayload::from(Bytes::from(data));

            store.put(&location, payload).await.map_err(map_s3_error)?;
        }

        // List S3 objects and delete those not in local
        let prefix_path = if prefix.is_empty() {
            None
        } else {
            Some(ObjectPath::from(prefix))
        };

        let mut stream = store.list(prefix_path.as_ref());
        while let Some(item) = stream.next().await {
            let meta = item.map_err(map_s3_error)?;
            let location = meta.location;

            // Calculate relative path from S3 key
            let relative_path = if prefix.is_empty() {
                location.to_string()
            } else {
                location
                    .as_ref()
                    .strip_prefix(prefix)
                    .unwrap_or(location.as_ref())
                    .trim_start_matches('/')
                    .to_string()
            };

            if relative_path.is_empty() {
                continue;
            }

            // Delete if not in local files
            if !local_files.contains(&relative_path) {
                store.delete(&location).await.map_err(map_s3_error)?;
            }
        }

        Ok(())
    }

    /// Collect all local files as relative paths with forward slashes.
    fn collect_local_files(cache_dir: &Path) -> Result<std::collections::HashSet<String>> {
        let mut files = std::collections::HashSet::new();
        Self::collect_files_recursive(cache_dir, cache_dir, &mut files)?;
        Ok(files)
    }

    /// Recursively collect files from a directory.
    fn collect_files_recursive(
        base_dir: &Path,
        current_dir: &Path,
        files: &mut std::collections::HashSet<String>,
    ) -> Result<()> {
        let entries = std::fs::read_dir(current_dir).map_err(Error::Io)?;

        for entry in entries {
            let entry = entry.map_err(Error::Io)?;
            let path = entry.path();

            if path.is_dir() {
                Self::collect_files_recursive(base_dir, &path, files)?;
            } else {
                let relative = path
                    .strip_prefix(base_dir)
                    .map_err(|e| Error::InvalidFormat(e.to_string()))?;

                // Normalize to forward slashes for cross-platform compatibility
                let normalized = relative
                    .components()
                    .map(|c| c.as_os_str().to_string_lossy())
                    .collect::<Vec<_>>()
                    .join("/");

                files.insert(normalized);
            }
        }

        Ok(())
    }

    /// Sync local changes to S3.
    ///
    /// This performs a full sync: uploads local files and deletes
    /// S3 objects that no longer exist locally.
    ///
    /// Deletions are scoped to the effective prefix (which uses `_alopex/`
    /// when no user prefix was specified) to prevent accidental data loss.
    pub fn sync(&self) -> Result<()> {
        // First flush local LsmKV
        self.inner.flush()?;

        // Then sync to S3 (upload + delete stale) using effective prefix
        self.runtime.block_on(async {
            Self::sync_to_s3(&self.store, &self.effective_prefix, &self.cache_dir).await
        })
    }

    /// Get the underlying LsmKV for direct access.
    pub fn inner(&self) -> &LsmKV {
        &self.inner
    }

    /// Get the cache directory path.
    pub fn cache_dir(&self) -> &Path {
        &self.cache_dir
    }
}

impl KVStore for S3KV {
    type Transaction<'a>
        = <LsmKV as KVStore>::Transaction<'a>
    where
        Self: 'a;
    type Manager<'a>
        = <LsmKV as KVStore>::Manager<'a>
    where
        Self: 'a;

    fn txn_manager(&self) -> Self::Manager<'_> {
        self.inner.txn_manager()
    }

    fn begin(&self, mode: TxnMode) -> Result<Self::Transaction<'_>> {
        self.inner.begin(mode)
    }
}

impl S3KV {
    /// Flush memtable to disk and sync to S3.
    pub fn flush(&self) -> Result<()> {
        self.sync()
    }
}

impl Drop for S3KV {
    fn drop(&mut self) {
        // Sync to S3 before dropping
        if let Err(e) = self.sync() {
            tracing::error!("Failed to sync S3 on drop: {}", e);
        }

        // Clean up cache directory if it was auto-created
        if self.cleanup_cache {
            if let Err(e) = std::fs::remove_dir_all(&self.cache_dir) {
                tracing::warn!("Failed to clean up S3 cache directory: {}", e);
            }
        }
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::*;

    #[test]
    fn test_s3_config_from_uri() {
        let config = S3Config::from_uri("s3://my-bucket/path/to/data").unwrap();
        assert_eq!(config.bucket, "my-bucket");
        assert_eq!(config.prefix, "path/to/data");
    }

    #[test]
    fn test_s3_config_from_uri_no_prefix() {
        let config = S3Config::from_uri("s3://my-bucket").unwrap();
        assert_eq!(config.bucket, "my-bucket");
        assert_eq!(config.prefix, "");
    }

    #[test]
    fn test_s3_config_invalid_scheme() {
        let result = S3Config::from_uri("http://my-bucket/path");
        assert!(result.is_err());
    }

    #[test]
    fn test_effective_prefix_uses_namespace_when_empty() {
        // When prefix is empty, effective prefix should use safety namespace
        let effective = S3KV::compute_effective_prefix("");
        assert_eq!(effective, ALOPEX_S3_NAMESPACE);
    }

    #[test]
    fn test_effective_prefix_preserves_user_prefix() {
        // When prefix is specified, effective prefix should preserve it
        let effective = S3KV::compute_effective_prefix("my/custom/path");
        assert_eq!(effective, "my/custom/path");
    }

    #[test]
    fn test_effective_prefix_removes_trailing_slash() {
        // Trailing slashes should be removed to prevent double-slash in S3 keys
        let effective = S3KV::compute_effective_prefix("my/path/");
        assert_eq!(effective, "my/path");
    }

    #[test]
    fn test_effective_prefix_removes_multiple_trailing_slashes() {
        // Multiple trailing slashes should all be removed
        let effective = S3KV::compute_effective_prefix("my/path///");
        assert_eq!(effective, "my/path");
    }

    #[test]
    fn test_effective_prefix_only_slash_uses_namespace() {
        // A prefix of just "/" should use the safety namespace
        let effective = S3KV::compute_effective_prefix("/");
        assert_eq!(effective, ALOPEX_S3_NAMESPACE);
    }
}
