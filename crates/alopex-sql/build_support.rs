use std::env;
use std::ffi::OsStr;
use std::fmt;
use std::fs::{self, File};
use std::io::{BufReader, Read};
use std::path::{Path, PathBuf};

use serde::Deserialize;
use sha2::{Digest, Sha256};

pub(crate) const REQUIRED_ALOPEX_VERSION: &str = "0.8.4";
pub(crate) const REQUIRED_CONTRACT_VERSION: &str = "0.4.0";
pub(crate) const VENDOR_MANIFEST_SHA256: &str =
    "db70742bea017a4d2683ad0d17f602b25dbcdfa7f512e3c283fbb9f7fcce298d";
const VENDOR_MANIFEST_SCHEMA: &str = "alopex-parser-vendor-manifest-v1";
const VENDOR_MANIFEST_RELATIVE_PATH: &str = "nim-sql-parser/vendor/parser-vendor-manifest.json";
const MAX_MANIFEST_BYTES: u64 = 1024 * 1024;
const MAX_SIDECAR_BYTES: u64 = 1024;
const REQUIRED_TARGETS: [&str; 4] = [
    "aarch64-apple-darwin",
    "x86_64-apple-darwin",
    "x86_64-pc-windows-msvc",
    "x86_64-unknown-linux-gnu",
];

#[derive(Debug)]
pub(crate) struct ResolveError(String);

impl fmt::Display for ResolveError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

#[derive(Debug, Eq, PartialEq)]
pub(crate) struct ResolvedNativeLibrary {
    pub(crate) directory: PathBuf,
    pub(crate) library_path: PathBuf,
    pub(crate) link_behavior: LinkBehavior,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum LinkBehavior {
    UnixRpath,
    WindowsRawDylib,
}

#[derive(Clone, Copy)]
struct TargetSpec {
    library_filename: &'static str,
    link_behavior: LinkBehavior,
}

#[derive(Deserialize)]
struct VendorManifest {
    alopex_version: String,
    assets: Vec<VendorAsset>,
    contract_version: String,
    schema: String,
}

#[derive(Deserialize)]
struct VendorAsset {
    library: LibraryIdentity,
    target: String,
}

#[derive(Deserialize)]
struct LibraryIdentity {
    path: String,
    sha256: String,
    size: u64,
}

/// ローカルで Nim パーサーを再ビルドした際、vendored マニフェストとの
/// 完全一致検証をスキップするための開発専用スイッチ。
///
/// vendored の `.so` は sha256 とバイトサイズがマニフェストに固定されている
/// ため、Nim ソースを 1 文字でも変更すると再ビルド結果は原理的に受理されない
/// (issue #131)。この検証は公開物の改竄防止という正当な目的を持つので既定では
/// 維持し、明示的なオプトインのときだけ緩和する。
///
/// 値は `1` のみを有効とする。`0` や空文字、その他の値は無効 (= 厳格検証) と
/// して扱い、「設定さえすれば何でも通る」曖昧さを排除する。
pub(crate) const ALLOW_LOCAL_BUILD_ENV: &str = "ALOPEX_NIM_PARSER_ALLOW_LOCAL_BUILD";

fn local_build_allowed() -> bool {
    // テスト時は常に厳格検証とする。既存の拒否テスト群
    // (rejects_library_size_mismatch 等) は「検証が働くこと」を証明するもので、
    // 実行環境にこの変数が設定されていると誤って緑になり、検証機構の破綻を
    // 見逃す。ビルドスクリプトとしての実行時だけ環境変数を読む。
    if cfg!(test) {
        return false;
    }
    env::var_os(ALLOW_LOCAL_BUILD_ENV).is_some_and(|value| value == "1")
}

/// 検証をスキップした事実を必ず可視化する。気付かないまま未検証の
/// パーサーを使い続けることを防ぐため、スキップ時は常に警告を出す。
fn warn_verification_skipped(what: &str) {
    println!(
        "cargo:warning={ALLOW_LOCAL_BUILD_ENV}=1 のため{what}を検証していません (issue #131)。リリース検証では使用しないこと。"
    );
}

pub(crate) fn resolve_native_library(
    manifest_dir: &Path,
    target: &str,
    explicit_dir: Option<&OsStr>,
) -> Result<ResolvedNativeLibrary, ResolveError> {
    let allow_local_build = local_build_allowed();
    let target_spec = target_spec(target)?;
    let manifest_path = manifest_dir.join(VENDOR_MANIFEST_RELATIVE_PATH);
    let manifest_bytes =
        read_small_regular_file(&manifest_path, "parser vendor manifest", MAX_MANIFEST_BYTES)?;
    let manifest_sha256 = sha256_bytes(&manifest_bytes);
    if manifest_sha256 != VENDOR_MANIFEST_SHA256 {
        if !allow_local_build {
            return Err(error(format!(
                "parser vendor manifest SHA-256 mismatch: expected {VENDOR_MANIFEST_SHA256}, found {manifest_sha256}"
            )));
        }
        warn_verification_skipped("パーサー vendor マニフェストの SHA-256");
    }

    let manifest: VendorManifest = serde_json::from_slice(&manifest_bytes)
        .map_err(|cause| error(format!("invalid parser vendor manifest: {cause}")))?;
    validate_manifest(&manifest)?;
    let asset = manifest
        .assets
        .iter()
        .find(|asset| asset.target == target)
        .ok_or_else(|| {
            error(format!(
                "parser vendor manifest has no asset for target {target}"
            ))
        })?;

    let (directory, directory_description) = if let Some(explicit_dir) = explicit_dir {
        if explicit_dir.is_empty() {
            return Err(error("explicit native parser directory is empty"));
        }
        (
            PathBuf::from(explicit_dir),
            "explicit native parser directory",
        )
    } else {
        (
            manifest_dir.join("nim-sql-parser/vendor").join(target),
            "vendored native parser directory",
        )
    };

    require_real_directory(&directory, directory_description)?;
    verify_target_directory(&directory, target_spec, asset, allow_local_build)?;

    Ok(ResolvedNativeLibrary {
        library_path: directory.join(target_spec.library_filename),
        directory,
        link_behavior: target_spec.link_behavior,
    })
}

fn target_spec(target: &str) -> Result<TargetSpec, ResolveError> {
    match target {
        "x86_64-unknown-linux-gnu" => Ok(TargetSpec {
            library_filename: "libalopex_sql_parser.so",
            link_behavior: LinkBehavior::UnixRpath,
        }),
        "x86_64-apple-darwin" | "aarch64-apple-darwin" => Ok(TargetSpec {
            library_filename: "libalopex_sql_parser.dylib",
            link_behavior: LinkBehavior::UnixRpath,
        }),
        "x86_64-pc-windows-msvc" => Ok(TargetSpec {
            library_filename: "alopex_sql_parser.dll",
            link_behavior: LinkBehavior::WindowsRawDylib,
        }),
        _ => Err(error(format!(
            "unsupported target for the native parser: {target}"
        ))),
    }
}

fn validate_manifest(manifest: &VendorManifest) -> Result<(), ResolveError> {
    if manifest.schema != VENDOR_MANIFEST_SCHEMA {
        return Err(error("parser vendor manifest schema mismatch"));
    }
    if manifest.alopex_version != REQUIRED_ALOPEX_VERSION {
        return Err(error("parser vendor manifest Alopex version mismatch"));
    }
    if manifest.contract_version != REQUIRED_CONTRACT_VERSION {
        return Err(error("parser vendor manifest contract mismatch"));
    }

    let targets: Vec<&str> = manifest
        .assets
        .iter()
        .map(|asset| asset.target.as_str())
        .collect();
    if targets != REQUIRED_TARGETS {
        return Err(error(
            "parser vendor manifest target matrix is incomplete, duplicated, or unsorted",
        ));
    }

    for asset in &manifest.assets {
        let spec = target_spec(&asset.target)?;
        let expected_path = format!(
            "alopex-sql-parser/{}/{}",
            asset.target, spec.library_filename
        );
        if asset.library.path != expected_path {
            return Err(error(format!(
                "parser vendor manifest library path mismatch for {}",
                asset.target
            )));
        }
        if asset.library.size == 0 {
            return Err(error(format!(
                "parser vendor manifest library size is zero for {}",
                asset.target
            )));
        }
        require_sha256(&asset.library.sha256, "parser vendor library SHA-256")?;
    }
    Ok(())
}

fn verify_target_directory(
    directory: &Path,
    target_spec: TargetSpec,
    asset: &VendorAsset,
    allow_local_build: bool,
) -> Result<(), ResolveError> {
    let contract_path = directory.join("CONTRACT_VERSION");
    let contract = read_small_regular_file(
        &contract_path,
        "native parser contract sidecar",
        MAX_SIDECAR_BYTES,
    )?;
    let expected_contract = format!("{REQUIRED_CONTRACT_VERSION}\n");
    if contract != expected_contract.as_bytes() {
        return Err(error(format!(
            "native parser contract sidecar mismatch: {}",
            contract_path.display()
        )));
    }

    // ローカル再ビルドを許可する場合、ライブラリ本体の同一性検証(サイズ・
    // SHA-256・SHA256SUMS サイドカー)をまとめてスキップする。契約バージョンの
    // 検証は上で必ず行う。あちらは FFI の互換性そのものを守るものであり、
    // ローカル開発でも緩めてはならない。
    if allow_local_build {
        warn_verification_skipped("native パーサーライブラリのサイズ・SHA-256");
        return Ok(());
    }

    let library_path = directory.join(target_spec.library_filename);
    let library_sha256 =
        sha256_regular_file(&library_path, "native parser library", asset.library.size)?;
    if library_sha256 != asset.library.sha256 {
        return Err(error(format!(
            "native parser library SHA-256 mismatch for {}: expected {}, found {}",
            asset.target, asset.library.sha256, library_sha256
        )));
    }

    let checksum_path = directory.join("SHA256SUMS");
    let checksum = read_small_regular_file(
        &checksum_path,
        "native parser checksum sidecar",
        MAX_SIDECAR_BYTES,
    )?;
    let expected_checksum = format!(
        "{}  {}\n",
        asset.library.sha256, target_spec.library_filename
    );
    if checksum != expected_checksum.as_bytes() {
        return Err(error(format!(
            "native parser checksum sidecar mismatch: {}",
            checksum_path.display()
        )));
    }
    Ok(())
}

fn require_real_directory(path: &Path, description: &str) -> Result<(), ResolveError> {
    let metadata = fs::symlink_metadata(path)
        .map_err(|cause| error(format!("missing {description} {}: {cause}", path.display())))?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(error(format!(
            "{description} must be a real directory: {}",
            path.display()
        )));
    }
    Ok(())
}

fn read_small_regular_file(
    path: &Path,
    description: &str,
    maximum_size: u64,
) -> Result<Vec<u8>, ResolveError> {
    let metadata = regular_file_metadata(path, description)?;
    if metadata.len() > maximum_size {
        return Err(error(format!(
            "{description} exceeds {maximum_size} bytes: {}",
            path.display()
        )));
    }
    fs::read(path).map_err(|cause| {
        error(format!(
            "could not read {description} {}: {cause}",
            path.display()
        ))
    })
}

fn sha256_regular_file(
    path: &Path,
    description: &str,
    expected_size: u64,
) -> Result<String, ResolveError> {
    let metadata = regular_file_metadata(path, description)?;
    if metadata.len() != expected_size {
        return Err(error(format!(
            "{description} size mismatch at {}: expected {expected_size}, found {}",
            path.display(),
            metadata.len()
        )));
    }

    let file = File::open(path).map_err(|cause| {
        error(format!(
            "could not open {description} {}: {cause}",
            path.display()
        ))
    })?;
    let mut reader = BufReader::new(file);
    let mut hasher = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let count = reader.read(&mut buffer).map_err(|cause| {
            error(format!(
                "could not hash {description} {}: {cause}",
                path.display()
            ))
        })?;
        if count == 0 {
            break;
        }
        hasher.update(&buffer[..count]);
    }
    Ok(format!("{:x}", hasher.finalize()))
}

fn regular_file_metadata(path: &Path, description: &str) -> Result<fs::Metadata, ResolveError> {
    let metadata = fs::symlink_metadata(path)
        .map_err(|cause| error(format!("missing {description} {}: {cause}", path.display())))?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(error(format!(
            "{description} must be a regular file: {}",
            path.display()
        )));
    }
    Ok(metadata)
}

fn require_sha256(value: &str, description: &str) -> Result<(), ResolveError> {
    if value.len() != 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(error(format!("{description} is not lowercase SHA-256")));
    }
    Ok(())
}

fn sha256_bytes(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

fn error(message: impl Into<String>) -> ResolveError {
    ResolveError(message.into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    const LINUX_TARGET: &str = "x86_64-unknown-linux-gnu";
    const WINDOWS_TARGET: &str = "x86_64-pc-windows-msvc";

    struct Fixture {
        _temporary: TempDir,
        crate_root: PathBuf,
        source_vendor: PathBuf,
    }

    impl Fixture {
        fn new(copy_linux: bool) -> Self {
            let temporary = TempDir::new().expect("fixture tempdir");
            let crate_root = temporary.path().join("alopex-sql");
            let vendor = crate_root.join("nim-sql-parser/vendor");
            fs::create_dir_all(&vendor).expect("fixture vendor directory");

            let source_vendor =
                PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("nim-sql-parser/vendor");
            fs::copy(
                source_vendor.join("parser-vendor-manifest.json"),
                vendor.join("parser-vendor-manifest.json"),
            )
            .expect("copy vendor manifest");
            if copy_linux {
                copy_target(&source_vendor, &vendor, LINUX_TARGET);
            }

            Self {
                _temporary: temporary,
                crate_root,
                source_vendor,
            }
        }

        fn explicit_dir(&self, target: &str) -> PathBuf {
            let explicit = self.crate_root.join("explicit");
            fs::create_dir_all(&explicit).expect("explicit directory");
            copy_target_contents(&self.source_vendor.join(target), &explicit);
            explicit
        }

        fn vendor_dir(&self, target: &str) -> PathBuf {
            self.crate_root.join("nim-sql-parser/vendor").join(target)
        }

        fn manifest(&self) -> PathBuf {
            self.crate_root
                .join("nim-sql-parser/vendor/parser-vendor-manifest.json")
        }
    }

    fn copy_target(source_vendor: &Path, destination_vendor: &Path, target: &str) {
        let destination = destination_vendor.join(target);
        fs::create_dir_all(&destination).expect("target directory");
        copy_target_contents(&source_vendor.join(target), &destination);
    }

    fn copy_target_contents(source: &Path, destination: &Path) {
        for name in [
            "CONTRACT_VERSION",
            "SHA256SUMS",
            library_name_for_source(source),
        ] {
            fs::copy(source.join(name), destination.join(name)).expect("copy target identity");
        }
    }

    fn library_name_for_source(source: &Path) -> &'static str {
        if source.ends_with(WINDOWS_TARGET) {
            "alopex_sql_parser.dll"
        } else if source.ends_with("aarch64-apple-darwin")
            || source.ends_with("x86_64-apple-darwin")
        {
            "libalopex_sql_parser.dylib"
        } else {
            "libalopex_sql_parser.so"
        }
    }

    fn failure(fixture: &Fixture, target: &str, explicit: Option<&Path>) -> String {
        resolve_native_library(&fixture.crate_root, target, explicit.map(Path::as_os_str))
            .expect_err("resolver must reject inconsistent input")
            .to_string()
    }

    #[test]
    fn resolves_verified_vendored_library() {
        let fixture = Fixture::new(true);
        let resolved = resolve_native_library(&fixture.crate_root, LINUX_TARGET, None)
            .expect("verified vendor library");

        assert_eq!(resolved.directory, fixture.vendor_dir(LINUX_TARGET));
        assert_eq!(
            resolved.library_path,
            fixture
                .vendor_dir(LINUX_TARGET)
                .join("libalopex_sql_parser.so")
        );
    }

    #[test]
    fn resolves_verified_explicit_directory() {
        let fixture = Fixture::new(false);
        let explicit = fixture.explicit_dir(LINUX_TARGET);
        let resolved = resolve_native_library(
            &fixture.crate_root,
            LINUX_TARGET,
            Some(explicit.as_os_str()),
        )
        .expect("verified explicit library");

        assert_eq!(resolved.directory, explicit);
    }

    #[test]
    fn rejects_explicit_directory_without_identity_sidecars() {
        let fixture = Fixture::new(false);
        let explicit = fixture.explicit_dir(LINUX_TARGET);
        fs::remove_file(explicit.join("SHA256SUMS")).expect("remove explicit checksum");

        assert!(failure(&fixture, LINUX_TARGET, Some(&explicit)).contains("checksum sidecar"));
    }

    #[test]
    fn rejects_missing_library_before_linking() {
        let fixture = Fixture::new(true);
        fs::remove_file(
            fixture
                .vendor_dir(LINUX_TARGET)
                .join("libalopex_sql_parser.so"),
        )
        .expect("remove fixture library");

        assert!(failure(&fixture, LINUX_TARGET, None).contains("native parser library"));
    }

    #[test]
    fn rejects_contract_sidecar_mismatch() {
        let fixture = Fixture::new(true);
        fs::write(
            fixture.vendor_dir(LINUX_TARGET).join("CONTRACT_VERSION"),
            b"0.3.0\n",
        )
        .expect("replace fixture contract");

        assert!(failure(&fixture, LINUX_TARGET, None).contains("contract sidecar"));
    }

    #[test]
    fn rejects_checksum_sidecar_mismatch() {
        let fixture = Fixture::new(true);
        fs::write(
            fixture.vendor_dir(LINUX_TARGET).join("SHA256SUMS"),
            format!("{}  libalopex_sql_parser.so\n", "0".repeat(64)),
        )
        .expect("replace fixture checksum");

        assert!(failure(&fixture, LINUX_TARGET, None).contains("checksum sidecar"));
    }

    #[test]
    fn rejects_library_digest_mismatch_with_unchanged_size() {
        let fixture = Fixture::new(true);
        let library = fixture
            .vendor_dir(LINUX_TARGET)
            .join("libalopex_sql_parser.so");
        let mut bytes = fs::read(&library).expect("read fixture library");
        bytes[0] ^= 1;
        fs::write(&library, bytes).expect("replace fixture library");

        assert!(failure(&fixture, LINUX_TARGET, None).contains("library SHA-256"));
    }

    #[test]
    fn rejects_library_size_mismatch() {
        let fixture = Fixture::new(true);
        let library = fixture
            .vendor_dir(LINUX_TARGET)
            .join("libalopex_sql_parser.so");
        let mut bytes = fs::read(&library).expect("read fixture library");
        bytes.push(0);
        fs::write(&library, bytes).expect("replace fixture library");

        assert!(failure(&fixture, LINUX_TARGET, None).contains("library size mismatch"));
    }

    #[test]
    fn rejects_explicit_directory_for_another_target() {
        let fixture = Fixture::new(false);
        let explicit = fixture.explicit_dir(WINDOWS_TARGET);

        assert!(failure(&fixture, LINUX_TARGET, Some(&explicit)).contains("native parser library"));
    }

    #[test]
    fn rejects_modified_manifest_before_asset_selection() {
        let fixture = Fixture::new(true);
        let mut bytes = fs::read(fixture.manifest()).expect("read fixture manifest");
        bytes[0] ^= 1;
        fs::write(fixture.manifest(), bytes).expect("replace fixture manifest");

        assert!(failure(&fixture, LINUX_TARGET, None).contains("vendor manifest SHA-256"));
    }

    #[test]
    fn rejects_unsupported_target() {
        let fixture = Fixture::new(false);

        assert!(failure(&fixture, "wasm32-unknown-unknown", None).contains("unsupported target"));
    }

    #[test]
    fn missing_vendor_does_not_fall_back_to_a_system_directory() {
        let fixture = Fixture::new(false);
        let message = failure(&fixture, LINUX_TARGET, None);

        assert!(message.contains("vendored native parser directory"));
        assert!(!message.contains("/usr/local"));
    }

    #[cfg(unix)]
    #[test]
    fn rejects_symlinked_explicit_directory() {
        use std::os::unix::fs::symlink;

        let fixture = Fixture::new(false);
        let real = fixture.explicit_dir(LINUX_TARGET);
        let linked = fixture.crate_root.join("linked-explicit");
        symlink(&real, &linked).expect("link explicit directory");

        assert!(
            failure(&fixture, LINUX_TARGET, Some(&linked)).contains("must be a real directory")
        );
    }
}
