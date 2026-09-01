use std::env;
use std::ffi::OsStr;
use std::fmt;
use std::fs::{self, File};
use std::io::{BufReader, Read};
use std::path::{Path, PathBuf};

use serde::Deserialize;
use sha2::{Digest, Sha256};

pub(crate) const REQUIRED_ALOPEX_VERSION: &str = "0.8.7";
pub(crate) const REQUIRED_CONTRACT_VERSION: &str = "0.23.0";
pub(crate) const VENDOR_MANIFEST_SHA256: &str =
    "db70742bea017a4d2683ad0d17f602b25dbcdfa7f512e3c283fbb9f7fcce298d";
const VENDOR_MANIFEST_SCHEMA: &str = "alopex-parser-vendor-manifest-v2";
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
    pub(crate) static_library_path: PathBuf,
}

#[derive(Clone, Copy)]
struct TargetSpec {
    library_filename: &'static str,
    static_library_filename: &'static str,
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
    static_library: LibraryIdentity,
    target: String,
}

#[derive(Deserialize)]
struct LibraryIdentity {
    path: String,
    sha256: String,
    size: u64,
}

/// 明示された developer parser output と vendored マニフェストとの完全一致
/// 検証をスキップするための開発専用スイッチ。
///
/// vendored の `.so` は sha256 とバイトサイズがマニフェストに固定されている
/// ため、Nim ソースを 1 文字でも変更すると再ビルド結果は原理的に受理されない
/// (issue #131)。この検証は公開物の改竄防止という正当な目的を持つので既定では
/// 維持し、明示的なオプトインのときだけ緩和する。
///
/// 値は `1` のみを有効とする。`0` や空文字、その他の値は無効 (= 厳格検証) と
/// して扱い、「設定さえすれば何でも通る」曖昧さを排除する。
pub(crate) const ALLOW_LOCAL_BUILD_ENV: &str = "ALOPEX_NIM_PARSER_ALLOW_LOCAL_BUILD";

// This entry point is consumed by build.rs; the standalone build-support test
// target includes this file directly and exercises the injected option path.
#[cfg_attr(test, allow(dead_code))]
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

/// build-time identity の証明範囲を必ず可視化する。sidecar の再ラベルだけでは
/// producer 実体を証明できず、runtime exported-contract gate が不可欠である。
fn warn_local_identity_scope() {
    println!(
        "cargo:warning={ALLOW_LOCAL_BUILD_ENV}=1 は明示 parser の sidecar/SHA 自己整合だけを検証します。producer 実体は runtime exported-contract gate で検証され、release staging の asset identity 証明には使えません。"
    );
}

#[cfg_attr(test, allow(dead_code))]
pub(crate) fn resolve_native_library(
    manifest_dir: &Path,
    target: &str,
    explicit_dir: Option<&OsStr>,
) -> Result<ResolvedNativeLibrary, ResolveError> {
    resolve_native_library_with_options(
        manifest_dir,
        target,
        explicit_dir,
        local_build_allowed(),
        VENDOR_MANIFEST_SHA256,
    )
}

fn resolve_native_library_with_options(
    manifest_dir: &Path,
    target: &str,
    explicit_dir: Option<&OsStr>,
    allow_local_build: bool,
    expected_manifest_sha256: &str,
) -> Result<ResolvedNativeLibrary, ResolveError> {
    let target_spec = target_spec(target)?;

    // An explicit developer parser is the only route allowed to bypass a stale
    // vendored manifest. Build time proves only target/file shape and that its
    // sidecars are self-consistent; the exported producer contract is checked
    // before every runtime decode. The release workflow uses this branch only
    // after the same job verifies the fresh target record and runs a native
    // exported-contract smoke. Later crate staging/publish uses the strict
    // manifest-bound path.
    if allow_local_build && let Some(explicit_dir) = explicit_dir {
        if explicit_dir.is_empty() {
            return Err(error("explicit native parser directory is empty"));
        }
        let directory = PathBuf::from(explicit_dir);
        require_real_directory(&directory, "explicit native parser directory")?;
        verify_local_source_directory(&directory, target_spec)?;
        warn_local_identity_scope();
        return Ok(ResolvedNativeLibrary {
            library_path: directory.join(target_spec.library_filename),
            static_library_path: directory.join(target_spec.static_library_filename),
            directory,
        });
    }

    let manifest_path = manifest_dir.join(VENDOR_MANIFEST_RELATIVE_PATH);
    let manifest_bytes =
        read_small_regular_file(&manifest_path, "parser vendor manifest", MAX_MANIFEST_BYTES)?;
    let manifest_sha256 = sha256_bytes(&manifest_bytes);
    if manifest_sha256 != expected_manifest_sha256 {
        return Err(error(format!(
            "parser vendor manifest SHA-256 mismatch: expected {expected_manifest_sha256}, found {manifest_sha256}"
        )));
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
    verify_target_directory(&directory, target_spec, asset)?;

    Ok(ResolvedNativeLibrary {
        library_path: directory.join(target_spec.library_filename),
        static_library_path: directory.join(target_spec.static_library_filename),
        directory,
    })
}

fn verify_contract_sidecar(directory: &Path) -> Result<(), ResolveError> {
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
    Ok(())
}

fn verify_local_source_directory(
    directory: &Path,
    target_spec: TargetSpec,
) -> Result<(), ResolveError> {
    verify_contract_sidecar(directory)?;
    let library_path = directory.join(target_spec.library_filename);
    let metadata = regular_file_metadata(&library_path, "native parser library")?;
    let library_sha256 =
        sha256_regular_file(&library_path, "native parser library", metadata.len())?;
    let static_library_path = directory.join(target_spec.static_library_filename);
    let static_metadata =
        regular_file_metadata(&static_library_path, "native parser static library")?;
    let static_library_sha256 = sha256_regular_file(
        &static_library_path,
        "native parser static library",
        static_metadata.len(),
    )?;
    let checksum_path = directory.join("SHA256SUMS");
    let checksum = read_small_regular_file(
        &checksum_path,
        "native parser checksum sidecar",
        MAX_SIDECAR_BYTES,
    )?;
    let expected_checksum = format!(
        "{library_sha256}  {}\n{static_library_sha256}  {}\n",
        target_spec.library_filename, target_spec.static_library_filename
    );
    if checksum != expected_checksum.as_bytes() {
        return Err(error(format!(
            "native parser checksum sidecar mismatch: {}",
            checksum_path.display()
        )));
    }
    Ok(())
}

fn target_spec(target: &str) -> Result<TargetSpec, ResolveError> {
    match target {
        "x86_64-unknown-linux-gnu" => Ok(TargetSpec {
            library_filename: "libalopex_sql_parser.so",
            static_library_filename: "libalopex_sql_parser.a",
        }),
        "x86_64-apple-darwin" | "aarch64-apple-darwin" => Ok(TargetSpec {
            library_filename: "libalopex_sql_parser.dylib",
            static_library_filename: "libalopex_sql_parser.a",
        }),
        "x86_64-pc-windows-msvc" => Ok(TargetSpec {
            library_filename: "alopex_sql_parser.dll",
            static_library_filename: "alopex_sql_parser.lib",
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
        let expected_static_path = format!(
            "alopex-sql-parser/{}/{}",
            asset.target, spec.static_library_filename
        );
        if asset.static_library.path != expected_static_path {
            return Err(error(format!(
                "parser vendor static library path mismatch for {}",
                asset.target
            )));
        }
        if asset.static_library.size == 0 {
            return Err(error(format!(
                "parser vendor static library size is zero for {}",
                asset.target
            )));
        }
        require_sha256(
            &asset.static_library.sha256,
            "parser vendor static library SHA-256",
        )?;
    }
    Ok(())
}

fn verify_target_directory(
    directory: &Path,
    target_spec: TargetSpec,
    asset: &VendorAsset,
) -> Result<(), ResolveError> {
    verify_contract_sidecar(directory)?;

    let library_path = directory.join(target_spec.library_filename);
    let library_sha256 =
        sha256_regular_file(&library_path, "native parser library", asset.library.size)?;
    if library_sha256 != asset.library.sha256 {
        return Err(error(format!(
            "native parser library SHA-256 mismatch for {}: expected {}, found {}",
            asset.target, asset.library.sha256, library_sha256
        )));
    }

    let static_library_path = directory.join(target_spec.static_library_filename);
    let static_library_sha256 = sha256_regular_file(
        &static_library_path,
        "native parser static library",
        asset.static_library.size,
    )?;
    if static_library_sha256 != asset.static_library.sha256 {
        return Err(error(format!(
            "native parser static library SHA-256 mismatch for {}: expected {}, found {}",
            asset.target, asset.static_library.sha256, static_library_sha256
        )));
    }

    let checksum_path = directory.join("SHA256SUMS");
    let checksum = read_small_regular_file(
        &checksum_path,
        "native parser checksum sidecar",
        MAX_SIDECAR_BYTES,
    )?;
    let expected_checksum = format!(
        "{}  {}\n{}  {}\n",
        asset.library.sha256,
        target_spec.library_filename,
        asset.static_library.sha256,
        target_spec.static_library_filename
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
            Self::with_contract(copy_linux, REQUIRED_CONTRACT_VERSION)
        }

        fn stale(copy_linux: bool) -> Self {
            Self::with_contract(copy_linux, "0.4.0")
        }

        fn with_contract(copy_linux: bool, contract: &str) -> Self {
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
            let manifest_path = vendor.join("parser-vendor-manifest.json");
            let mut manifest: serde_json::Value = serde_json::from_slice(
                &fs::read(&manifest_path).expect("read copied vendor manifest"),
            )
            .expect("decode copied vendor manifest");
            manifest["contract_version"] = serde_json::Value::String(contract.to_owned());
            manifest["alopex_version"] =
                serde_json::Value::String(REQUIRED_ALOPEX_VERSION.to_owned());
            manifest["schema"] =
                serde_json::Value::String("alopex-parser-vendor-manifest-v2".to_owned());
            for asset in manifest["assets"].as_array_mut().expect("manifest assets") {
                let target = asset["target"].as_str().expect("manifest target");
                let dynamic = asset["library"].clone();
                let mut static_library = dynamic;
                static_library["path"] = serde_json::Value::String(format!(
                    "alopex-sql-parser/{target}/{}",
                    static_library_name_for_target(target)
                ));
                asset["static_library"] = static_library;
            }
            fs::write(
                &manifest_path,
                serde_json::to_vec(&manifest).expect("encode fixture vendor manifest"),
            )
            .expect("write fixture vendor manifest");
            if copy_linux {
                copy_target(&source_vendor, &vendor, LINUX_TARGET);
                fs::write(
                    vendor.join(LINUX_TARGET).join("CONTRACT_VERSION"),
                    format!("{contract}\n"),
                )
                .expect("write fixture target contract");
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
            fs::write(
                explicit.join("CONTRACT_VERSION"),
                format!("{REQUIRED_CONTRACT_VERSION}\n"),
            )
            .expect("write explicit target contract");
            explicit
        }

        fn vendor_dir(&self, target: &str) -> PathBuf {
            self.crate_root.join("nim-sql-parser/vendor").join(target)
        }

        fn manifest(&self) -> PathBuf {
            self.crate_root
                .join("nim-sql-parser/vendor/parser-vendor-manifest.json")
        }

        fn manifest_sha256(&self) -> String {
            sha256_bytes(&fs::read(self.manifest()).expect("read fixture manifest"))
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
            library_name_for_source(source),
            static_library_name_for_source(source),
        ] {
            let source_path = source.join(name);
            if source_path.exists() {
                fs::copy(source_path, destination.join(name)).expect("copy target identity");
            } else if name == static_library_name_for_source(source) {
                fs::copy(
                    source.join(library_name_for_source(source)),
                    destination.join(name),
                )
                .expect("copy fixture static library");
            } else {
                panic!("missing target identity fixture: {}", source_path.display());
            }
        }
        let dynamic_name = library_name_for_source(source);
        let static_name = static_library_name_for_source(source);
        let dynamic_sha = sha256_bytes(
            &fs::read(destination.join(dynamic_name)).expect("read fixture dynamic library"),
        );
        let static_sha = sha256_bytes(
            &fs::read(destination.join(static_name)).expect("read fixture static library"),
        );
        fs::write(
            destination.join("SHA256SUMS"),
            format!("{dynamic_sha}  {dynamic_name}\n{static_sha}  {static_name}\n"),
        )
        .expect("write fixture checksum");
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

    fn static_library_name_for_source(source: &Path) -> &'static str {
        static_library_name_for_target(
            source
                .file_name()
                .and_then(|name| name.to_str())
                .expect("target directory name"),
        )
    }

    fn static_library_name_for_target(target: &str) -> &'static str {
        if target == WINDOWS_TARGET {
            "alopex_sql_parser.lib"
        } else {
            "libalopex_sql_parser.a"
        }
    }

    fn failure(fixture: &Fixture, target: &str, explicit: Option<&Path>) -> String {
        resolve_native_library_with_options(
            &fixture.crate_root,
            target,
            explicit.map(Path::as_os_str),
            false,
            &fixture.manifest_sha256(),
        )
        .expect_err("resolver must reject inconsistent input")
        .to_string()
    }

    #[test]
    fn resolves_verified_vendored_library() {
        let fixture = Fixture::new(true);
        let resolved = resolve_native_library_with_options(
            &fixture.crate_root,
            LINUX_TARGET,
            None,
            false,
            &fixture.manifest_sha256(),
        )
        .expect("verified vendor library");

        assert_eq!(resolved.directory, fixture.vendor_dir(LINUX_TARGET));
        assert_eq!(
            resolved.library_path,
            fixture
                .vendor_dir(LINUX_TARGET)
                .join("libalopex_sql_parser.so")
        );
        assert_eq!(
            resolved.static_library_path,
            fixture
                .vendor_dir(LINUX_TARGET)
                .join("libalopex_sql_parser.a")
        );
    }

    #[test]
    fn resolves_verified_explicit_directory() {
        let fixture = Fixture::new(false);
        let explicit = fixture.explicit_dir(LINUX_TARGET);
        let resolved = resolve_native_library_with_options(
            &fixture.crate_root,
            LINUX_TARGET,
            Some(explicit.as_os_str()),
            false,
            &fixture.manifest_sha256(),
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
            b"0.4.0\n",
        )
        .expect("replace fixture contract");

        assert!(failure(&fixture, LINUX_TARGET, None).contains("contract sidecar"));
    }

    #[test]
    fn rejects_checksum_sidecar_mismatch() {
        let fixture = Fixture::new(true);
        fs::write(
            fixture.vendor_dir(LINUX_TARGET).join("SHA256SUMS"),
            format!(
                "{}  libalopex_sql_parser.so\n{}  libalopex_sql_parser.a\n",
                "0".repeat(64),
                "0".repeat(64)
            ),
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
        let expected_manifest_sha256 = fixture.manifest_sha256();
        let mut bytes = fs::read(fixture.manifest()).expect("read fixture manifest");
        bytes[0] ^= 1;
        fs::write(fixture.manifest(), bytes).expect("replace fixture manifest");

        let message = resolve_native_library_with_options(
            &fixture.crate_root,
            LINUX_TARGET,
            None,
            false,
            &expected_manifest_sha256,
        )
        .expect_err("modified manifest must be rejected")
        .to_string();
        assert!(message.contains("vendor manifest SHA-256"));
    }

    #[test]
    fn checked_in_v040_vendor_is_rejected_by_current_requirements() {
        let crate_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let message = resolve_native_library_with_options(
            &crate_root,
            LINUX_TARGET,
            None,
            false,
            VENDOR_MANIFEST_SHA256,
        )
        .expect_err("the immutable pre-frame vendor must not satisfy contract 0.23.0")
        .to_string();

        assert!(message.contains("invalid parser vendor manifest"));
    }

    #[test]
    fn local_opt_in_without_an_explicit_source_directory_does_not_bypass_stale_vendor() {
        let crate_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let message = resolve_native_library_with_options(
            &crate_root,
            LINUX_TARGET,
            None,
            true,
            VENDOR_MANIFEST_SHA256,
        )
        .expect_err("the local bypass requires both opt-in and an explicit source directory")
        .to_string();

        assert!(message.contains("invalid parser vendor manifest"));
    }

    #[test]
    fn local_mode_proves_only_current_sidecar_and_checksum_self_consistency() {
        let fixture = Fixture::stale(false);
        let explicit = fixture.explicit_dir(LINUX_TARGET);
        let resolved = resolve_native_library_with_options(
            &fixture.crate_root,
            LINUX_TARGET,
            Some(explicit.as_os_str()),
            true,
            &fixture.manifest_sha256(),
        )
        .expect("build time accepts self-consistent identity sidecars");

        assert_eq!(resolved.directory, explicit);
        // This fixture deliberately contains the historical producer bytes.
        // Acceptance here is not a contract-compatibility claim: the runtime
        // exported-version gate rejects those bytes before MessagePack decode.
    }

    #[test]
    fn local_source_mode_rejects_stale_contract_sidecar() {
        let fixture = Fixture::stale(false);
        let explicit = fixture.explicit_dir(LINUX_TARGET);
        fs::write(explicit.join("CONTRACT_VERSION"), b"0.4.0\n").expect("restore stale sidecar");

        let message = resolve_native_library_with_options(
            &fixture.crate_root,
            LINUX_TARGET,
            Some(explicit.as_os_str()),
            true,
            &fixture.manifest_sha256(),
        )
        .expect_err("local source mode must reject stale sidecar")
        .to_string();
        assert!(message.contains("contract sidecar"));
    }

    #[test]
    fn local_source_mode_rejects_pre_values_contract_0_6_sidecar() {
        let fixture = Fixture::with_contract(false, "0.6.0");
        let explicit = fixture.explicit_dir(LINUX_TARGET);
        fs::write(explicit.join("CONTRACT_VERSION"), b"0.6.0\n")
            .expect("restore pre-VALUES sidecar");

        let message = resolve_native_library_with_options(
            &fixture.crate_root,
            LINUX_TARGET,
            Some(explicit.as_os_str()),
            true,
            &fixture.manifest_sha256(),
        )
        .expect_err("contract 0.6.0 must not load as the VALUES-capable producer")
        .to_string();
        assert!(message.contains("contract sidecar"));
    }

    #[test]
    fn local_source_mode_rejects_pre_predicate_contract_0_7_sidecar() {
        let fixture = Fixture::with_contract(false, "0.7.0");
        let explicit = fixture.explicit_dir(LINUX_TARGET);
        fs::write(explicit.join("CONTRACT_VERSION"), b"0.7.0\n")
            .expect("restore pre-predicate sidecar");

        let message = resolve_native_library_with_options(
            &fixture.crate_root,
            LINUX_TARGET,
            Some(explicit.as_os_str()),
            true,
            &fixture.manifest_sha256(),
        )
        .expect_err("contract 0.7.0 must not load as the row-predicate producer")
        .to_string();
        assert!(message.contains("contract sidecar"));
    }

    #[test]
    fn local_source_mode_rejects_pre_try_cast_contract_0_8_sidecar() {
        let fixture = Fixture::with_contract(false, "0.8.0");
        let explicit = fixture.explicit_dir(LINUX_TARGET);
        fs::write(explicit.join("CONTRACT_VERSION"), b"0.8.0\n")
            .expect("restore pre-TRY_CAST sidecar");

        let message = resolve_native_library_with_options(
            &fixture.crate_root,
            LINUX_TARGET,
            Some(explicit.as_os_str()),
            true,
            &fixture.manifest_sha256(),
        )
        .expect_err("contract 0.8.0 must not load as the TRY_CAST producer")
        .to_string();
        assert!(message.contains("contract sidecar"));
    }

    #[test]
    fn local_source_mode_rejects_pre_fetch_pagination_contract_0_9_sidecar() {
        let fixture = Fixture::with_contract(false, "0.9.0");
        let explicit = fixture.explicit_dir(LINUX_TARGET);
        fs::write(explicit.join("CONTRACT_VERSION"), b"0.9.0\n")
            .expect("restore pre-FETCH-pagination sidecar");

        let message = resolve_native_library_with_options(
            &fixture.crate_root,
            LINUX_TARGET,
            Some(explicit.as_os_str()),
            true,
            &fixture.manifest_sha256(),
        )
        .expect_err("contract 0.9.0 must not load as the FETCH-pagination producer")
        .to_string();
        assert!(message.contains("contract sidecar"));
    }

    #[test]
    fn local_source_mode_rejects_pre_distinct_on_contract_0_10_sidecar() {
        let fixture = Fixture::with_contract(false, "0.10.0");
        let explicit = fixture.explicit_dir(LINUX_TARGET);
        fs::write(explicit.join("CONTRACT_VERSION"), b"0.10.0\n")
            .expect("restore pre-DISTINCT-ON sidecar");

        let message = resolve_native_library_with_options(
            &fixture.crate_root,
            LINUX_TARGET,
            Some(explicit.as_os_str()),
            true,
            &fixture.manifest_sha256(),
        )
        .expect_err("contract 0.10.0 must not load as the DISTINCT ON producer")
        .to_string();
        assert!(message.contains("contract sidecar"));
    }

    #[test]
    fn local_source_mode_rejects_pre_aggregate_filter_contract_0_11_sidecar() {
        let fixture = Fixture::with_contract(false, "0.11.0");
        let explicit = fixture.explicit_dir(LINUX_TARGET);
        fs::write(explicit.join("CONTRACT_VERSION"), b"0.11.0\n")
            .expect("restore pre-aggregate-FILTER sidecar");

        let message = resolve_native_library_with_options(
            &fixture.crate_root,
            LINUX_TARGET,
            Some(explicit.as_os_str()),
            true,
            &fixture.manifest_sha256(),
        )
        .expect_err("contract 0.11.0 must not load as the aggregate-FILTER producer")
        .to_string();
        assert!(message.contains("contract sidecar"));
    }

    #[test]
    fn local_source_mode_rejects_pre_grouping_sets_contract_0_12_sidecar() {
        let fixture = Fixture::with_contract(false, "0.12.0");
        let explicit = fixture.explicit_dir(LINUX_TARGET);
        fs::write(explicit.join("CONTRACT_VERSION"), b"0.12.0\n")
            .expect("restore pre-grouping-sets sidecar");

        let message = resolve_native_library_with_options(
            &fixture.crate_root,
            LINUX_TARGET,
            Some(explicit.as_os_str()),
            true,
            &fixture.manifest_sha256(),
        )
        .expect_err("contract 0.12.0 must not load as the grouping-sets producer")
        .to_string();
        assert!(message.contains("contract sidecar"));
    }

    #[test]
    fn local_source_mode_rejects_pre_lateral_contract_0_13_sidecar() {
        let fixture = Fixture::with_contract(false, "0.13.0");
        let explicit = fixture.explicit_dir(LINUX_TARGET);
        fs::write(explicit.join("CONTRACT_VERSION"), b"0.13.0\n")
            .expect("restore pre-lateral sidecar");

        let message = resolve_native_library_with_options(
            &fixture.crate_root,
            LINUX_TARGET,
            Some(explicit.as_os_str()),
            true,
            &fixture.manifest_sha256(),
        )
        .expect_err("contract 0.13.0 must not load as the LATERAL producer")
        .to_string();
        assert!(message.contains("contract sidecar"));
    }

    #[test]
    fn local_source_mode_rejects_a_checksum_not_bound_to_the_library() {
        let fixture = Fixture::stale(false);
        let explicit = fixture.explicit_dir(LINUX_TARGET);
        fs::write(
            explicit.join("SHA256SUMS"),
            format!("{}  libalopex_sql_parser.so\n", "0".repeat(64)),
        )
        .expect("replace local checksum sidecar");

        let message = resolve_native_library_with_options(
            &fixture.crate_root,
            LINUX_TARGET,
            Some(explicit.as_os_str()),
            true,
            &fixture.manifest_sha256(),
        )
        .expect_err("local source mode must bind the checksum to the actual library")
        .to_string();
        assert!(message.contains("checksum sidecar"));
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
