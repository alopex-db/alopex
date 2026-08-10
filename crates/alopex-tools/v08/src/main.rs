use std::collections::{BTreeMap, BTreeSet};
use std::ffi::{CStr, CString, OsStr, OsString, c_char, c_int, c_void};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
#[cfg(unix)]
use std::os::unix::ffi::OsStrExt;
use std::path::{Component, Path, PathBuf};
use std::process::ExitCode;

use alopex_sql::CreateContinuousAggregate;
use flate2::read::GzDecoder;
use serde::Deserialize;
use sha2::{Digest, Sha256};

mod embedded {
    include!("../../src/bin/verify_release_embedded.rs");

    pub(super) fn entry() -> ExitCode {
        main()
    }
}

const NATIVE_SMOKE_FLAG: &str = "--parser-native-smoke";
const VENDOR_MANIFEST: &str =
    include_str!("../../../alopex-sql/nim-sql-parser/vendor/parser-vendor-manifest-v0.8.4.json");
const MAX_NATIVE_PAYLOAD_BYTES: usize = 1_048_576;
const CANONICAL_CONTINUOUS_AGGREGATE_SQL: &str = r#"CREATE CONTINUOUS AGGREGATE cpu_hourly
AS
SELECT
  TIME_BUCKET(INTERVAL '1 hour', time) AS time,
  host,
  AVG(usage_user) AS usage_user_avg
FROM cpu_metrics
GROUP BY TIME_BUCKET(INTERVAL '1 hour', time), host
WITH (
  retention = '30d',
  refresh_interval = '1h'
);"#;

#[derive(Debug, Deserialize)]
struct VendorManifest {
    schema: String,
    alopex_version: String,
    contract_version: String,
    packing: PackingPolicy,
    assets: Vec<ParserAsset>,
}

#[derive(Debug, Deserialize)]
struct PackingPolicy {
    archive_size_limit: u64,
    decompressed_size_limit: u64,
    member_count_limit: usize,
    member_size_limit: u64,
}

#[derive(Debug, Deserialize)]
struct ParserAsset {
    target: String,
    archive: ArchiveIdentity,
    library: FileIdentity,
    build_identity: FileIdentity,
}

#[derive(Debug, Deserialize)]
struct ArchiveIdentity {
    filename: String,
    size: u64,
    sha256: String,
}

#[derive(Debug, Deserialize)]
struct FileIdentity {
    path: String,
    size: u64,
    sha256: String,
}

fn parse_native_smoke_args(args: &[OsString]) -> Result<PathBuf, String> {
    match args {
        [archive] if !archive.is_empty() => Ok(PathBuf::from(archive)),
        [] => Err(format!(
            "{NATIVE_SMOKE_FLAG} requires one explicit parser archive; repository fallback is disabled"
        )),
        _ => Err(format!(
            "{NATIVE_SMOKE_FLAG} accepts exactly one explicit parser archive"
        )),
    }
}

fn run_native_smoke_cli(args: &[OsString]) -> Result<(), String> {
    let archive_path = parse_native_smoke_args(args)?;
    let manifest: VendorManifest = serde_json::from_str(VENDOR_MANIFEST)
        .map_err(|error| format!("tracked parser vendor manifest is invalid JSON: {error}"))?;
    validate_manifest(&manifest)?;

    let target = host_target()?;
    let asset = select_host_asset(&manifest, target, &archive_path)?;
    verify_archive_identity(&archive_path, &asset.archive, &manifest.packing)?;

    let extraction = tempfile::Builder::new()
        .prefix("alopex-parser-native-smoke-")
        .tempdir()
        .map_err(|error| format!("cannot create fresh extraction directory: {error}"))?;
    let library_path =
        extract_declared_archive(&archive_path, asset, &manifest.packing, extraction.path())?;
    smoke_extracted_library(&library_path, extraction.path(), &manifest.contract_version)?;

    eprintln!(
        "[alopex-tools:v0.8] extracted parser smoke passed target={} archive_sha256={} library_sha256={} contract={}",
        asset.target, asset.archive.sha256, asset.library.sha256, manifest.contract_version
    );
    Ok(())
}

fn validate_manifest(manifest: &VendorManifest) -> Result<(), String> {
    if manifest.schema != "alopex-parser-vendor-manifest-v1" {
        return Err(format!(
            "unsupported parser manifest schema `{}`",
            manifest.schema
        ));
    }
    if manifest.alopex_version != "0.8.4" {
        return Err(format!(
            "parser manifest Alopex version is `{}`, expected `0.8.4`",
            manifest.alopex_version
        ));
    }
    if manifest.contract_version != "0.4.0" {
        return Err(format!(
            "parser manifest contract is `{}`, expected `0.4.0`",
            manifest.contract_version
        ));
    }
    let actual: BTreeSet<&str> = manifest
        .assets
        .iter()
        .map(|asset| asset.target.as_str())
        .collect();
    let expected: BTreeSet<&str> = [
        "aarch64-apple-darwin",
        "x86_64-apple-darwin",
        "x86_64-pc-windows-msvc",
        "x86_64-unknown-linux-gnu",
    ]
    .into_iter()
    .collect();
    if manifest.assets.len() != expected.len() || actual != expected {
        return Err(format!(
            "parser manifest target matrix mismatch: expected {expected:?}, found {actual:?}"
        ));
    }
    Ok(())
}

fn host_target() -> Result<&'static str, String> {
    if cfg!(all(
        target_os = "linux",
        target_arch = "x86_64",
        target_env = "gnu"
    )) {
        Ok("x86_64-unknown-linux-gnu")
    } else if cfg!(all(target_os = "macos", target_arch = "x86_64")) {
        Ok("x86_64-apple-darwin")
    } else if cfg!(all(target_os = "macos", target_arch = "aarch64")) {
        Ok("aarch64-apple-darwin")
    } else if cfg!(all(
        target_os = "windows",
        target_arch = "x86_64",
        target_env = "msvc"
    )) {
        Ok("x86_64-pc-windows-msvc")
    } else {
        Err(format!(
            "unsupported native-smoke host {}/{}",
            std::env::consts::OS,
            std::env::consts::ARCH
        ))
    }
}

fn select_host_asset<'a>(
    manifest: &'a VendorManifest,
    target: &str,
    archive_path: &Path,
) -> Result<&'a ParserAsset, String> {
    let asset = manifest
        .assets
        .iter()
        .find(|asset| asset.target == target)
        .ok_or_else(|| {
            format!("tracked manifest has no parser asset for host target `{target}`")
        })?;
    let actual_name = archive_path
        .file_name()
        .ok_or_else(|| format!("parser archive has no filename: {}", archive_path.display()))?;
    if actual_name != OsStr::new(&asset.archive.filename) {
        return Err(format!(
            "archive filename mismatch for target `{target}`: expected `{}`, found `{}`",
            asset.archive.filename,
            actual_name.to_string_lossy()
        ));
    }
    Ok(asset)
}

fn verify_archive_identity(
    archive_path: &Path,
    identity: &ArchiveIdentity,
    packing: &PackingPolicy,
) -> Result<(), String> {
    let metadata = fs::symlink_metadata(archive_path).map_err(|error| {
        format!(
            "explicit parser archive is unavailable (no repository fallback): {}: {error}",
            archive_path.display()
        )
    })?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(format!(
            "explicit parser archive must be a regular non-symlink file: {}",
            archive_path.display()
        ));
    }
    if metadata.len() > packing.archive_size_limit {
        return Err(format!(
            "parser archive exceeds {} byte limit: {}",
            packing.archive_size_limit,
            archive_path.display()
        ));
    }
    if metadata.len() != identity.size {
        return Err(format!(
            "parser archive size mismatch: expected {}, found {}",
            identity.size,
            metadata.len()
        ));
    }
    let digest = sha256_file(archive_path)?;
    if digest != identity.sha256 {
        return Err(format!(
            "parser archive SHA-256 mismatch: expected {}, found {digest}",
            identity.sha256
        ));
    }
    Ok(())
}

fn sha256_file(path: &Path) -> Result<String, String> {
    let mut file = File::open(path)
        .map_err(|error| format!("cannot open {} for hashing: {error}", path.display()))?;
    let mut hasher = Sha256::new();
    let mut buffer = [0u8; 64 * 1024];
    loop {
        let count = file
            .read(&mut buffer)
            .map_err(|error| format!("cannot hash {}: {error}", path.display()))?;
        if count == 0 {
            break;
        }
        hasher.update(&buffer[..count]);
    }
    Ok(format!("{:x}", hasher.finalize()))
}

fn sha256_bytes(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

fn extract_declared_archive(
    archive_path: &Path,
    asset: &ParserAsset,
    packing: &PackingPolicy,
    destination: &Path,
) -> Result<PathBuf, String> {
    let archive = File::open(archive_path)
        .map_err(|error| format!("cannot open parser archive for extraction: {error}"))?;
    let limit = packing
        .decompressed_size_limit
        .checked_add(1)
        .ok_or("invalid decompressed size limit")?;
    let mut decoder = GzDecoder::new(archive).take(limit);
    let mut tar_bytes = Vec::new();
    decoder
        .read_to_end(&mut tar_bytes)
        .map_err(|error| format!("cannot decompress parser archive: {error}"))?;
    if u64::try_from(tar_bytes.len()).unwrap_or(u64::MAX) > packing.decompressed_size_limit {
        return Err(format!(
            "parser archive exceeds {} decompressed-byte limit",
            packing.decompressed_size_limit
        ));
    }

    let expected: BTreeMap<&str, &FileIdentity> = [
        (asset.library.path.as_str(), &asset.library),
        (asset.build_identity.path.as_str(), &asset.build_identity),
    ]
    .into_iter()
    .collect();
    let mut seen = BTreeSet::new();
    let mut offset = 0usize;
    let mut terminated = false;

    while offset < tar_bytes.len() {
        let header_end = offset
            .checked_add(512)
            .ok_or("tar header offset overflow")?;
        let header = tar_bytes
            .get(offset..header_end)
            .ok_or("truncated tar header")?;
        if header.iter().all(|byte| *byte == 0) {
            if !tar_bytes[offset..].iter().all(|byte| *byte == 0) {
                return Err("non-zero data follows tar terminator".to_string());
            }
            terminated = true;
            break;
        }
        verify_tar_header_checksum(header)?;
        if seen.len() >= packing.member_count_limit {
            return Err(format!(
                "parser archive exceeds {} member limit",
                packing.member_count_limit
            ));
        }

        let path = tar_path(header)?;
        validate_relative_archive_path(&path)?;
        let type_flag = header[156];
        if type_flag != 0 && type_flag != b'0' {
            return Err(format!(
                "parser archive member `{path}` is not a regular file"
            ));
        }
        let size = parse_tar_octal(&header[124..136], "member size")?;
        if size > packing.member_size_limit {
            return Err(format!(
                "parser archive member `{path}` exceeds {} byte limit",
                packing.member_size_limit
            ));
        }
        let identity = expected
            .get(path.as_str())
            .ok_or_else(|| format!("undeclared parser archive member `{path}`"))?;
        if !seen.insert(path.clone()) {
            return Err(format!("duplicate parser archive member `{path}`"));
        }
        if size != identity.size {
            return Err(format!(
                "parser archive member `{path}` size mismatch: expected {}, found {size}",
                identity.size
            ));
        }

        let data_start = header_end;
        let size_usize = usize::try_from(size)
            .map_err(|_| format!("parser archive member `{path}` is too large for this host"))?;
        let data_end = data_start
            .checked_add(size_usize)
            .ok_or_else(|| format!("parser archive member `{path}` offset overflow"))?;
        let body = tar_bytes
            .get(data_start..data_end)
            .ok_or_else(|| format!("truncated parser archive member `{path}`"))?;
        let digest = sha256_bytes(body);
        if digest != identity.sha256 {
            return Err(format!(
                "parser archive member `{path}` SHA-256 mismatch: expected {}, found {digest}",
                identity.sha256
            ));
        }

        let output = destination.join(&path);
        let parent = output
            .parent()
            .ok_or_else(|| format!("parser archive member `{path}` has no parent"))?;
        fs::create_dir_all(parent)
            .map_err(|error| format!("cannot create extraction directory: {error}"))?;
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&output)
            .map_err(|error| format!("cannot create extracted member `{path}`: {error}"))?;
        file.write_all(body)
            .map_err(|error| format!("cannot write extracted member `{path}`: {error}"))?;
        file.sync_all()
            .map_err(|error| format!("cannot sync extracted member `{path}`: {error}"))?;

        let padded_size = size_usize
            .checked_add(511)
            .ok_or_else(|| format!("parser archive member `{path}` padding overflow"))?
            / 512
            * 512;
        offset = data_start
            .checked_add(padded_size)
            .ok_or_else(|| format!("parser archive member `{path}` next offset overflow"))?;
    }

    if !terminated {
        return Err("parser archive has no tar terminator".to_string());
    }
    let expected_paths: BTreeSet<String> =
        expected.keys().map(|path| (*path).to_string()).collect();
    if seen != expected_paths {
        return Err(format!(
            "parser archive declared-member mismatch: expected {expected_paths:?}, found {seen:?}"
        ));
    }

    let library_path = destination.join(&asset.library.path);
    verify_extracted_identity(&library_path, &asset.library)?;
    verify_extracted_identity(
        &destination.join(&asset.build_identity.path),
        &asset.build_identity,
    )?;
    Ok(library_path)
}

fn verify_extracted_identity(path: &Path, identity: &FileIdentity) -> Result<(), String> {
    let metadata = fs::symlink_metadata(path)
        .map_err(|error| format!("extracted member is missing {}: {error}", path.display()))?;
    if metadata.file_type().is_symlink() || !metadata.is_file() || metadata.len() != identity.size {
        return Err(format!(
            "extracted member identity changed: {}",
            path.display()
        ));
    }
    let digest = sha256_file(path)?;
    if digest != identity.sha256 {
        return Err(format!(
            "extracted member SHA-256 changed: expected {}, found {digest}",
            identity.sha256
        ));
    }
    Ok(())
}

fn verify_tar_header_checksum(header: &[u8]) -> Result<(), String> {
    let expected = parse_tar_octal(&header[148..156], "header checksum")?;
    let actual: u64 = header
        .iter()
        .enumerate()
        .map(|(index, byte)| {
            if (148..156).contains(&index) {
                u64::from(b' ')
            } else {
                u64::from(*byte)
            }
        })
        .sum();
    if expected != actual {
        return Err(format!(
            "tar header checksum mismatch: expected {expected}, found {actual}"
        ));
    }
    Ok(())
}

fn parse_tar_octal(field: &[u8], label: &str) -> Result<u64, String> {
    let trimmed = field
        .iter()
        .copied()
        .skip_while(|byte| *byte == 0 || *byte == b' ')
        .take_while(|byte| *byte != 0 && *byte != b' ')
        .collect::<Vec<_>>();
    if trimmed.is_empty() {
        return Ok(0);
    }
    let text =
        std::str::from_utf8(&trimmed).map_err(|_| format!("tar {label} is not ASCII octal"))?;
    u64::from_str_radix(text, 8).map_err(|_| format!("tar {label} is not valid octal"))
}

fn tar_path(header: &[u8]) -> Result<String, String> {
    let name = tar_text(&header[..100], "member name")?;
    let prefix = tar_text(&header[345..500], "member prefix")?;
    if name.is_empty() {
        return Err("tar member has an empty name".to_string());
    }
    Ok(if prefix.is_empty() {
        name.to_string()
    } else {
        format!("{prefix}/{name}")
    })
}

fn tar_text<'a>(field: &'a [u8], label: &str) -> Result<&'a str, String> {
    let end = field
        .iter()
        .position(|byte| *byte == 0)
        .unwrap_or(field.len());
    std::str::from_utf8(&field[..end]).map_err(|_| format!("tar {label} is not UTF-8"))
}

fn validate_relative_archive_path(path: &str) -> Result<(), String> {
    let parsed = Path::new(path);
    if parsed.is_absolute()
        || path
            .split('/')
            .any(|component| component.is_empty() || component == "." || component == "..")
        || parsed
            .components()
            .any(|component| !matches!(component, Component::Normal(_)))
    {
        return Err(format!("unsafe parser archive path `{path}`"));
    }
    Ok(())
}

#[repr(C)]
struct NativeParseResult {
    kind: c_int,
    buffer_ptr: *mut c_void,
    buffer_len: c_int,
    error_ptr: *mut c_char,
    error_len: c_int,
}

type InitFn = unsafe extern "C" fn();
type ParseSqlFn = unsafe extern "C" fn(*const c_char, c_int) -> NativeParseResult;
type FreeBufferFn = unsafe extern "C" fn(*mut c_void);
type ParserVersionFn = unsafe extern "C" fn() -> *const c_char;

struct DynamicLibrary {
    handle: *mut c_void,
    path: PathBuf,
}

impl DynamicLibrary {
    fn open(path: &Path) -> Result<Self, String> {
        #[cfg(unix)]
        {
            use std::os::unix::ffi::OsStrExt;

            let path = CString::new(path.as_os_str().as_bytes())
                .map_err(|_| format!("native library path contains NUL: {}", path.display()))?;
            clear_dlerror();
            let handle = unsafe { dlopen(path.as_ptr(), RTLD_NOW | RTLD_LOCAL) };
            if handle.is_null() {
                return Err(format!(
                    "cannot load extracted native library: {}",
                    dlerror_text()
                ));
            }
            Ok(Self {
                handle,
                path: PathBuf::from(OsStr::from_bytes(path.as_bytes())),
            })
        }
        #[cfg(windows)]
        {
            use std::os::windows::ffi::OsStrExt;

            let wide: Vec<u16> = path.as_os_str().encode_wide().chain([0]).collect();
            let handle = unsafe { LoadLibraryW(wide.as_ptr()) };
            if handle.is_null() {
                return Err(format!(
                    "cannot load extracted native library (Windows error {})",
                    unsafe { GetLastError() }
                ));
            }
            Ok(Self {
                handle,
                path: path.to_path_buf(),
            })
        }
        #[cfg(not(any(unix, windows)))]
        {
            let _ = path;
            Err("dynamic loading is unsupported on this host".to_string())
        }
    }

    fn symbol<T: Copy>(&self, name: &'static CStr) -> Result<T, String> {
        #[cfg(unix)]
        let pointer = {
            clear_dlerror();
            let pointer = unsafe { dlsym(self.handle, name.as_ptr()) };
            if pointer.is_null() {
                return Err(format!(
                    "missing native symbol `{}`: {}",
                    name.to_string_lossy(),
                    dlerror_text()
                ));
            }
            pointer
        };
        #[cfg(windows)]
        let pointer = {
            let pointer = unsafe { GetProcAddress(self.handle, name.as_ptr().cast()) };
            if pointer.is_null() {
                return Err(format!(
                    "missing native symbol `{}` (Windows error {})",
                    name.to_string_lossy(),
                    unsafe { GetLastError() }
                ));
            }
            pointer
        };
        #[cfg(not(any(unix, windows)))]
        let pointer = {
            let _ = name;
            return Err("dynamic symbols are unsupported on this host".to_string());
        };

        self.verify_symbol_origin(pointer, name)?;
        if std::mem::size_of::<T>() != std::mem::size_of::<*mut c_void>() {
            return Err("native symbol pointer has an incompatible ABI size".to_string());
        }
        Ok(unsafe { std::mem::transmute_copy(&pointer) })
    }

    fn verify_symbol_origin(&self, pointer: *mut c_void, name: &CStr) -> Result<(), String> {
        #[cfg(unix)]
        let loaded_path = {
            let mut info = DlInfo {
                dli_fname: std::ptr::null(),
                dli_fbase: std::ptr::null_mut(),
                dli_sname: std::ptr::null(),
                dli_saddr: std::ptr::null_mut(),
            };
            if unsafe { dladdr(pointer.cast_const(), &mut info) } == 0 || info.dli_fname.is_null() {
                return Err(format!(
                    "cannot prove extracted origin of symbol `{}`",
                    name.to_string_lossy()
                ));
            }
            PathBuf::from(OsStr::from_bytes(
                unsafe { CStr::from_ptr(info.dli_fname) }.to_bytes(),
            ))
        };
        #[cfg(windows)]
        let loaded_path = {
            use std::os::windows::ffi::OsStringExt;

            let mut buffer = vec![0u16; 32_768];
            let length = unsafe {
                GetModuleFileNameW(
                    self.handle,
                    buffer.as_mut_ptr(),
                    u32::try_from(buffer.len()).expect("module path buffer fits u32"),
                )
            };
            if length == 0 || usize::try_from(length).unwrap_or(usize::MAX) >= buffer.len() {
                return Err(format!(
                    "cannot prove extracted origin of symbol `{}` (Windows error {})",
                    name.to_string_lossy(),
                    unsafe { GetLastError() }
                ));
            }
            buffer.truncate(usize::try_from(length).expect("module path length fits usize"));
            PathBuf::from(OsString::from_wide(&buffer))
        };
        #[cfg(not(any(unix, windows)))]
        let loaded_path = {
            let _ = (pointer, name);
            return Err("cannot verify dynamic symbol origin on this host".to_string());
        };

        let loaded_path = fs::canonicalize(&loaded_path).map_err(|error| {
            format!(
                "cannot canonicalize loaded module path {}: {error}",
                loaded_path.display()
            )
        })?;
        if loaded_path != self.path {
            return Err(format!(
                "symbol `{}` resolved outside the extracted library: expected {}, found {}",
                name.to_string_lossy(),
                self.path.display(),
                loaded_path.display()
            ));
        }
        Ok(())
    }
}

impl Drop for DynamicLibrary {
    fn drop(&mut self) {
        #[cfg(unix)]
        unsafe {
            dlclose(self.handle);
        }
        #[cfg(windows)]
        unsafe {
            FreeLibrary(self.handle);
        }
    }
}

#[cfg(target_os = "linux")]
const RTLD_NOW: c_int = 2;
#[cfg(target_os = "linux")]
const RTLD_LOCAL: c_int = 0;

#[cfg(all(unix, not(target_os = "linux")))]
const RTLD_NOW: c_int = 2;
#[cfg(all(unix, not(target_os = "linux")))]
const RTLD_LOCAL: c_int = 4;

#[cfg(unix)]
#[repr(C)]
struct DlInfo {
    dli_fname: *const c_char,
    dli_fbase: *mut c_void,
    dli_sname: *const c_char,
    dli_saddr: *mut c_void,
}

#[cfg(target_os = "linux")]
#[link(name = "dl")]
unsafe extern "C" {
    fn dlopen(filename: *const c_char, flags: c_int) -> *mut c_void;
    fn dlsym(handle: *mut c_void, symbol: *const c_char) -> *mut c_void;
    fn dlclose(handle: *mut c_void) -> c_int;
    fn dlerror() -> *const c_char;
    fn dladdr(address: *const c_void, info: *mut DlInfo) -> c_int;
}

#[cfg(all(unix, not(target_os = "linux")))]
unsafe extern "C" {
    fn dlopen(filename: *const c_char, flags: c_int) -> *mut c_void;
    fn dlsym(handle: *mut c_void, symbol: *const c_char) -> *mut c_void;
    fn dlclose(handle: *mut c_void) -> c_int;
    fn dlerror() -> *const c_char;
    fn dladdr(address: *const c_void, info: *mut DlInfo) -> c_int;
}

#[cfg(unix)]
fn clear_dlerror() {
    unsafe {
        dlerror();
    }
}

#[cfg(unix)]
fn dlerror_text() -> String {
    let message = unsafe { dlerror() };
    if message.is_null() {
        "unknown dynamic-loader error".to_string()
    } else {
        unsafe { CStr::from_ptr(message) }
            .to_string_lossy()
            .into_owned()
    }
}

#[cfg(windows)]
#[link(name = "kernel32")]
unsafe extern "system" {
    fn LoadLibraryW(filename: *const u16) -> *mut c_void;
    fn GetProcAddress(module: *mut c_void, name: *const u8) -> *mut c_void;
    fn GetModuleFileNameW(module: *mut c_void, filename: *mut u16, size: u32) -> u32;
    fn FreeLibrary(module: *mut c_void) -> c_int;
    fn GetLastError() -> u32;
}

fn smoke_extracted_library(
    library_path: &Path,
    extraction_root: &Path,
    expected_contract: &str,
) -> Result<(), String> {
    let root = fs::canonicalize(extraction_root)
        .map_err(|error| format!("cannot canonicalize extraction root: {error}"))?;
    let library_path = fs::canonicalize(library_path)
        .map_err(|error| format!("cannot canonicalize extracted library: {error}"))?;
    if !library_path.starts_with(&root) {
        return Err(format!(
            "extracted library escaped the fresh extraction root: {}",
            library_path.display()
        ));
    }

    let library = DynamicLibrary::open(&library_path)?;
    let init: InitFn = library.symbol(c"alopex_parser_init")?;
    let parse_sql: ParseSqlFn = library.symbol(c"alopex_parse_sql")?;
    let free_buffer: FreeBufferFn = library.symbol(c"alopex_free_buffer")?;
    let parser_version: ParserVersionFn = library.symbol(c"alopex_parser_version")?;

    unsafe { init() };
    let version_ptr = unsafe { parser_version() };
    if version_ptr.is_null() {
        return Err("extracted parser returned a null contract version".to_string());
    }
    let contract = unsafe { CStr::from_ptr(version_ptr) }
        .to_str()
        .map_err(|_| "extracted parser contract version is not UTF-8".to_string())?;
    if contract != expected_contract {
        return Err(format!(
            "extracted parser contract mismatch: expected `{expected_contract}`, found `{contract}`"
        ));
    }

    let sql = CString::new(CANONICAL_CONTINUOUS_AGGREGATE_SQL)
        .expect("canonical Continuous Aggregate SQL contains no NUL");
    let sql_len = c_int::try_from(CANONICAL_CONTINUOUS_AGGREGATE_SQL.len())
        .expect("canonical SQL length fits c_int");
    let result = unsafe { parse_sql(sql.as_ptr(), sql_len) };
    let payload = unsafe { take_native_payload(result, free_buffer)? };
    let statement_payload = single_statement_payload(&payload)?;
    let decoded = CreateContinuousAggregate::decode_staged_messagepack(contract, statement_payload)
        .map_err(|error| format!("Rust bridge rejected extracted parser payload: {error}"))?;
    if decoded.name != "cpu_hourly"
        || decoded.query.projection.len() != 3
        || decoded.query.from.len() != 1
        || decoded.query.group_by.as_ref().map(Vec::len) != Some(2)
        || decoded.options.len() != 2
        || decoded.options[0].key != "retention"
        || decoded.options[0].value != "30d"
        || decoded.options[1].key != "refresh_interval"
        || decoded.options[1].value != "1h"
    {
        return Err(format!(
            "extracted parser decoded an unexpected canonical definition: {decoded:?}"
        ));
    }
    Ok(())
}

unsafe fn take_native_payload(
    result: NativeParseResult,
    free_buffer: FreeBufferFn,
) -> Result<Vec<u8>, String> {
    match result.kind {
        0 => {
            let payload = unsafe {
                copy_native_buffer(result.buffer_ptr.cast_const(), result.buffer_len, "payload")
            };
            if !result.buffer_ptr.is_null() {
                unsafe { free_buffer(result.buffer_ptr) };
            }
            payload
        }
        1 => {
            let message = unsafe {
                copy_native_buffer(
                    result.error_ptr.cast_const().cast(),
                    result.error_len,
                    "error",
                )
            };
            if !result.error_ptr.is_null() {
                unsafe { free_buffer(result.error_ptr.cast()) };
            }
            let message = message?;
            Err(format!(
                "extracted parser rejected canonical SQL: {}",
                String::from_utf8_lossy(&message)
            ))
        }
        kind => {
            if !result.buffer_ptr.is_null() {
                unsafe { free_buffer(result.buffer_ptr) };
            }
            if !result.error_ptr.is_null() {
                unsafe { free_buffer(result.error_ptr.cast()) };
            }
            Err(format!(
                "extracted parser returned unknown result kind {kind}"
            ))
        }
    }
}

unsafe fn copy_native_buffer(
    pointer: *const c_void,
    length: c_int,
    label: &str,
) -> Result<Vec<u8>, String> {
    let length = usize::try_from(length)
        .map_err(|_| format!("extracted parser returned a negative {label} length"))?;
    if length == 0 || length > MAX_NATIVE_PAYLOAD_BYTES {
        return Err(format!(
            "extracted parser returned invalid {label} length {length}"
        ));
    }
    if pointer.is_null() {
        return Err(format!(
            "extracted parser returned null {label} pointer for {length} bytes"
        ));
    }
    Ok(unsafe { std::slice::from_raw_parts(pointer.cast::<u8>(), length) }.to_vec())
}

fn single_statement_payload(payload: &[u8]) -> Result<&[u8], String> {
    let (count, header_len) = match payload {
        [marker @ 0x90..=0x9f, ..] => (u32::from(marker & 0x0f), 1),
        [0xdc, high, low, ..] => (u32::from(u16::from_be_bytes([*high, *low])), 3),
        [0xdd, a, b, c, d, ..] => (u32::from_be_bytes([*a, *b, *c, *d]), 5),
        _ => {
            return Err(
                "extracted parser payload is not a MessagePack statement array".to_string(),
            );
        }
    };
    if count != 1 {
        return Err(format!(
            "canonical DDL must produce exactly one statement, found {count}"
        ));
    }
    let statement = payload
        .get(header_len..)
        .filter(|statement| !statement.is_empty())
        .ok_or("extracted parser returned an empty statement payload")?;
    Ok(statement)
}

fn main() -> ExitCode {
    let args: Vec<OsString> = std::env::args_os().skip(1).collect();
    if args.first().is_some_and(|arg| arg == NATIVE_SMOKE_FLAG) {
        return match run_native_smoke_cli(&args[1..]) {
            Ok(()) => ExitCode::SUCCESS,
            Err(error) => {
                eprintln!("native parser smoke failed: {error}");
                ExitCode::from(2)
            }
        };
    }
    embedded::entry()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn native_smoke_requires_one_explicit_archive_without_fallback() {
        let error = parse_native_smoke_args(&[]).expect_err("missing archive must fail closed");
        assert!(error.contains("explicit parser archive"), "{error}");

        let error =
            parse_native_smoke_args(&[OsString::from("one.tar.gz"), OsString::from("two.tar.gz")])
                .expect_err("ambiguous archive arguments must fail closed");
        assert!(error.contains("exactly one"), "{error}");
    }

    #[test]
    fn bridge_payload_requires_exactly_one_top_level_statement() {
        assert_eq!(single_statement_payload(&[0x91, 0x80]).unwrap(), &[0x80]);
        assert!(single_statement_payload(&[0x90]).is_err());
        assert!(single_statement_payload(&[0x92, 0x80, 0x80]).is_err());
        assert!(single_statement_payload(&[0xdc, 0x00, 0x01, 0x80]).is_ok());
        assert!(single_statement_payload(&[0xdd, 0x00, 0x00, 0x00, 0x01, 0x80]).is_ok());
    }

    #[test]
    fn embedded_manifest_selects_only_the_host_archive_filename() {
        let manifest: VendorManifest = serde_json::from_str(VENDOR_MANIFEST).unwrap();
        validate_manifest(&manifest).unwrap();
        let target = host_target().unwrap();
        let asset = manifest
            .assets
            .iter()
            .find(|asset| asset.target == target)
            .unwrap();
        let accepted = Path::new(&asset.archive.filename);
        assert_eq!(
            select_host_asset(&manifest, target, accepted)
                .unwrap()
                .target,
            target
        );

        let error = select_host_asset(&manifest, target, Path::new("wrong-target.tar.gz"))
            .expect_err("a differently named or targeted archive must be rejected");
        assert!(error.contains("filename mismatch"), "{error}");
    }

    #[test]
    fn archive_digest_mismatch_fails_before_any_extraction_or_loading() {
        let directory = tempfile::tempdir().unwrap();
        let archive = directory.path().join("fixture.tar.gz");
        fs::write(&archive, b"signed bytes changed").unwrap();
        let identity = ArchiveIdentity {
            filename: "fixture.tar.gz".to_string(),
            size: 20,
            sha256: "00".repeat(32),
        };
        let packing = PackingPolicy {
            archive_size_limit: 1024,
            decompressed_size_limit: 1024,
            member_count_limit: 2,
            member_size_limit: 1024,
        };

        let error = verify_archive_identity(&archive, &identity, &packing)
            .expect_err("changed archive bytes must fail before extraction");
        assert!(error.contains("SHA-256 mismatch"), "{error}");
        assert_eq!(fs::read_dir(directory.path()).unwrap().count(), 1);
    }

    #[test]
    fn archive_paths_cannot_escape_the_fresh_directory() {
        assert!(validate_relative_archive_path("alopex-sql-parser/target/lib.so").is_ok());
        assert!(validate_relative_archive_path("../lib.so").is_err());
        assert!(validate_relative_archive_path("/tmp/lib.so").is_err());
        assert!(validate_relative_archive_path("a/./lib.so").is_err());
    }
}
