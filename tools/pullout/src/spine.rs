use std::ffi::{c_char, CString};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::OnceLock;

use base64::Engine;
use libloading::{Library, Symbol};
use serde::Deserialize;
use thiserror::Error;

const RK_ROOT_KEY_SIZE: usize = 32;
const RK_WORK_KEY_JSON_CAPACITY: usize = 2048;

const RK_OK: i32 = 0;
const RK_ERR_ARG: i32 = -1;
const RK_ERR_IO: i32 = -2;
const RK_ERR_CRYPTO: i32 = -3;
const RK_ERR_FORMAT: i32 = -4;
const RK_ERR_AUTH: i32 = -5;

type DecryptWorkKeyFn = unsafe extern "C" fn(
    *const c_char,
    *const c_char,
    *const c_char,
    *const u8,
    usize,
    *mut c_char,
    usize,
    *mut usize,
) -> i32;

type DecryptDataFn = unsafe extern "C" fn(
    *const u8,
    usize,
    *const c_char,
    *mut u8,
    usize,
    *mut usize,
) -> i32;

#[derive(Debug, Error)]
pub enum SpineError {
    #[error("invalid spine token format")]
    InvalidToken,
    #[error("unsupported linux arch for spine_keeper: {0}")]
    UnsupportedArch(&'static str),
    #[error("read work key {path}: {source}")]
    ReadWorkKey {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("spine library not found: {path}")]
    LibraryNotFound { path: PathBuf },
    #[error("load spine library {path}: {source}")]
    LoadLibrary {
        path: PathBuf,
        source: libloading::Error,
    },
    #[error("hard spine key id mismatch: expected {expected}, got {got}")]
    KeyIdMismatch { expected: String, got: String },
    #[error("invalid hard spine length: {len}")]
    InvalidHardSpineLength { len: usize },
    #[error("work key json: {0}")]
    WorkKeyJson(#[from] serde_json::Error),
    #[error("base64 decode hard spine: {0}")]
    HardSpineBase64(#[from] base64::DecodeError),
    #[error("plaintext is not valid utf-8")]
    PlaintextUtf8(#[from] std::string::FromUtf8Error),
    #[error("{action} failed: {message}")]
    Native { action: &'static str, message: String },
}

#[derive(Deserialize)]
struct MediumSpineEnvelope {
    #[serde(rename = "keyId")]
    key_id: String,
    #[serde(rename = "keyBase64")]
    key_base64: String,
}

struct SpineEngine {
    _lib: Library,
    decrypt_work_key: DecryptWorkKeyFn,
    decrypt_data: DecryptDataFn,
}

static ENGINE: OnceLock<SpineEngine> = OnceLock::new();
static VERBOSE: AtomicBool = AtomicBool::new(false);

/// 由 CLI `-v` 开启 stderr 调试日志。
pub fn set_verbose(enabled: bool) {
    VERBOSE.store(enabled, Ordering::Relaxed);
}

fn trace_enabled() -> bool {
    VERBOSE.load(Ordering::Relaxed)
}

fn trace(msg: impl AsRef<str>) {
    if trace_enabled() {
        eprintln!("[pullout] {}", msg.as_ref());
        let _ = std::io::stderr().flush();
    }
}

fn status_message(status: i32) -> String {
    match status {
        RK_OK => "ok".into(),
        RK_ERR_ARG => "invalid arguments".into(),
        RK_ERR_IO => "I/O error".into(),
        RK_ERR_CRYPTO => "crypto error".into(),
        RK_ERR_FORMAT => "invalid format".into(),
        RK_ERR_AUTH => "authentication failed".into(),
        other => format!("unknown status {other}"),
    }
}

fn raise_status(status: i32, action: &'static str) -> Result<(), SpineError> {
    if status == RK_OK {
        return Ok(());
    }
    Err(SpineError::Native {
        action,
        message: status_message(status),
    })
}

pub fn linux_lib_arch() -> Result<&'static str, SpineError> {
    match std::env::consts::ARCH {
        "x86_64" => Ok("x86_64"),
        "aarch64" => Ok("aarch64"),
        other => Err(SpineError::UnsupportedArch(other)),
    }
}

fn opt_home() -> PathBuf {
    resolve_oem::opt_root()
}

/// `librootkey_crypto.so` 随 spine_keeper 装在 OEM Python site-packages 下（仅作路径，不调用 Python）。
fn default_spine_so_path(arch: &str) -> PathBuf {
    let lib_root = opt_home().join("python/lib");
    trace(format!("scan librootkey_crypto under {}", lib_root.display()));
    let fallback = lib_root
        .join("python3.12/site-packages/spine_keeper/lib/linux")
        .join(arch)
        .join("librootkey_crypto.so");
    if let Ok(entries) = std::fs::read_dir(&lib_root) {
        let mut py_dirs: Vec<PathBuf> = entries
            .filter_map(Result::ok)
            .map(|e| e.path())
            .filter(|p| {
                p.file_name()
                    .and_then(|n| n.to_str())
                    .is_some_and(|n| n.starts_with("python3."))
            })
            .collect();
        py_dirs.sort();
        for py in py_dirs.into_iter().rev() {
            let candidate = py
                .join("site-packages/spine_keeper/lib/linux")
                .join(arch)
                .join("librootkey_crypto.so");
            trace(format!("  try {}", candidate.display()));
            if candidate.is_file() {
                trace(format!("  found {}", candidate.display()));
                return candidate;
            }
        }
    }
    trace(format!("  fallback {}", fallback.display()));
    fallback
}

/// 默认 `librootkey_crypto.so`（`PULLOUT_LIB` 可覆盖；否则在 `/opt/{oem}/python` 下查找）。
pub fn default_library_path() -> Result<PathBuf, SpineError> {
    if let Ok(p) = std::env::var("PULLOUT_LIB") {
        let p = p.trim();
        if !p.is_empty() {
            trace(format!("PULLOUT_LIB={p}"));
            return Ok(PathBuf::from(p));
        }
    }
    let arch = linux_lib_arch()?;
    trace(format!("target arch={arch}"));
    let path = default_spine_so_path(arch);
    if path.is_file() {
        return Ok(path);
    }
    Err(SpineError::LibraryNotFound { path })
}

fn ensure_yotta_home_env(home: &Path) {
    std::env::set_var("YOTTA_HOME", home);
}

fn load_engine(path: &Path) -> Result<SpineEngine, SpineError> {
    if !path.is_file() {
        return Err(SpineError::LibraryNotFound {
            path: path.to_path_buf(),
        });
    }
    trace(format!("dlopen {}", path.display()));
    // SAFETY: 符号签名与 spine_keeper 自带 librootkey_crypto 一致。
    unsafe {
        let lib = Library::new(path).map_err(|source| SpineError::LoadLibrary {
            path: path.to_path_buf(),
            source,
        })?;
        let decrypt_work_key: Symbol<DecryptWorkKeyFn> =
            lib.get(b"rk_decrypt_work_key_with_material_files\0")
                .map_err(|source| SpineError::LoadLibrary {
                    path: path.to_path_buf(),
                    source,
                })?;
        let decrypt_data: Symbol<DecryptDataFn> = lib.get(b"rk_decrypt_data\0").map_err(|source| {
            SpineError::LoadLibrary {
                path: path.to_path_buf(),
                source,
            }
        })?;
        trace("resolved rk_decrypt_work_key_with_material_files, rk_decrypt_data");
        Ok(SpineEngine {
            decrypt_work_key: *decrypt_work_key,
            decrypt_data: *decrypt_data,
            _lib: lib,
        })
    }
}

fn engine() -> Result<&'static SpineEngine, SpineError> {
    if let Some(e) = ENGINE.get() {
        return Ok(e);
    }
    let path = default_library_path()?;
    let loaded = load_engine(&path)?;
    let _ = ENGINE.set(loaded);
    ENGINE.get().ok_or_else(|| SpineError::Native {
        action: "init_engine",
        message: "spine engine init race".into(),
    })
}

fn pull_out_medium_spine(spine: &SpineEngine, work_key_blob: &[u8]) -> Result<String, SpineError> {
    trace(format!(
        "rk_decrypt_work_key_with_material_files (work key blob {} bytes)",
        work_key_blob.len()
    ));
    let mut out = vec![0u8; work_key_blob.len().max(RK_WORK_KEY_JSON_CAPACITY)];
    let mut out_len = 0usize;
    let status = unsafe {
        (spine.decrypt_work_key)(
            std::ptr::null(),
            std::ptr::null(),
            std::ptr::null(),
            work_key_blob.as_ptr(),
            work_key_blob.len(),
            out.as_mut_ptr().cast::<c_char>(),
            out.len(),
            &mut out_len,
        )
    };
    raise_status(status, "pull_out_medium_spine")?;
    trace(format!("work key json ready ({out_len} bytes)"));
    let json = std::str::from_utf8(&out[..out_len]).map_err(|_| SpineError::Native {
        action: "pull_out_medium_spine",
        message: "invalid utf-8 in work key json".into(),
    })?;
    Ok(json.to_string())
}

fn pull_out_hard_spine_from_file(
    spine: &SpineEngine,
    key_id: &str,
    yotta: &Path,
) -> Result<Vec<u8>, SpineError> {
    let path = yotta.join("key").join("v1").join(format!("{key_id}.key"));
    trace(format!("read work key {}", path.display()));
    let blob = std::fs::read(&path).map_err(|source| SpineError::ReadWorkKey {
        path: path.clone(),
        source,
    })?;
    let json = pull_out_medium_spine(spine, &blob)?;
    let envelope: MediumSpineEnvelope = serde_json::from_str(&json)?;
    if envelope.key_id != key_id {
        return Err(SpineError::KeyIdMismatch {
            expected: key_id.to_string(),
            got: envelope.key_id,
        });
    }
    let hard = base64::engine::general_purpose::STANDARD.decode(envelope.key_base64.trim())?;
    if hard.len() != RK_ROOT_KEY_SIZE {
        return Err(SpineError::InvalidHardSpineLength { len: hard.len() });
    }
    trace("hard spine derived from work key");
    Ok(hard)
}

fn pull_out_spiny_cactus(
    spine: &SpineEngine,
    hard_spine: &[u8],
    token: &str,
) -> Result<Vec<u8>, SpineError> {
    trace(format!("rk_decrypt_data (token {} bytes)", token.len()));
    let c_token = CString::new(token.as_bytes()).map_err(|_| SpineError::InvalidToken)?;
    let mut out = vec![0u8; token.len()];
    let mut out_len = 0usize;
    let status = unsafe {
        (spine.decrypt_data)(
            hard_spine.as_ptr(),
            hard_spine.len(),
            c_token.as_ptr(),
            out.as_mut_ptr(),
            out.len(),
            &mut out_len,
        )
    };
    raise_status(status, "pull_out_spiny_cactus")?;
    out.truncate(out_len);
    trace(format!("plaintext ready ({out_len} bytes)"));
    Ok(out)
}

/// 解密 spine 密文 token，返回 UTF-8 明文（dlopen `librootkey_crypto.so`）。
pub fn pull_out_spine(ciphertext: &str) -> Result<String, SpineError> {
    let token = ciphertext.trim();
    trace(format!(
        "pull_out_spine start (ciphertext {} bytes after trim)",
        token.len()
    ));
    let parts: Vec<&str> = token.splitn(4, ':').collect();
    if parts.len() != 4 || parts[0].is_empty() {
        return Err(SpineError::InvalidToken);
    }
    let key_id = parts[0];
    trace(format!("token key_id={key_id}"));
    let yotta = opt_home();
    trace(format!(
        "OEM={} YOTTA_HOME={}",
        resolve_oem::oem_name(),
        yotta.display()
    ));
    ensure_yotta_home_env(&yotta);

    trace("loading spine engine (dlopen)...");
    let spine = engine()?;
    trace("spine engine ready");
    let hard = pull_out_hard_spine_from_file(spine, key_id, &yotta)?;
    let plain = pull_out_spiny_cactus(spine, &hard, token)?;
    trace("done");
    String::from_utf8(plain).map_err(Into::into)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_token_format() {
        let parts: Vec<_> = "config:aa:bb:cc".splitn(4, ':').collect();
        assert_eq!(parts.len(), 4);
        assert_eq!(parts[0], "config");
        let bad: Vec<_> = "nocolon".splitn(4, ':').collect();
        assert_ne!(bad.len(), 4);
    }

    #[test]
    fn default_library_path_shape() {
        std::env::remove_var("PULLOUT_LIB");
        std::env::remove_var("OEM_NAME");
        let arch = linux_lib_arch().unwrap();
        let p = default_spine_so_path(arch);
        let s = p.to_string_lossy();
        assert!(s.starts_with("/opt/yotta/python/lib/"));
        assert!(s.contains("site-packages/spine_keeper/lib/linux/"));
        assert!(s.ends_with("librootkey_crypto.so"));
    }
}
