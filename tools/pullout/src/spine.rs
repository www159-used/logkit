use std::ffi::{c_char, CString};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
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
    #[error("spawn {python}: {source}")]
    PythonSpawn {
        python: PathBuf,
        source: std::io::Error,
    },
    #[error("python spine_keeper: {message}")]
    PythonRun { message: String },
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

/// 调试日志写 stderr；默认开启，设 `PULLOUT_QUIET=1` 关闭。
pub fn trace_enabled() -> bool {
    !matches!(
        std::env::var("PULLOUT_QUIET").as_deref(),
        Ok("1") | Ok("true") | Ok("yes")
    )
}

pub fn trace(msg: impl AsRef<str>) {
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
    PathBuf::from(format!("/opt/{}", resolve_oem::oem_name()))
}

fn spine_keeper_lib_under_python(arch: &str) -> PathBuf {
    let lib_root = opt_home().join("python/lib");
    trace(format!("scan spine_keeper lib under {}", lib_root.display()));
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
    let path = spine_keeper_lib_under_python(arch);
    if path.is_file() {
        return Ok(path);
    }
    Err(SpineError::LibraryNotFound { path })
}

fn ensure_yotta_home_env(home: &Path) {
    std::env::set_var("YOTTA_HOME", home);
}

fn python_bin() -> PathBuf {
    if let Ok(p) = std::env::var("PULLOUT_PYTHON") {
        let p = p.trim();
        if !p.is_empty() {
            return PathBuf::from(p);
        }
    }
    let oem = resolve_oem::oem_name();
    PathBuf::from(format!("/opt/{oem}/python/bin/python"))
}

fn load_engine(path: &Path) -> Result<SpineEngine, SpineError> {
    if !path.is_file() {
        return Err(SpineError::LibraryNotFound {
            path: path.to_path_buf(),
        });
    }
    trace(format!("dlopen {}", path.display()));
    // SAFETY: 符号签名与 spine_keeper Python binding 一致。
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PulloutBackend {
    Native,
    Python,
}

/// `PULLOUT_BACKEND=python|native`；musl static-pie 默认 python，其它平台默认 native。
fn pullout_backend() -> PulloutBackend {
    match std::env::var("PULLOUT_BACKEND")
        .ok()
        .map(|s| s.trim().to_ascii_lowercase())
        .as_deref()
    {
        Some("python") | Some("py") => PulloutBackend::Python,
        Some("native") | Some("dlopen") => PulloutBackend::Native,
        _ => {
            #[cfg(target_env = "musl")]
            {
                PulloutBackend::Python
            }
            #[cfg(not(target_env = "musl"))]
            {
                PulloutBackend::Native
            }
        }
    }
}

fn dynamic_loading_unsupported(err: &SpineError) -> bool {
    match err {
        SpineError::LoadLibrary { source, .. } => source
            .to_string()
            .contains("Dynamic loading not supported"),
        _ => false,
    }
}

fn pull_out_spine_python(token: &str, yotta: &Path) -> Result<String, SpineError> {
    let python = python_bin();
    trace(format!(
        "python {} (YOTTA_HOME={})",
        python.display(),
        yotta.display()
    ));
    let script = r#"
import os, sys
os.environ["YOTTA_HOME"] = os.environ.get("YOTTA_HOME") or ""
from spine_keeper import SpineUtils
data = SpineUtils().pull_out_spine(sys.stdin.read().strip())
sys.stdout.buffer.write(data if isinstance(data, bytes) else data.encode("utf-8"))
"#;
    let mut child = Command::new(&python)
        .arg("-c")
        .arg(script)
        .env("YOTTA_HOME", yotta)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|source| SpineError::PythonSpawn {
            python: python.clone(),
            source,
        })?;
    {
        use std::io::Write as _;
        let mut stdin = child.stdin.take().ok_or_else(|| SpineError::PythonRun {
            message: "python stdin unavailable".into(),
        })?;
        stdin.write_all(token.as_bytes()).map_err(|e| SpineError::PythonRun {
            message: format!("write python stdin: {e}"),
        })?;
    }
    let output = child.wait_with_output().map_err(|e| SpineError::PythonRun {
        message: format!("wait python: {e}"),
    })?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(SpineError::PythonRun {
            message: format!("exit {:?}: {}", output.status.code(), stderr.trim()),
        });
    }
    String::from_utf8(output.stdout).map_err(|e| SpineError::PythonRun {
        message: format!("python stdout utf-8: {e}"),
    })
}

fn pull_out_spine_native(token: &str, key_id: &str, yotta: &Path) -> Result<String, SpineError> {
    trace("loading spine engine (native dlopen)...");
    let spine = engine()?;
    trace("spine engine ready");
    let hard = pull_out_hard_spine_from_file(spine, key_id, yotta)?;
    let plain = pull_out_spiny_cactus(spine, &hard, token)?;
    String::from_utf8(plain).map_err(Into::into)
}

/// 解密 spine 密文 token，返回 UTF-8 明文（等同 Python `SpineUtils.pull_out_spine`）。
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

    match pullout_backend() {
        PulloutBackend::Python => {
            #[cfg(target_env = "musl")]
            trace("backend=python (musl；PULLOUT_BACKEND=native 可强制 dlopen)");
            #[cfg(not(target_env = "musl"))]
            trace("backend=python (PULLOUT_BACKEND)");
            let plain = pull_out_spine_python(token, &yotta)?;
            trace("done (python)");
            return Ok(plain);
        }
        PulloutBackend::Native => trace("backend=native (dlopen librootkey_crypto.so)"),
    }

    match pull_out_spine_native(token, key_id, &yotta) {
        Ok(plain) => {
            trace("done (native)");
            Ok(plain)
        }
        Err(e) if dynamic_loading_unsupported(&e) => {
            trace(format!("native dlopen unavailable ({e}), fallback to python"));
            let plain = pull_out_spine_python(token, &yotta)?;
            trace("done (python fallback)");
            Ok(plain)
        }
        Err(e) => Err(e),
    }
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
        let p = spine_keeper_lib_under_python(arch);
        let s = p.to_string_lossy();
        assert!(s.starts_with("/opt/yotta/python/lib/"));
        assert!(s.contains("site-packages/spine_keeper/lib/linux/"));
        assert!(s.ends_with("librootkey_crypto.so"));
    }
}
