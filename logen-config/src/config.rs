//! TOML configuration: embedded reference merged with optional user file.

use std::path::{Path, PathBuf};

use serde::Deserialize;
use toml::Value;

use crate::embed::ref_toml_string;
use crate::LogenError;

/// [common] — `logen` 与 `logend` 共用项（运行态隔离目录等）。
#[derive(Debug, Clone, Deserialize)]
pub struct CommonSection {
    /// 单实例根目录（**多实例须使用不同路径**）。其下固定：`logend.sock`、`logend.pid`、`logend.log`。
    pub tmp_dir: String,
}

fn default_daemon_log_level() -> String {
    "info".to_string()
}

/// [daemon] — 除路径外的守护进程约定（sock / pid / 日志路径由 [`CommonSection::tmp_dir`] 推导）。
#[derive(Debug, Clone, Deserialize)]
pub struct DaemonSection {
    pub pid_record_suffix: String,
    /// logend **`tracing_subscriber::EnvFilter`** 默认规格（仅当未设置 **`RUST_LOG`** 时生效）；如 **`info`**、**`debug`**。
    #[serde(default = "default_daemon_log_level")]
    pub log_level: String,
}

/// [client] — 预留节；客户端 Unix 套接字与守护进程相同，由 [`CommonSection::tmp_dir`] 推导。
#[derive(Debug, Clone, Deserialize, Default)]
pub struct ClientSection {}

/// [protocol.grpc] — gRPC options (Unix socket transport; see `logen-proto`).
#[derive(Debug, Clone, Deserialize)]
pub struct GrpcSection {
    pub max_decoding_message_size_bytes: u32,
    pub max_encoding_message_size_bytes: u32,
    pub ping_reply_text: String,
    /// Synthetic HTTP authority for tonic [`Endpoint`] when the transport is UDS (not used for TCP).
    pub client_connect_uri: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ProtocolSection {
    pub grpc: GrpcSection,
}

/// [worker] — 造日志实例；默认在 `logend` **进程内** 直接消费内存中的实例配置运行。
#[derive(Debug, Clone, Deserialize)]
pub struct WorkerSection {
    /// 造日志写入路径的根目录（**必填**）；实例 YAML 里 `output` 为**相对该目录**的路径。
    pub worker_output_dir: String,
    pub heartbeat_timeout_secs: u64,
    pub heartbeat_interval_secs: u64,
    /// worker 专用 tokio runtime 线程数；省略时使用 tokio 默认线程数。
    #[serde(default)]
    pub runtime_threads: Option<usize>,
}

/// 合并后的全局配置。
#[derive(Debug, Clone, Deserialize)]
pub struct LogenConfig {
    pub common: CommonSection,
    pub daemon: DaemonSection,
    #[serde(default)]
    pub client: ClientSection,
    pub protocol: ProtocolSection,
    /// `[worker]`：旧键 `[log_server]` / `[log_worker]` 在 `load_merged` 中会并入本节。
    pub worker: WorkerSection,
}

impl LogenConfig {
    #[inline]
    pub fn tmp_dir_path(&self) -> PathBuf {
        PathBuf::from(self.common.tmp_dir.trim())
    }

    /// Unix 监听套接字：`{tmp_dir}/logend.sock`
    #[inline]
    pub fn daemon_socket_path(&self) -> PathBuf {
        self.tmp_dir_path().join("logend.sock")
    }

    /// 守护进程 pid 文件：`{tmp_dir}/logend.pid`
    #[inline]
    pub fn daemon_pid_path(&self) -> PathBuf {
        self.tmp_dir_path().join("logend.pid")
    }

    /// 守护进程日志：`{tmp_dir}/logend.log`
    #[inline]
    pub fn daemon_log_path(&self) -> PathBuf {
        self.tmp_dir_path().join("logend.log")
    }

    /// 与 [`Self::daemon_socket_path`] 相同，供 `logen` 连接。
    #[inline]
    pub fn client_socket_path(&self) -> String {
        self.daemon_socket_path().to_string_lossy().into_owned()
    }
}

fn merge_toml_values(base: Value, over: Value) -> Value {
    use toml::Value::*;
    match (base, over) {
        (Table(mut bt), Table(ot)) => {
            for (k, v) in ot {
                let merged = if let Some(bv) = bt.get(&k) {
                    merge_toml_values(bv.clone(), v)
                } else {
                    v
                };
                bt.insert(k, merged);
            }
            Table(bt)
        }
        (_, o) => o,
    }
}

/// 将旧节名 `[log_server]`、`[log_worker]` 并入 `[worker]`（后处理的节覆盖同名字段）。
fn fold_legacy_worker_sections(root: &mut toml::map::Map<String, Value>) {
    fold_section_into_worker(root, "log_server");
    fold_section_into_worker(root, "log_worker");
}

fn fold_section_into_worker(root: &mut toml::map::Map<String, Value>, from_key: &str) {
    let Some(Value::Table(src)) = root.remove(from_key) else {
        return;
    };
    match root.remove("worker") {
        Some(Value::Table(mut w)) => {
            for (k, v) in src {
                w.insert(k, v);
            }
            root.insert("worker".into(), Value::Table(w));
        }
        None | Some(_) => {
            root.insert("worker".into(), Value::Table(src));
        }
    }
}

/// 已删除的配置键：合并旧表后可能仍存在，删掉以免反序列化失败。
fn strip_obsolete_worker_keys(root: &mut toml::map::Map<String, Value>) {
    let Some(Value::Table(w)) = root.get_mut("worker") else {
        return;
    };
    w.remove("worker_executable");
}

/// Load embedded `conf.ref.toml`, optionally deep-merged with user TOML (`--defaults-file`).
pub fn load_merged(user_defaults: Option<&Path>) -> Result<LogenConfig, LogenError> {
    let ref_str = ref_toml_string()?;
    let mut doc: Value = toml::from_str(&ref_str)?;

    if let Some(p) = user_defaults {
        let user_s =
            std::fs::read_to_string(p).map_err(|e| LogenError::read_file(p.to_path_buf(), e))?;
        let user_v: Value = toml::from_str(&user_s)?;
        doc = merge_toml_values(doc, user_v);
    }

    if let Value::Table(ref mut root) = doc {
        fold_legacy_worker_sections(root);
        strip_obsolete_worker_keys(root);
    }

    let mut cfg: LogenConfig = doc
        .try_into()
        .map_err(|e| LogenError::MergedInvalid(e.to_string()))?;
    let t = cfg.common.tmp_dir.trim();
    if t.is_empty() {
        return Err(LogenError::MergedInvalid(
            "[common].tmp_dir must be non-empty (directory for logend.sock, logend.pid, logend.log)"
                .into(),
        ));
    }
    cfg.common.tmp_dir = t.to_string();
    Ok(cfg)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 测试内容：未配置 `[worker].runtime_threads` 时保持兼容默认行为。
    /// 输入：仅加载内嵌 `conf.ref.toml`，不提供用户覆盖配置。
    /// 预期：`cfg.worker.runtime_threads` 为 `None`，交由运行时层使用 tokio 默认线程数。
    #[test]
    fn load_merged_accepts_missing_worker_runtime_threads() {
        let cfg = load_merged(None).expect("embedded defaults");
        assert_eq!(cfg.worker.runtime_threads, None);
    }

    /// 测试内容：用户 TOML 可显式覆盖 `[worker].runtime_threads`。
    /// 输入：临时 `defaults.toml`，仅写入 `[worker].runtime_threads = 3`。
    /// 预期：`load_merged` 成功合并，且 `cfg.worker.runtime_threads == Some(3)`。
    #[test]
    fn load_merged_reads_explicit_worker_runtime_threads() {
        let dir = std::env::temp_dir().join(format!("logen-config-{}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("create temp dir");
        let path = dir.join("defaults.toml");
        std::fs::write(
            &path,
            r#"
[worker]
runtime_threads = 3
"#,
        )
        .expect("write defaults");

        let cfg = load_merged(Some(path.as_path())).expect("merged config");
        assert_eq!(cfg.worker.runtime_threads, Some(3));
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_dir(&dir);
    }
}
