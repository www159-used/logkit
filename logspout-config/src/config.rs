//! TOML configuration: embedded reference merged with optional user file.

use std::path::{Path, PathBuf};

use serde::Deserialize;
use toml::Value;

use crate::embed::ref_toml_string;
use crate::LogspoutError;

/// [common] — `logspout` 与 `logspout-daemon` 共用项（运行态隔离目录等）。
#[derive(Debug, Clone, Deserialize)]
pub struct CommonSection {
    /// 单实例根目录（**多实例须使用不同路径**）。其下固定：`logspout-daemon.sock`、`logspout-daemon.pid`、`logspout-daemon.log`。
    pub tmp_dir: String,
}

/// [daemon] — 除路径外的守护进程约定（sock / pid / 日志路径由 [`CommonSection::tmp_dir`] 推导）。
#[derive(Debug, Clone, Deserialize)]
pub struct DaemonSection {
    pub pid_record_suffix: String,
}

/// [client] — 预留节；客户端 Unix 套接字与守护进程相同，由 [`CommonSection::tmp_dir`] 推导。
#[derive(Debug, Clone, Deserialize, Default)]
pub struct ClientSection {}

/// [protocol.grpc] — gRPC options (Unix socket transport; see `logspout-proto`).
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

/// [log_server] — worker 子进程参数（造日志由同二进制 `logspout-daemon worker` 完成，无需配置 exe）。
#[derive(Debug, Clone, Deserialize)]
pub struct LogServerSection {
    /// 造日志写入路径的根目录（**必填**）；daemon spawn worker 时将该路径设为子进程 **cwd**，producer YAML 里 `output` 为相对该目录的路径。
    pub worker_output_dir: String,
    pub heartbeat_timeout_secs: u64,
    pub heartbeat_interval_secs: u64,
}

/// 合并后的全局配置。
#[derive(Debug, Clone, Deserialize)]
pub struct LogspoutConfig {
    pub common: CommonSection,
    pub daemon: DaemonSection,
    #[serde(default)]
    pub client: ClientSection,
    pub protocol: ProtocolSection,
    pub log_server: LogServerSection,
}

impl LogspoutConfig {
    #[inline]
    pub fn tmp_dir_path(&self) -> PathBuf {
        PathBuf::from(self.common.tmp_dir.trim())
    }

    /// Unix 监听套接字：`{tmp_dir}/logspout-daemon.sock`
    #[inline]
    pub fn daemon_socket_path(&self) -> PathBuf {
        self.tmp_dir_path().join("logspout-daemon.sock")
    }

    /// 守护进程 pid 文件：`{tmp_dir}/logspout-daemon.pid`
    #[inline]
    pub fn daemon_pid_path(&self) -> PathBuf {
        self.tmp_dir_path().join("logspout-daemon.pid")
    }

    /// 守护进程日志：`{tmp_dir}/logspout-daemon.log`
    #[inline]
    pub fn daemon_log_path(&self) -> PathBuf {
        self.tmp_dir_path().join("logspout-daemon.log")
    }

    /// 与 [`Self::daemon_socket_path`] 相同，供 `logspout` 连接。
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

/// Load embedded `conf.ref.toml`, optionally deep-merged with user TOML (`--defaults-file`).
pub fn load_merged(user_defaults: Option<&Path>) -> Result<LogspoutConfig, LogspoutError> {
    let ref_str = ref_toml_string()?;
    let mut doc: Value = toml::from_str(&ref_str)?;

    if let Some(p) = user_defaults {
        let user_s = std::fs::read_to_string(p)
            .map_err(|e| LogspoutError::read_file(p.to_path_buf(), e))?;
        let user_v: Value = toml::from_str(&user_s)?;
        doc = merge_toml_values(doc, user_v);
    }

    let mut cfg: LogspoutConfig = doc
        .try_into()
        .map_err(|e| LogspoutError::MergedInvalid(e.to_string()))?;
    let t = cfg.common.tmp_dir.trim();
    if t.is_empty() {
        return Err(LogspoutError::MergedInvalid(
            "[common].tmp_dir must be non-empty (directory for logspout-daemon.sock, logspout-daemon.pid, logspout-daemon.log)"
                .into(),
        ));
    }
    cfg.common.tmp_dir = t.to_string();
    Ok(cfg)
}
