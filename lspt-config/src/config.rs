//! TOML configuration: embedded reference merged with optional user file.

use std::path::Path;

use serde::Deserialize;
use toml::Value;

use crate::embed::ref_toml_string;
use crate::LsptError;

/// [daemon] — paths and conventions for lsptd.
#[derive(Debug, Clone, Deserialize)]
pub struct DaemonSection {
    pub socket_path: String,
    pub log_file: String,
    pub pid_file: String,
    pub pid_record_suffix: String,
}

/// [client] — paths for lspt CLI (e.g. UDS path).
#[derive(Debug, Clone, Deserialize)]
pub struct ClientSection {
    pub socket_path: String,
}

/// [protocol.grpc] — gRPC options (Unix socket transport; see `lspt-proto`).
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

/// [log_server] — worker 子进程参数（造日志由同二进制 `lsptd worker` 完成，无需配置 exe）。
#[derive(Debug, Clone, Deserialize)]
pub struct LogServerSection {
    /// 造日志写入路径的根目录（**必填**）；lsptd spawn worker 时将该路径设为子进程 **cwd**，producer YAML 里 `output` 为相对该目录的路径。
    pub worker_output_dir: String,
    pub heartbeat_timeout_secs: u64,
    pub heartbeat_interval_secs: u64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct LsptConfig {
    pub daemon: DaemonSection,
    pub client: ClientSection,
    pub protocol: ProtocolSection,
    pub log_server: LogServerSection,
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
pub fn load_merged(user_defaults: Option<&Path>) -> Result<LsptConfig, LsptError> {
    let ref_str = ref_toml_string()?;
    let mut doc: Value = toml::from_str(&ref_str)?;

    if let Some(p) = user_defaults {
        let user_s = std::fs::read_to_string(p)
            .map_err(|e| LsptError::read_file(p.to_path_buf(), e))?;
        let user_v: Value = toml::from_str(&user_s)?;
        doc = merge_toml_values(doc, user_v);
    }

    let cfg: LsptConfig = doc
        .try_into()
        .map_err(|e| LsptError::MergedInvalid(e.to_string()))?;
    Ok(cfg)
}
