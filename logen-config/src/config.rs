//! TOML configuration: embedded reference merged with optional user file.

use std::net::SocketAddr;
use std::path::{Path, PathBuf};

use serde::Deserialize;
use toml::Value;

use crate::embed::ref_toml_string;
use crate::LogenError;

/// tonic 在 Unix 传输下仍需形式合法的 HTTP URI；不对该地址建 TCP。
pub const LOCAL_GRPC_AUTHORITY_URI: &str = "http://127.0.0.1:1";

/// [client] — `logen` 连接 `logend`（类似 MySQL `[client]`）。
#[derive(Debug, Clone, Deserialize, Default)]
pub struct ClientSection {
    #[serde(default)]
    pub transport: ClientTransport,
    #[serde(default)]
    pub host: Option<String>,
    #[serde(default)]
    pub port: Option<u16>,
    /// 覆盖 Unix 套接字路径（默认与 `[logend]` 的 UDS 相同）。
    #[serde(default)]
    pub socket: Option<String>,
}

/// `logen` → `logend` 传输方式。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ClientTransport {
    #[default]
    Unix,
    Tcp,
}

fn default_log_level() -> String {
    "info".to_string()
}

/// [logend] — 守护进程、控制面 gRPC 与内嵌 worker 的全部服务端配置。
#[derive(Debug, Clone, Deserialize)]
pub struct LogendSection {
    /// 单实例根目录（**多实例须使用不同路径**）。其下默认：`logend.sock`、`logend.pid`、`logend.log`。
    pub tmp_dir: String,
    pub pid_record_suffix: String,
    /// 未设置 **`RUST_LOG`** 时作为 logend tracing 默认规格。
    #[serde(default = "default_log_level")]
    pub log_level: String,
    /// 覆盖 UDS 监听路径（默认 `{tmp_dir}/logend.sock`）。
    #[serde(default)]
    pub socket: Option<String>,
    /// 可选 TCP 监听（如 `127.0.0.1:19407`）；省略或空则仅 UDS。
    #[serde(default)]
    pub listen: Option<String>,
    /// 造日志写入根目录（**必填**）；实例 YAML 的 `output` 相对此目录。
    pub worker_output_dir: String,
    pub heartbeat_timeout_secs: u64,
    pub heartbeat_interval_secs: u64,
    #[serde(default)]
    pub runtime_threads: Option<usize>,
    pub max_decoding_message_size_bytes: u32,
    pub max_encoding_message_size_bytes: u32,
    pub ping_reply_text: String,
}

/// 合并后的全局配置（`[client]` + `[logend]`）。
#[derive(Debug, Clone, Deserialize)]
pub struct LogenConfig {
    #[serde(default)]
    pub client: ClientSection,
    pub logend: LogendSection,
}

/// CLI / 环境变量对 `[client]` 的覆盖（`-S` / `-H` / `-P`）。
#[derive(Debug, Clone, Default)]
pub struct ClientOverrides<'a> {
    pub socket: Option<&'a Path>,
    pub host: Option<&'a str>,
    pub port: Option<u16>,
}

/// 解析后的 client 连接参数，供 `logen` 建 tonic Channel。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientConnect {
    pub transport: ClientTransport,
    pub endpoint_uri: String,
    pub unix_socket: Option<PathBuf>,
}

impl LogendSection {
    #[inline]
    pub fn tmp_dir_path(&self) -> PathBuf {
        PathBuf::from(self.tmp_dir.trim())
    }

    #[inline]
    pub fn socket_path(&self) -> PathBuf {
        self.socket
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(PathBuf::from)
            .unwrap_or_else(|| self.tmp_dir_path().join("logend.sock"))
    }

    #[inline]
    pub fn pid_path(&self) -> PathBuf {
        self.tmp_dir_path().join("logend.pid")
    }

    #[inline]
    pub fn log_path(&self) -> PathBuf {
        self.tmp_dir_path().join("logend.log")
    }

    pub fn tcp_listen_addr(&self) -> Result<Option<SocketAddr>, LogenError> {
        let Some(raw) = self.listen.as_deref() else {
            return Ok(None);
        };
        let t = raw.trim();
        if t.is_empty() {
            return Ok(None);
        }
        t.parse::<SocketAddr>()
            .map(Some)
            .map_err(|e| LogenError::MergedInvalid(format!("[logend].listen invalid {t:?}: {e}")))
    }
}

impl LogenConfig {
    #[inline]
    pub fn worker_heartbeat_uri(&self) -> &'static str {
        LOCAL_GRPC_AUTHORITY_URI
    }

    pub fn resolve_client_connect(
        &self,
        ov: ClientOverrides<'_>,
    ) -> Result<ClientConnect, LogenError> {
        if let Some(path) = ov.socket {
            return Ok(ClientConnect {
                transport: ClientTransport::Unix,
                endpoint_uri: LOCAL_GRPC_AUTHORITY_URI.to_string(),
                unix_socket: Some(path.to_path_buf()),
            });
        }

        let host = ov
            .host
            .or(self.client.host.as_deref())
            .map(str::trim)
            .filter(|s| !s.is_empty());
        let port = ov.port.or(self.client.port);

        let use_tcp = matches!(self.client.transport, ClientTransport::Tcp)
            || host.is_some()
            || port.is_some();

        if use_tcp {
            let host = host.ok_or_else(|| {
                LogenError::MergedInvalid(
                    "[client].host required for tcp (or pass -H/--host)".into(),
                )
            })?;
            let port = port.ok_or_else(|| {
                LogenError::MergedInvalid(
                    "[client].port required for tcp (or pass -P/--port)".into(),
                )
            })?;
            return Ok(ClientConnect {
                transport: ClientTransport::Tcp,
                endpoint_uri: format!("http://{host}:{port}/"),
                unix_socket: None,
            });
        }

        let unix_socket = self
            .client
            .socket
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(PathBuf::from)
            .unwrap_or_else(|| self.logend.socket_path());

        Ok(ClientConnect {
            transport: ClientTransport::Unix,
            endpoint_uri: LOCAL_GRPC_AUTHORITY_URI.to_string(),
            unix_socket: Some(unix_socket),
        })
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
pub fn load_merged(user_defaults: Option<&Path>) -> Result<LogenConfig, LogenError> {
    let ref_str = ref_toml_string()?;
    let mut doc: Value = toml::from_str(&ref_str)?;

    if let Some(p) = user_defaults {
        let user_s =
            std::fs::read_to_string(p).map_err(|e| LogenError::read_file(p.to_path_buf(), e))?;
        let user_v: Value = toml::from_str(&user_s)?;
        doc = merge_toml_values(doc, user_v);
    }

    let mut cfg: LogenConfig = doc
        .try_into()
        .map_err(|e| LogenError::MergedInvalid(e.to_string()))?;

    let t = cfg.logend.tmp_dir.trim();
    if t.is_empty() {
        return Err(LogenError::MergedInvalid(
            "[logend].tmp_dir must be non-empty (directory for logend.sock, logend.pid, logend.log)"
                .into(),
        ));
    }
    cfg.logend.tmp_dir = t.to_string();

    let out = cfg.logend.worker_output_dir.trim();
    if out.is_empty() {
        return Err(LogenError::MergedInvalid(
            "[logend].worker_output_dir must be non-empty".into(),
        ));
    }
    cfg.logend.worker_output_dir = out.to_string();

    Ok(cfg)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 测试内容：未配置 `[logend].runtime_threads` 时保持默认行为。
    /// 输入：仅加载内嵌 `conf.ref.toml`。
    /// 预期：`cfg.logend.runtime_threads` 为 `None`。
    #[test]
    fn load_merged_accepts_missing_runtime_threads() {
        let cfg = load_merged(None).expect("embedded defaults");
        assert_eq!(cfg.logend.runtime_threads, None);
    }

    /// 测试内容：用户 TOML 可显式覆盖 `[logend].runtime_threads`。
    /// 输入：临时文件仅含 `[logend].runtime_threads = 3`。
    /// 预期：合并后 `cfg.logend.runtime_threads == Some(3)`。
    #[test]
    fn load_merged_reads_explicit_runtime_threads() {
        let dir = std::env::temp_dir().join(format!("logen-config-{}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("create temp dir");
        let path = dir.join("defaults.toml");
        std::fs::write(
            &path,
            r#"
[logend]
runtime_threads = 3
"#,
        )
        .expect("write defaults");

        let cfg = load_merged(Some(path.as_path())).expect("merged config");
        assert_eq!(cfg.logend.runtime_threads, Some(3));
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_dir(&dir);
    }

    /// 测试内容：默认 `[client]` 解析为本地 Unix 套接字。
    /// 输入：内嵌默认配置，无 CLI 覆盖。
    /// 预期：transport=Unix，套接字为 `{tmp_dir}/logend.sock`。
    #[test]
    fn resolve_client_connect_defaults_to_unix() {
        let cfg = load_merged(None).expect("defaults");
        let conn = cfg
            .resolve_client_connect(ClientOverrides::default())
            .expect("resolve");
        assert_eq!(conn.transport, ClientTransport::Unix);
        assert_eq!(conn.endpoint_uri, LOCAL_GRPC_AUTHORITY_URI);
        assert_eq!(
            conn.unix_socket,
            Some(PathBuf::from("/tmp/logend/logend.sock"))
        );
    }

    /// 测试内容：`[client]` TCP 配置解析为 HTTP endpoint URI。
    /// 输入：用户 TOML `transport=tcp`、`host`、`port`。
    /// 预期：`endpoint_uri` 为 `http://10.0.0.5:19407/`。
    #[test]
    fn resolve_client_connect_tcp_from_config() {
        let dir = std::env::temp_dir().join(format!("logen-config-tcp-{}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("create temp dir");
        let path = dir.join("client.toml");
        std::fs::write(
            &path,
            r#"
[client]
transport = "tcp"
host = "10.0.0.5"
port = 19407
"#,
        )
        .expect("write client");

        let cfg = load_merged(Some(path.as_path())).expect("merged");
        let conn = cfg
            .resolve_client_connect(ClientOverrides::default())
            .expect("resolve");
        assert_eq!(conn.transport, ClientTransport::Tcp);
        assert_eq!(conn.endpoint_uri, "http://10.0.0.5:19407/");
        assert_eq!(conn.unix_socket, None);
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_dir(&dir);
    }

    /// 测试内容：CLI `-S` 强制 Unix 并覆盖 `[client]` TCP 设置。
    /// 输入：TCP 配置 + `ClientOverrides { socket: Some("/tmp/x.sock") }`。
    /// 预期：transport=Unix，套接字为 `/tmp/x.sock`。
    #[test]
    fn resolve_client_connect_cli_socket_overrides_tcp() {
        let dir = std::env::temp_dir().join(format!("logen-config-override-{}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("create temp dir");
        let path = dir.join("client.toml");
        std::fs::write(
            &path,
            r#"
[client]
transport = "tcp"
host = "10.0.0.5"
port = 19407
"#,
        )
        .expect("write client");

        let cfg = load_merged(Some(path.as_path())).expect("merged");
        let sock = PathBuf::from("/tmp/x.sock");
        let conn = cfg
            .resolve_client_connect(ClientOverrides {
                socket: Some(sock.as_path()),
                ..Default::default()
            })
            .expect("resolve");
        assert_eq!(conn.transport, ClientTransport::Unix);
        assert_eq!(conn.unix_socket, Some(sock));
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_dir(&dir);
    }
}
