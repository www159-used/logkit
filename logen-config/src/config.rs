//! TOML configuration: embedded reference merged with optional user file.

use std::net::SocketAddr;
use std::path::{Path, PathBuf};

use serde::Deserialize;
use toml::Value;

use crate::embed::ref_toml_string;
use crate::LogenError;

/// tonic 在 Unix 传输下仍需形式合法的 HTTP URI；不对该地址建 TCP。
pub const LOCAL_GRPC_AUTHORITY_URI: &str = "http://127.0.0.1:1";

/// logen TCP 约定端口：指定 `-H` / `[client].host` 且未写 `-P` / `port` 时使用。
pub const CONVENTIONAL_CLIENT_TCP_PORT: u16 = 11159;

/// [client] — `logen` 连接 `logend`（类似 MySQL `[client]`）。
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "kebab-case", deny_unknown_fields)]
pub struct ClientSection {
    #[serde(default)]
    pub host: Option<String>,
    #[serde(default)]
    pub port: Option<u16>,
    /// 覆盖 Unix 套接字路径（默认与 `[logend]` 的 UDS 相同）。
    #[serde(default)]
    pub socket: Option<String>,
    /// `logen start` 时是否自动发现 Kafka 传输（client.conf / server.properties）；默认 **true**。
    #[serde(default = "default_auto_kafka_protocol")]
    pub auto_kafka_protocol: bool,
}

fn default_auto_kafka_protocol() -> bool {
    true
}

impl Default for ClientSection {
    fn default() -> Self {
        Self {
            host: None,
            port: None,
            socket: None,
            auto_kafka_protocol: default_auto_kafka_protocol(),
        }
    }
}

fn default_log_level() -> String {
    "info".to_string()
}

/// [logend] — 守护进程、控制面 gRPC 与内嵌 worker 的全部服务端配置。
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "kebab-case", deny_unknown_fields)]
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
    /// TCP 绑定地址（与 [`Self::port`] 成对配置；类似 mysqld `bind-address`）。
    #[serde(default)]
    pub bind: Option<String>,
    /// TCP 端口（与 [`Self::bind`] 成对配置；类似 mysqld `port`；嵌入默认见 `conf.ref.toml`）。
    #[serde(default)]
    pub port: Option<u16>,
    /// 造日志写入根目录（**必填**，且必须为绝对路径）；实例 YAML 未写 `output` 时，daemon 在此目录下生成默认文件。
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

/// CLI / 环境变量对 `[client]` 的覆盖。
#[derive(Debug, Clone, Copy, Default)]
pub struct ClientOverrides<'a> {
    pub socket: Option<&'a Path>,
    pub host: Option<&'a str>,
    pub port: Option<u16>,
    pub auto_kafka_protocol: Option<bool>,
}

/// 解析后的 client 连接参数。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClientConnect {
    Unix { socket: PathBuf },
    Tcp { host: String, port: u16 },
}

impl ClientConnect {
    /// Unix 模式下 tonic 使用的形式 URI（静态，不对该地址建 TCP）。
    #[inline]
    pub fn endpoint_uri(&self) -> &'static str {
        LOCAL_GRPC_AUTHORITY_URI
    }

    /// TCP 模式的 `http://host:port/`（仅用于错误信息或测试）。
    pub fn tcp_uri(&self) -> Option<String> {
        match self {
            Self::Tcp { host, port } => Some(format!("http://{host}:{port}/")),
            Self::Unix { .. } => None,
        }
    }
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

    /// 本机 worker 心跳用的 Unix 连接（与 `[client]` 远端设置无关）。
    #[inline]
    pub fn local_unix_connect(&self) -> ClientConnect {
        ClientConnect::Unix {
            socket: self.socket_path(),
        }
    }

    pub fn tcp_listen_addr(&self) -> Result<Option<SocketAddr>, LogenError> {
        let bind = self
            .bind
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty());
        let port = self.port;
        match (bind, port) {
            (None, None) => Ok(None),
            (Some(host), Some(port)) => {
                let addr = format!("{host}:{port}");
                addr.parse::<SocketAddr>().map(Some).map_err(|e| {
                    LogenError::MergedInvalid(format!("[logend] bind+port invalid {addr:?}: {e}"))
                })
            }
            (Some(_), None) => Err(LogenError::MergedInvalid(
                "[logend].port required when bind is set".into(),
            )),
            (None, Some(_)) => Err(LogenError::MergedInvalid(
                "[logend].bind required when port is set".into(),
            )),
        }
    }
}

impl LogenConfig {
    pub fn resolve_client_connect(
        &self,
        ov: ClientOverrides<'_>,
    ) -> Result<ClientConnect, LogenError> {
        if let Some(path) = ov.socket {
            return Ok(ClientConnect::Unix {
                socket: path.to_path_buf(),
            });
        }

        let host = ov
            .host
            .or(self.client.host.as_deref())
            .map(str::trim)
            .filter(|s| !s.is_empty());

        if let Some(host) = host {
            let port = ov
                .port
                .or(self.client.port)
                .unwrap_or(CONVENTIONAL_CLIENT_TCP_PORT);
            return Ok(ClientConnect::Tcp {
                host: host.to_string(),
                port,
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

        Ok(ClientConnect::Unix {
            socket: unix_socket,
        })
    }

    #[inline]
    pub fn resolve_client_auto_kafka_protocol(&self, ov: ClientOverrides<'_>) -> bool {
        ov.auto_kafka_protocol
            .unwrap_or(self.client.auto_kafka_protocol)
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
            "[logend].tmp-dir must be non-empty (directory for logend.sock, logend.pid, logend.log)"
                .into(),
        ));
    }
    cfg.logend.tmp_dir = t.to_string();

    let out = cfg.logend.worker_output_dir.trim();
    if out.is_empty() {
        return Err(LogenError::MergedInvalid(
            "[logend].worker-output-dir must be non-empty".into(),
        ));
    }
    if !Path::new(out).is_absolute() {
        return Err(LogenError::MergedInvalid(
            "[logend].worker-output-dir must be an absolute path".into(),
        ));
    }
    cfg.logend.worker_output_dir = out.to_string();

    Ok(cfg)
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU32, Ordering};

    use super::*;

    static TEMP_SEQ: AtomicU32 = AtomicU32::new(0);

    struct TempMerged {
        _dir: PathBuf,
        cfg: LogenConfig,
    }

    impl TempMerged {
        fn load(content: &str) -> Self {
            let dir = std::env::temp_dir().join(format!(
                "logen-config-{}-{}",
                std::process::id(),
                TEMP_SEQ.fetch_add(1, Ordering::Relaxed)
            ));
            std::fs::create_dir_all(&dir).expect("create temp dir");
            let path = dir.join("defaults.toml");
            std::fs::write(&path, content).expect("write defaults");
            let cfg = load_merged(Some(path.as_path())).expect("merged config");
            Self { _dir: dir, cfg }
        }

        fn load_err(content: &str) -> LogenError {
            let dir = std::env::temp_dir().join(format!(
                "logen-config-bad-{}-{}",
                std::process::id(),
                TEMP_SEQ.fetch_add(1, Ordering::Relaxed)
            ));
            std::fs::create_dir_all(&dir).expect("create temp dir");
            let path = dir.join("defaults.toml");
            std::fs::write(&path, content).expect("write defaults");
            let err = load_merged(Some(path.as_path())).expect_err("config should fail");
            let _ = std::fs::remove_file(path);
            let _ = std::fs::remove_dir(dir);
            err
        }
    }

    impl Drop for TempMerged {
        fn drop(&mut self) {
            let path = self._dir.join("defaults.toml");
            let _ = std::fs::remove_file(path);
            let _ = std::fs::remove_dir(&self._dir);
        }
    }

    /// 测试内容：内嵌默认 `[client].auto-kafka-protocol` 为 true。
    /// 输入：`load_merged(None)`。
    /// 预期：`auto_kafka_protocol == true`。
    #[test]
    fn client_auto_kafka_protocol_defaults_true() {
        let cfg = load_merged(None).expect("defaults");
        assert!(cfg.client.auto_kafka_protocol);
    }

    /// 测试内容：TOML 可显式关闭 `[client].auto-kafka-protocol`。
    /// 输入：临时 TOML `auto-kafka-protocol = false`。
    /// 预期：合并后为 false。
    #[test]
    fn client_auto_kafka_protocol_can_disable() {
        let t = TempMerged::load(
            r#"
[client]
auto-kafka-protocol = false
"#,
        );
        assert!(!t.cfg.client.auto_kafka_protocol);
    }

    /// 测试内容：旧下划线 TOML 键不再作为规范写法接受。
    /// 输入：临时 TOML `auto_kafka_protocol = false`。
    /// 预期：`load_merged` 返回 `MergedInvalid`。
    #[test]
    fn client_auto_kafka_protocol_underscore_key_rejected() {
        let err = TempMerged::load_err(
            r#"
[client]
auto_kafka_protocol = false
"#,
        );
        assert!(err.to_string().contains("unknown field"));
    }

    /// 测试内容：未配置 `[logend].runtime-threads` 时保持默认行为。
    /// 输入：仅加载内嵌 `conf.ref.toml`。
    /// 预期：`cfg.logend.runtime_threads` 为 `None`。
    #[test]
    fn load_merged_accepts_missing_runtime_threads() {
        let cfg = load_merged(None).expect("embedded defaults");
        assert_eq!(cfg.logend.runtime_threads, None);
    }

    /// 测试内容：用户 TOML 可显式覆盖 `[logend].runtime-threads`。
    /// 输入：临时文件仅含 `[logend].runtime-threads = 3`。
    /// 预期：合并后 `cfg.logend.runtime_threads == Some(3)`。
    #[test]
    fn load_merged_reads_explicit_runtime_threads() {
        let t = TempMerged::load(
            r#"
[logend]
runtime-threads = 3
"#,
        );
        assert_eq!(t.cfg.logend.runtime_threads, Some(3));
    }

    /// 测试内容：`[logend]` 旧下划线键不再接受。
    /// 输入：临时 TOML `worker_output_dir = "./output"`。
    /// 预期：`load_merged` 返回 `MergedInvalid`。
    #[test]
    fn logend_underscore_key_rejected() {
        let err = TempMerged::load_err(
            r#"
[logend]
worker_output_dir = "./output"
"#,
        );
        assert!(err.to_string().contains("unknown field"));
    }

    /// 测试内容：`[logend].worker-output-dir` 允许绝对路径。
    /// 输入：临时 TOML `worker-output-dir = "/var/tmp/logkit-output"`。
    /// 预期：合并后保留该绝对路径。
    #[test]
    fn logend_worker_output_dir_accepts_absolute_path() {
        let t = TempMerged::load(
            r#"
[logend]
worker-output-dir = "/var/tmp/logkit-output"
"#,
        );
        assert_eq!(t.cfg.logend.worker_output_dir, "/var/tmp/logkit-output");
    }

    /// 测试内容：`[logend].worker-output-dir` 拒绝相对路径。
    /// 输入：临时 TOML `worker-output-dir = "./output"`。
    /// 预期：`load_merged` 返回 `MergedInvalid` 且错误含 absolute path。
    #[test]
    fn logend_worker_output_dir_rejects_relative_path() {
        let err = TempMerged::load_err(
            r#"
[logend]
worker-output-dir = "./output"
"#,
        );
        assert!(err.to_string().contains("absolute path"), "{err}");
    }

    /// 测试内容：默认 `[client]` 解析为本地 Unix 套接字。
    /// 输入：内嵌默认配置，无 CLI 覆盖。
    /// 预期：解析为 Unix，套接字为 `{tmp_dir}/logend.sock`。
    #[test]
    fn resolve_client_connect_defaults_to_unix() {
        let cfg = load_merged(None).expect("defaults");
        let conn = cfg
            .resolve_client_connect(ClientOverrides::default())
            .expect("resolve");
        assert_eq!(conn.endpoint_uri(), LOCAL_GRPC_AUTHORITY_URI);
        match conn {
            ClientConnect::Unix { socket } => {
                assert_eq!(socket, PathBuf::from("/tmp/logend/logend.sock"));
            }
            _ => panic!("expected unix"),
        }
    }

    /// 测试内容：仅 `-H` / `[client].host` 未指定 port 时使用约定端口 11159。
    /// 输入：`ClientOverrides { host: Some("10.0.0.5"), .. }`。
    /// 预期：`endpoint_uri` 为 `http://10.0.0.5:11159/`。
    #[test]
    fn resolve_client_connect_default_port_with_host_only() {
        let cfg = load_merged(None).expect("defaults");
        let conn = cfg
            .resolve_client_connect(ClientOverrides {
                host: Some("10.0.0.5"),
                ..Default::default()
            })
            .expect("resolve");
        assert_eq!(
            conn.tcp_uri(),
            Some(format!("http://10.0.0.5:{CONVENTIONAL_CLIENT_TCP_PORT}/"))
        );
        assert!(matches!(conn, ClientConnect::Tcp { .. }));
    }

    /// 测试内容：`[client]` 显式 port 不被约定默认值覆盖。
    /// 输入：`host`、`port = 22222`。
    /// 预期：`endpoint_uri` 使用 22222 而非 11159。
    #[test]
    fn resolve_client_connect_honors_explicit_port() {
        let t = TempMerged::load(
            r#"
[client]
host = "10.0.0.5"
port = 22222
"#,
        );
        let conn = t
            .cfg
            .resolve_client_connect(ClientOverrides::default())
            .expect("resolve");
        assert_eq!(conn.tcp_uri(), Some("http://10.0.0.5:22222/".to_string()));
    }

    /// 测试内容：TOML 仅写 `host` 时使用约定端口。
    /// 输入：TOML `host`，无 `port`。
    /// 预期：`endpoint_uri` 端口为 `CONVENTIONAL_CLIENT_TCP_PORT`。
    #[test]
    fn resolve_client_connect_host_only_uses_default_port() {
        let t = TempMerged::load(
            r#"
[client]
host = "10.0.0.5"
"#,
        );
        let conn = t
            .cfg
            .resolve_client_connect(ClientOverrides::default())
            .expect("resolve");
        assert_eq!(
            conn.tcp_uri(),
            Some(format!("http://10.0.0.5:{CONVENTIONAL_CLIENT_TCP_PORT}/"))
        );
    }

    /// 测试内容：仅配置 `[client].port` 时仍走 UDS。
    /// 输入：内嵌默认 + `ClientOverrides { port: Some(11159), .. }`。
    /// 预期：保持 Unix，不进入 TCP。
    #[test]
    fn resolve_client_connect_port_alone_stays_unix() {
        let cfg = load_merged(None).expect("defaults");
        let conn = cfg
            .resolve_client_connect(ClientOverrides {
                port: Some(CONVENTIONAL_CLIENT_TCP_PORT),
                ..Default::default()
            })
            .expect("resolve");
        assert!(matches!(conn, ClientConnect::Unix { .. }));
    }

    /// 测试内容：`[logend].bind` 与 `port` 成对解析为 TCP 监听地址。
    /// 输入：`bind = "127.0.0.1"`、`port = CONVENTIONAL_CLIENT_TCP_PORT`。
    /// 预期：`tcp_listen_addr()` 为 `127.0.0.1:11159`。
    #[test]
    fn logend_tcp_listen_addr_from_bind_and_port() {
        let t = TempMerged::load(&format!(
            r#"
[logend]
bind = "127.0.0.1"
port = {CONVENTIONAL_CLIENT_TCP_PORT}
"#
        ));
        let addr = t
            .cfg
            .logend
            .tcp_listen_addr()
            .expect("parse")
            .expect("some addr");
        assert_eq!(
            addr,
            format!("127.0.0.1:{CONVENTIONAL_CLIENT_TCP_PORT}")
                .parse()
                .unwrap()
        );
    }

    /// 测试内容：仅配置 `[logend].port` 无 `bind` 时报错。
    /// 输入：临时 TOML 仅 `port = 11160`，并显式覆盖掉默认的 `bind`。
    /// 预期：`tcp_listen_addr()` 返回 `MergedInvalid`。
    #[test]
    fn logend_tcp_requires_bind_when_port_set() {
        let mut t = TempMerged::load(
            r#"
[logend]
port = 11160
"#,
        );
        t.cfg.logend.bind = None;
        let err = t.cfg.logend.tcp_listen_addr().unwrap_err();
        assert!(err.to_string().contains("bind required"));
    }

    /// 测试内容：CLI `-S` / `--socket` 强制 Unix 并覆盖 `[client]` TCP 设置。
    /// 输入：TCP 配置 + `ClientOverrides { socket: Some("/tmp/x.sock") }`。
    /// 预期：解析为 Unix，套接字为 `/tmp/x.sock`。
    #[test]
    fn resolve_client_connect_cli_socket_overrides_tcp() {
        let t = TempMerged::load(
            r#"
[client]
host = "10.0.0.5"
port = 22222
"#,
        );
        let sock = PathBuf::from("/tmp/x.sock");
        let conn = t
            .cfg
            .resolve_client_connect(ClientOverrides {
                socket: Some(sock.as_path()),
                ..Default::default()
            })
            .expect("resolve");
        match conn {
            ClientConnect::Unix { socket } => assert_eq!(socket, sock),
            _ => panic!("expected unix"),
        }
    }

    /// 测试内容：CLI 可显式关闭 Kafka 自动补全。
    /// 输入：内嵌默认 + `ClientOverrides { auto_kafka_protocol: Some(false) }`。
    /// 预期：命令行覆盖优先，结果为 false。
    #[test]
    fn resolve_client_auto_kafka_protocol_cli_false() {
        let cfg = load_merged(None).expect("defaults");
        assert!(!cfg.resolve_client_auto_kafka_protocol(ClientOverrides {
            auto_kafka_protocol: Some(false),
            ..Default::default()
        }));
    }

    /// 测试内容：CLI 可显式开启 Kafka 自动补全并覆盖 TOML。
    /// 输入：TOML `auto-kafka-protocol = false` + `ClientOverrides { auto_kafka_protocol: Some(true) }`。
    /// 预期：命令行覆盖优先，结果为 true。
    #[test]
    fn resolve_client_auto_kafka_protocol_cli_true_overrides_toml() {
        let t = TempMerged::load(
            r#"
[client]
auto-kafka-protocol = false
"#,
        );
        assert!(t.cfg.resolve_client_auto_kafka_protocol(ClientOverrides {
            auto_kafka_protocol: Some(true),
            ..Default::default()
        }));
    }
}
