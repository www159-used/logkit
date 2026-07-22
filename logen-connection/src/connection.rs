use std::path::PathBuf;

use logen_config::{resolve_logkit_home, CONVENTIONAL_CLIENT_TCP_PORT};
use serde::{Deserialize, Serialize};

use crate::connection_id::ConnectionId;
use crate::error::ConnectionError;

/// 连接类型：本机 Unix 套接字或远端 TCP。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConnectionKind {
    Local,
    Remote,
}

/// Console 持久化的 logend 连接配置（不由 logend 管理）。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LogendConnection {
    pub id: ConnectionId,
    pub name: String,
    pub kind: ConnectionKind,
    /// 本机 UDS；空串表示 `$HOME/.logkit/logend.sock`。
    #[serde(default)]
    pub socket: String,
    #[serde(default)]
    pub host: String,
    #[serde(default)]
    pub port: u16,
    #[serde(default)]
    pub defaults_file: String,
    #[serde(default)]
    pub auto_kafka_protocol: Option<bool>,
    #[serde(default)]
    pub notes: String,
}

impl LogendConnection {
    pub fn new_local(name: impl Into<String>) -> Self {
        Self {
            id: ConnectionId::new_v4(),
            name: name.into(),
            kind: ConnectionKind::Local,
            socket: String::new(),
            host: String::new(),
            port: CONVENTIONAL_CLIENT_TCP_PORT,
            defaults_file: String::new(),
            auto_kafka_protocol: None,
            notes: String::new(),
        }
    }

    pub fn new_remote(name: impl Into<String>, host: impl Into<String>, port: u16) -> Self {
        Self {
            id: ConnectionId::new_v4(),
            name: name.into(),
            kind: ConnectionKind::Remote,
            socket: String::new(),
            host: host.into(),
            port: if port == 0 {
                CONVENTIONAL_CLIENT_TCP_PORT
            } else {
                port
            },
            defaults_file: String::new(),
            auto_kafka_protocol: None,
            notes: String::new(),
        }
    }

    /// 解析为可连接的端点描述与 [`logen_config::ClientConnect`] 参数。
    pub fn resolve(&self) -> Result<ResolvedEndpoint, ConnectionError> {
        match self.kind {
            ConnectionKind::Local => {
                let socket = if self.socket.trim().is_empty() {
                    resolve_logkit_home(None)?.join("logend.sock")
                } else {
                    PathBuf::from(self.socket.trim())
                };
                Ok(ResolvedEndpoint {
                    display: format!("unix://{}", socket.display()),
                    connect: logen_config::ClientConnect::Unix { socket },
                })
            }
            ConnectionKind::Remote => {
                let host = self.host.trim();
                if host.is_empty() {
                    return Err(ConnectionError::msg("remote connection: host is required"));
                }
                let port = if self.port == 0 {
                    CONVENTIONAL_CLIENT_TCP_PORT
                } else {
                    self.port
                };
                Ok(ResolvedEndpoint {
                    display: format!("tcp://{host}:{port}"),
                    connect: logen_config::ClientConnect::Tcp {
                        host: host.to_string(),
                        port,
                    },
                })
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct ResolvedEndpoint {
    pub display: String,
    pub connect: logen_config::ClientConnect,
}

/// 默认本机套接字路径（供表单 placeholder）。
pub fn default_local_socket_display() -> Result<String, ConnectionError> {
    Ok(resolve_logkit_home(None)?
        .join("logend.sock")
        .display()
        .to_string())
}
