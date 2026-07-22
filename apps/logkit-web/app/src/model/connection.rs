use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};

use super::LogendServerVersion;

/// 连接 stable 标识（Copy；JSON / URL 仍为 UUID 字符串）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ConnectionId(uuid::Uuid);

impl ConnectionId {
    pub fn parse(s: &str) -> Option<Self> {
        uuid::Uuid::parse_str(s).ok().map(Self)
    }
}

impl fmt::Display for ConnectionId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl FromStr for ConnectionId {
    type Err = uuid::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        uuid::Uuid::parse_str(s).map(Self)
    }
}

#[cfg(feature = "ssr")]
impl From<logen_connection::ConnectionId> for ConnectionId {
    fn from(id: logen_connection::ConnectionId) -> Self {
        Self(id.into_uuid())
    }
}

#[cfg(feature = "ssr")]
impl From<ConnectionId> for logen_connection::ConnectionId {
    fn from(id: ConnectionId) -> Self {
        logen_connection::ConnectionId::from(id.0)
    }
}

/// 与 [`logen_config::CONVENTIONAL_CLIENT_TCP_PORT`] 一致，供 UI 默认值展示。
pub const DEFAULT_LOGEND_PORT: u16 = 11159;

/// 与 [`logen_connection::LogendConnection`] JSON 对齐，供 UI / server fn 共用。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LogendConnection {
    pub id: ConnectionId,
    pub name: String,
    pub kind: ConnectionKind,
    #[serde(default)]
    pub socket: String,
    #[serde(default)]
    pub host: String,
    #[serde(default)]
    pub port: u16,
    #[serde(default)]
    pub defaults_file: String,
    pub auto_kafka_protocol: Option<bool>,
    #[serde(default)]
    pub notes: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConnectionKind {
    Local,
    Remote,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PingResult {
    pub ok: bool,
    pub pong: String,
    pub endpoint: String,
    pub server_version: LogendServerVersion,
}

impl LogendConnection {
    /// 供卡片/标题展示；本机空 socket 仍显示 `$HOME/.logkit/logend.sock` 占位。
    pub fn endpoint_display(&self) -> String {
        match self.kind {
            ConnectionKind::Local => {
                if self.socket.trim().is_empty() {
                    "unix://$HOME/.logkit/logend.sock".into()
                } else {
                    format!("unix://{}", self.socket.trim())
                }
            }
            ConnectionKind::Remote => {
                let port = if self.port == 0 {
                    DEFAULT_LOGEND_PORT
                } else {
                    self.port
                };
                format!("tcp://{}:{port}", self.host.trim())
            }
        }
    }
}

/// 按名称排序插入或覆盖连接。
pub fn upsert_sorted(list: &mut Vec<LogendConnection>, conn: LogendConnection) {
    if let Some(idx) = list.iter().position(|c| c.id == conn.id) {
        list[idx] = conn;
    } else {
        list.push(conn);
    }
    list.sort_by_key(|c| c.name.to_lowercase());
}

#[cfg(feature = "ssr")]
impl From<logen_connection::LogendConnection> for LogendConnection {
    fn from(c: logen_connection::LogendConnection) -> Self {
        Self {
            id: c.id.into(),
            name: c.name,
            kind: match c.kind {
                logen_connection::ConnectionKind::Local => ConnectionKind::Local,
                logen_connection::ConnectionKind::Remote => ConnectionKind::Remote,
            },
            socket: c.socket,
            host: c.host,
            port: c.port,
            defaults_file: c.defaults_file,
            auto_kafka_protocol: c.auto_kafka_protocol,
            notes: c.notes,
        }
    }
}

#[cfg(feature = "ssr")]
impl From<LogendConnection> for logen_connection::LogendConnection {
    fn from(c: LogendConnection) -> Self {
        Self {
            id: c.id.into(),
            name: c.name,
            kind: match c.kind {
                ConnectionKind::Local => logen_connection::ConnectionKind::Local,
                ConnectionKind::Remote => logen_connection::ConnectionKind::Remote,
            },
            socket: c.socket,
            host: c.host,
            port: c.port,
            defaults_file: c.defaults_file,
            auto_kafka_protocol: c.auto_kafka_protocol,
            notes: c.notes,
        }
    }
}

#[cfg(feature = "ssr")]
impl From<logen_connection::PingResult> for PingResult {
    fn from(r: logen_connection::PingResult) -> Self {
        Self {
            ok: r.ok,
            pong: r.pong,
            endpoint: r.endpoint,
            server_version: r.server_version.into(),
        }
    }
}
