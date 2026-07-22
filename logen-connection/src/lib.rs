//! Console 侧 logend 连接 client（CS 架构）。
//!
//! 连接配置的 CRUD 在本地 SQLite（`$HOME/.logkit/logen-connection.db`），不由 logend 管理；
//! Ping 与 Worker 操作经 gRPC 代理到 logend。

mod client;
mod connection;
mod connection_id;
mod error;
mod server_version;

pub use client::{Client, PingResult, StartWorkerResult, WorkerSummary};
pub use connection::{
    default_local_socket_display, ConnectionKind, LogendConnection, ResolvedEndpoint,
};
pub use connection_id::ConnectionId;
pub use error::ConnectionError;
pub use server_version::LogendServerVersion;
