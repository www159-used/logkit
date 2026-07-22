use logen_proto::PingRequest;
use serde::{Deserialize, Serialize};

use crate::connection::LogendConnection;
use crate::error::ConnectionError;
use crate::server_version::LogendServerVersion;

use super::logend;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PingResult {
    pub ok: bool,
    pub pong: String,
    pub endpoint: String,
    pub server_version: LogendServerVersion,
}

pub(super) async fn ping_connection(conn: &LogendConnection) -> Result<PingResult, ConnectionError> {
    let resolved = conn.resolve()?;
    let mut client = logend::logen_client(&resolved.connect).await?;
    let reply = client
        .ping(PingRequest {})
        .await
        .map_err(|s| ConnectionError::msg(format!("ping failed: {s}")))?;
    let inner = reply.into_inner();
    Ok(PingResult {
        ok: true,
        pong: inner.pong,
        endpoint: resolved.display,
        server_version: LogendServerVersion::from_proto(inner.version),
    })
}
