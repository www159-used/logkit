mod logend;
mod ping;
mod store;
mod worker;

pub use ping::PingResult;
pub use worker::{StartWorkerResult, WorkerSummary};

use crate::connection::LogendConnection;
use crate::connection_id::ConnectionId;
use crate::error::ConnectionError;
use crate::server_version::LogendServerVersion;

/// Console 侧 logend 连接 client。
///
/// - 连接 CRUD：本地 SQLite，不由 logend 管理。
/// - Ping / Worker：经 gRPC 代理到 logend。
#[derive(Debug, Clone)]
pub struct Client {
    store: store::Store,
}

impl Client {
    pub fn open_default() -> Result<Self, ConnectionError> {
        Ok(Self {
            store: store::Store::open_default()?,
        })
    }

    pub fn list(&self) -> Result<Vec<LogendConnection>, ConnectionError> {
        self.store.load()
    }

    pub fn get(&self, id: ConnectionId) -> Result<LogendConnection, ConnectionError> {
        self.store.get(id)
    }

    pub fn upsert(&self, connection: LogendConnection) -> Result<LogendConnection, ConnectionError> {
        self.store.upsert(connection)
    }

    pub fn delete(&self, id: ConnectionId) -> Result<ConnectionId, ConnectionError> {
        self.store.delete(id)
    }

    pub async fn ping(&self, id: ConnectionId) -> Result<PingResult, ConnectionError> {
        let conn = self.store.get(id)?;
        ping::ping_connection(&conn).await
    }

    pub async fn workers_page(
        &self,
        id: ConnectionId,
        id_prefix: &str,
    ) -> Result<(LogendConnection, Vec<WorkerSummary>, LogendServerVersion), ConnectionError> {
        let conn = self.store.get(id)?;
        let (ping, workers) = tokio::join!(
            ping::ping_connection(&conn),
            worker::stat_workers(&conn, id_prefix),
        );
        Ok((conn, workers?, ping?.server_version))
    }

    pub async fn stat_workers(
        &self,
        id: ConnectionId,
        id_prefix: &str,
    ) -> Result<Vec<WorkerSummary>, ConnectionError> {
        let conn = self.store.get(id)?;
        worker::stat_workers(&conn, id_prefix).await
    }

    pub async fn stop_worker(
        &self,
        id: ConnectionId,
        worker_id: &str,
    ) -> Result<String, ConnectionError> {
        let conn = self.store.get(id)?;
        worker::stop_worker(&conn, worker_id).await
    }

    pub async fn start_worker(
        &self,
        id: ConnectionId,
        script: &str,
        config_label: &str,
    ) -> Result<StartWorkerResult, ConnectionError> {
        let conn = self.store.get(id)?;
        worker::run_control_script(&conn, script, config_label).await
    }
}
