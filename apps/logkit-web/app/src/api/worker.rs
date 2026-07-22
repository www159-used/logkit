use leptos::prelude::*;

use crate::model::{ConnectionId, LogendConnection, LogendServerVersion, StartWorkerResult, WorkerStartForm, WorkerSummary, WorkerSinkKind};

#[cfg(feature = "ssr")]
use crate::model::build_control_script;
#[cfg(feature = "ssr")]
use super::support::{client, err, map_workers};

#[server]
pub async fn load_connection_for_worker(
    connection_id: ConnectionId,
) -> Result<(LogendConnection, LogendServerVersion), ServerFnError> {
    #[cfg(feature = "ssr")]
    {
        let conn = client()?.get(connection_id.into()).map_err(err)?;
        let ping = client()?.ping(connection_id.into()).await.map_err(err)?;
        Ok((
            LogendConnection::from(conn),
            ping.server_version.into(),
        ))
    }
    #[cfg(not(feature = "ssr"))]
    {
        let _ = connection_id;
        Err(ServerFnError::ServerError("SSR only".into()))
    }
}

#[server]
pub async fn load_workers_page(
    connection_id: ConnectionId,
) -> Result<(LogendConnection, Vec<WorkerSummary>, LogendServerVersion), ServerFnError> {
    #[cfg(feature = "ssr")]
    {
        let (conn, workers, version) = client()?
            .workers_page(connection_id.into(), "")
            .await
            .map_err(err)?;
        Ok((LogendConnection::from(conn), map_workers(workers), version.into()))
    }
    #[cfg(not(feature = "ssr"))]
    {
        let _ = connection_id;
        Err(ServerFnError::ServerError("SSR only".into()))
    }
}

#[server]
pub async fn start_connection_worker(
    connection_id: ConnectionId,
    form: WorkerStartForm,
) -> Result<StartWorkerResult, ServerFnError> {
    #[cfg(feature = "ssr")]
    {
        if matches!(form.sink_kind, WorkerSinkKind::File { .. }) {
            let ping = client()?
                .ping(connection_id.into())
                .await
                .map_err(err)?;
            if !ping.server_version.supports_file_sink() {
                return Err(ServerFnError::ServerError(format!(
                    "当前 logend 版本过旧，不支持 file_sink。请升级 logend 至 {} 及以上（本仓库 cargo build -p logend --release）。",
                    crate::version_support::MIN_LOGEND_FILE_SINK
                )));
            }
        }
        let script = build_control_script(&form).map_err(err)?;
        let config_label = if form.label.trim().is_empty() {
            "console".into()
        } else {
            form.label.trim().to_string()
        };
        Ok(client()?
            .start_worker(connection_id.into(), &script, &config_label)
            .await
            .map_err(err)?
            .into())
    }
    #[cfg(not(feature = "ssr"))]
    {
        let _ = (connection_id, form);
        Err(ServerFnError::ServerError("SSR only".into()))
    }
}

#[server]
pub async fn load_worker_detail(
    connection_id: ConnectionId,
    worker_id: String,
) -> Result<(LogendConnection, WorkerSummary), ServerFnError> {
    #[cfg(feature = "ssr")]
    {
        let conn = client()?.get(connection_id.into()).map_err(err)?;
        let workers = client()?
            .stat_workers(connection_id.into(), &worker_id)
            .await
            .map_err(err)?;
        let worker = workers
            .into_iter()
            .find(|w| w.id == worker_id)
            .ok_or_else(|| err(format!("worker not found: {worker_id}")))?;
        Ok((LogendConnection::from(conn), WorkerSummary::from(worker)))
    }
    #[cfg(not(feature = "ssr"))]
    {
        let _ = (connection_id, worker_id);
        Err(ServerFnError::ServerError("SSR only".into()))
    }
}

#[server]
pub async fn stop_connection_worker(
    connection_id: ConnectionId,
    worker_id: String,
) -> Result<String, ServerFnError> {
    #[cfg(feature = "ssr")]
    {
        let status = client()?
            .stop_worker(connection_id.into(), &worker_id)
            .await
            .map_err(err)?;
        Ok(status)
    }
    #[cfg(not(feature = "ssr"))]
    {
        let _ = (connection_id, worker_id);
        Err(ServerFnError::ServerError("SSR only".into()))
    }
}
