use leptos::prelude::*;

use crate::model::{ConnectionId, LogendConnection, PingResult};

#[cfg(feature = "ssr")]
use logen_connection::LogendConnection as CoreConn;
#[cfg(feature = "ssr")]
use super::support::{client, err, map_connections};

#[server]
pub async fn get_connection(id: ConnectionId) -> Result<LogendConnection, ServerFnError> {
    #[cfg(feature = "ssr")]
    {
        Ok(client()?.get(id.into()).map_err(err)?.into())
    }
    #[cfg(not(feature = "ssr"))]
    {
        let _ = id;
        Err(ServerFnError::ServerError("SSR only".into()))
    }
}

#[server]
pub async fn list_connections() -> Result<Vec<LogendConnection>, ServerFnError> {
    #[cfg(feature = "ssr")]
    {
        Ok(map_connections(client()?.list().map_err(err)?))
    }
    #[cfg(not(feature = "ssr"))]
    {
        Err(ServerFnError::ServerError("SSR only".into()))
    }
}

#[server]
pub async fn save_connection(
    connection: LogendConnection,
) -> Result<LogendConnection, ServerFnError> {
    #[cfg(feature = "ssr")]
    {
        let core: CoreConn = connection.into();
        Ok(client()?.upsert(core).map_err(err)?.into())
    }
    #[cfg(not(feature = "ssr"))]
    {
        let _ = connection;
        Err(ServerFnError::ServerError("SSR only".into()))
    }
}

#[server]
pub async fn delete_connection(id: ConnectionId) -> Result<ConnectionId, ServerFnError> {
    #[cfg(feature = "ssr")]
    {
        Ok(client()?.delete(id.into()).map_err(err)?.into())
    }
    #[cfg(not(feature = "ssr"))]
    {
        let _ = id;
        Err(ServerFnError::ServerError("SSR only".into()))
    }
}

#[server]
pub async fn ping_saved_connection(id: ConnectionId) -> Result<PingResult, ServerFnError> {
    #[cfg(feature = "ssr")]
    {
        let r = client()?.ping(id.into()).await.map_err(err)?;
        Ok(r.into())
    }
    #[cfg(not(feature = "ssr"))]
    {
        let _ = id;
        Err(ServerFnError::ServerError("SSR only".into()))
    }
}

#[server]
pub async fn default_local_socket() -> Result<String, ServerFnError> {
    #[cfg(feature = "ssr")]
    {
        logen_connection::default_local_socket_display().map_err(err)
    }
    #[cfg(not(feature = "ssr"))]
    {
        Err(ServerFnError::ServerError("SSR only".into()))
    }
}

#[server]
pub async fn new_local_connection(name: String) -> Result<LogendConnection, ServerFnError> {
    #[cfg(feature = "ssr")]
    {
        Ok(LogendConnection::from(
            logen_connection::LogendConnection::new_local(name),
        ))
    }
    #[cfg(not(feature = "ssr"))]
    {
        let _ = name;
        Err(ServerFnError::ServerError("SSR only".into()))
    }
}

#[server]
pub async fn new_remote_connection(
    name: String,
    host: String,
    port: u16,
) -> Result<LogendConnection, ServerFnError> {
    #[cfg(feature = "ssr")]
    {
        Ok(LogendConnection::from(
            logen_connection::LogendConnection::new_remote(name, host, port),
        ))
    }
    #[cfg(not(feature = "ssr"))]
    {
        let _ = (name, host, port);
        Err(ServerFnError::ServerError("SSR only".into()))
    }
}
