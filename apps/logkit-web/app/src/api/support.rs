#[cfg(feature = "ssr")]
use std::sync::OnceLock;

#[cfg(feature = "ssr")]
use leptos::prelude::ServerFnError;
#[cfg(feature = "ssr")]
use logen_connection::Client;

#[cfg(feature = "ssr")]
use crate::model::{LogendConnection, WorkerSummary};

#[cfg(feature = "ssr")]
static CLIENT: OnceLock<Result<Client, String>> = OnceLock::new();

#[cfg(feature = "ssr")]
pub(crate) fn err(e: impl ToString) -> ServerFnError {
    ServerFnError::ServerError(e.to_string())
}

#[cfg(feature = "ssr")]
pub(crate) fn client() -> Result<Client, ServerFnError> {
    CLIENT
        .get_or_init(|| Client::open_default().map_err(|e| e.to_string()))
        .clone()
        .map_err(err)
}

#[cfg(feature = "ssr")]
pub(crate) fn map_connections(list: Vec<logen_connection::LogendConnection>) -> Vec<LogendConnection> {
    list.into_iter().map(LogendConnection::from).collect()
}

#[cfg(feature = "ssr")]
pub(crate) fn map_workers(list: Vec<logen_connection::WorkerSummary>) -> Vec<WorkerSummary> {
    list.into_iter().map(WorkerSummary::from).collect()
}
