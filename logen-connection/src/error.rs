use thiserror::Error;

#[derive(Debug, Error)]
pub enum ConnectionError {
    #[error("{0}")]
    Msg(String),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Sqlite(#[from] rusqlite::Error),
    #[error(transparent)]
    Config(#[from] logen_config::LogenError),
    #[error(transparent)]
    Grpc(#[from] tonic::Status),
}

impl ConnectionError {
    pub fn msg(s: impl Into<String>) -> Self {
        Self::Msg(s.into())
    }
}
