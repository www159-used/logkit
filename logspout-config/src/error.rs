//! Unified error type for config loading and related failures.

use std::path::PathBuf;

/// Errors produced by logspout-config and expected to be handled or displayed by binaries.
#[derive(Debug, thiserror::Error)]
pub enum LogspoutError {
    #[error("embedded asset missing: {0}")]
    EmbeddedMissing(String),

    #[error("utf-8 decode of embedded asset failed")]
    Utf8(#[from] std::str::Utf8Error),

    #[error("failed to read {path}: {source}")]
    ReadFile {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    #[error("failed to write {path}: {source}")]
    WriteFile {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    #[error("unix domain path {path}: {source}")]
    UnixIo {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    #[error("connect to unix socket {path} failed: {source}")]
    SocketConnect {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    #[error("socket io on {path}: {source}")]
    SocketIo {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    #[error("toml deserialize: {0}")]
    TomlDeserialize(#[from] toml::de::Error),

    #[error("toml serialize: {0}")]
    TomlSerialize(#[from] toml::ser::Error),

    #[error("merged config document invalid: {0}")]
    MergedInvalid(String),

    #[error("cli: {0}")]
    Cli(String),

    #[error("grpc: {0}")]
    Grpc(String),
}

impl LogspoutError {
    pub fn read_file(path: impl Into<PathBuf>, source: std::io::Error) -> Self {
        Self::ReadFile {
            path: path.into(),
            source,
        }
    }

    pub fn write_file(path: impl Into<PathBuf>, source: std::io::Error) -> Self {
        Self::WriteFile {
            path: path.into(),
            source,
        }
    }

    pub fn unix_io(path: impl Into<PathBuf>, source: std::io::Error) -> Self {
        Self::UnixIo {
            path: path.into(),
            source,
        }
    }

    pub fn socket_connect(path: impl Into<PathBuf>, source: std::io::Error) -> Self {
        Self::SocketConnect {
            path: path.into(),
            source,
        }
    }

    pub fn socket_io(path: impl Into<PathBuf>, source: std::io::Error) -> Self {
        Self::SocketIo {
            path: path.into(),
            source,
        }
    }
}
