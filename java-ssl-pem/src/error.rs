//! [`JavaSslPemError`]：JKS → PEM 过程中的失败原因。

use std::io;
use std::path::Path;

use thiserror::Error;

/// `java-ssl-pem` crate 的统一错误类型（不依赖 Kafka / rdkafka）。
#[derive(Debug, Error)]
pub enum JavaSslPemError {
    #[error("java-ssl-pem · JKS · {role} · `{path}`: {detail}")]
    Jks {
        role: &'static str,
        path: String,
        detail: String,
    },

    #[error("java-ssl-pem · mTLS: {detail}")]
    ClientIdentity { detail: String },

    #[error("java-ssl-pem · X.509 / PEM: {detail}")]
    CertEncoding { detail: String },

    #[error("java-ssl-pem · I/O · {operation} · `{path}`")]
    Io {
        operation: &'static str,
        path: String,
        #[source]
        source: io::Error,
    },
}

impl JavaSslPemError {
    pub(crate) fn io(path: &Path, operation: &'static str, source: io::Error) -> Self {
        Self::Io {
            operation,
            path: path.display().to_string(),
            source,
        }
    }

    pub(crate) fn jks(role: &'static str, path: &Path, detail: impl Into<String>) -> Self {
        Self::Jks {
            role,
            path: path.display().to_string(),
            detail: detail.into(),
        }
    }
}
