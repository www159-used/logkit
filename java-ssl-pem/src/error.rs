//! [`JavaSslPemError`]：Java 风格 TLS 材料 → PEM 路径过程中的失败原因。

use std::io;
use std::path::Path;

use thiserror::Error;

/// `java-ssl-pem` crate 的统一错误类型（不依赖 Kafka / rdkafka）。
#[derive(Debug, Error)]
pub enum JavaSslPemError {
    #[error("java-ssl-pem · trust · `{field}`: {detail}")]
    TrustField {
        field: &'static str,
        detail: String,
    },

    #[error("java-ssl-pem · trust · {label} · `{path}`: {detail}")]
    TrustPath {
        label: &'static str,
        path: String,
        detail: String,
    },

    #[error("java-ssl-pem · JKS · {role} · `{path}`: {detail}")]
    Jks {
        role: &'static str,
        path: String,
        detail: String,
    },

    #[error("java-ssl-pem · PKCS#12 · `{path}`: {detail}")]
    Pkcs12 { path: String, detail: String },

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

    #[error("java-ssl-pem · temp dir (`{label}`)")]
    TempDir {
        label: &'static str,
        #[source]
        source: io::Error,
    },

    #[error("java-ssl-pem · OpenSSL: {detail}")]
    OpenSsl { detail: String },

    /// 互斥字段同时出现、或 trust / identity 无法唯一解析时的配置错误。
    #[error("java-ssl-pem · config: {detail}")]
    Config { detail: String },
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
