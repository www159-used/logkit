use crate::version_support;
use serde::{Deserialize, Serialize};

/// logend Ping 返回的服务端版本（与 [`logen_connection::LogendServerVersion`] 字段对齐）。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LogendServerVersion {
    pub logend_version: String,
    pub logen_version: String,
}

impl Default for LogendServerVersion {
    fn default() -> Self {
        Self {
            logend_version: String::new(),
            logen_version: String::new(),
        }
    }
}

impl LogendServerVersion {
    pub fn supports_file_sink(&self) -> bool {
        version_support::supports_file_sink(&self.logend_version)
    }

    pub fn display_short(&self) -> String {
        if self.logend_version.is_empty() {
            "logend 版本未知".into()
        } else if self.logen_version.is_empty() || self.logen_version == self.logend_version {
            format!("logend {}", self.logend_version)
        } else {
            format!(
                "logend {} / logen {}",
                self.logend_version, self.logen_version
            )
        }
    }
}

#[cfg(feature = "ssr")]
impl From<logen_connection::LogendServerVersion> for LogendServerVersion {
    fn from(v: logen_connection::LogendServerVersion) -> Self {
        Self {
            logend_version: v.logend_version,
            logen_version: v.logen_version,
        }
    }
}
