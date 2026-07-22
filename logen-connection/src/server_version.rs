use logen_proto::version_support;
use serde::{Deserialize, Serialize};

/// logend Ping 返回的服务端版本。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LogendServerVersion {
    pub logend_version: String,
    pub logen_version: String,
}

impl Default for LogendServerVersion {
    fn default() -> Self {
        Self::unknown()
    }
}

impl LogendServerVersion {
    pub fn unknown() -> Self {
        Self {
            logend_version: String::new(),
            logen_version: String::new(),
        }
    }

    pub fn from_proto(version: Option<logen_proto::ServerVersion>) -> Self {
        match version {
            Some(v) => Self {
                logend_version: v.logend_version,
                logen_version: v.logen_version,
            },
            None => Self::unknown(),
        }
    }

    pub fn supports_file_sink(&self) -> bool {
        version_support::supports_file_sink(&self.logend_version)
    }

    /// 供 Console 展示：`logend 2.1.0` 或 `logend 2.1.0 / logen 2.1.0`。
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

#[cfg(test)]
mod tests {
    use super::*;

    /// 测试内容：从 proto 解析 ServerVersion 及 unknown 回退。
    /// 输入：Some(ServerVersion) 与 None。
    /// 预期：字段映射正确；None 时版本为空且不支持 file_sink。
    #[test]
    fn from_proto_and_unknown() {
        let parsed = LogendServerVersion::from_proto(Some(logen_proto::ServerVersion {
            logend_version: "2.1.0".into(),
            logen_version: "2.1.0".into(),
        }));
        assert_eq!(parsed.logend_version, "2.1.0");
        assert!(parsed.supports_file_sink());

        let unknown = LogendServerVersion::from_proto(None);
        assert!(unknown.logend_version.is_empty());
        assert!(!unknown.supports_file_sink());
    }
}
