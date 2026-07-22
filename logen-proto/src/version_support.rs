//! logend semver 能力边界（Console / logen-connection 共用）。

use semver::Version;

/// `file_sink()` 自该 logend 版本起可用。
pub const MIN_LOGEND_FILE_SINK: &str = "2.1.0";

fn parse_version(s: &str) -> Option<Version> {
    let s = s.trim();
    if s.is_empty() {
        return None;
    }
    Version::parse(s).ok()
}

fn min_logend_file_sink() -> Option<Version> {
    parse_version(MIN_LOGEND_FILE_SINK)
}

/// 是否支持 `file_sink()`（按 logend semver）。
pub fn supports_file_sink(logend_version: &str) -> bool {
    let Some(v) = parse_version(logend_version) else {
        return false;
    };
    let Some(min) = min_logend_file_sink() else {
        return false;
    };
    v >= min
}

#[cfg(test)]
mod tests {
    /// 测试内容：file_sink 能力按 logend semver 判断。
    /// 输入：空串、2.0.0、2.1.0、2.2.0。
    /// 预期：仅 logend >= 2.1.0 时 supports_file_sink 为 true。
    #[test]
    fn file_sink_semver_gate() {
        assert!(!super::supports_file_sink(""));
        assert!(!super::supports_file_sink("2.0.0"));
        assert!(super::supports_file_sink("2.1.0"));
        assert!(super::supports_file_sink("2.2.0"));
    }
}
