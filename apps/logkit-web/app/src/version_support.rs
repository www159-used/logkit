//! logend semver 能力边界（WASM hydrate 与 SSR 共用，不依赖 logen-proto/tonic）。

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
