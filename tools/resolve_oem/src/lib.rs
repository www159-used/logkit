//! 内部工具，用来找 OEM。

pub const FALLBACK_OEM: &str = "yotta";

/// 读取 `OEM_NAME`，否则返回 [`FALLBACK_OEM`]。
pub fn oem_name() -> String {
    std::env::var("OEM_NAME")
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| FALLBACK_OEM.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fallback_oem_when_unset() {
        std::env::remove_var("OEM_NAME");
        assert_eq!(oem_name(), "yotta");
    }
}
