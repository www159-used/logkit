//! OEM 名解析：优先环境变量 **`OEM_NAME`**（trim、非空），否则回退 **`yotta`**。
//!
//! 具体安装路径（如 Kafka `client.conf`）由调用方用 [`oem_name()`] 自行拼接。

/// `OEM_NAME` 未设置、为空或仅空白时的默认 OEM。
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
