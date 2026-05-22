//! `min-interval`：经 [`humantime`] 解析为 [`Duration`]；`0ms` 表示不限速。

use std::time::Duration;

use humantime::{format_duration, parse_duration};
use serde::{
    de::{Error, Visitor},
    ser::Serializer,
    Deserializer,
};

/// 用 [`humantime::parse_duration`] 解析 `min-interval` 字符串。
pub fn parse_min_interval(s: &str) -> Result<Duration, String> {
    parse_duration(s.trim()).map_err(|e| format!("min-interval: {e}"))
}

pub fn deserialize_min_interval<'de, D>(deserializer: D) -> Result<Duration, D::Error>
where
    D: Deserializer<'de>,
{
    struct V;
    impl<'de> Visitor<'de> for V {
        type Value = Duration;

        fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
            write!(f, "duration string with unit, e.g. 100ms, 1s, 2m, 0ms")
        }

        fn visit_str<E: Error>(self, s: &str) -> Result<Duration, E> {
            parse_min_interval(s).map_err(E::custom)
        }

        fn visit_string<E: Error>(self, s: String) -> Result<Duration, E> {
            parse_min_interval(&s).map_err(E::custom)
        }
    }

    deserializer.deserialize_str(V)
}

pub fn serialize_min_interval<S>(value: &Duration, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    serializer.serialize_str(&format_duration(*value).to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 测试内容：无单位纯数字应拒绝。
    /// 输入：`"1000"`、`"1"`。
    /// 预期：`parse_min_interval` 返回 `Err`。
    #[test]
    fn bare_number_requires_unit() {
        assert!(parse_min_interval("1000").is_err());
        assert!(parse_min_interval("1").is_err());
    }

    /// 测试内容：humantime 解析常见单位。
    /// 输入：`100ms`、`1s`、`2m`、`1h`。
    /// 预期：与对应 `Duration` 一致。
    #[test]
    fn unit_suffixes() {
        assert_eq!(parse_min_interval("100ms").unwrap(), Duration::from_millis(100));
        assert_eq!(parse_min_interval("1s").unwrap(), Duration::from_secs(1));
        assert_eq!(
            parse_min_interval("2m").unwrap(),
            Duration::from_secs(120)
        );
        assert_eq!(
            parse_min_interval("1h").unwrap(),
            Duration::from_secs(3600)
        );
    }

    /// 测试内容：小数字符串可带小数秒。
    /// 输入：`1.5s`。
    /// 预期：1.5 秒。
    #[test]
    fn fractional_seconds() {
        assert_eq!(
            parse_min_interval("1.5s").unwrap(),
            Duration::from_millis(1500)
        );
    }

    /// 测试内容：亚毫秒精度保留。
    /// 输入：`500us`。
    /// 预期：500 微秒。
    #[test]
    fn sub_millisecond_preserved() {
        assert_eq!(
            parse_min_interval("500us").unwrap(),
            Duration::from_micros(500)
        );
    }

    /// 测试内容：零值表示不限速（humantime 接受 `0`、`0ms`、`0s`）。
    /// 输入：`0`、`0ms`、`0s`。
    /// 预期：均为 `Duration::ZERO`。
    #[test]
    fn zero_means_unlimited() {
        assert_eq!(parse_min_interval("0").unwrap(), Duration::ZERO);
        assert_eq!(parse_min_interval("0ms").unwrap(), Duration::ZERO);
        assert_eq!(parse_min_interval("0s").unwrap(), Duration::ZERO);
    }

    /// 测试内容：humantime 不认识的单位应拒绝。
    /// 输入：`12fortnight`。
    /// 预期：`Err`。
    #[test]
    fn invalid_unit() {
        assert!(parse_min_interval("12fortnight").is_err());
    }

    /// 测试内容：humantime 支持周单位。
    /// 输入：`1w`。
    /// 预期：7 天。
    #[test]
    fn week_unit() {
        assert_eq!(
            parse_min_interval("1w").unwrap(),
            Duration::from_secs(7 * 24 * 3600)
        );
    }
}
