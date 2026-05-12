//! `sink.max-size`：整数字节，或人类可读字符串（如 `64KiB`、`10MiB`，底数为 1024）。

use std::fmt;

use serde::{
    de::{Error, Visitor},
    Deserializer,
};

/// 解析 `max-size`：`65536`、`64KiB`、`1.5 MiB` 等。单位不区分大小写，乘数为 1024 的幂（与 KiB/MiB 一致）。
pub fn parse_human_size_bytes(s: &str) -> Result<u64, String> {
    let (num_str, unit) = split_number_and_unit(s.trim())?;
    let n: f64 = num_str
        .parse()
        .map_err(|_| format!("max-size: cannot parse number in {s:?}"))?;
    if n < 0.0 || !n.is_finite() {
        return Err("max-size: must be a non-negative finite number".into());
    }
    let mult = unit_multiplier(&unit)?;
    let bytes_f = n * mult as f64;
    if !bytes_f.is_finite() {
        return Err("max-size: numeric overflow".into());
    }
    let rounded = bytes_f.round();
    if rounded > u64::MAX as f64 {
        return Err("max-size: exceeds u64".into());
    }
    Ok(rounded as u64)
}

fn split_number_and_unit(s: &str) -> Result<(&str, String), String> {
    if s.is_empty() {
        return Err("max-size: empty string".into());
    }
    let mut i = s.len();
    while i > 0 {
        let b = s.as_bytes()[i - 1];
        if b.is_ascii_alphabetic() || b.is_ascii_whitespace() {
            i -= 1;
        } else {
            break;
        }
    }
    let (num_part, unit_raw) = s.split_at(i);
    let num = num_part.trim();
    let unit = unit_raw.trim().to_ascii_lowercase();
    if num.is_empty() {
        return Err(format!("max-size: missing numeric part in {s:?}"));
    }
    Ok((num, unit))
}

fn unit_multiplier(unit: &str) -> Result<u128, String> {
    match unit {
        "" | "b" => Ok(1),
        "k" | "kb" | "ki" | "kib" => Ok(1024),
        "m" | "mb" | "mi" | "mib" => Ok(1024u128.pow(2)),
        "g" | "gb" | "gi" | "gib" => Ok(1024u128.pow(3)),
        "t" | "tb" | "ti" | "tib" => Ok(1024u128.pow(4)),
        "p" | "pb" | "pi" | "pib" => Ok(1024u128.pow(5)),
        _ => Err(format!(
            "max-size: unknown unit {unit:?} (allowed: b, KiB/MiB/GiB… or k/m/g/t/p aliases)"
        )),
    }
}

pub fn deserialize_max_size<'de, D>(deserializer: D) -> Result<u64, D::Error>
where
    D: Deserializer<'de>,
{
    struct V;
    impl<'de> Visitor<'de> for V {
        type Value = u64;

        fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
            write!(
                f,
                "integer byte count or human-readable size, e.g. 65536, 64KiB, 10MiB"
            )
        }

        fn visit_u64<E: Error>(self, v: u64) -> Result<u64, E> {
            Ok(v)
        }

        fn visit_u128<E: Error>(self, v: u128) -> Result<u64, E> {
            u64::try_from(v).map_err(|_| E::custom("max-size: exceeds u64"))
        }

        fn visit_i64<E: Error>(self, v: i64) -> Result<u64, E> {
            u64::try_from(v).map_err(|_| E::custom("max-size: must be non-negative"))
        }

        fn visit_i128<E: Error>(self, v: i128) -> Result<u64, E> {
            u64::try_from(v).map_err(|_| E::custom("max-size: must be non-negative"))
        }

        fn visit_f64<E: Error>(self, v: f64) -> Result<u64, E> {
            if !v.is_finite() || v < 0.0 || v > u64::MAX as f64 {
                return Err(E::custom("max-size: invalid floating-point value"));
            }
            Ok(v.round() as u64)
        }

        fn visit_str<E: Error>(self, s: &str) -> Result<u64, E> {
            parse_human_size_bytes(s).map_err(E::custom)
        }

        fn visit_string<E: Error>(self, s: String) -> Result<u64, E> {
            parse_human_size_bytes(&s).map_err(E::custom)
        }
    }

    deserializer.deserialize_any(V)
}

pub fn deserialize_opt_max_size<'de, D>(deserializer: D) -> Result<Option<u64>, D::Error>
where
    D: Deserializer<'de>,
{
    struct V;
    impl<'de> Visitor<'de> for V {
        type Value = Option<u64>;

        fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
            write!(f, "optional integer byte count or human-readable size")
        }

        fn visit_none<E>(self) -> Result<Self::Value, E> {
            Ok(None)
        }

        fn visit_unit<E>(self) -> Result<Self::Value, E> {
            Ok(None)
        }

        fn visit_some<D2>(self, deserializer: D2) -> Result<Self::Value, D2::Error>
        where
            D2: Deserializer<'de>,
        {
            deserialize_max_size(deserializer).map(Some)
        }

        fn visit_u64<E: Error>(self, v: u64) -> Result<Self::Value, E> {
            Ok(Some(v))
        }

        fn visit_u128<E: Error>(self, v: u128) -> Result<Self::Value, E> {
            u64::try_from(v)
                .map(Some)
                .map_err(|_| E::custom("max-size: exceeds u64"))
        }

        fn visit_i64<E: Error>(self, v: i64) -> Result<Self::Value, E> {
            u64::try_from(v)
                .map(Some)
                .map_err(|_| E::custom("max-size: must be non-negative"))
        }

        fn visit_i128<E: Error>(self, v: i128) -> Result<Self::Value, E> {
            u64::try_from(v)
                .map(Some)
                .map_err(|_| E::custom("max-size: must be non-negative"))
        }

        fn visit_f64<E: Error>(self, v: f64) -> Result<Self::Value, E> {
            if !v.is_finite() || v < 0.0 || v > u64::MAX as f64 {
                return Err(E::custom("max-size: invalid floating-point value"));
            }
            Ok(Some(v.round() as u64))
        }

        fn visit_str<E: Error>(self, s: &str) -> Result<Self::Value, E> {
            parse_human_size_bytes(s).map(Some).map_err(E::custom)
        }

        fn visit_string<E: Error>(self, s: String) -> Result<Self::Value, E> {
            parse_human_size_bytes(&s).map(Some).map_err(E::custom)
        }
    }

    deserializer.deserialize_any(V)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 测试内容：纯数字字符串按字节解析。
    /// 输入：`65536`、`0`。
    /// 预期：分别得到对应无符号整数。
    #[test]
    fn plain_integer_bytes() {
        assert_eq!(parse_human_size_bytes("65536").unwrap(), 65536);
        assert_eq!(parse_human_size_bytes("0").unwrap(), 0);
    }

    /// 测试内容：KiB/MiB 及常见别名、空格、`mb` 十进制兆字节写法。
    /// 输入：`64KiB`、`64kib`、`1 MiB`、`10mb`。
    /// 预期：与二进制/十进制约定一致的换算结果。
    #[test]
    fn kib_mib() {
        assert_eq!(parse_human_size_bytes("64KiB").unwrap(), 65536);
        assert_eq!(parse_human_size_bytes("64kib").unwrap(), 65536);
        assert_eq!(parse_human_size_bytes("1 MiB").unwrap(), 1024 * 1024);
        assert_eq!(parse_human_size_bytes("10mb").unwrap(), 10 * 1024 * 1024);
    }

    /// 测试内容：带小数的人类可读大小按 MiB 换算并四舍五入为整数字节。
    /// 输入：`1.5MiB`。
    /// 预期：等于 `round(1.5 * 1048576)`。
    #[test]
    fn fractional() {
        assert_eq!(
            parse_human_size_bytes("1.5MiB").unwrap(),
            (1.5_f64 * 1048576_f64).round() as u64
        );
    }

    /// 测试内容：无法识别的单位返回错误。
    /// 输入：`12wb`。
    /// 预期：`parse_human_size_bytes` 返回 `Err`。
    #[test]
    fn invalid_unit() {
        assert!(parse_human_size_bytes("12wb").is_err());
    }
}
