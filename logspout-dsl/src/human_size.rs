//! `sink.max-size`：整数字节，或人类可读字符串（如 `64KiB`、`10MiB`，底数为 1024）。

use std::fmt;

use serde::{Deserializer, de::{Error, Visitor}};

/// 解析 `max-size`：`65536`、`64KiB`、`1.5 MiB` 等。单位不区分大小写，乘数为 1024 的幂（与 KiB/MiB 一致）。
pub fn parse_human_size_bytes(s: &str) -> Result<u64, String> {
    let (num_str, unit) = split_number_and_unit(s.trim())?;
    let n: f64 = num_str
        .parse()
        .map_err(|_| format!("max-size: 无法解析数字: {s:?}"))?;
    if n < 0.0 || !n.is_finite() {
        return Err("max-size: 须为非负有限数".into());
    }
    let mult = unit_multiplier(&unit)?;
    let bytes_f = n * mult as f64;
    if !bytes_f.is_finite() {
        return Err("max-size: 计算溢出".into());
    }
    let rounded = bytes_f.round();
    if rounded > u64::MAX as f64 {
        return Err("max-size: 超出 u64".into());
    }
    Ok(rounded as u64)
}

fn split_number_and_unit(s: &str) -> Result<(&str, String), String> {
    if s.is_empty() {
        return Err("max-size: 空字符串".into());
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
        return Err(format!("max-size: 缺少数值: {s:?}"));
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
            "max-size: 未知单位 {unit:?}（可用 b、KiB/MiB/GiB… 或 k/m/g/t/p 与常见别名）"
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
            write!(f, "整数（字节）或人类可读大小，如 65536、64KiB、10MiB")
        }

        fn visit_u64<E: Error>(self, v: u64) -> Result<u64, E> {
            Ok(v)
        }

        fn visit_u128<E: Error>(self, v: u128) -> Result<u64, E> {
            u64::try_from(v).map_err(|_| E::custom("max-size: 超出 u64"))
        }

        fn visit_i64<E: Error>(self, v: i64) -> Result<u64, E> {
            u64::try_from(v).map_err(|_| E::custom("max-size: 须为非负数"))
        }

        fn visit_i128<E: Error>(self, v: i128) -> Result<u64, E> {
            u64::try_from(v).map_err(|_| E::custom("max-size: 须为非负数"))
        }

        fn visit_f64<E: Error>(self, v: f64) -> Result<u64, E> {
            if !v.is_finite() || v < 0.0 || v > u64::MAX as f64 {
                return Err(E::custom("max-size: 无效的浮点数"));
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
            write!(f, "可选的整数（字节）或人类可读大小")
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
                .map_err(|_| E::custom("max-size: 超出 u64"))
        }

        fn visit_i64<E: Error>(self, v: i64) -> Result<Self::Value, E> {
            u64::try_from(v)
                .map(Some)
                .map_err(|_| E::custom("max-size: 须为非负数"))
        }

        fn visit_i128<E: Error>(self, v: i128) -> Result<Self::Value, E> {
            u64::try_from(v)
                .map(Some)
                .map_err(|_| E::custom("max-size: 须为非负数"))
        }

        fn visit_f64<E: Error>(self, v: f64) -> Result<Self::Value, E> {
            if !v.is_finite() || v < 0.0 || v > u64::MAX as f64 {
                return Err(E::custom("max-size: 无效的浮点数"));
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

    #[test]
    fn plain_integer_bytes() {
        assert_eq!(parse_human_size_bytes("65536").unwrap(), 65536);
        assert_eq!(parse_human_size_bytes("0").unwrap(), 0);
    }

    #[test]
    fn kib_mib() {
        assert_eq!(parse_human_size_bytes("64KiB").unwrap(), 65536);
        assert_eq!(parse_human_size_bytes("64kib").unwrap(), 65536);
        assert_eq!(parse_human_size_bytes("1 MiB").unwrap(), 1024 * 1024);
        assert_eq!(parse_human_size_bytes("10mb").unwrap(), 10 * 1024 * 1024);
    }

    #[test]
    fn fractional() {
        assert_eq!(
            parse_human_size_bytes("1.5MiB").unwrap(),
            (1.5_f64 * 1048576_f64).round() as u64
        );
    }

    #[test]
    fn invalid_unit() {
        assert!(parse_human_size_bytes("12wb").is_err());
    }
}
