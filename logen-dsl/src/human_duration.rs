//! `min-interval`：整数毫秒，或带单位字符串（如 `100ms`、`1s`、`2m`）。

use std::fmt;

use serde::{
    de::{Error, Visitor},
    Deserializer,
};

/// 解析 `min-interval`：`1000`、`100ms`、`1s`、`1.5m` 等。无单位时按 **毫秒**（兼容旧配置）。
pub fn parse_human_duration_ms(s: &str) -> Result<u64, String> {
    let (num_str, unit) = split_number_and_unit(s.trim())?;
    let n: f64 = num_str
        .parse()
        .map_err(|_| format!("min-interval: cannot parse number in {s:?}"))?;
    if n < 0.0 || !n.is_finite() {
        return Err("min-interval: must be a non-negative finite number".into());
    }
    let factor = unit_to_ms_factor(&unit)?;
    let ms_f = n * factor;
    if !ms_f.is_finite() {
        return Err("min-interval: numeric overflow".into());
    }
    let rounded = ms_f.round();
    if rounded < 1.0 {
        return Err("min-interval: must be at least 1ms after conversion".into());
    }
    if rounded > u64::MAX as f64 {
        return Err("min-interval: exceeds u64".into());
    }
    Ok(rounded as u64)
}

fn split_number_and_unit(s: &str) -> Result<(&str, String), String> {
    if s.is_empty() {
        return Err("min-interval: empty string".into());
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
        return Err(format!("min-interval: missing numeric part in {s:?}"));
    }
    Ok((num, unit))
}

fn unit_to_ms_factor(unit: &str) -> Result<f64, String> {
    match unit {
        "" => Ok(1.0),
        "ns" | "nanosecond" | "nanoseconds" => Ok(1e-6),
        "us" | "µs" | "microsecond" | "microseconds" => Ok(1e-3),
        "ms" | "millisecond" | "milliseconds" => Ok(1.0),
        "s" | "sec" | "second" | "seconds" => Ok(1000.0),
        "m" | "min" | "minute" | "minutes" => Ok(60_000.0),
        "h" | "hr" | "hour" | "hours" => Ok(3_600_000.0),
        "d" | "day" | "days" => Ok(86_400_000.0),
        _ => Err(format!(
            "min-interval: unknown unit {unit:?} (allowed: ns, us, ms, s, m, h, d and common aliases)"
        )),
    }
}

pub fn deserialize_min_interval<'de, D>(deserializer: D) -> Result<u64, D::Error>
where
    D: Deserializer<'de>,
{
    struct V;
    impl<'de> Visitor<'de> for V {
        type Value = u64;

        fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
            write!(
                f,
                "integer milliseconds or duration string, e.g. 1000, 100ms, 1s, 2m"
            )
        }

        fn visit_u64<E: Error>(self, v: u64) -> Result<u64, E> {
            Ok(v.max(1))
        }

        fn visit_u128<E: Error>(self, v: u128) -> Result<u64, E> {
            let v = u64::try_from(v).map_err(|_| E::custom("min-interval: exceeds u64"))?;
            Ok(v.max(1))
        }

        fn visit_i64<E: Error>(self, v: i64) -> Result<u64, E> {
            let v = u64::try_from(v).map_err(|_| E::custom("min-interval: must be non-negative"))?;
            Ok(v.max(1))
        }

        fn visit_i128<E: Error>(self, v: i128) -> Result<u64, E> {
            let v = u64::try_from(v).map_err(|_| E::custom("min-interval: must be non-negative"))?;
            Ok(v.max(1))
        }

        fn visit_f64<E: Error>(self, v: f64) -> Result<u64, E> {
            if !v.is_finite() || v < 0.0 || v > u64::MAX as f64 {
                return Err(E::custom("min-interval: invalid floating-point value"));
            }
            Ok((v.round() as u64).max(1))
        }

        fn visit_str<E: Error>(self, s: &str) -> Result<u64, E> {
            parse_human_duration_ms(s).map_err(E::custom)
        }

        fn visit_string<E: Error>(self, s: String) -> Result<u64, E> {
            parse_human_duration_ms(&s).map_err(E::custom)
        }
    }

    deserializer.deserialize_any(V)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plain_integer_is_milliseconds() {
        assert_eq!(parse_human_duration_ms("1000").unwrap(), 1000);
        assert_eq!(parse_human_duration_ms("1").unwrap(), 1);
    }

    #[test]
    fn unit_suffixes() {
        assert_eq!(parse_human_duration_ms("100ms").unwrap(), 100);
        assert_eq!(parse_human_duration_ms("1s").unwrap(), 1000);
        assert_eq!(parse_human_duration_ms("2m").unwrap(), 120_000);
        assert_eq!(parse_human_duration_ms("1h").unwrap(), 3_600_000);
    }

    #[test]
    fn fractional_seconds() {
        assert_eq!(parse_human_duration_ms("1.5s").unwrap(), 1500);
    }

    #[test]
    fn sub_ms_rounds_up_to_one_ms_minimum() {
        assert_eq!(parse_human_duration_ms("500us").unwrap(), 1);
    }

    #[test]
    fn invalid_unit() {
        assert!(parse_human_duration_ms("1w").is_err());
    }
}
