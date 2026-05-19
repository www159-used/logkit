//! 内部工具：读取 Kafka `client.conf` 中的 SSL 相关项并输出为 YAML 片段。

use std::env;
use std::ffi::OsString;
use std::fs;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};

fn default_client_conf_path() -> PathBuf {
    resolve_oem::kafka_client_conf_path()
}

fn expect_client_conf_file(path: &Path) -> PathBuf {
    if path.is_file() {
        return path.to_path_buf();
    }
    eprintln!("parse_client_conf: 不是可读文件: {}", path.display());
    std::process::exit(1);
}

fn parse_args() -> PathBuf {
    let default_path = default_client_conf_path();
    let mut it = env::args_os().skip(1);
    let a1: Option<OsString> = it.next();
    let a2: Option<OsString> = it.next();
    if a2.is_some() {
        eprintln!(
            "用法: parse_client_conf [PATH]\n\
             PATH：`client.conf` 文件路径；至多一个参数。\n\
             省略 PATH 时读取: {}",
            default_path.display()
        );
        std::process::exit(2);
    }
    match a1 {
        None => default_path,
        Some(p) => expect_client_conf_file(Path::new(&p)),
    }
}

fn parse_kv_line(line: &str) -> Option<(&str, &str)> {
    let t = line.trim_end();
    if t.is_empty() || t.starts_with('#') || t.starts_with('!') {
        return None;
    }
    let (k, v) = t.split_once('=')?;
    let k = k.trim();
    if k.is_empty() {
        return None;
    }
    Some((k, v.trim()))
}

fn emit_kafka_yaml_key(key: &str) -> bool {
    key == "security.protocol" || key.starts_with("ssl.")
}

fn yaml_scalar(value: &str) -> String {
    if value.is_empty() {
        return "\"\"".to_string();
    }
    let reserved = matches!(
        value,
        "true" | "false" | "null" | "~" | "yes" | "no" | "on" | "off"
    );
    let plain = !reserved
        && !value.starts_with('-')
        && value.chars().all(|c| {
            c.is_ascii_alphanumeric() || matches!(c, '_' | '.' | '/' | '-' | '+')
        });
    if plain {
        return value.to_string();
    }
    let mut out = String::with_capacity(value.len() + 2);
    out.push('"');
    for ch in value.chars() {
        match ch {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

fn main() {
    let path = parse_args();
    let f = fs::File::open(&path).unwrap_or_else(|e| {
        eprintln!("parse_client_conf: 打开 {}: {e}", path.display());
        std::process::exit(1);
    });
    let mut rows: Vec<(String, String)> = Vec::new();
    for line in BufReader::new(f).lines() {
        let line = line.unwrap_or_else(|e| {
            eprintln!("parse_client_conf: read: {e}");
            std::process::exit(1);
        });
        if let Some((k, v)) = parse_kv_line(&line) {
            if emit_kafka_yaml_key(k) {
                rows.push((k.to_string(), v.to_string()));
            }
        }
    }
    let has_security = rows.iter().any(|(k, _)| k == "security.protocol");
    let has_ssl = rows.iter().any(|(k, _)| k.starts_with("ssl."));
    if has_ssl && !has_security {
        println!("    security.protocol: SSL");
    }
    for (k, v) in rows {
        println!("    {}: {}", k, yaml_scalar(&v));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 测试内容：默认 Kafka client.conf 路径随 OEM 名变化。
    /// 输入：当前 [`resolve_oem::oem_name`]。
    /// 预期：与 `/run/{oem}_manager_agent/process/kafka/config/client.conf` 一致。
    #[test]
    fn default_path_follows_oem_name() {
        let oem = resolve_oem::oem_name();
        let expected = format!("/run/{oem}_manager_agent/process/kafka/config/client.conf");
        assert_eq!(default_client_conf_path(), PathBuf::from(expected));
    }

    /// 测试内容：空字符串 YAML 标量加引号。
    /// 输入：`""`。
    /// 预期：输出 `""`（带引号的空串）。
    #[test]
    fn yaml_scalar_empty_is_quoted_empty() {
        assert_eq!(yaml_scalar(""), "\"\"");
    }

    /// 测试内容：普通路径与 TLS 版本号不加引号。
    /// 输入：JKS 路径、`TLSv1.3`。
    /// 预期：原样输出，无额外引号。
    #[test]
    fn yaml_scalar_plain_paths_and_tls() {
        assert_eq!(yaml_scalar("/opt/yotta/cert/x.jks"), "/opt/yotta/cert/x.jks");
        assert_eq!(yaml_scalar("TLSv1.3"), "TLSv1.3");
    }

    /// 测试内容：含冒号或 `@` 的值需 YAML 引号包裹。
    /// 输入：`a:b`、`x@y`。
    /// 预期：双引号字符串形式。
    #[test]
    fn yaml_scalar_quotes_special_chars() {
        assert_eq!(yaml_scalar("a:b"), "\"a:b\"");
        assert_eq!(yaml_scalar("x@y"), "\"x@y\"");
    }

    /// 测试内容：键值行去空白且仅按首个 `=` 分割。
    /// 输入：带首尾空格的 `ssl.keystore.password= ab=cd`。
    /// 预期：键 `ssl.keystore.password`，值 `ab=cd`。
    #[test]
    fn parse_kv_trims_and_splits_first_equals() {
        assert_eq!(
            parse_kv_line("  ssl.keystore.password= ab=cd  "),
            Some(("ssl.keystore.password", "ab=cd"))
        );
    }
}
