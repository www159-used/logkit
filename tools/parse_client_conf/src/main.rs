//! 读取 **`client.conf`**，将其中 **`ssl.*`** 键行输出到 stdout。
//!
//! **用法**：`parse_client_conf` 后可选跟 **一个** `client.conf` **文件**路径；**省略**则读默认绝对路径 **`/run/{OEM}_manager_agent/process/kafka/config/client.conf`**，其中 **`{OEM}`** 为 `resolve_oem::oem_name()`（环境变量 **`OEM_NAME`**，缺省 **`yotta`**）。

use std::env;
use std::ffi::OsString;
use std::fs;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};

fn default_client_conf_path() -> PathBuf {
    PathBuf::from(format!(
        "/run/{}_manager_agent/process/kafka/config/client.conf",
        resolve_oem::oem_name()
    ))
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

fn key_starts_with_ssl(line: &str) -> bool {
    let t = line.trim_end();
    if t.is_empty() || t.starts_with('#') || t.starts_with('!') {
        return false;
    }
    let Some((k, _)) = t.split_once('=') else {
        return false;
    };
    k.trim().starts_with("ssl.")
}

fn main() {
    let path = parse_args();
    let f = fs::File::open(&path).unwrap_or_else(|e| {
        eprintln!("parse_client_conf: 打开 {}: {e}", path.display());
        std::process::exit(1);
    });
    for line in BufReader::new(f).lines() {
        let line = line.unwrap_or_else(|e| {
            eprintln!("parse_client_conf: read: {e}");
            std::process::exit(1);
        });
        if key_starts_with_ssl(&line) {
            println!("{}", line.trim_end());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_path_follows_oem_name() {
        let oem = resolve_oem::oem_name();
        let expected = format!("/run/{oem}_manager_agent/process/kafka/config/client.conf");
        assert_eq!(default_client_conf_path(), PathBuf::from(expected));
    }
}
