//! OEM 名与常见安装路径（`OEM_NAME`，默认 `yotta`）。

use std::net::IpAddr;
use std::path::PathBuf;

use thiserror::Error;

pub const FALLBACK_OEM: &str = "yotta";

#[derive(Debug, Error)]
pub enum LocalIpError {
    #[error("detect local IP: {0}")]
    Detect(String),
    #[error("no non-loopback address (got {0})")]
    NoNonLoopback(IpAddr),
}

/// 读取 `OEM_NAME`，否则返回 [`FALLBACK_OEM`]。
pub fn oem_name() -> String {
    std::env::var("OEM_NAME")
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| FALLBACK_OEM.to_string())
}

/// `/opt/{oem}`
pub fn opt_root() -> PathBuf {
    let oem = oem_name();
    PathBuf::from(format!("/opt/{oem}"))
}

/// `/opt/{oem}/cert`
pub fn cert_dir() -> PathBuf {
    opt_root().join("cert")
}

pub fn ca_cert_path() -> PathBuf {
    cert_dir().join("ca.cert")
}

pub fn agent_pem_path() -> PathBuf {
    cert_dir().join("agent.pem")
}

pub fn agent_key_path() -> PathBuf {
    cert_dir().join("agent.key")
}

/// `/data/{oem}/mysql/mysql.sock`
pub fn mysql_socket_path() -> PathBuf {
    let oem = oem_name();
    PathBuf::from(format!("/data/{oem}/mysql/mysql.sock"))
}

/// `/opt/{oem}/mysql/bin/mysql`
pub fn mysql_bin_path() -> PathBuf {
    let oem = oem_name();
    PathBuf::from(format!("/opt/{oem}/mysql/bin/mysql"))
}

/// `/run/{oem}_manager_agent/process/kafka/config/client.conf`
pub fn kafka_client_conf_path() -> PathBuf {
    let oem = oem_name();
    PathBuf::from(format!(
        "/run/{oem}_manager_agent/process/kafka/config/client.conf"
    ))
}

/// 优先返回非 loopback 的本机 IP（供 HTTPS 上游 Host 等）。
pub fn local_ip_non_loopback() -> Result<IpAddr, LocalIpError> {
    if let Ok(ifaces) = local_ip_address::list_afinet_netifas() {
        for (_name, ip) in ifaces {
            if !ip.is_loopback() {
                return Ok(ip);
            }
        }
    }
    let ip = local_ip_address::local_ip().map_err(|e| LocalIpError::Detect(e.to_string()))?;
    if ip.is_loopback() {
        return Err(LocalIpError::NoNonLoopback(ip));
    }
    Ok(ip)
}

/// 尽力获取本机 IP 字符串；失败返回空串（兼容 Kafka agent 等宽松场景）。
pub fn local_ip_or_empty() -> String {
    local_ip_non_loopback()
        .map(|ip| ip.to_string())
        .or_else(|_| {
            local_ip_address::local_ip()
                .map(|ip| ip.to_string())
                .map_err(|e| e.to_string())
        })
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fallback_oem_when_unset() {
        std::env::remove_var("OEM_NAME");
        assert_eq!(oem_name(), "yotta");
    }

    #[test]
    fn paths_follow_oem() {
        std::env::remove_var("OEM_NAME");
        assert_eq!(opt_root(), PathBuf::from("/opt/yotta"));
        assert_eq!(cert_dir(), PathBuf::from("/opt/yotta/cert"));
        assert_eq!(ca_cert_path(), PathBuf::from("/opt/yotta/cert/ca.cert"));
        assert_eq!(
            mysql_bin_path(),
            PathBuf::from("/opt/yotta/mysql/bin/mysql")
        );
        assert_eq!(
            kafka_client_conf_path(),
            PathBuf::from("/run/yotta_manager_agent/process/kafka/config/client.conf")
        );
    }
}
