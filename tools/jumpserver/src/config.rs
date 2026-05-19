use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use serde::Deserialize;
use thiserror::Error;

/// 内置默认：上游 HTTPS 54400 → 本机 HTTP 15440（`port_maps` 键为上游、值为监听）。
pub const DEFAULT_PORT_MAP: &[(u32, u32)] = &[(54400, 15440)];

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("read config {path}: {source}")]
    Read {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("parse config {path}: {source}")]
    Parse {
        path: PathBuf,
        source: serde_yaml::Error,
    },
    #[error("no port mappings configured")]
    EmptyMappings,
    #[error("duplicate listen port {port}")]
    DuplicateListen { port: u32 },
    #[error("port {port} out of TCP range (max 65535)")]
    PortOutOfRange { port: u32 },
    #[error("detect local IP: {0}")]
    LocalIp(String),
}

#[derive(Debug, Clone)]
pub struct TlsPaths {
    pub ca: PathBuf,
    pub cert: PathBuf,
    pub key: PathBuf,
}

impl TlsPaths {
    pub fn defaults_for_oem(oem: &str) -> Self {
        let dir = PathBuf::from(format!("/opt/{oem}/cert"));
        Self {
            ca: dir.join("ca.cert"),
            cert: dir.join("agent.pem"),
            key: dir.join("agent.key"),
        }
    }
}

#[derive(Debug, Clone)]
pub struct RuntimeConfig {
    pub upstream_host: String,
    pub tls: TlsPaths,
    /// 等同 curl --insecure：不校验上游服务端证书（含主机名 SAN）
    pub tls_insecure: bool,
    /// listen_port → upstream_port（HTTPS）
    pub listen_to_upstream: BTreeMap<u32, u32>,
}

#[derive(Debug, Default, Deserialize)]
struct FileTls {
    ca: Option<PathBuf>,
    cert: Option<PathBuf>,
    key: Option<PathBuf>,
    insecure: Option<bool>,
}

#[derive(Debug, Default, Deserialize)]
struct FileConfig {
    upstream_host: Option<String>,
    tls: Option<FileTls>,
    /// `54400: 15440` — 上游 HTTPS 端口 → 本机 HTTP 监听端口
    port_maps: Option<BTreeMap<u32, u32>>,
    mappings: Option<BTreeMap<u32, u32>>,
}

/// 本机非回环地址，供 HTTPS 上游 Host（与 logen-worker 一致用 `local-ip-address`）。
pub fn default_upstream_host() -> Result<String, ConfigError> {
    if let Ok(ifaces) = local_ip_address::list_afinet_netifas() {
        for (_name, ip) in ifaces {
            if !ip.is_loopback() {
                return Ok(ip.to_string());
            }
        }
    }
    let ip = local_ip_address::local_ip().map_err(|e| ConfigError::LocalIp(e.to_string()))?;
    if ip.is_loopback() {
        return Err(ConfigError::LocalIp(format!(
            "no non-loopback address (got {ip})"
        )));
    }
    Ok(ip.to_string())
}

/// 校验并转为 TCP 端口（`u16`）。
pub fn tcp_port(port: u32) -> Result<u16, ConfigError> {
    u16::try_from(port).map_err(|_| ConfigError::PortOutOfRange { port })
}

pub fn load(path: Option<&Path>) -> Result<RuntimeConfig, ConfigError> {
    let oem = resolve_oem::oem_name();
    let mut upstream_host = default_upstream_host()?;
    let mut tls = TlsPaths::defaults_for_oem(&oem);
    let mut tls_insecure = true;
    let mut listen_to_upstream: BTreeMap<u32, u32> = DEFAULT_PORT_MAP
        .iter()
        .map(|&(up, listen)| (listen, up))
        .collect();

    if let Some(path) = path {
        let raw = std::fs::read_to_string(path).map_err(|source| ConfigError::Read {
            path: path.to_path_buf(),
            source,
        })?;
        let file: FileConfig =
            serde_yaml::from_str(&raw).map_err(|source| ConfigError::Parse {
                path: path.to_path_buf(),
                source,
            })?;
        if let Some(h) = file.upstream_host.filter(|s| !s.trim().is_empty()) {
            upstream_host = h;
        }
        if let Some(t) = file.tls {
            if let Some(p) = t.ca {
                tls.ca = p;
            }
            if let Some(p) = t.cert {
                tls.cert = p;
            }
            if let Some(p) = t.key {
                tls.key = p;
            }
            if let Some(v) = t.insecure {
                tls_insecure = v;
            }
        }
        let maps = file.port_maps.or(file.mappings);
        if let Some(maps) = maps {
            listen_to_upstream.clear();
            for (upstream_port, listen_port) in maps {
                listen_to_upstream.insert(listen_port, upstream_port);
            }
        }
    }

    if listen_to_upstream.is_empty() {
        return Err(ConfigError::EmptyMappings);
    }

    let mut seen = std::collections::HashSet::new();
    for &listen in listen_to_upstream.keys() {
        tcp_port(listen)?;
        if !seen.insert(listen) {
            return Err(ConfigError::DuplicateListen { port: listen });
        }
    }
    for &upstream in listen_to_upstream.values() {
        tcp_port(upstream)?;
    }

    Ok(RuntimeConfig {
        upstream_host,
        tls,
        tls_insecure,
        listen_to_upstream,
    })
}

/// 合并 CLI / 环境变量对 insecure 的覆盖（在 [`load`] 之后调用）。
pub fn apply_insecure_override(cfg: &mut RuntimeConfig, cli_insecure: bool) {
    if cli_insecure {
        cfg.tls_insecure = true;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_oem_cert_paths() {
        let t = TlsPaths::defaults_for_oem("yotta");
        assert_eq!(t.ca, PathBuf::from("/opt/yotta/cert/ca.cert"));
        assert_eq!(t.cert, PathBuf::from("/opt/yotta/cert/agent.pem"));
        assert_eq!(t.key, PathBuf::from("/opt/yotta/cert/agent.key"));
    }

    #[test]
    fn builtin_default_mapping() {
        let cfg = load(None).unwrap();
        assert_eq!(cfg.listen_to_upstream.get(&15440), Some(&54400));
        let ip: IpAddr = cfg.upstream_host.parse().expect("upstream_host is IP");
        assert!(!ip.is_loopback());
    }

    #[test]
    fn rejects_port_above_65535() {
        assert!(matches!(
            tcp_port(154400),
            Err(ConfigError::PortOutOfRange { port: 154400 })
        ));
    }

    #[test]
    fn file_port_maps_replace_builtin() {
        let dir = std::env::temp_dir().join(format!("jumpserver-cfg-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("override.yaml");
        std::fs::write(&path, "port_maps:\n  54401: 15441\n").unwrap();

        let cfg = load(Some(&path)).unwrap();
        assert_eq!(cfg.listen_to_upstream.get(&15440), None);
        assert_eq!(cfg.listen_to_upstream.get(&15441), Some(&54401));

        let _ = std::fs::remove_dir_all(&dir);
    }
}
