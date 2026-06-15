use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use resolve_oem::LocalIpError;
use serde::Deserialize;
use thiserror::Error;

/// 内置默认：上游 HTTPS → 本机 HTTP（`port_maps` 键为上游、值为监听）。
pub const DEFAULT_PORT_MAP: &[(u32, u32)] = &[(54400, 15440), (9400, 1940)];

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
    #[error("{0}")]
    LocalIp(#[from] LocalIpError),
}

#[derive(Debug, Clone)]
pub struct TlsPaths {
    pub ca: PathBuf,
    pub cert: PathBuf,
    pub key: PathBuf,
}

impl TlsPaths {
    pub fn defaults() -> Self {
        Self {
            ca: resolve_oem::ca_cert_path(),
            cert: resolve_oem::agent_pem_path(),
            key: resolve_oem::agent_key_path(),
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

pub fn default_upstream_host() -> Result<String, ConfigError> {
    Ok(resolve_oem::local_ip_non_loopback()?.to_string())
}

/// 校验并转为 TCP 端口（`u16`）。
pub fn tcp_port(port: u32) -> Result<u16, ConfigError> {
    u16::try_from(port).map_err(|_| ConfigError::PortOutOfRange { port })
}

pub fn load(path: Option<&Path>) -> Result<RuntimeConfig, ConfigError> {
    let mut upstream_host = default_upstream_host()?;
    let mut tls = TlsPaths::defaults();
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
        let file: FileConfig = serde_yaml::from_str(&raw).map_err(|source| ConfigError::Parse {
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
    use std::net::IpAddr;

    use super::*;

    /// 测试内容：未配置 TLS 文件时 [`TlsPaths::defaults`] 走 resolve-oem 约定路径。
    /// 输入：清除 `OEM_NAME`。
    /// 预期：ca/cert/key 位于 `/opt/yotta/cert/` 下默认文件名。
    #[test]
    fn default_oem_cert_paths() {
        std::env::remove_var("OEM_NAME");
        let t = TlsPaths::defaults();
        assert_eq!(t.ca, PathBuf::from("/opt/yotta/cert/ca.cert"));
        assert_eq!(t.cert, PathBuf::from("/opt/yotta/cert/agent.pem"));
        assert_eq!(t.key, PathBuf::from("/opt/yotta/cert/agent.key"));
    }

    /// 测试内容：无配置文件时使用内置端口映射与上游 Host。
    /// 输入：`load(None)`。
    /// 预期：监听 `15440` → 上游 `54400`；`1940` → `9400`；`upstream_host` 可解析为非 loopback IP。
    #[test]
    fn builtin_default_mapping() {
        let cfg = load(None).unwrap();
        assert_eq!(cfg.listen_to_upstream.get(&15440), Some(&54400));
        assert_eq!(cfg.listen_to_upstream.get(&1940), Some(&9400));
        let ip: IpAddr = cfg.upstream_host.parse().expect("upstream_host is IP");
        assert!(!ip.is_loopback());
    }

    /// 测试内容：TCP 端口上界校验。
    /// 输入：`tcp_port(154400)`。
    /// 预期：返回 [`ConfigError::PortOutOfRange`]。
    #[test]
    fn rejects_port_above_65535() {
        assert!(matches!(
            tcp_port(154400),
            Err(ConfigError::PortOutOfRange { port: 154400 })
        ));
    }

    /// 测试内容：YAML 中 `port_maps` 整表替换内置映射。
    /// 输入：临时文件 `port_maps: 54401: 15441`。
    /// 预期：无 `15440` 内置项；`15441` 监听对应上游 `54401`。
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
