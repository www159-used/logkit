//! 发现 Kafka 传输配置（`client.conf` SSL / `server.properties` PLAINTEXT 等），供 `logen start` 与 CLI 共用。

use std::fs;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};

use serde_yaml::{Mapping, Value};
use thiserror::Error;

pub const DEFAULT_TOPIC: &str = "log_river";
pub const DEFAULT_KAFKA_PORT: u16 = 9092;

#[derive(Debug, Error)]
pub enum KafkaProtocolError {
    #[error("read {0}: {1}")]
    Io(PathBuf, #[source] std::io::Error),
    #[error("kafka config: {0}")]
    Conf(String),
    #[error(
        "no kafka client.conf or server.properties found (set KAFKA_CLIENT_CONF / KAFKA_SERVER_PROPERTIES or OEM_NAME)"
    )]
    NotFound,
}

/// 控制配置发现与 broker 解析。
#[derive(Debug, Clone, Default)]
pub struct KafkaProtocolOptions {
    pub client_conf: Option<PathBuf>,
    pub server_properties: Option<PathBuf>,
    pub broker_host: Option<String>,
    pub brokers: Option<String>,
}

impl KafkaProtocolOptions {
    pub fn with_broker_host(mut self, host: impl Into<String>) -> Self {
        self.broker_host = Some(host.into());
        self
    }
}

#[derive(Debug, Clone)]
pub struct KafkaDiscovered {
    pub source: PathBuf,
    pub bootstrap_brokers: Vec<String>,
    pub kafka_props: Vec<(String, String)>,
}

pub fn default_client_conf_path() -> PathBuf {
    resolve_oem::kafka_client_conf_path()
}

fn manager_process_root() -> PathBuf {
    let oem = resolve_oem::oem_name();
    PathBuf::from(format!("/run/{oem}_manager_agent/process"))
}

fn path_prefers_kafka(path: &Path) -> bool {
    path.components().any(|c| {
        c.as_os_str().to_string_lossy().contains("kafka")
            || c.as_os_str().to_string_lossy().contains("KAFKA")
    })
}

fn pick_best_config(mut paths: Vec<PathBuf>) -> Option<PathBuf> {
    if paths.is_empty() {
        return None;
    }
    let mut scored: Vec<(u8, PathBuf)> = paths
        .drain(..)
        .map(|p| (u8::from(!path_prefers_kafka(&p)), p))
        .collect();
    scored.sort_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.cmp(&b.1)));
    scored.into_iter().next().map(|(_, p)| p)
}

/// 单次扫描 `/run/{oem}_manager_agent/process/*/config/` 下的 Kafka 配置。
fn scan_manager_kafka_configs() -> (Option<PathBuf>, Option<PathBuf>) {
    let root = manager_process_root();
    let Ok(entries) = fs::read_dir(&root) else {
        return (None, None);
    };
    let mut client_confs = Vec::new();
    let mut server_props = Vec::new();
    for entry in entries.flatten() {
        let cfg = entry.path().join("config");
        let client_conf = cfg.join("client.conf");
        let server_properties = cfg.join("server.properties");
        if client_conf.is_file() {
            client_confs.push(client_conf);
        }
        if server_properties.is_file() {
            server_props.push(server_properties);
        }
    }
    (
        pick_best_config(client_confs),
        pick_best_config(server_props),
    )
}

pub fn discover_kafka_config(
    opts: &KafkaProtocolOptions,
) -> Result<KafkaDiscovered, KafkaProtocolError> {
    if let Some(p) = opts.client_conf.as_ref().filter(|p| p.is_file()) {
        return read_client_conf_discovered(p);
    }
    if let Ok(v) = std::env::var("KAFKA_CLIENT_CONF") {
        let p = PathBuf::from(v.trim());
        if p.is_file() {
            return read_client_conf_discovered(&p);
        }
    }
    if let Some(p) = opts.server_properties.as_ref().filter(|p| p.is_file()) {
        return read_server_properties_discovered(p);
    }
    if let Ok(v) = std::env::var("KAFKA_SERVER_PROPERTIES") {
        let p = PathBuf::from(v.trim());
        if p.is_file() {
            return read_server_properties_discovered(&p);
        }
    }
    let default = default_client_conf_path();
    if default.is_file() {
        return read_client_conf_discovered(&default);
    }
    let (client_conf, server_properties) = scan_manager_kafka_configs();
    if let Some(p) = client_conf {
        return read_client_conf_discovered(&p);
    }
    if let Some(p) = server_properties {
        return read_server_properties_discovered(&p);
    }
    Err(KafkaProtocolError::NotFound)
}

pub fn parse_kv_line(line: &str) -> Option<(&str, &str)> {
    let t = line.trim_end();
    if t.is_empty() || t.starts_with('#') || t.starts_with('!') {
        return None;
    }
    let (k, v) = if let Some((k, v)) = t.split_once('=') {
        (k.trim(), v.trim())
    } else {
        let mut parts = t.split_whitespace();
        let k = parts.next()?;
        let v = parts.next()?;
        (k, v)
    };
    if k.is_empty() {
        return None;
    }
    Some((k, v))
}

fn is_transport_prop(key: &str) -> bool {
    key == "security.protocol" || key.starts_with("ssl.") || key.starts_with("sasl.")
}

pub fn parse_brokers(value: &str) -> Vec<String> {
    value
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .collect()
}

pub fn broker_port(brokers: &[String]) -> u16 {
    brokers
        .first()
        .and_then(|b| b.rsplit_once(':'))
        .and_then(|(_, port)| port.parse().ok())
        .unwrap_or(DEFAULT_KAFKA_PORT)
}

fn build_brokers_local(conf_brokers: &[String]) -> Vec<String> {
    let port = broker_port(conf_brokers);
    let ip = resolve_oem::local_ip_or_empty();
    let local = if ip.is_empty() {
        format!("127.0.0.1:{port}")
    } else {
        format!("{ip}:{port}")
    };
    vec![local]
}

fn build_brokers_for_host(host: &str, conf_brokers: &[String]) -> Vec<String> {
    let host = host.trim();
    let port = broker_port(conf_brokers);
    vec![format!("{host}:{port}")]
}

pub fn resolve_brokers(
    opts: &KafkaProtocolOptions,
    conf_brokers: &[String],
) -> Result<Vec<String>, KafkaProtocolError> {
    if let Some(raw) = opts.brokers.as_deref() {
        let list = parse_brokers(raw);
        if list.is_empty() {
            return Err(KafkaProtocolError::Conf("--brokers 不能为空".into()));
        }
        return Ok(list);
    }
    if !conf_brokers.is_empty() {
        return Ok(conf_brokers.to_vec());
    }
    if let Some(host) = opts.broker_host.as_deref().filter(|h| !h.trim().is_empty()) {
        return Ok(build_brokers_for_host(host, conf_brokers));
    }
    Ok(build_brokers_local(conf_brokers))
}

type ClientConfResult = (Vec<String>, Vec<(String, String)>);

pub fn read_client_conf(
    path: &Path,
) -> Result<ClientConfResult, KafkaProtocolError> {
    let d = read_client_conf_discovered(path)?;
    Ok((d.bootstrap_brokers, d.kafka_props))
}

fn read_client_conf_discovered(path: &Path) -> Result<KafkaDiscovered, KafkaProtocolError> {
    let f = fs::File::open(path).map_err(|e| KafkaProtocolError::Io(path.to_path_buf(), e))?;
    let mut brokers = Vec::new();
    let mut props: Vec<(String, String)> = Vec::new();
    for line in BufReader::new(f).lines() {
        let line = line.map_err(|e| KafkaProtocolError::Io(path.to_path_buf(), e))?;
        if let Some((k, v)) = parse_kv_line(&line) {
            if k == "bootstrap.servers" {
                brokers = parse_brokers(v);
            } else if is_transport_prop(k) {
                props.push((k.to_string(), v.to_string()));
            }
        }
    }
    ensure_jks_keystore_alias(&mut props)?;
    Ok(KafkaDiscovered {
        source: path.to_path_buf(),
        bootstrap_brokers: brokers,
        kafka_props: props,
    })
}

/// 多私钥 JKS 且未写 `ssl.keystore.alias` 时，解析并写入（优先 `agent`）。
fn ensure_jks_keystore_alias(props: &mut Vec<(String, String)>) -> Result<(), KafkaProtocolError> {
    if props
        .iter()
        .any(|(k, v)| k == "ssl.keystore.alias" && !v.trim().is_empty())
    {
        return Ok(());
    }
    let Some(loc) = props
        .iter()
        .find(|(k, _)| k == "ssl.keystore.location")
        .map(|(_, v)| v.trim().to_string())
        .filter(|s| !s.is_empty())
    else {
        return Ok(());
    };
    let ext = Path::new(&loc)
        .extension()
        .and_then(|e| e.to_str())
        .unwrap_or("")
        .to_ascii_lowercase();
    if ext != "jks" {
        return Ok(());
    }
    let password = props
        .iter()
        .find(|(k, _)| k == "ssl.keystore.password")
        .map(|(_, v)| v.as_str())
        .unwrap_or("");
    let alias = java_ssl_pem::resolve_jks_key_alias(Path::new(&loc), password, None)
        .map_err(|e| KafkaProtocolError::Conf(e.to_string()))?;
    props.push(("ssl.keystore.alias".into(), alias));
    Ok(())
}

fn read_server_properties_discovered(path: &Path) -> Result<KafkaDiscovered, KafkaProtocolError> {
    let f = fs::File::open(path).map_err(|e| KafkaProtocolError::Io(path.to_path_buf(), e))?;
    let mut listeners = None;
    let mut advertised_listeners = None;
    let mut port = None;
    let mut host_name = None;
    let mut security_protocol = None;
    for line in BufReader::new(f).lines() {
        let line = line.map_err(|e| KafkaProtocolError::Io(path.to_path_buf(), e))?;
        if let Some((k, v)) = parse_kv_line(&line) {
            match k {
                "advertised.listeners" => advertised_listeners = Some(v.to_string()),
                "listeners" => listeners = Some(v.to_string()),
                "port" => port = v.parse::<u16>().ok(),
                "advertised.host.name" => {
                    let host = v.trim();
                    if !host.is_empty() {
                        host_name = Some(host.to_string());
                    }
                }
                "security.inter.broker.protocol" => security_protocol = Some(v.to_uppercase()),
                _ => {}
            }
        }
    }
    parse_server_properties_fields(
        path,
        advertised_listeners.as_deref(),
        listeners.as_deref(),
        port,
        host_name.as_deref(),
        security_protocol,
    )
}

fn parse_server_properties_fields(
    path: &Path,
    advertised_listeners: Option<&str>,
    listeners: Option<&str>,
    port: Option<u16>,
    host_name: Option<&str>,
    mut security_protocol: Option<String>,
) -> Result<KafkaDiscovered, KafkaProtocolError> {
    let listeners = advertised_listeners.or(listeners);
    let port = port.unwrap_or(DEFAULT_KAFKA_PORT);
    let mut brokers = Vec::new();

    if let Some(raw) = listeners {
        if let Some(parsed) = parse_listener_brokers(raw) {
            brokers = parsed.brokers;
            if security_protocol.is_none() {
                security_protocol = Some(parsed.protocol);
            }
        }
    }

    if brokers.is_empty() {
        if let Some(host) = host_name.filter(|host| !host.trim().is_empty()) {
            brokers.push(format!("{host}:{port}"));
        }
    }

    if brokers.is_empty() {
        return Err(KafkaProtocolError::Conf(format!(
            "{}: 无法从 listeners/advertised.host.name 解析 broker",
            path.display()
        )));
    }

    let protocol = security_protocol.unwrap_or_else(|| "PLAINTEXT".into());
    let props = vec![("security.protocol".into(), protocol)];

    Ok(KafkaDiscovered {
        source: path.to_path_buf(),
        bootstrap_brokers: brokers,
        kafka_props: props,
    })
}

struct ParsedListeners {
    brokers: Vec<String>,
    protocol: String,
}

fn parse_listener_brokers(raw: &str) -> Option<ParsedListeners> {
    let mut brokers = Vec::new();
    let mut protocol = None;
    for part in raw.split(',') {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }
        if let Some((proto, host_port)) = part.split_once("://") {
            protocol.get_or_insert_with(|| proto.to_uppercase());
            brokers.push(host_port.to_string());
        } else {
            brokers.push(part.to_string());
        }
    }
    if brokers.is_empty() {
        return None;
    }
    Some(ParsedListeners {
        brokers,
        protocol: protocol.unwrap_or_else(|| "PLAINTEXT".into()),
    })
}

fn kafka_mapping_from_discovered(
    opts: &KafkaProtocolOptions,
    discovered: &KafkaDiscovered,
    existing_kafka: Option<&Value>,
) -> Result<Value, KafkaProtocolError> {
    let brokers = resolve_brokers(opts, &discovered.bootstrap_brokers)?;

    let mut kafka = Mapping::new();
    if !kafka_has_brokers(existing_kafka) {
        let mut broker_seq = serde_yaml::Sequence::new();
        for b in &brokers {
            broker_seq.push(Value::String(b.clone()));
        }
        kafka.insert(Value::String("brokers".into()), Value::Sequence(broker_seq));
    }
    if !kafka_has_topic(existing_kafka) {
        kafka.insert(
            Value::String("topic".into()),
            Value::String(DEFAULT_TOPIC.into()),
        );
    }

    if !kafka_has_security_protocol(existing_kafka) {
        let has_security = discovered
            .kafka_props
            .iter()
            .any(|(k, _)| k == "security.protocol");
        let has_ssl = discovered
            .kafka_props
            .iter()
            .any(|(k, _)| k.starts_with("ssl."));
        let has_sasl = discovered
            .kafka_props
            .iter()
            .any(|(k, _)| k.starts_with("sasl."));
        if !has_security {
            let inferred = if has_sasl && has_ssl {
                Some("SASL_SSL")
            } else if has_sasl {
                Some("SASL_PLAINTEXT")
            } else if has_ssl {
                Some("SSL")
            } else {
                None
            };
            if let Some(proto) = inferred {
                kafka.insert(
                    Value::String("security.protocol".into()),
                    Value::String(proto.into()),
                );
            }
        }
        for (k, v) in &discovered.kafka_props {
            if is_transport_prop(k) {
                kafka.insert(Value::String(k.clone()), Value::String(v.clone()));
            }
        }
    } else {
        if !kafka_has_ssl_fields(existing_kafka) {
            for (k, v) in &discovered.kafka_props {
                if k.starts_with("ssl.") {
                    kafka.insert(Value::String(k.clone()), Value::String(v.clone()));
                }
            }
        }
        if !kafka_has_sasl_fields(existing_kafka) {
            for (k, v) in &discovered.kafka_props {
                if k.starts_with("sasl.") {
                    kafka.insert(Value::String(k.clone()), Value::String(v.clone()));
                }
            }
        }
    }

    Ok(Value::Mapping(kafka))
}

fn sink_document(kafka: Value) -> Value {
    let mut sink = Mapping::new();
    sink.insert(Value::String("type".into()), Value::String("kafka".into()));
    sink.insert(Value::String("kafka".into()), kafka);
    let mut doc = Mapping::new();
    doc.insert(Value::String("sink".into()), Value::Mapping(sink));
    Value::Mapping(doc)
}

/// 生成 `{ sink: { type: kafka, kafka: … } }` 文档片段，供 [`merge_worker_documents`] 合并。
pub fn kafka_sink_overlay(
    opts: &KafkaProtocolOptions,
    existing_kafka: Option<&Value>,
) -> Result<Value, KafkaProtocolError> {
    let discovered = discover_kafka_config(opts)?;
    let kafka = kafka_mapping_from_discovered(opts, &discovered, existing_kafka)?;
    Ok(sink_document(kafka))
}

pub fn document_needs_kafka_transport(doc: &Value) -> bool {
    let Some(sink) = doc.get("sink") else {
        return false;
    };
    if sink.get("type").and_then(|t| t.as_str()) != Some("kafka") {
        return false;
    }
    let kafka = sink.get("kafka");
    !kafka_has_brokers(kafka) || !kafka_has_security_protocol(kafka)
}

fn kafka_has_brokers(kafka: Option<&Value>) -> bool {
    kafka
        .and_then(|k| k.get("brokers"))
        .and_then(|b| b.as_sequence())
        .is_some_and(|s| !s.is_empty())
}

fn kafka_has_topic(kafka: Option<&Value>) -> bool {
    kafka
        .and_then(|k| k.get("topic"))
        .and_then(|t| t.as_str())
        .is_some_and(|s| !s.trim().is_empty())
}

fn kafka_has_security_protocol(kafka: Option<&Value>) -> bool {
    kafka
        .and_then(|k| k.get("security.protocol"))
        .and_then(|v| v.as_str())
        .is_some_and(|s| !s.trim().is_empty())
}

fn kafka_has_ssl_fields(kafka: Option<&Value>) -> bool {
    let Some(Value::Mapping(map)) = kafka else {
        return false;
    };
    map.keys()
        .any(|k| k.as_str().is_some_and(|s| s.starts_with("ssl.")))
}

fn kafka_has_sasl_fields(kafka: Option<&Value>) -> bool {
    let Some(Value::Mapping(map)) = kafka else {
        return false;
    };
    map.keys()
        .any(|k| k.as_str().is_some_and(|s| s.starts_with("sasl.")))
}

pub fn render_kafka_sink_yaml(opts: &KafkaProtocolOptions) -> Result<String, KafkaProtocolError> {
    let discovered = discover_kafka_config(opts)?;
    let kafka = kafka_mapping_from_discovered(opts, &discovered, None)?;
    let overlay = sink_document(kafka);
    let yaml = serde_yaml::to_string(&overlay)
        .map_err(|e| KafkaProtocolError::Conf(format!("serialize sink YAML: {e}")))?;
    Ok(format!(
        "# Generated by kafka-protocol from {}\n{yaml}",
        discovered.source.display()
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    /// 测试内容：默认 client.conf 路径随 OEM 变化。
    /// 输入：当前 OEM 名。
    /// 预期：路径含 `{oem}_manager_agent`。
    #[test]
    fn default_path_follows_oem_name() {
        let oem = resolve_oem::oem_name();
        let expected = format!("/run/{oem}_manager_agent/process/kafka/config/client.conf");
        assert_eq!(default_client_conf_path(), PathBuf::from(expected));
    }

    /// 测试内容：`parse_server_properties` 解析 PLAINTEXT listeners。
    /// 输入：rzy 风格 advertised.listeners。
    /// 预期：broker 与 security.protocol=PLAINTEXT。
    #[test]
    fn server_properties_plaintext_listeners() {
        let map: Vec<(String, String)> = vec![
            (
                "advertised.listeners".into(),
                "PLAINTEXT://192.168.41.138:9092".into(),
            ),
            ("port".into(), "9092".into()),
            ("security.inter.broker.protocol".into(), "PLAINTEXT".into()),
        ];
        let d = parse_server_properties_fields(
            Path::new("/tmp/server.properties"),
            map.iter()
                .find(|(k, _)| k == "advertised.listeners")
                .map(|(_, v)| v.as_str()),
            map.iter()
                .find(|(k, _)| k == "listeners")
                .map(|(_, v)| v.as_str()),
            map.iter()
                .find(|(k, _)| k == "port")
                .and_then(|(_, v)| v.parse::<u16>().ok()),
            map.iter()
                .find(|(k, _)| k == "advertised.host.name")
                .map(|(_, v)| v.as_str()),
            map.iter()
                .find(|(k, _)| k == "security.inter.broker.protocol")
                .map(|(_, v)| v.to_uppercase()),
        )
        .unwrap();
        assert_eq!(d.bootstrap_brokers, vec!["192.168.41.138:9092".to_string()]);
        assert!(d
            .kafka_props
            .iter()
            .any(|(k, v)| k == "security.protocol" && v == "PLAINTEXT"));
    }

    /// 测试内容：仅 kafka type 无 broker/security 时需要 overlay。
    /// 输入：只有 sink.type。
    /// 预期：`document_needs_kafka_transport` 为 true。
    #[test]
    fn needs_transport_when_kafka_bare() {
        let doc: Value = serde_yaml::from_str(
            r#"
sink:
  type: kafka
"#,
        )
        .unwrap();
        assert!(document_needs_kafka_transport(&doc));
    }

    /// 测试内容：从 client.conf 提取 bootstrap 与 ssl。
    /// 输入：临时 client.conf。
    /// 预期：两条 bootstrap、含 security.protocol。
    #[test]
    fn read_client_conf_extracts_bootstrap_and_ssl() {
        let dir = std::env::temp_dir().join(format!("kafka-protocol-{}", std::process::id()));
        fs::create_dir_all(&dir).unwrap();
        let path = dir.join("client.conf");
        let mut f = fs::File::create(&path).unwrap();
        writeln!(f, "bootstrap.servers=10.0.0.1:9092,10.0.0.2:9092").unwrap();
        writeln!(f, "security.protocol=SSL").unwrap();
        writeln!(f, "ssl.truststore.location=/opt/cert/ts.jks").unwrap();
        let (brokers, rows) = read_client_conf(&path).unwrap();
        assert_eq!(brokers.len(), 2);
        assert!(rows.iter().any(|(k, _)| k == "security.protocol"));
        let _ = fs::remove_dir_all(&dir);
    }

    /// 测试内容：discovered brokers 非空时不被本机 IP 覆盖。
    /// 输入：server.properties 风格 bootstrap。
    /// 预期：保留原 broker 地址。
    #[test]
    fn resolve_brokers_keeps_discovered_list() {
        let opts = KafkaProtocolOptions::default();
        assert_eq!(
            resolve_brokers(&opts, &["192.168.41.138:9092".into()]).unwrap(),
            vec!["192.168.41.138:9092".to_string()]
        );
    }

    /// 测试内容：`sasl.*` 键从 client.conf 透传到 `kafka_props`（对齐 librdkafka 客户端配置键名）。
    /// 输入：含 `sasl.mechanism=PLAIN` / `sasl.username` / `sasl.password` 的临时 client.conf。
    /// 预期：`kafka_props` 含字面键名 `sasl.mechanism`、`sasl.username`、`sasl.password`。
    #[test]
    fn read_client_conf_passes_sasl_keys_through() {
        let dir = std::env::temp_dir().join(format!("kafka-protocol-sasl-{}", std::process::id()));
        fs::create_dir_all(&dir).unwrap();
        let path = dir.join("client.conf");
        let mut f = fs::File::create(&path).unwrap();
        writeln!(f, "security.protocol=SASL_PLAINTEXT").unwrap();
        writeln!(f, "sasl.mechanism=PLAIN").unwrap();
        writeln!(f, "sasl.username=alice").unwrap();
        writeln!(f, "sasl.password=s3cret").unwrap();
        let (_brokers, rows) = read_client_conf(&path).unwrap();
        for key in ["sasl.mechanism", "sasl.username", "sasl.password"] {
            assert!(
                rows.iter().any(|(k, _)| k == key),
                "expected kafka_props to contain {key}, got {rows:?}"
            );
        }
        let _ = fs::remove_dir_all(&dir);
    }

    /// 测试内容：overlay 将 discovered 中的 `sasl.*` 写入 sink.kafka。
    /// 输入：仅含 SASL 传输键的 `KafkaDiscovered`。
    /// 预期：映射含 `SASL_PLAINTEXT` 与 username/password。
    #[test]
    fn overlay_merges_sasl_props_from_discovered() {
        let discovered = KafkaDiscovered {
            source: PathBuf::from("/tmp/client.conf"),
            bootstrap_brokers: vec!["10.0.0.1:9092".into()],
            kafka_props: vec![
                ("security.protocol".into(), "SASL_PLAINTEXT".into()),
                ("sasl.mechanism".into(), "PLAIN".into()),
                ("sasl.username".into(), "alice".into()),
                ("sasl.password".into(), "s3cret".into()),
            ],
        };
        let kafka = kafka_mapping_from_discovered(
            &KafkaProtocolOptions::default(),
            &discovered,
            None,
        )
        .unwrap();
        assert_eq!(
            kafka.get("security.protocol").and_then(|v| v.as_str()),
            Some("SASL_PLAINTEXT")
        );
        assert_eq!(
            kafka.get("sasl.mechanism").and_then(|v| v.as_str()),
            Some("PLAIN")
        );
        assert_eq!(
            kafka.get("sasl.username").and_then(|v| v.as_str()),
            Some("alice")
        );
    }

    /// 测试内容：已有 security.protocol 时仍可补齐缺失的 sasl.*。
    /// 输入：existing 仅 `SASL_SSL`；discovered 含 sasl + ssl。
    /// 预期：overlay 片段含 sasl.username，不覆盖 protocol。
    #[test]
    fn overlay_fills_sasl_when_protocol_already_set() {
        let existing: Value = serde_yaml::from_str(
            r#"
security.protocol: SASL_SSL
"#,
        )
        .unwrap();
        let discovered = KafkaDiscovered {
            source: PathBuf::from("/tmp/client.conf"),
            bootstrap_brokers: vec!["10.0.0.1:9093".into()],
            kafka_props: vec![
                ("security.protocol".into(), "SSL".into()),
                ("sasl.mechanism".into(), "SCRAM-SHA-256".into()),
                ("sasl.username".into(), "bob".into()),
                ("sasl.password".into(), "pw".into()),
                ("ssl.ca.location".into(), "/ca.crt".into()),
            ],
        };
        let kafka = kafka_mapping_from_discovered(
            &KafkaProtocolOptions::default(),
            &discovered,
            Some(&existing),
        )
        .unwrap();
        // 合并结果不含 existing 键（由调用方 deep-merge）；片段只带补齐项
        assert!(kafka.get("security.protocol").is_none());
        assert_eq!(
            kafka.get("sasl.username").and_then(|v| v.as_str()),
            Some("bob")
        );
        assert_eq!(
            kafka.get("ssl.ca.location").and_then(|v| v.as_str()),
            Some("/ca.crt")
        );
    }
}
