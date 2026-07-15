//! librdkafka ClientConfig 构建：传输模式、SASL/TLS、producer 默认与 extras。

use std::collections::BTreeMap;

use logen_model::{KafkaConfig, KafkaSinkMode};
use rdkafka::config::ClientConfig;
use rdkafka::message::{Header, OwnedHeaders};
use rdkafka::producer::FutureProducer;
use serde_yaml::Value;

use super::super::kafka_agent;
use super::super::kafka_jks;
use super::super::{KafkaConfigError, SinkError};

pub(super) fn cfg_err(msg: impl Into<String>) -> KafkaConfigError {
    KafkaConfigError::new(msg)
}

pub(super) const PRODUCER_QUEUE_MAX_KBYTES: &str = "65536";
pub(super) const PRODUCER_BATCH_SIZE: &str = "65536";
pub(super) const PRODUCER_LINGER_MS: &str = "10";
pub(super) const PRODUCER_MESSAGE_MAX_BYTES: &str = "10485760";
pub(super) const PRODUCER_COMPRESSION: &str = "lz4";
pub(super) const PRODUCER_SOCKET_TIMEOUT_MS: &str = "60000";

fn apply_builtin_producer_profile(cfg: &mut ClientConfig) {
    cfg.set("queue.buffering.max.kbytes", PRODUCER_QUEUE_MAX_KBYTES);
    cfg.set("batch.size", PRODUCER_BATCH_SIZE);
    cfg.set("queue.buffering.max.ms", PRODUCER_LINGER_MS);
    cfg.set("message.max.bytes", PRODUCER_MESSAGE_MAX_BYTES);
    cfg.set("compression.type", PRODUCER_COMPRESSION);
    cfg.set("socket.timeout.ms", PRODUCER_SOCKET_TIMEOUT_MS);
}

fn yaml_value_to_config_string(v: &Value) -> Option<String> {
    match v {
        Value::Null => None,
        Value::Bool(b) => Some(b.to_string()),
        Value::Number(n) => Some(n.to_string()),
        Value::String(s) => Some(s.clone()),
        Value::Mapping(_) | Value::Sequence(_) | Value::Tagged(_) => serde_json::to_string(v).ok(),
    }
}

fn apply_kafka_extras(cfg: &mut ClientConfig, extras: &BTreeMap<String, Value>) {
    for (k, v) in extras {
        let key = k.trim();
        if key.is_empty() {
            continue;
        }
        match yaml_value_to_config_string(v) {
            Some(val) => {
                cfg.set(key, val);
            }
            None => {
                tracing::warn!("kafka.extras: skip key {key:?} (null or unsupported YAML value)")
            }
        }
    }
}

fn effective_produce_topic(k: &KafkaConfig) -> String {
    match k.mode {
        KafkaSinkMode::Agent => kafka_agent::KAFKA_AGENT_TOPIC.to_string(),
        KafkaSinkMode::Common => k.topic.as_deref().unwrap_or("").trim().to_string(),
    }
}

pub(super) fn create_future_producer(
    k: &KafkaConfig,
) -> Result<(FutureProducer, String), SinkError> {
    let transport = kafka_transport_mode(k)?;
    let (cfg, topic) = build_rdkafka_client_config(k, transport)?;
    let producer: FutureProducer = cfg
        .create()
        .map_err(|e| SinkError::Kafka(format!("failed to create Kafka producer: {e}")))?;
    Ok((producer, topic))
}

pub(super) fn owned_headers_from_kafka_cfg(
    headers: Option<&BTreeMap<String, Option<String>>>,
) -> Result<Option<OwnedHeaders>, KafkaConfigError> {
    let Some(map) = headers else {
        return Ok(None);
    };
    if map.is_empty() {
        return Ok(None);
    }
    let mut h = OwnedHeaders::new();
    for (k, vo) in map {
        let key = k.trim();
        if key.is_empty() {
            return Err(cfg_err("kafka.headers: empty header key is not allowed"));
        }
        h = match vo {
            None => h.insert(Header {
                key,
                value: None::<&[u8]>,
            }),
            Some(s) => h.insert(Header {
                key,
                value: Some(s.as_bytes()),
            }),
        };
    }
    Ok(Some(h))
}

fn required_acks_rdkafka(v: Option<&str>) -> Result<&'static str, KafkaConfigError> {
    let s = v.map(str::trim).filter(|s| !s.is_empty()).unwrap_or("1");
    if let Ok(n) = s.parse::<i64>() {
        return match n {
            -1 => Ok("all"),
            0 => Ok("0"),
            1 => Ok("1"),
            _ => Err(cfg_err(format!(
                "kafka.request.required.acks: unsupported integer {n} (expected -1, 0, or 1)"
            ))),
        };
    }
    match s.to_ascii_lowercase().as_str() {
        "1" | "leader" | "one" => Ok("1"),
        "all" => Ok("all"),
        "none" | "0" => Ok("0"),
        _ => Err(cfg_err(format!(
            "kafka.request.required.acks: unknown string {s:?}"
        ))),
    }
}

pub(super) fn normalize_brokers(k: &KafkaConfig) -> Vec<String> {
    k.brokers
        .as_ref()
        .into_iter()
        .flat_map(|v| v.iter())
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect()
}

fn compression_rdkafka(cs: Option<&str>) -> Result<Option<&'static str>, KafkaConfigError> {
    let Some(raw) = cs else {
        return Ok(None);
    };
    let s = raw.trim();
    if s.is_empty() {
        return Ok(None);
    }
    Ok(Some(match s.to_ascii_lowercase().as_str() {
        "none" | "uncompressed" => "none",
        "gzip" => "gzip",
        "snappy" => "snappy",
        "lz4" => "lz4",
        "zstd" => "zstd",
        other => {
            return Err(cfg_err(format!(
                "kafka.compression.type: unknown or unsupported {other:?} (try none, gzip, snappy, lz4, zstd)"
            )));
        }
    }))
}

fn parse_timeout_ms(s: Option<&str>) -> Result<u64, KafkaConfigError> {
    let Some(s) = s.map(str::trim).filter(|s| !s.is_empty()) else {
        return Ok(30_000);
    };
    s.parse::<u64>().map_err(|_| {
        cfg_err("kafka.message.timeout.ms: invalid string (expected positive integer ms)")
    })
}

fn delivery_timeout_ms_default(message_ms: u64) -> u64 {
    message_ms.saturating_add(5000).max(10_000)
}

pub(super) fn kafka_transport_mode(
    k: &KafkaConfig,
) -> Result<KafkaTransportMode, KafkaConfigError> {
    let proto = k.security_protocol_norm();
    let sasl = k.has_sasl_material();
    match proto.as_deref() {
        None | Some("PLAINTEXT") => {
            if sasl {
                return Err(cfg_err(
                    "security.protocol is PLAINTEXT (or unset) but SASL fields are set; set security.protocol to SASL_PLAINTEXT or SASL_SSL.",
                ));
            }
            Ok(KafkaTransportMode::Plaintext)
        }
        Some("SSL") => {
            if sasl {
                return Err(cfg_err(
                    "security.protocol is SSL but SASL fields are set; set security.protocol to SASL_SSL.",
                ));
            }
            Ok(KafkaTransportMode::Tls)
        }
        Some("SASL_PLAINTEXT") => Ok(KafkaTransportMode::SaslPlaintext),
        Some("SASL_SSL") => Ok(KafkaTransportMode::SaslTls),
        Some(other) => Err(cfg_err(format!(
            "security.protocol: unsupported value {other:?}"
        ))),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum KafkaTransportMode {
    Plaintext,
    Tls,
    SaslPlaintext,
    SaslTls,
}

pub(super) fn configure_librdkafka_sasl(
    cfg: &mut ClientConfig,
    k: &KafkaConfig,
) -> Result<(), KafkaConfigError> {
    let mech = k
        .sasl_mechanism
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| {
            cfg_err(
                "sasl.mechanism is required for SASL protocols (e.g. PLAIN, SCRAM-SHA-256, SCRAM-SHA-512)",
            )
        })?;
    let user = k
        .sasl_username
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| cfg_err("sasl.username is required for SASL protocols"))?;
    let pass = k
        .sasl_password
        .as_deref()
        .filter(|s| !s.is_empty())
        .ok_or_else(|| cfg_err("sasl.password is required for SASL protocols"))?;

    cfg.set("sasl.mechanism", mech);
    cfg.set("sasl.username", user);
    cfg.set("sasl.password", pass);

    if k.sasl_jaas_config.is_some() {
        tracing::warn!(
            "sasl.jaas.config is ignored (Java-only); use sasl.username / sasl.password for librdkafka"
        );
    }

    Ok(())
}

pub(super) fn build_rdkafka_client_config(
    k: &KafkaConfig,
    transport: KafkaTransportMode,
) -> Result<(ClientConfig, String), SinkError> {
    let brokers = normalize_brokers(k);
    if brokers.is_empty() {
        return Err(cfg_err("kafka.brokers must list at least one broker").into());
    }
    let topic_trimmed = effective_produce_topic(k);
    if topic_trimmed.is_empty() {
        return Err(cfg_err(
            "kafka.topic must be non-empty (or use sink.kafka.mode: agent for fixed topic raw_message)",
        )
        .into());
    }

    let tls_enabled = matches!(
        transport,
        KafkaTransportMode::Tls | KafkaTransportMode::SaslTls
    );
    if !tls_enabled && k.has_ssl_material() {
        return Err(cfg_err(
            "TLS-related ssl.* is set but security.protocol is not SSL or SASL_SSL. \
             For TLS set security.protocol to SSL/SASL_SSL and supply trust/client material (PEM or JKS as documented).",
        )
        .into());
    }

    let mut cfg = ClientConfig::new();
    apply_builtin_producer_profile(&mut cfg);
    cfg.set("bootstrap.servers", brokers.join(","));
    cfg.set("client.id", "logen-worker");
    cfg.set("log.connection.close", "false");

    if !k.extras.contains_key("message.timeout.ms") {
        let message_ms = parse_timeout_ms(k.message_timeout_ms.as_deref())?;
        cfg.set("message.timeout.ms", message_ms.to_string());
        if !k.extras.contains_key("delivery.timeout.ms") {
            let delivery_ms = match k.delivery_timeout_ms.as_deref() {
                Some(s) => parse_timeout_ms(Some(s))?,
                None => delivery_timeout_ms_default(message_ms),
            };
            cfg.set("delivery.timeout.ms", delivery_ms.to_string());
        }
    } else if !k.extras.contains_key("delivery.timeout.ms") {
        let delivery_ms = parse_timeout_ms(k.delivery_timeout_ms.as_deref())?;
        cfg.set("delivery.timeout.ms", delivery_ms.to_string());
    }

    if !k.extras.contains_key("request.required.acks") {
        cfg.set(
            "request.required.acks",
            required_acks_rdkafka(k.request_required_acks.as_deref())?,
        );
    }
    if !k.extras.contains_key("compression.type") {
        if let Some(ct) = compression_rdkafka(k.compression_type.as_deref())? {
            cfg.set("compression.type", ct);
        }
    }

    match transport {
        KafkaTransportMode::Plaintext => {
            cfg.set("security.protocol", "PLAINTEXT");
        }
        KafkaTransportMode::Tls => {
            cfg.set("security.protocol", "ssl");
            kafka_jks::configure_librdkafka_ssl(&mut cfg, k)?;
        }
        KafkaTransportMode::SaslPlaintext => {
            cfg.set("security.protocol", "sasl_plaintext");
            configure_librdkafka_sasl(&mut cfg, k)?;
        }
        KafkaTransportMode::SaslTls => {
            cfg.set("security.protocol", "sasl_ssl");
            kafka_jks::configure_librdkafka_ssl(&mut cfg, k)?;
            configure_librdkafka_sasl(&mut cfg, k)?;
        }
    }

    apply_kafka_extras(&mut cfg, &k.extras);

    Ok((cfg, topic_trimmed))
}
