use std::collections::BTreeMap;
use std::time::Duration;

use async_trait::async_trait;
use logspout_dsl::{KafkaConfig, KafkaSinkMode};
use rdkafka::config::ClientConfig;
use rdkafka::error::KafkaError;
use rdkafka::message::{Header, OwnedHeaders};
use rdkafka::producer::{BaseRecord, DefaultProducerContext, FutureProducer, FutureRecord, Producer};
use thiserror::Error;
use tokio::time::timeout;

use super::context_id::next_context_id;
use super::kafka_agent::{self, KafkaAgentRuntimeState};

use super::{EmitLineError, LogLineSink};

/// Kafka 行 sink 的配置校验、建连与 produce 阶段的错误。
#[derive(Debug, Error)]
pub enum KafkaLineSinkError {
    #[error("{0}")]
    InvalidConfig(String),

    #[error(transparent)]
    JavaSslPem(#[from] java_ssl_pem::JavaSslPemError),

    #[error("failed to create Kafka producer: {0}")]
    ProducerCreate(#[source] KafkaError),

    #[error("{0}")]
    Produce(String),
}

fn invalid_kafka_cfg(msg: impl Into<String>) -> KafkaLineSinkError {
    KafkaLineSinkError::InvalidConfig(msg.into())
}

fn effective_produce_topic(k: &KafkaConfig) -> String {
    match k.mode {
        KafkaSinkMode::Agent => kafka_agent::KAFKA_AGENT_TOPIC.to_string(),
        KafkaSinkMode::Common => k.topic.as_deref().unwrap_or("").trim().to_string(),
    }
}

fn wall_clock_ms_i64() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis().min(i64::MAX as u128) as i64)
        .unwrap_or(0)
}

pub struct KafkaLineSink {
    producer: FutureProducer,
    brokers_display: String,
    topic: String,
    /// 每条消息附带的 record headers（与配置一致，逐条 clone）。`agent` 模式为 `None`。
    headers: Option<OwnedHeaders>,
    /// 克隆的 Kafka 段配置（用于错误上下文与 TLS/SASL 提示）。
    #[allow(dead_code)]
    pub kafka_config: KafkaConfig,
    tls_enabled: bool,
    sasl_keys_in_yaml: bool,
    /// `send` 入队等待上限；投递另受 `message.timeout.ms` 等约束。
    queue_timeout: Duration,
    agent_state: Option<KafkaAgentRuntimeState>,
}

fn kafka_frame_size_error_hint(err_display: &str) -> Option<&'static str> {
    if err_display.to_ascii_lowercase().contains("frame size") {
        Some(
            "Hint: \"frame size too big\" often means the peer is not Kafka PLAINTEXT (e.g. SSL listener, HTTP, or wrong port). \
             Check broker listeners/ports or probe with kcat.",
        )
    } else {
        None
    }
}

fn format_produce_err(
    e: &KafkaError,
    brokers_display: &str,
    topic: &str,
    tls_enabled: bool,
    sasl_keys_in_yaml: bool,
    k: &KafkaConfig,
) -> String {
    let err_display = e.to_string();
    let mut s = format!(
        "kafka produce failed (brokers=[{}], topic={:?}): {}",
        brokers_display, topic, err_display
    );
    if let Some(h) = kafka_frame_size_error_hint(&err_display) {
        s.push('\n');
        s.push_str(h);
    }
    if sasl_keys_in_yaml {
        s.push_str(
            "\nNote: SASL-related fields are set but this worker only wires PLAINTEXT and SSL (no SASL); use a broker listener without SASL or extend SASL mapping.",
        );
    }
    if !tls_enabled && likely_encrypted_broker_config(k) {
        s.push_str(
            "\nNote: configuration suggests TLS (security.protocol=SSL/SASL_SSL or ssl.*) but security.protocol is not SSL; \
             an encrypted-only listener will fail.",
        );
    }
    s
}

fn security_protocol_upper(k: &KafkaConfig) -> Option<String> {
    k.security_protocol
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(|s| s.to_ascii_uppercase())
}

fn uses_tls_security_protocol(k: &KafkaConfig) -> bool {
    security_protocol_upper(k)
        .as_deref()
        .map(|p| matches!(p, "SSL" | "SASL_SSL"))
        .unwrap_or(false)
}

fn sasl_keys_present(k: &KafkaConfig) -> bool {
    k.sasl_mechanism.is_some()
        || k.sasl_jaas_config.is_some()
        || k.sasl_username.is_some()
        || k.sasl_password.is_some()
}

fn ssl_options_present(k: &KafkaConfig) -> bool {
    k.ssl_endpoint_identification_algorithm.is_some()
        || k.ssl_ca_pem.is_some()
        || k.ssl_ca_location.is_some()
        || k.ssl_truststore_location.is_some()
        || k.ssl_certificate_pem.is_some()
        || k.ssl_certificate_location.is_some()
        || k.ssl_private_key_pem.is_some()
        || k.ssl_key_location.is_some()
        || k.ssl_key_pem.is_some()
        || k.ssl_keystore_location.is_some()
        || k.ssl_truststore_password.is_some()
        || k.ssl_keystore_password.is_some()
        || k.ssl_keystore_alias.is_some()
        || k.ssl_protocol.is_some()
        || k.ssl_enabled_protocols.is_some()
}

fn likely_encrypted_broker_config(k: &KafkaConfig) -> bool {
    uses_tls_security_protocol(k) || ssl_options_present(k)
}

fn owned_headers_from_kafka_cfg(
    headers: Option<&BTreeMap<String, Option<String>>>,
) -> Result<Option<OwnedHeaders>, KafkaLineSinkError> {
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
            return Err(invalid_kafka_cfg(
                "kafka.headers: empty header key is not allowed",
            ));
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

fn required_acks_rdkafka(v: Option<&str>) -> Result<&'static str, KafkaLineSinkError> {
    let Some(s) = v.map(str::trim).filter(|s| !s.is_empty()) else {
        return Ok("1");
    };
    if let Ok(i) = s.parse::<i64>() {
        return match i {
            -1 => Ok("all"),
            0 => Ok("0"),
            1 => Ok("1"),
            _ => Err(invalid_kafka_cfg(format!(
                "kafka.acks: unsupported integer {i} (expected -1, 0, or 1)"
            ))),
        };
    }
    required_acks_rdkafka_str(s)
}

fn required_acks_rdkafka_str(s: &str) -> Result<&'static str, KafkaLineSinkError> {
    if s.is_empty() {
        return Ok("1");
    }
    if let Ok(n) = s.parse::<i64>() {
        return match n {
            -1 => Ok("all"),
            0 => Ok("0"),
            1 => Ok("1"),
            _ => Err(invalid_kafka_cfg(format!(
                "kafka.acks: unsupported integer {n}"
            ))),
        };
    }
    match s.to_ascii_lowercase().as_str() {
        "all" => Ok("all"),
        "none" => Ok("0"),
        "leader" | "one" => Ok("1"),
        _ => Err(invalid_kafka_cfg(format!(
            "kafka.acks: unknown string {s:?}"
        ))),
    }
}

/// librdkafka `compression.type`：none、gzip、snappy、lz4、zstd。
fn compression_rdkafka(cs: Option<&str>) -> Result<Option<&'static str>, KafkaLineSinkError> {
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
            return Err(invalid_kafka_cfg(format!(
                "kafka.compression: unknown or unsupported {other:?} (try none, gzip, snappy, lz4, zstd)"
            )));
        }
    }))
}

fn parse_timeout_ms(s: Option<&str>) -> Result<u64, KafkaLineSinkError> {
    let Some(s) = s.map(str::trim).filter(|s| !s.is_empty()) else {
        return Ok(30_000);
    };
    s.parse::<u64>()
        .map_err(|_| invalid_kafka_cfg("kafka.timeout-ms: invalid string (expected positive integer ms)"))
}

fn timeout_ms_from_kafka(k: &KafkaConfig) -> Result<u64, KafkaLineSinkError> {
    parse_timeout_ms(k.timeout_ms.as_deref())
}

fn kafka_transport_mode(k: &KafkaConfig) -> Result<KafkaTransportMode, KafkaLineSinkError> {
    let proto = security_protocol_upper(k);
    let sasl = sasl_keys_present(k);
    match proto.as_deref() {
        None | Some("PLAINTEXT") => {
            if sasl {
                return Err(invalid_kafka_cfg(
                    "security.protocol is PLAINTEXT (or unset) but SASL fields are set; set security.protocol to SASL_PLAINTEXT/SASL_SSL and configure sasl.* for librdkafka (not fully wired in this worker yet).",
                ));
            }
            Ok(KafkaTransportMode::Plaintext)
        }
        Some("SSL") => Ok(KafkaTransportMode::Tls),
        Some("SASL_PLAINTEXT") | Some("SASL_SSL") => Err(invalid_kafka_cfg(format!(
            "security.protocol={:?}: SASL is not wired in this worker yet; use SSL or PLAINTEXT, or extend ClientConfig mapping.",
            proto.as_deref().unwrap_or("")
        ))),
        Some(other) => Err(invalid_kafka_cfg(format!(
            "security.protocol: unsupported value {other:?}"
        ))),
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum KafkaTransportMode {
    Plaintext,
    Tls,
}

fn build_rdkafka_client_config(
    k: &KafkaConfig,
    transport: KafkaTransportMode,
) -> Result<(ClientConfig, Duration), KafkaLineSinkError> {
    let brokers: Vec<String> = k
        .brokers
        .as_ref()
        .into_iter()
        .flat_map(|v| v.iter())
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();
    if brokers.is_empty() {
        return Err(invalid_kafka_cfg("kafka.brokers must list at least one broker"));
    }
    let topic_trimmed = effective_produce_topic(k);
    if topic_trimmed.is_empty() {
        return Err(invalid_kafka_cfg("kafka.topic must be non-empty (or use sink.kafka.mode: agent for fixed topic raw_message)"));
    }

    let tls_enabled = transport == KafkaTransportMode::Tls;
    if !tls_enabled && likely_encrypted_broker_config(k) {
        return Err(invalid_kafka_cfg(
            "TLS-related ssl.* or security.protocol is set but security.protocol is not SSL (e.g. still PLAINTEXT). \
             For TLS set security.protocol to SSL and supply trust/client material (PEM or JKS/P12 as documented).",
        ));
    }

    let timeout_ms = timeout_ms_from_kafka(k)?;
    let queue_timeout = Duration::from_millis(timeout_ms.clamp(1000, 300_000));

    let mut cfg = ClientConfig::new();
    cfg.set("bootstrap.servers", brokers.join(","));
    cfg.set("client.id", "logspout-worker");
    cfg.set("log.connection.close", "false");
    cfg.set("message.timeout.ms", timeout_ms.to_string());
    cfg.set(
        "delivery.timeout.ms",
        timeout_ms.saturating_add(5000).max(10_000).to_string(),
    );
    cfg.set("request.required.acks", required_acks_rdkafka(k.acks.as_deref())?);
    if let Some(ct) = compression_rdkafka(k.compression.as_deref())? {
        cfg.set("compression.type", ct);
    }

    match transport {
        KafkaTransportMode::Plaintext => {
            cfg.set("security.protocol", "PLAINTEXT");
        }
        KafkaTransportMode::Tls => {
            cfg.set("security.protocol", "ssl");
            super::kafka_jks::configure_librdkafka_ssl(&mut cfg, k)?;
        }
    }

    Ok((cfg, queue_timeout))
}

impl KafkaLineSink {
    pub fn try_new(k: &KafkaConfig) -> Result<Self, KafkaLineSinkError> {
        let brokers_display = k
            .brokers
            .as_ref()
            .into_iter()
            .flat_map(|v| v.iter())
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect::<Vec<_>>()
            .join(", ");

        let headers = if k.mode == KafkaSinkMode::Agent {
            None
        } else {
            owned_headers_from_kafka_cfg(k.headers.as_ref())?
        };
        let transport = kafka_transport_mode(k)?;
        let tls_enabled = transport == KafkaTransportMode::Tls;

        let (cfg, queue_timeout) = build_rdkafka_client_config(k, transport)?;

        let producer: FutureProducer = cfg
            .create()
            .map_err(KafkaLineSinkError::ProducerCreate)?;

        let topic = effective_produce_topic(k);
        let agent_state = if k.mode == KafkaSinkMode::Agent {
            Some(kafka_agent::build_runtime_state(k).map_err(KafkaLineSinkError::InvalidConfig)?)
        } else {
            None
        };

        Ok(Self {
            producer,
            brokers_display,
            topic,
            headers,
            kafka_config: k.clone(),
            tls_enabled,
            sasl_keys_in_yaml: sasl_keys_present(k),
            queue_timeout,
            agent_state,
        })
    }
}

#[async_trait]
impl LogLineSink for KafkaLineSink {
    async fn emit_line(&mut self, line: &str) -> Result<(), EmitLineError> {
        let producer = self.producer.clone();
        let topic = self.topic.clone();
        let topic_for_send = topic.clone();
        let headers = self.headers.clone();
        let brokers_display = self.brokers_display.clone();
        let tls_enabled = self.tls_enabled;
        let sasl_keys = self.sasl_keys_in_yaml;
        let kafka_config = self.kafka_config.clone();
        let queue_to = rdkafka::util::Timeout::After(self.queue_timeout);
        let send_cap = self.queue_timeout.saturating_mul(4).max(Duration::from_secs(30));

        let (payload, key) = if let Some(state) = &self.agent_state {
            let ts = wall_clock_ms_i64();
            let msg = kafka_agent::build_message(state, line, next_context_id(), ts);
            (msg.payload, msg.key)
        } else {
            (line.to_string(), None)
        };

        let fut = async move {
            let mut rec =
                FutureRecord::<'_, str, str>::to(topic_for_send.as_str()).payload(payload.as_str());
            if let Some(ref k) = key {
                rec = rec.key(k.as_str());
            }
            if let Some(h) = headers {
                rec = rec.headers(h);
            }
            producer.send(rec, queue_to).await
        };

        match timeout(send_cap, fut).await {
            Err(_) => Err(KafkaLineSinkError::Produce(format!(
                "kafka produce timed out after {:?} (brokers=[{}], topic={:?})",
                send_cap, brokers_display, topic
            ))
            .into()),
            Ok(dr) => match dr {
                Ok(_) => Ok(()),
                Err((e, _)) => Err(KafkaLineSinkError::Produce(format_produce_err(
                    &e,
                    &brokers_display,
                    &topic,
                    tls_enabled,
                    sasl_keys,
                    &kafka_config,
                ))
                .into()),
            },
        }
    }
}

/// 使用与 [`KafkaLineSink`] 相同的 librdkafka 配置拉取一次集群 metadata（broker 数 / topic 数）；集成测试见 `tests/kafka_probe.rs`。
pub(crate) fn probe_kafka_ssl_cluster(k: &KafkaConfig) -> Result<(usize, usize), KafkaLineSinkError> {
    let transport = kafka_transport_mode(k)?;
    let (cfg, _) = build_rdkafka_client_config(k, transport)?;
    let producer: FutureProducer = cfg.create().map_err(KafkaLineSinkError::ProducerCreate)?;
    let meta = producer
        .client()
        .fetch_metadata(None, Duration::from_secs(30))
        .map_err(|e| KafkaLineSinkError::InvalidConfig(format!("fetch_metadata: {e}")))?;
    let nb = meta.brokers().len();
    let nt = meta.topics().len();
    Ok((nb, nt))
}

/// 使用与 [`KafkaLineSink`] 相同的 TLS 配置发送**一条** UTF-8 文本到 `k.topic`（同步 flush）；集成测试见 `tests/kafka_probe.rs`。
pub(crate) fn produce_one_kafka_ssl_line(k: &KafkaConfig, payload: &str) -> Result<(), KafkaLineSinkError> {
    let transport = kafka_transport_mode(k)?;
    let (cfg, _) = build_rdkafka_client_config(k, transport)?;
    let producer: rdkafka::producer::ThreadedProducer<DefaultProducerContext> = cfg
        .create()
        .map_err(KafkaLineSinkError::ProducerCreate)?;
    let topic = effective_produce_topic(k);
    if topic.is_empty() {
        return Err(invalid_kafka_cfg("kafka.topic must be non-empty (or use sink.kafka.mode: agent for fixed topic raw_message)"));
    }
    producer
        .send(BaseRecord::<(), str>::to(topic.as_str()).payload(payload))
        .map_err(|(e, _)| KafkaLineSinkError::Produce(format!("kafka send: {e}")))?;
    producer
        .flush(Duration::from_secs(30))
        .map_err(|e| KafkaLineSinkError::Produce(format!("kafka flush: {e}")))?;
    drop(producer);
    Ok(())
}
