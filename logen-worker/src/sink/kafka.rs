use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use backon::{BackoffBuilder, ExponentialBuilder};
use futures_util::stream::{FuturesUnordered, StreamExt};
use logen_dsl::{KafkaConfig, KafkaSinkMode};
use rdkafka::config::ClientConfig;
use rdkafka::error::KafkaError;
use rdkafka::message::{Header, Message, OwnedHeaders, OwnedMessage};
use rdkafka::producer::future_producer::OwnedDeliveryResult;
use rdkafka::producer::{DeliveryFuture, FutureProducer, FutureRecord, Producer};
use rdkafka::types::RDKafkaErrorCode;
use serde_yaml::Value;
use tokio::sync::mpsc;

use super::context_id::next_context_id;
use super::kafka_agent::{self, RuntimeAgentConfig};
use super::{KafkaConfigError, LogLineSink, SinkError};

fn cfg_err(msg: impl Into<String>) -> KafkaConfigError {
    KafkaConfigError::new(msg)
}

const PRODUCER_QUEUE_MAX_KBYTES: &str = "65536";
const PRODUCER_BATCH_SIZE: &str = "65536";
const PRODUCER_LINGER_MS: &str = "10";
const PRODUCER_MESSAGE_MAX_BYTES: &str = "10485760";
const PRODUCER_COMPRESSION: &str = "lz4";
const PRODUCER_SOCKET_TIMEOUT_MS: &str = "60000";
const QUEUE_FULL_BACKOFF_MS_MIN: u64 = 1;
const QUEUE_FULL_BACKOFF_MS_MAX: u64 = 100;
const DELIVERY_TIMEOUT_BACKOFF_MS_MIN: u64 = 10;
const DELIVERY_TIMEOUT_RETRY_LIMIT: u32 = 3;
const DELIVERY_TIMEOUT_BACKOFF_MS_MAX: u64 = 1000;

#[derive(Clone)]
struct LineRecord {
    payload: String,
    key: Option<String>,
}

pub struct KafkaLineSink {
    producer: FutureProducer,
    topic: Arc<str>,
    worker_id: Arc<str>,
    brokers_display: String,
    /// 每条消息附带的 record headers（与配置一致，逐条 clone）。`agent` 模式为 `None`。
    headers: Option<OwnedHeaders>,
    /// 克隆的 Kafka 段配置（用于错误上下文与 TLS/SASL 提示）。
    kafka_config: KafkaConfig,
    tls_enabled: bool,
    sasl_keys_in_yaml: bool,
    runtime_agent_config: Option<RuntimeAgentConfig>,
    retry_total: Arc<AtomicU64>,
    deliveries: FuturesUnordered<DeliveryFuture>,
}

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

fn create_future_producer(
    k: &KafkaConfig,
) -> Result<(FutureProducer, String, KafkaTransportMode), SinkError> {
    let transport = kafka_transport_mode(k)?;
    let (cfg, topic) = build_rdkafka_client_config(k, transport)?;
    let producer: FutureProducer = cfg
        .create()
        .map_err(|e| SinkError::Kafka(format!("failed to create Kafka producer: {e}")))?;
    Ok((producer, topic, transport))
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

fn is_queue_full_error(e: &KafkaError) -> bool {
    matches!(
        e,
        KafkaError::MessageProduction(RDKafkaErrorCode::QueueFull)
    )
}

fn is_message_timed_out_error(e: &KafkaError) -> bool {
    matches!(
        e,
        KafkaError::MessageProduction(RDKafkaErrorCode::MessageTimedOut)
    )
}

fn queue_full_backoff_builder() -> ExponentialBuilder {
    ExponentialBuilder::default()
        .with_min_delay(Duration::from_millis(QUEUE_FULL_BACKOFF_MS_MIN))
        .with_max_delay(Duration::from_millis(QUEUE_FULL_BACKOFF_MS_MAX))
        .without_max_times()
}

fn delivery_timeout_backoff_builder() -> ExponentialBuilder {
    ExponentialBuilder::default()
        .with_min_delay(Duration::from_millis(DELIVERY_TIMEOUT_BACKOFF_MS_MIN))
        .with_max_delay(Duration::from_millis(DELIVERY_TIMEOUT_BACKOFF_MS_MAX))
        .with_max_times(DELIVERY_TIMEOUT_RETRY_LIMIT as usize)
}

fn should_log_queue_full_retry(attempt: u32) -> bool {
    attempt == 0 || (attempt + 1).is_power_of_two()
}

fn should_log_delivery_timeout_retry(attempt: u32) -> bool {
    attempt == 0 || attempt + 1 == DELIVERY_TIMEOUT_RETRY_LIMIT
}

fn build_future_record<'a>(
    topic: &'a str,
    line: &'a LineRecord,
    headers: Option<&OwnedHeaders>,
) -> FutureRecord<'a, str, str> {
    let mut record = FutureRecord::to(topic).payload(line.payload.as_str());
    if let Some(ref key) = line.key {
        record = record.key(key.as_str());
    }
    if let Some(h) = headers {
        record = record.headers(h.clone());
    }
    record
}

fn line_record_from_owned_message(msg: OwnedMessage) -> LineRecord {
    let payload = msg
        .payload()
        .map(|p| String::from_utf8_lossy(p).into_owned())
        .unwrap_or_default();
    let key = msg.key().map(|k| String::from_utf8_lossy(k).into_owned());
    LineRecord { payload, key }
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

fn normalize_brokers(k: &KafkaConfig) -> Vec<String> {
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

fn kafka_transport_mode(k: &KafkaConfig) -> Result<KafkaTransportMode, KafkaConfigError> {
    let proto = security_protocol_upper(k);
    let sasl = sasl_keys_present(k);
    match proto.as_deref() {
        None | Some("PLAINTEXT") => {
            if sasl {
                return Err(cfg_err(
                    "security.protocol is PLAINTEXT (or unset) but SASL fields are set; set security.protocol to SASL_PLAINTEXT/SASL_SSL and configure sasl.* for librdkafka (not fully wired in this worker yet).",
                ));
            }
            Ok(KafkaTransportMode::Plaintext)
        }
        Some("SSL") => Ok(KafkaTransportMode::Tls),
        Some("SASL_PLAINTEXT") | Some("SASL_SSL") => Err(cfg_err(format!(
            "security.protocol={:?}: SASL is not wired in this worker yet; use SSL or PLAINTEXT, or extend ClientConfig mapping.",
            proto.as_deref().unwrap_or("")
        ))),
        Some(other) => Err(cfg_err(format!("security.protocol: unsupported value {other:?}"))),
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

    let tls_enabled = transport == KafkaTransportMode::Tls;
    if !tls_enabled && likely_encrypted_broker_config(k) {
        return Err(cfg_err(
            "TLS-related ssl.* or security.protocol is set but security.protocol is not SSL (e.g. still PLAINTEXT). \
             For TLS set security.protocol to SSL and supply trust/client material (PEM or JKS/P12 as documented).",
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
            super::kafka_jks::configure_librdkafka_ssl(&mut cfg, k)?;
        }
    }

    apply_kafka_extras(&mut cfg, &k.extras);

    Ok((cfg, topic_trimmed))
}

impl KafkaLineSink {
    pub fn new(
        k: &KafkaConfig,
        worker_id: &str,
        retry_total: Arc<AtomicU64>,
    ) -> Result<Self, SinkError> {
        let brokers_display = normalize_brokers(k).join(", ");
        let headers = if k.mode == KafkaSinkMode::Agent {
            None
        } else {
            owned_headers_from_kafka_cfg(k.headers.as_ref())?
        };
        let (producer, topic, transport) = create_future_producer(k)?;
        let tls_enabled = transport == KafkaTransportMode::Tls;
        let runtime_agent_config = if k.mode == KafkaSinkMode::Agent {
            Some(kafka_agent::build_runtime_agent_config(k)?)
        } else {
            None
        };

        Ok(Self {
            producer,
            topic: Arc::from(topic),
            worker_id: Arc::from(worker_id),
            brokers_display,
            headers,
            kafka_config: k.clone(),
            tls_enabled,
            sasl_keys_in_yaml: sasl_keys_present(k),
            runtime_agent_config,
            retry_total,
            deliveries: FuturesUnordered::new(),
        })
    }

    fn build_line_record(&self, line: &str) -> LineRecord {
        if let Some(c) = self.runtime_agent_config.as_ref() {
            let ts = super::log_id::wall_clock_ms_u64().min(i64::MAX as u64) as i64;
            let msg = kafka_agent::build_agent_message(c, line, next_context_id(), ts);
            return LineRecord {
                payload: msg.payload,
                key: msg.key,
            };
        }
        LineRecord {
            payload: line.to_string(),
            key: None,
        }
    }

    fn produce_err(&self, e: &KafkaError) -> SinkError {
        SinkError::Kafka(format_produce_err(
            e,
            &self.brokers_display,
            self.topic.as_ref(),
            self.tls_enabled,
            self.sasl_keys_in_yaml,
            &self.kafka_config,
        ))
    }

    fn classify_delivery(
        &self,
        delivery: OwnedDeliveryResult,
    ) -> Result<Option<LineRecord>, SinkError> {
        match delivery {
            Ok(_) => Ok(None),
            Err((e, msg)) if is_message_timed_out_error(&e) => {
                tracing::error!(
                    worker_id = %self.worker_id.as_ref(),
                    topic = %self.topic.as_ref(),
                    "kafka delivery failed: {e}"
                );
                Ok(Some(line_record_from_owned_message(msg)))
            }
            Err((e, _)) => Err(self.produce_err(&e)),
        }
    }

    async fn handle_delivery_outcome(
        &mut self,
        res: Result<OwnedDeliveryResult, futures_channel::oneshot::Canceled>,
    ) -> Result<(), SinkError> {
        let delivery = res.map_err(|_| {
            SinkError::Kafka(format!(
                "kafka producer dropped before delivery (worker_id={}, topic={})",
                self.worker_id.as_ref(),
                self.topic.as_ref()
            ))
        })?;
        if let Some(line) = self.classify_delivery(delivery)? {
            self.retry_timed_out_line(line).await?;
        }
        Ok(())
    }

    async fn send_result_with_queue_full_retry(
        &mut self,
        line: &LineRecord,
    ) -> Result<DeliveryFuture, SinkError> {
        let topic = self.topic.clone();
        let mut attempt = 0u32;
        let mut queue_full_backoff = queue_full_backoff_builder().build();
        loop {
            let record = build_future_record(topic.as_ref(), line, self.headers.as_ref());
            match self.producer.send_result(record) {
                Ok(fut) => return Ok(fut),
                Err((e, _)) if is_queue_full_error(&e) => {
                    let backoff = queue_full_backoff
                        .next()
                        .unwrap_or(Duration::from_millis(QUEUE_FULL_BACKOFF_MS_MAX));
                    self.retry_total.fetch_add(1, Ordering::Relaxed);
                    if should_log_queue_full_retry(attempt) {
                        tracing::warn!(
                            topic = %topic.as_ref(),
                            attempt = attempt + 1,
                            backoff_ms = backoff.as_millis(),
                            "kafka producer queue full; retrying send"
                        );
                    }
                    attempt = attempt.saturating_add(1);
                    tokio::time::sleep(backoff).await;
                }
                Err((e, _)) => return Err(self.produce_err(&e)),
            }
        }
    }

    async fn retry_timed_out_line(&mut self, mut line: LineRecord) -> Result<(), SinkError> {
        let topic = self.topic.clone();
        let mut timeout_attempt = 0u32;
        let mut delivery_timeout_backoff = delivery_timeout_backoff_builder().build();
        loop {
            let Some(backoff) = delivery_timeout_backoff.next() else {
                return Err(SinkError::Kafka(format!(
                    "kafka delivery timed out after {DELIVERY_TIMEOUT_RETRY_LIMIT} retries (topic={})",
                    topic.as_ref()
                )));
            };
            self.retry_total.fetch_add(1, Ordering::Relaxed);
            if should_log_delivery_timeout_retry(timeout_attempt) {
                tracing::warn!(
                    topic = %topic.as_ref(),
                    attempt = timeout_attempt + 1,
                    backoff_ms = backoff.as_millis(),
                    "kafka delivery timed out; retrying send"
                );
            }
            timeout_attempt = timeout_attempt.saturating_add(1);
            tokio::time::sleep(backoff).await;

            let fut = self.send_result_with_queue_full_retry(&line).await?;
            let delivery = fut.await.map_err(|_| {
                SinkError::Kafka(format!(
                    "kafka producer dropped before delivery (worker_id={}, topic={})",
                    self.worker_id.as_ref(),
                    topic.as_ref()
                ))
            })?;
            if let Some(next) = self.classify_delivery(delivery)? {
                line = next;
            } else {
                return Ok(());
            }
        }
    }
}

impl Drop for KafkaLineSink {
    fn drop(&mut self) {
        if let Err(e) = self.producer.flush(Duration::from_secs(15)) {
            tracing::warn!(
                worker_id = %self.worker_id.as_ref(),
                topic = %self.topic.as_ref(),
                in_flight = self.deliveries.len(),
                "kafka flush on drop: {e}"
            );
        }
    }
}

#[async_trait]
impl LogLineSink for KafkaLineSink {
    async fn drain_lines(&mut self, mut line_rx: mpsc::Receiver<String>) -> Result<(), SinkError> {
        loop {
            tokio::select! {
                res = self.deliveries.next(), if !self.deliveries.is_empty() => {
                    if let Some(r) = res {
                        self.handle_delivery_outcome(r).await?;
                    }
                }
                line = line_rx.recv() => {
                    match line {
                        Some(line) => {
                            let line_rec = self.build_line_record(&line);
                            let fut = self.send_result_with_queue_full_retry(&line_rec).await?;
                            self.deliveries.push(fut);
                        }
                        None => {
                            while let Some(res) = self.deliveries.next().await {
                                self.handle_delivery_outcome(res).await?;
                            }
                            return Ok(());
                        }
                    }
                }
            }
        }
    }
}

/// 集成测试：metadata 探针。
pub(crate) fn probe_kafka_ssl_cluster(k: &KafkaConfig) -> Result<(usize, usize), SinkError> {
    let (producer, _, _) = create_future_producer(k)?;
    let meta = producer
        .client()
        .fetch_metadata(None, Duration::from_secs(30))
        .map_err(|e| SinkError::Kafka(format!("fetch_metadata: {e}")))?;
    Ok((meta.brokers().len(), meta.topics().len()))
}

/// 集成测试：发送一条并 flush。
pub(crate) fn produce_one_kafka_ssl_line(k: &KafkaConfig, payload: &str) -> Result<(), SinkError> {
    let (producer, topic, _) = create_future_producer(k)?;
    let delivery = producer
        .send_result(FutureRecord::<(), str>::to(topic.as_str()).payload(payload))
        .map_err(|(e, _)| SinkError::Kafka(format!("kafka send: {e}")))?;
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| SinkError::Kafka(format!("tokio runtime: {e}")))?;
    rt.block_on(async {
        match delivery.await {
            Ok(Ok(_)) => Ok(()),
            Ok(Err((e, _))) => Err(SinkError::Kafka(format!("kafka delivery: {e}"))),
            Err(_) => Err(SinkError::Kafka(
                "kafka producer dropped before delivery".into(),
            )),
        }
    })?;
    producer
        .flush(Duration::from_secs(30))
        .map_err(|e| SinkError::Kafka(format!("kafka flush: {e}")))?;
    Ok(())
}

#[cfg(test)]
mod producer_profile_tests {
    use super::*;
    use logen_dsl::KafkaConfig;

    fn minimal_plaintext_kafka() -> KafkaConfig {
        KafkaConfig {
            brokers: Some(vec!["127.0.0.1:9092".to_string()]),
            topic: Some("t1".to_string()),
            ..Default::default()
        }
    }

    #[test]
    fn builtin_producer_profile_maps_java_defaults() {
        let k = minimal_plaintext_kafka();
        let (cfg, _) = build_rdkafka_client_config(&k, KafkaTransportMode::Plaintext).unwrap();
        assert_eq!(
            cfg.get("queue.buffering.max.kbytes").map(String::from),
            Some(PRODUCER_QUEUE_MAX_KBYTES.to_string())
        );
        assert_eq!(
            cfg.get("batch.size").map(String::from),
            Some(PRODUCER_BATCH_SIZE.to_string())
        );
        assert_eq!(
            cfg.get("queue.buffering.max.ms").map(String::from),
            Some(PRODUCER_LINGER_MS.to_string())
        );
        assert_eq!(
            cfg.get("message.max.bytes").map(String::from),
            Some(PRODUCER_MESSAGE_MAX_BYTES.to_string())
        );
        assert_eq!(
            cfg.get("compression.type").map(String::from),
            Some(PRODUCER_COMPRESSION.to_string())
        );
        assert_eq!(
            cfg.get("socket.timeout.ms").map(String::from),
            Some(PRODUCER_SOCKET_TIMEOUT_MS.to_string())
        );
    }

    /// 测试内容：YAML `compression.type` 覆盖内置 lz4。
    /// 输入：`compression.type: gzip`。
    /// 预期：librdkafka 配置为 `gzip`。
    #[test]
    fn yaml_compression_type_overrides_builtin_lz4() {
        let k = KafkaConfig {
            compression_type: Some("gzip".into()),
            ..minimal_plaintext_kafka()
        };
        let (cfg, _) = build_rdkafka_client_config(&k, KafkaTransportMode::Plaintext).unwrap();
        assert_eq!(
            cfg.get("compression.type").map(String::from),
            Some("gzip".to_string())
        );
    }

    /// 测试内容：extras 中 `compression.type` 覆盖一等字段。
    /// 输入：字段 `gzip`，extras `zstd`。
    /// 预期：最终为 `zstd`。
    #[test]
    fn extras_compression_type_overrides_field() {
        let mut extras = BTreeMap::new();
        extras.insert("compression.type".into(), Value::String("zstd".into()));
        let k = KafkaConfig {
            compression_type: Some("gzip".into()),
            extras,
            ..minimal_plaintext_kafka()
        };
        let (cfg, _) = build_rdkafka_client_config(&k, KafkaTransportMode::Plaintext).unwrap();
        assert_eq!(
            cfg.get("compression.type").map(String::from),
            Some("zstd".to_string())
        );
    }

    #[test]
    fn kafka_extras_override_builtin_profile() {
        let mut extras = BTreeMap::new();
        extras.insert("batch.size".into(), Value::Number(131072.into()));
        let k = KafkaConfig {
            extras,
            ..minimal_plaintext_kafka()
        };
        let (cfg, _) = build_rdkafka_client_config(&k, KafkaTransportMode::Plaintext).unwrap();
        assert_eq!(
            cfg.get("batch.size").map(String::from),
            Some("131072".to_string())
        );
    }

    /// 测试内容：`QueueFull` 属于可重试错误，其它生产错误不应误判。
    /// 输入：`KafkaError::MessageProduction(QueueFull)` 与 `KafkaError::Canceled`。
    /// 预期：前者返回 `true`，后者返回 `false`。
    #[test]
    fn detect_queue_full_error_for_retry() {
        assert!(is_queue_full_error(&KafkaError::MessageProduction(
            RDKafkaErrorCode::QueueFull
        )));
        assert!(!is_queue_full_error(&KafkaError::Canceled));
    }

    /// 测试内容：Kafka delivery timeout 会被识别为有限次可恢复重试错误。
    /// 输入：`KafkaError::MessageProduction(MessageTimedOut)` 与 `KafkaError::Canceled`。
    /// 预期：前者返回 `true`，后者返回 `false`。
    #[test]
    fn detect_message_timed_out_error_for_retry() {
        assert!(is_message_timed_out_error(&KafkaError::MessageProduction(
            RDKafkaErrorCode::MessageTimedOut
        )));
        assert!(!is_message_timed_out_error(&KafkaError::Canceled));
    }
}
