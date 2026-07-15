//! Kafka 行 sink：ClientConfig 构建见 [`client_config`]，投递/退避见 [`produce`]。

mod client_config;
mod produce;

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use backon::BackoffBuilder;
use futures_util::stream::{FuturesUnordered, StreamExt};
use logen_model::{KafkaConfig, KafkaSinkMode};
use rdkafka::error::KafkaError;
use rdkafka::message::OwnedHeaders;
use rdkafka::producer::future_producer::OwnedDeliveryResult;
use rdkafka::producer::{DeliveryFuture, FutureProducer, FutureRecord, Producer};
use tokio::sync::mpsc;

use super::context_id::next_context_id;
use super::kafka_agent::{self, RuntimeAgentConfig};
use super::{LogLineSink, SinkError};
use client_config::{create_future_producer, normalize_brokers, owned_headers_from_kafka_cfg};
use produce::{
    build_future_record, delivery_timeout_backoff_builder, format_produce_err,
    is_message_timed_out_error, is_queue_full_error, line_record_from_owned_message,
    queue_full_backoff_builder, should_log_delivery_timeout_retry, should_log_queue_full_retry,
    LineRecord, DELIVERY_TIMEOUT_RETRY_LIMIT, QUEUE_FULL_BACKOFF_MS_MAX,
};

pub struct KafkaLineSink {
    producer: FutureProducer,
    topic: Arc<str>,
    worker_id: Arc<str>,
    brokers_display: String,
    /// 每条消息附带的 record headers（与配置一致，逐条 clone）。`agent` 模式为 `None`。
    headers: Option<OwnedHeaders>,
    /// 克隆的 Kafka 段配置（用于错误上下文与 TLS/SASL 提示）。
    kafka_config: KafkaConfig,
    runtime_agent_config: Option<RuntimeAgentConfig>,
    retry_total: Arc<AtomicU64>,
    deliveries: FuturesUnordered<DeliveryFuture>,
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
        let (producer, topic) = create_future_producer(k)?;
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
            runtime_agent_config,
            retry_total,
            deliveries: FuturesUnordered::new(),
        })
    }

    fn build_line_record(&self, line: &str) -> LineRecord {
        if let Some(c) = self.runtime_agent_config.as_ref() {
            let ts = super::log_id::wall_clock_ms_u64().min(i64::MAX as u64) as i64;
            return kafka_agent::build_agent_message(c, line, next_context_id(), ts);
        }
        LineRecord {
            payload: line.as_bytes().to_vec(),
            key: None,
        }
    }

    fn produce_err(&self, e: &KafkaError) -> SinkError {
        SinkError::Kafka(format_produce_err(
            e,
            &self.brokers_display,
            self.topic.as_ref(),
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
    let (producer, _) = create_future_producer(k)?;
    let meta = producer
        .client()
        .fetch_metadata(None, Duration::from_secs(30))
        .map_err(|e| SinkError::Kafka(format!("fetch_metadata: {e}")))?;
    Ok((meta.brokers().len(), meta.topics().len()))
}

/// 集成测试：发送一条并 flush。
pub(crate) fn produce_one_kafka_ssl_line(k: &KafkaConfig, payload: &str) -> Result<(), SinkError> {
    let (producer, topic) = create_future_producer(k)?;
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
    use super::client_config::{
        build_rdkafka_client_config, configure_librdkafka_sasl, kafka_transport_mode,
        KafkaTransportMode, PRODUCER_BATCH_SIZE, PRODUCER_COMPRESSION, PRODUCER_LINGER_MS,
        PRODUCER_MESSAGE_MAX_BYTES, PRODUCER_QUEUE_MAX_KBYTES, PRODUCER_SOCKET_TIMEOUT_MS,
    };
    use super::*;
    use logen_model::KafkaConfig;
    use rdkafka::config::ClientConfig;
    use rdkafka::types::RDKafkaErrorCode;
    use serde_yaml::Value;
    use std::collections::BTreeMap;

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

    /// 测试内容：`SASL_PLAINTEXT` 映射为 SaslPlaintext，并写入 mechanism/username/password。
    /// 输入：完整 SASL_PLAINTEXT + PLAIN 字段。
    /// 预期：`security.protocol=sasl_plaintext`，三字段齐全。
    #[test]
    fn sasl_plaintext_wires_mechanism_username_password() {
        let k = KafkaConfig {
            security_protocol: Some("SASL_PLAINTEXT".into()),
            sasl_mechanism: Some("PLAIN".into()),
            sasl_username: Some("u".into()),
            sasl_password: Some("p".into()),
            ..minimal_plaintext_kafka()
        };
        assert!(matches!(
            kafka_transport_mode(&k).unwrap(),
            KafkaTransportMode::SaslPlaintext
        ));
        let (cfg, _) = build_rdkafka_client_config(&k, KafkaTransportMode::SaslPlaintext).unwrap();
        assert_eq!(
            cfg.get("security.protocol").map(String::from),
            Some("sasl_plaintext".into())
        );
        assert_eq!(
            cfg.get("sasl.mechanism").map(String::from),
            Some("PLAIN".into())
        );
        assert_eq!(cfg.get("sasl.username").map(String::from), Some("u".into()));
        assert_eq!(cfg.get("sasl.password").map(String::from), Some("p".into()));
    }

    /// 测试内容：SASL 协议缺 username 时 fail-fast。
    /// 输入：`SASL_PLAINTEXT` + mechanism，无 username。
    /// 预期：`configure_librdkafka_sasl` 报错含 `sasl.username`。
    #[test]
    fn sasl_requires_username() {
        let k = KafkaConfig {
            security_protocol: Some("SASL_PLAINTEXT".into()),
            sasl_mechanism: Some("PLAIN".into()),
            sasl_password: Some("p".into()),
            ..minimal_plaintext_kafka()
        };
        let err = configure_librdkafka_sasl(&mut ClientConfig::new(), &k).unwrap_err();
        assert!(err.to_string().contains("sasl.username"), "{err}");
    }

    /// 测试内容：PLAINTEXT 却带 sasl.* 字段时报错。
    /// 输入：未设 protocol + sasl.username。
    /// 预期：`kafka_transport_mode` 提示改用 SASL_*。
    #[test]
    fn plaintext_rejects_sasl_fields() {
        let k = KafkaConfig {
            sasl_username: Some("u".into()),
            ..minimal_plaintext_kafka()
        };
        let err = kafka_transport_mode(&k).unwrap_err();
        assert!(err.to_string().contains("SASL_"), "{err}");
    }
}
