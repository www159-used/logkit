//! Kafka produce 辅助：错误文案、退避、FutureRecord 拼装。

use std::time::Duration;

use backon::ExponentialBuilder;
use logen_model::KafkaConfig;
use rdkafka::error::KafkaError;
use rdkafka::message::{Message, OwnedHeaders, OwnedMessage};
use rdkafka::producer::FutureRecord;
use rdkafka::types::RDKafkaErrorCode;

use super::super::kafka_agent::KafkaAgentMessage;

pub(super) type LineRecord = KafkaAgentMessage;

pub(super) const QUEUE_FULL_BACKOFF_MS_MIN: u64 = 1;
pub(super) const QUEUE_FULL_BACKOFF_MS_MAX: u64 = 100;
pub(super) const DELIVERY_TIMEOUT_BACKOFF_MS_MIN: u64 = 10;
pub(super) const DELIVERY_TIMEOUT_RETRY_LIMIT: u32 = 3;
pub(super) const DELIVERY_TIMEOUT_BACKOFF_MS_MAX: u64 = 1000;

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

pub(super) fn format_produce_err(
    e: &KafkaError,
    brokers_display: &str,
    topic: &str,
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
    if k.has_sasl_material() {
        s.push_str(
            "\nNote: SASL-related fields are set. Ensure sasl.mechanism, sasl.username, and sasl.password are correct and the broker supports the mechanism.",
        );
    }
    s
}

pub(super) fn is_queue_full_error(e: &KafkaError) -> bool {
    matches!(
        e,
        KafkaError::MessageProduction(RDKafkaErrorCode::QueueFull)
    )
}

pub(super) fn is_message_timed_out_error(e: &KafkaError) -> bool {
    matches!(
        e,
        KafkaError::MessageProduction(RDKafkaErrorCode::MessageTimedOut)
    )
}

pub(super) fn queue_full_backoff_builder() -> ExponentialBuilder {
    ExponentialBuilder::default()
        .with_min_delay(Duration::from_millis(QUEUE_FULL_BACKOFF_MS_MIN))
        .with_max_delay(Duration::from_millis(QUEUE_FULL_BACKOFF_MS_MAX))
        .without_max_times()
}

pub(super) fn delivery_timeout_backoff_builder() -> ExponentialBuilder {
    ExponentialBuilder::default()
        .with_min_delay(Duration::from_millis(DELIVERY_TIMEOUT_BACKOFF_MS_MIN))
        .with_max_delay(Duration::from_millis(DELIVERY_TIMEOUT_BACKOFF_MS_MAX))
        .with_max_times(DELIVERY_TIMEOUT_RETRY_LIMIT as usize)
}

pub(super) fn should_log_queue_full_retry(attempt: u32) -> bool {
    attempt == 0 || (attempt + 1).is_power_of_two()
}

pub(super) fn should_log_delivery_timeout_retry(attempt: u32) -> bool {
    attempt == 0 || attempt + 1 == DELIVERY_TIMEOUT_RETRY_LIMIT
}

pub(super) fn build_future_record<'a>(
    topic: &'a str,
    line: &'a LineRecord,
    headers: Option<&OwnedHeaders>,
) -> FutureRecord<'a, str, [u8]> {
    let mut record = FutureRecord::to(topic).payload(line.payload.as_slice());
    if let Some(ref key) = line.key {
        record = record.key(key.as_str());
    }
    if let Some(h) = headers {
        record = record.headers(h.clone());
    }
    record
}

pub(super) fn line_record_from_owned_message(msg: OwnedMessage) -> LineRecord {
    let payload = msg.payload().map(|p| p.to_vec()).unwrap_or_default();
    let key = msg.key().map(|k| String::from_utf8_lossy(k).into_owned());
    LineRecord { payload, key }
}
