use fake::faker::internet::en::Password;
use fake::Fake;
use logen_model::{validate_agent_source_id, KafkaAgentFormat, KafkaConfig, KafkaSinkMode};
use logen_proto::EventInfo;
use prost::Message;
use uuid::Uuid;

use super::log_id::next_log_id;
use super::{KafkaConfigError, SinkError};

pub const KAFKA_AGENT_TOPIC: &str = "raw_message";

#[derive(Debug, Clone)]
pub struct RuntimeAgentConfig {
    pub format: KafkaAgentFormat,
    pub domain: String,
    pub domain_token: String,
    pub appname: String,
    pub source: String,
    pub token: String,
    pub tag: String,
    pub hostname: String,
    pub ip: String,
    pub source_type: String,
    pub source_id: String,
    pub flag: i64,
    pub fields: String,
}

#[derive(Debug, Clone)]
pub struct KafkaAgentMessage {
    pub payload: Vec<u8>,
    pub key: Option<String>,
}

fn random_alphanum(len: usize) -> String {
    Password(len..len.saturating_add(1)).fake()
}

fn detect_hostname() -> String {
    hostname::get()
        .ok()
        .and_then(|h| h.into_string().ok())
        .unwrap_or_default()
}

fn detect_local_ip() -> String {
    resolve_oem::local_ip_or_empty()
}

fn trimmed_non_empty(opt: Option<&str>) -> Option<&str> {
    opt.map(str::trim).filter(|s| !s.is_empty())
}

fn or_random(opt: Option<&str>, rnd_len: usize) -> String {
    match trimmed_non_empty(opt) {
        Some(s) => s.to_string(),
        None => random_alphanum(rnd_len),
    }
}

fn non_empty_string(s: &str) -> Option<String> {
    (!s.is_empty()).then(|| s.to_string())
}

/// 由已通过校验的 `KafkaConfig`（`mode == Agent`）构造 [`RuntimeAgentConfig`]。
pub fn build_runtime_agent_config(k: &KafkaConfig) -> Result<RuntimeAgentConfig, SinkError> {
    if k.mode != KafkaSinkMode::Agent {
        return Err(SinkError::Internal(
            "build_runtime_agent_config requires mode agent".into(),
        ));
    }
    let agent = k
        .agent
        .as_ref()
        .ok_or_else(|| SinkError::Internal("missing sink.kafka.agent after validation".into()))?;
    let domain = agent.domain.as_deref().unwrap_or("").trim().to_string();

    let source_id = match trimmed_non_empty(agent.source_id.as_deref()) {
        Some(s) => {
            if !validate_agent_source_id(s) {
                return Err(KafkaConfigError::new(
                    "sink.kafka.agent.source_id must be a 36-character UUID (8-4-4-4-12 hex with hyphens)",
                )
                .into());
            }
            s.to_string()
        }
        None => Uuid::new_v4().to_string(),
    };

    let hostname = trimmed_non_empty(agent.hostname.as_deref())
        .map(str::to_string)
        .unwrap_or_else(detect_hostname);
    let ip = trimmed_non_empty(agent.ip.as_deref())
        .map(str::to_string)
        .unwrap_or_else(detect_local_ip);

    let domain_token = if agent.format == KafkaAgentFormat::Json {
        or_random(agent.domain_token.as_deref(), 16)
    } else {
        String::new()
    };

    Ok(RuntimeAgentConfig {
        format: agent.format,
        domain,
        domain_token,
        appname: or_random(agent.appname.as_deref(), 12),
        source: or_random(agent.source.as_deref(), 12),
        token: or_random(agent.token.as_deref(), 16),
        tag: or_random(agent.tag.as_deref(), 8),
        hostname,
        ip,
        source_type: random_alphanum(12),
        source_id,
        flag: agent.flag.unwrap_or(0),
        fields: agent
            .fields
            .as_deref()
            .map(|s| s.to_string())
            .unwrap_or_default(),
    })
}

#[derive(serde::Serialize)]
struct AgentConfig<'agent_config> {
    #[serde(skip_serializing_if = "str::is_empty")]
    domain: &'agent_config str,
    #[serde(skip_serializing_if = "str::is_empty")]
    domain_token: &'agent_config str,
    #[serde(skip_serializing_if = "str::is_empty")]
    appname: &'agent_config str,
    #[serde(skip_serializing_if = "str::is_empty")]
    tag: &'agent_config str,
    #[serde(skip_serializing_if = "str::is_empty")]
    token: &'agent_config str,
    #[serde(skip_serializing_if = "str::is_empty")]
    hostname: &'agent_config str,
    #[serde(skip_serializing_if = "str::is_empty")]
    log_id: &'agent_config str,
    context_id: i64,
    timestamp: i64,
    recv_timestamp: i64,
    log_timestamp: i64,
    raw_message: &'agent_config str,
    source_update_timestamp: i64,
    #[serde(skip_serializing_if = "str::is_empty")]
    source: &'agent_config str,
    #[serde(skip_serializing_if = "str::is_empty")]
    ip: &'agent_config str,
    flag: i64,
    #[serde(skip_serializing_if = "str::is_empty")]
    fields: &'agent_config str,
    #[serde(skip_serializing_if = "str::is_empty")]
    source_type: &'agent_config str,
    #[serde(skip_serializing_if = "str::is_empty")]
    source_id: &'agent_config str,
}

pub fn build_agent_message(
    runtime_config: &RuntimeAgentConfig,
    raw_message: &str,
    context_id: i64,
    ts_ms: i64,
) -> KafkaAgentMessage {
    let log_id = next_log_id();
    let payload = match runtime_config.format {
        KafkaAgentFormat::Json => build_agent_json_payload(
            runtime_config,
            raw_message,
            context_id,
            ts_ms,
            log_id.as_str(),
        ),
        KafkaAgentFormat::Pb => build_event_info(
            runtime_config,
            raw_message,
            context_id,
            ts_ms,
            log_id.as_str(),
        )
        .encode_to_vec(),
    };
    KafkaAgentMessage {
        payload,
        key: Some(log_id),
    }
}

fn build_agent_json_payload(
    runtime_config: &RuntimeAgentConfig,
    raw_message: &str,
    context_id: i64,
    ts_ms: i64,
    log_id: &str,
) -> Vec<u8> {
    let agent_config = AgentConfig {
        domain: runtime_config.domain.as_str(),
        domain_token: runtime_config.domain_token.as_str(),
        appname: runtime_config.appname.as_str(),
        tag: runtime_config.tag.as_str(),
        token: runtime_config.token.as_str(),
        hostname: runtime_config.hostname.as_str(),
        log_id,
        context_id,
        timestamp: ts_ms,
        recv_timestamp: ts_ms,
        log_timestamp: ts_ms,
        raw_message,
        source_update_timestamp: ts_ms,
        source: runtime_config.source.as_str(),
        ip: runtime_config.ip.as_str(),
        flag: runtime_config.flag,
        fields: runtime_config.fields.as_str(),
        source_type: runtime_config.source_type.as_str(),
        source_id: runtime_config.source_id.as_str(),
    };
    // 字段均为 &str / i64，serde 失败仅表示编程错误。
    serde_json::to_string(&agent_config)
        .expect("agent JSON serialization")
        .into_bytes()
}

fn build_event_info(
    runtime_config: &RuntimeAgentConfig,
    raw_message: &str,
    context_id: i64,
    ts_ms: i64,
    log_id: &str,
) -> EventInfo {
    EventInfo {
        domain: non_empty_string(&runtime_config.domain),
        appname: non_empty_string(&runtime_config.appname),
        tag: non_empty_string(&runtime_config.tag),
        token: non_empty_string(&runtime_config.token),
        hostname: non_empty_string(&runtime_config.hostname),
        log_id: Some(log_id.to_string()),
        context_id: Some(context_id),
        timestamp: Some(ts_ms),
        recv_timestamp: Some(ts_ms),
        log_timestamp: Some(ts_ms),
        raw_message: Some(raw_message.to_string()),
        source_update_timestamp: Some(ts_ms),
        source: non_empty_string(&runtime_config.source),
        ip: non_empty_string(&runtime_config.ip),
        flag: Some(runtime_config.flag),
        fields: non_empty_string(&runtime_config.fields),
        source_type: non_empty_string(&runtime_config.source_type),
        source_id: Some(runtime_config.source_id.as_bytes().to_vec()),
    }
}

#[cfg(test)]
mod tests {
    use logen_model::KafkaAgentFormat;
    use logen_proto::EventInfo;
    use prost::Message;

    use crate::agent_fixtures::{self, BENCH_YAML, NO_DOMAIN_YAML};

    use super::*;

    /// 测试内容：agent 模式在 `domain` 为空时应省略 `domain` 字段，但仍保留默认 `flag`。
    /// 输入：`mode: agent` 且 `agent.domain` 省略的最小 Kafka 配置，生成一条 `{}` 原始消息。
    /// 预期：输出 JSON 不含 `domain`，且包含默认 `flag: 0`。
    #[test]
    fn build_payload_omits_empty_domain() {
        let runtime_config =
            agent_fixtures::agent_runtime_config(NO_DOMAIN_YAML, KafkaAgentFormat::Json).unwrap();
        assert!(runtime_config.domain.is_empty());
        let j =
            String::from_utf8(build_agent_message(&runtime_config, "{}", 1, 1700000000000).payload)
                .unwrap();
        assert!(!j.contains("\"domain\""));
        assert!(j.contains("\"flag\":0"));
    }

    /// 测试内容：agent 模式生成的 JSON 中 `log_id` 与 Kafka message key 应保持一致。
    /// 输入：带 `domain` 的最小 agent Kafka 配置，以及一条 JSON 原始消息。
    /// 预期：输出包含 `domain`、`raw_message`、`context_id`；解析出的 `log_id` 与 `key` 相同，且默认 `flag` 为 `0`。
    #[test]
    fn build_agent_message_contains_domain_raw_message_and_key_matches_log_id() {
        let runtime_config =
            agent_fixtures::agent_runtime_config(BENCH_YAML, KafkaAgentFormat::Json).unwrap();
        let m = build_agent_message(&runtime_config, r#"{"x":1}"#, 123, 1700000000000);
        let j = String::from_utf8(m.payload).unwrap();
        assert!(j.contains("\"domain\":\"dom1\""));
        assert!(j.contains("\"raw_message\":\"{\\\"x\\\":1}\""));
        assert!(j.contains("\"context_id\":123"));
        let v: serde_json::Value = serde_json::from_str(&j).unwrap();
        let log_id = v["log_id"].as_str().unwrap();
        assert_eq!(v["flag"].as_i64(), Some(0));
        assert_eq!(m.key.as_deref(), Some(log_id));
    }

    /// 测试内容：agent `format: pb` 时 payload 为可解码的 `EventInfo` 且 `log_id` 与 key 一致。
    /// 输入：`format: pb`、带 `domain` 与 `source_id` 的配置及一条原始消息。
    /// 预期：`EventInfo` 字段正确；`source_id` 为 UTF-8 bytes；`key` 等于 `log_id`。
    #[test]
    fn build_agent_message_pb_encodes_event_info() {
        let runtime_config =
            agent_fixtures::agent_runtime_config(BENCH_YAML, KafkaAgentFormat::Pb).unwrap();
        assert_eq!(runtime_config.format, KafkaAgentFormat::Pb);
        assert!(runtime_config.domain_token.is_empty());
        let m = build_agent_message(&runtime_config, r#"{"x":1}"#, 123, 1_700_000_000_000);
        let event = EventInfo::decode(m.payload.as_slice()).expect("decode EventInfo");
        assert_eq!(event.domain.as_deref(), Some("dom1"));
        assert_eq!(event.raw_message.as_deref(), Some(r#"{"x":1}"#));
        assert_eq!(event.context_id, Some(123));
        assert_eq!(event.flag, Some(0));
        let log_id = event.log_id.as_deref().expect("log_id");
        assert_eq!(m.key.as_deref(), Some(log_id));
        let sid = event.source_id.expect("source_id bytes");
        assert_eq!(
            std::str::from_utf8(&sid).unwrap(),
            "43983bfc-2db3-47a5-a3a8-d832b2855d51"
        );
    }

    /// 测试内容：PB 模式下空 `domain` 不写入 `EventInfo.domain`。
    /// 输入：`format: pb` 且省略 `domain` 的配置。
    /// 预期：解码后 `domain` 为 `None`，`flag` 仍为 `0`。
    #[test]
    fn build_agent_message_pb_omits_empty_domain() {
        let runtime_config =
            agent_fixtures::agent_runtime_config(NO_DOMAIN_YAML, KafkaAgentFormat::Pb).unwrap();
        let m = build_agent_message(&runtime_config, "{}", 1, 1_700_000_000_000);
        let event = EventInfo::decode(m.payload.as_slice()).unwrap();
        assert!(event.domain.is_none());
        assert_eq!(event.flag, Some(0));
    }
}
