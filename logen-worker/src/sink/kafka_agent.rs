//! Kafka `agent` 模式：紧凑 JSON 外壳与启动期字段解析。

use fake::faker::internet::en::Password;
use fake::Fake;
use logen_dsl::{validate_agent_source_id, KafkaConfig, KafkaSinkMode};
use uuid::Uuid;

use super::log_id::next_log_id;

pub const KAFKA_AGENT_TOPIC: &str = "raw_message";

/// 进程内固定的 agent 元数据（每条仅补时间戳、`context_id`、`raw_message`）。
#[derive(Debug, Clone)]
pub struct KafkaAgentRuntimeState {
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
    pub payload: String,
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
    local_ip_address::local_ip()
        .map(|ip| ip.to_string())
        .unwrap_or_default()
}

fn or_random(opt: Option<&str>, rnd_len: usize) -> String {
    let t = opt.map(str::trim).filter(|s| !s.is_empty());
    match t {
        Some(s) => s.to_string(),
        None => random_alphanum(rnd_len),
    }
}

/// 由已通过校验的 `KafkaConfig`（`mode == Agent`）构造运行时状态。
pub fn build_runtime_state(k: &KafkaConfig) -> Result<KafkaAgentRuntimeState, String> {
    if k.mode != KafkaSinkMode::Agent {
        return Err("internal: build_runtime_state requires mode agent".into());
    }
    let agent = k
        .agent
        .as_ref()
        .ok_or_else(|| "missing sink.kafka.agent".to_string())?;
    let domain = agent
        .domain
        .as_deref()
        .unwrap_or("")
        .trim()
        .to_string();

    let source_id = match agent.source_id.as_deref().map(str::trim).filter(|s| !s.is_empty()) {
        Some(s) => {
            if !validate_agent_source_id(s) {
                return Err("sink.kafka.agent.source_id must be a 36-character UUID (8-4-4-4-12 hex with hyphens)".into());
            }
            s.to_string()
        }
        None => Uuid::new_v4().to_string(),
    };

    let hostname = match agent.hostname.as_deref().map(str::trim).filter(|s| !s.is_empty()) {
        Some(s) => s.to_string(),
        None => detect_hostname(),
    };
    let ip = match agent.ip.as_deref().map(str::trim).filter(|s| !s.is_empty()) {
        Some(s) => s.to_string(),
        None => detect_local_ip(),
    };

    Ok(KafkaAgentRuntimeState {
        domain,
        domain_token: or_random(agent.domain_token.as_deref(), 16),
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
struct AgentEnvelope<'agent_envelope> {
    #[serde(skip_serializing_if = "str::is_empty")]
    domain: &'agent_envelope str,
    #[serde(skip_serializing_if = "str::is_empty")]
    domain_token: &'agent_envelope str,
    #[serde(skip_serializing_if = "str::is_empty")]
    appname: &'agent_envelope str,
    #[serde(skip_serializing_if = "str::is_empty")]
    tag: &'agent_envelope str,
    #[serde(skip_serializing_if = "str::is_empty")]
    token: &'agent_envelope str,
    #[serde(skip_serializing_if = "str::is_empty")]
    hostname: &'agent_envelope str,
    #[serde(skip_serializing_if = "str::is_empty")]
    log_id: &'agent_envelope str,
    context_id: i64,
    timestamp: i64,
    recv_timestamp: i64,
    log_timestamp: i64,
    raw_message: &'agent_envelope str,
    source_update_timestamp: i64,
    #[serde(skip_serializing_if = "str::is_empty")]
    source: &'agent_envelope str,
    #[serde(skip_serializing_if = "str::is_empty")]
    ip: &'agent_envelope str,
    flag: i64,
    #[serde(skip_serializing_if = "str::is_empty")]
    fields: &'agent_envelope str,
    #[serde(skip_serializing_if = "str::is_empty")]
    source_type: &'agent_envelope str,
    #[serde(skip_serializing_if = "str::is_empty")]
    source_id: &'agent_envelope str,
}

pub fn build_message(
    state: &KafkaAgentRuntimeState,
    raw_message: &str,
    context_id: i64,
    ts_ms: i64,
) -> KafkaAgentMessage {
    let log_id = next_log_id();
    let env = AgentEnvelope {
        domain: state.domain.as_str(),
        domain_token: state.domain_token.as_str(),
        appname: state.appname.as_str(),
        tag: state.tag.as_str(),
        token: state.token.as_str(),
        hostname: state.hostname.as_str(),
        log_id: log_id.as_str(),
        context_id,
        timestamp: ts_ms,
        recv_timestamp: ts_ms,
        log_timestamp: ts_ms,
        raw_message,
        source_update_timestamp: ts_ms,
        source: state.source.as_str(),
        ip: state.ip.as_str(),
        flag: state.flag,
        fields: state.fields.as_str(),
        source_type: state.source_type.as_str(),
        source_id: state.source_id.as_str(),
    };
    KafkaAgentMessage {
        payload: match serde_json::to_string(&env) {
            Ok(s) => s,
            Err(e) => {
                tracing::error!("agent envelope serialization failed: {e}");
                format!(
                    "{{\"log_id\":\"{}\",\"context_id\":{},\"timestamp\":{},\"recv_timestamp\":{},\"log_timestamp\":{},\"source_update_timestamp\":{},\"raw_message\":\"serialization_error\"}}",
                    log_id, context_id, ts_ms, ts_ms, ts_ms, ts_ms
                )
            }
        },
        key: Some(log_id),
    }
}

#[cfg(test)]
mod tests {
    use logen_dsl::{KafkaAgentConfig, KafkaConfig, KafkaSinkMode};

    use super::*;

    fn sample_agent_kafka() -> KafkaConfig {
        KafkaConfig {
            mode: KafkaSinkMode::Agent,
            agent: Some(KafkaAgentConfig {
                domain: Some("dom1".into()),
                ..Default::default()
            }),
            brokers: Some(vec!["127.0.0.1:9092".into()]),
            ..Default::default()
        }
    }

    /// 测试内容：agent 模式在 `domain` 为空时应省略 `domain` 字段，但仍保留默认 `flag`。
    /// 输入：`mode: agent` 且 `agent.domain` 省略的最小 Kafka 配置，生成一条 `{}` 原始消息。
    /// 预期：输出 JSON 不含 `domain`，且包含默认 `flag: 0`。
    #[test]
    fn build_payload_omits_empty_domain() {
        let k = KafkaConfig {
            mode: KafkaSinkMode::Agent,
            agent: Some(KafkaAgentConfig {
                domain: None,
                ..Default::default()
            }),
            brokers: Some(vec!["127.0.0.1:9092".into()]),
            ..Default::default()
        };
        let st = build_runtime_state(&k).unwrap();
        assert!(st.domain.is_empty());
        let j = build_message(&st, "{}", 1, 1700000000000).payload;
        assert!(!j.contains("\"domain\""));
        assert!(j.contains("\"flag\":0"));
    }

    /// 测试内容：agent 模式生成的 JSON 中 `log_id` 与 Kafka message key 应保持一致。
    /// 输入：带 `domain` 的最小 agent Kafka 配置，以及一条 JSON 原始消息。
    /// 预期：输出包含 `domain`、`raw_message`、`context_id`；解析出的 `log_id` 与 `key` 相同，且默认 `flag` 为 `0`。
    #[test]
    fn build_message_contains_domain_raw_message_and_key_matches_log_id() {
        let k = sample_agent_kafka();
        let st = build_runtime_state(&k).unwrap();
        let m = build_message(&st, r#"{"x":1}"#, 123, 1700000000000);
        assert!(m.payload.contains("\"domain\":\"dom1\""));
        assert!(m.payload.contains("\"raw_message\":\"{\\\"x\\\":1}\""));
        assert!(m.payload.contains("\"context_id\":123"));
        let v: serde_json::Value = serde_json::from_str(&m.payload).unwrap();
        let log_id = v["log_id"].as_str().unwrap();
        assert_eq!(v["flag"].as_i64(), Some(0));
        assert_eq!(m.key.as_deref(), Some(log_id));
    }
}
