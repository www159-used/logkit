//! `sink.kafka:` 映射块（与 librdkafka / Java client 键名对齐）。

use std::collections::BTreeMap;

use serde::de::Error;
use serde::{Deserialize, Deserializer, Serialize};
use serde_yaml::Value;

/// `acks` / `timeout-ms` 等：YAML 里可写字符串或未加引号的数字，统一落成 `Option<String>`。
fn deserialize_optional_scalar_string<'de, D>(deserializer: D) -> Result<Option<String>, D::Error>
where
    D: Deserializer<'de>,
{
    match Option::<serde_yaml::Value>::deserialize(deserializer)? {
        None | Some(serde_yaml::Value::Null) => Ok(None),
        Some(serde_yaml::Value::String(s)) => Ok(Some(s)),
        Some(serde_yaml::Value::Number(n)) => Ok(Some(n.to_string())),
        Some(serde_yaml::Value::Bool(b)) => Ok(Some(b.to_string())),
        Some(other) => Err(Error::custom(format!(
            "expected string, number, or bool; got {other:?}"
        ))),
    }
}

/// `headers:` 映射的值可为 string / number / bool / null（null 表示无 header 字节，与原先语义一致）。
fn deserialize_optional_headers_map<'de, D>(
    deserializer: D,
) -> Result<Option<BTreeMap<String, Option<String>>>, D::Error>
where
    D: Deserializer<'de>,
{
    match Option::<serde_yaml::Value>::deserialize(deserializer)? {
        None | Some(serde_yaml::Value::Null) => Ok(None),
        Some(serde_yaml::Value::Mapping(m)) => {
            let mut out = BTreeMap::new();
            for (k, v) in m {
                let key = k
                    .as_str()
                    .ok_or_else(|| Error::custom("kafka.headers: keys must be strings"))?
                    .to_string();
                let val = match v {
                    serde_yaml::Value::Null => None,
                    serde_yaml::Value::String(s) => Some(s),
                    serde_yaml::Value::Number(n) => Some(n.to_string()),
                    serde_yaml::Value::Bool(b) => Some(b.to_string()),
                    _ => {
                        return Err(Error::custom(
                            "kafka.headers: values must be string, number, bool, or null",
                        ));
                    }
                };
                out.insert(key, val);
            }
            Ok(Some(out))
        }
        Some(other) => Err(Error::custom(format!(
            "kafka.headers: expected mapping, got {other:?}"
        ))),
    }
}

/// Kafka 行 sink 模式：`common` 为模板整行直发；`agent` 为紧凑 JSON 外壳 + 固定 topic。
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum KafkaSinkMode {
    #[default]
    Common,
    Agent,
}

/// `sink.kafka.agent:` 可选覆盖项；**`domain` 可省略**（空则 JSON 外壳不写 `domain` 字段）。其余未填字段由 worker 在启动时生成或取本机信息。
/// 未建模键落入 **`extras`**（反序列化时吸收，序列化时原样写回）。
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct KafkaAgentConfig {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub domain: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub domain_token: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub appname: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub token: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tag: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hostname: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ip: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub log_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub flag: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fields: Option<String>,
    /// 未在结构体上建模的 **`agent:`** 键（任意 YAML 值）。
    #[serde(flatten, default, skip_serializing_if = "BTreeMap::is_empty")]
    pub extras: BTreeMap<String, Value>,
}

/// `sink.kafka:`：已知字段映射到结构体；**未建模的键**落入 **`extras`**（便于整段粘贴 Java client / 其它扩展键）。
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct KafkaConfig {
    #[serde(default)]
    pub mode: KafkaSinkMode,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent: Option<KafkaAgentConfig>,
    /// **`None`** 仅当键在 YAML 中省略时；**有效配置**下须含至少一个非空 broker（见 [`crate::validate_template_sink`] / worker 校验）。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub brokers: Option<Vec<String>>,
    /// **`None`** 仅当键省略；**`mode: common`** 下须为 **trim 后非空** topic；**`agent`** 下由 worker 固定为 `raw_message`。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub topic: Option<String>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_optional_headers_map"
    )]
    pub headers: Option<BTreeMap<String, Option<String>>>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_optional_scalar_string"
    )]
    pub acks: Option<String>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        rename = "timeout-ms",
        deserialize_with = "deserialize_optional_scalar_string"
    )]
    pub timeout_ms: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub compression: Option<String>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        rename = "security.protocol",
        alias = "security-protocol"
    )]
    pub security_protocol: Option<String>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        rename = "ssl.endpoint.identification.algorithm"
    )]
    pub ssl_endpoint_identification_algorithm: Option<String>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        rename = "ssl.ca.pem"
    )]
    pub ssl_ca_pem: Option<String>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        rename = "ssl.ca.location",
        alias = "ssl-ca-location"
    )]
    pub ssl_ca_location: Option<String>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        rename = "ssl.truststore.location"
    )]
    pub ssl_truststore_location: Option<String>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        rename = "ssl.truststore.password"
    )]
    pub ssl_truststore_password: Option<String>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        rename = "ssl.certificate.pem"
    )]
    pub ssl_certificate_pem: Option<String>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        rename = "ssl.certificate.location"
    )]
    pub ssl_certificate_location: Option<String>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        rename = "ssl.private.key.pem"
    )]
    pub ssl_private_key_pem: Option<String>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        rename = "ssl.key.location"
    )]
    pub ssl_key_location: Option<String>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        rename = "ssl.key.pem"
    )]
    pub ssl_key_pem: Option<String>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        rename = "ssl.keystore.location"
    )]
    pub ssl_keystore_location: Option<String>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        rename = "ssl.keystore.password"
    )]
    pub ssl_keystore_password: Option<String>,
    /// 可选。客户端 **JKS** 含多个私钥时，用于**覆盖**默认选择（默认：私钥别名升序第一个，贴近常见 Java「只配 location+password」体验；与某 JDK 遍历顺序不保证完全一致）。
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        rename = "ssl.keystore.alias",
        alias = "ssl-keystore-alias"
    )]
    pub ssl_keystore_alias: Option<String>,
    /// Accepted for compatibility; the current TLS stack does not apply TLS version pins from YAML.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        rename = "ssl.protocol"
    )]
    pub ssl_protocol: Option<String>,
    /// Accepted for compatibility; not applied by the current client.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        rename = "ssl.enabled.protocols"
    )]
    pub ssl_enabled_protocols: Option<String>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        rename = "sasl.mechanism"
    )]
    pub sasl_mechanism: Option<String>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        rename = "sasl.jaas.config"
    )]
    pub sasl_jaas_config: Option<String>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        rename = "sasl.username"
    )]
    pub sasl_username: Option<String>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        rename = "sasl.password"
    )]
    pub sasl_password: Option<String>,
    /// 未在结构体上建模的 **`kafka:`** 键（任意 YAML 值）。
    #[serde(flatten, default, skip_serializing_if = "BTreeMap::is_empty")]
    pub extras: BTreeMap<String, Value>,
}

/// Agent 模式若配置 `source_id`，须为 **36 字符标准 UUID**（8-4-4-4-12，含连字符；十六进制大小写均可）。
pub fn validate_agent_source_id(s: &str) -> bool {
    let s = s.trim();
    if s.len() != 36 {
        return false;
    }
    let b = s.as_bytes();
    if !(b.get(8) == Some(&b'-')
        && b.get(13) == Some(&b'-')
        && b.get(18) == Some(&b'-')
        && b.get(23) == Some(&b'-'))
    {
        return false;
    }
    for (i, &ch) in b.iter().enumerate() {
        if matches!(i, 8 | 13 | 18 | 23) {
            continue;
        }
        if !ch.is_ascii_hexdigit() {
            return false;
        }
    }
    true
}

#[cfg(test)]
mod tests {
    use serde::Deserialize;

    use super::*;

    #[derive(Deserialize)]
    struct Wrap {
        kafka: KafkaConfig,
    }

    /// 测试内容：Kafka 映射片段仅含 `brokers` 与 `topic` 时能反序列化。
    #[test]
    fn deserialize_kafka_section_minimal() {
        let y = r#"
kafka:
  brokers: ["127.0.0.1:9092"]
  topic: t1
"#;
        let w: Wrap = serde_yaml::from_str(y).unwrap();
        let k = &w.kafka;
        assert_eq!(k.mode, KafkaSinkMode::Common);
        assert_eq!(k.topic.as_deref(), Some("t1"));
        assert_eq!(k.brokers, Some(vec!["127.0.0.1:9092".to_string()]));
        assert!(k.acks.is_none());
    }

    #[test]
    fn deserialize_kafka_mode_agent_without_topic() {
        let y = r#"
kafka:
  mode: agent
  brokers: ["127.0.0.1:9092"]
  agent:
    domain: acme
"#;
        let w: Wrap = serde_yaml::from_str(y).unwrap();
        let k = &w.kafka;
        assert_eq!(k.mode, KafkaSinkMode::Agent);
        assert!(k.topic.is_none());
        let a = k.agent.as_ref().expect("agent");
        assert_eq!(a.domain.as_deref(), Some("acme"));
    }

    #[test]
    fn deserialize_kafka_collects_unmodeled_keys_in_extras() {
        let y = r#"
kafka:
  brokers: ["127.0.0.1:9092"]
  topic: t1
  client.id: mycid
  metadata.max.age.ms: 12345
"#;
        let w: Wrap = serde_yaml::from_str(y).unwrap();
        let k = &w.kafka;
        assert_eq!(
            k.extras.get("client.id"),
            Some(&Value::String("mycid".into()))
        );
        assert_eq!(
            k.extras.get("metadata.max.age.ms"),
            Some(&Value::Number(12345.into()))
        );
    }

    #[test]
    fn deserialize_kafka_agent_extras_absorbs_unknown() {
        let y = r#"
kafka:
  mode: agent
  brokers: ["127.0.0.1:9092"]
  agent:
    domain: acme
    future.flag: true
"#;
        let w: Wrap = serde_yaml::from_str(y).unwrap();
        let a = w.kafka.agent.as_ref().unwrap();
        assert_eq!(a.extras.get("future.flag"), Some(&Value::Bool(true)));
    }

    #[test]
    fn validate_agent_source_id_accepts_hyphenated_uuid() {
        assert!(validate_agent_source_id("01234567-89ab-cdef-0123-456789abcdef"));
        assert!(validate_agent_source_id("01234567-89AB-CDEF-0123-456789ABCDEF"));
    }

    #[test]
    fn validate_agent_source_id_rejects_non_uuid() {
        assert!(!validate_agent_source_id("0123456789abcdef0123456789abcdef"));
        assert!(!validate_agent_source_id("01234567-89ab-cdef-0123-456789abcde"));
        assert!(!validate_agent_source_id("not-a-uuid-at-all-here-xxxxxxxxxx"));
    }

    #[test]
    fn deserialize_kafka_options_and_unknown_keys() {
        let y = r#"
kafka:
  brokers: ["127.0.0.1:9092"]
  topic: t1
  security-protocol: SSL
  ssl-ca-location: /tmp/ca.pem
  acks: all
  timeout-ms: 12000
  compression: gzip
  client.id: logspout-test
  metadata.max.age.ms: 300000
"#;
        let w: Wrap = serde_yaml::from_str(y).unwrap();
        let k = &w.kafka;
        assert_eq!(k.acks.as_deref(), Some("all"));
        assert_eq!(k.timeout_ms.as_deref(), Some("12000"));
        assert_eq!(k.compression.as_deref(), Some("gzip"));
        assert_eq!(k.security_protocol.as_deref(), Some("SSL"));
        assert_eq!(k.ssl_ca_location.as_deref(), Some("/tmp/ca.pem"));
    }

    #[test]
    fn deserialize_kafka_acks_integer() {
        let y = r#"
kafka:
  brokers: ["b:9092"]
  topic: t
  acks: -1
"#;
        let w: Wrap = serde_yaml::from_str(y).unwrap();
        assert_eq!(w.kafka.acks.as_deref(), Some("-1"));
    }

    #[test]
    fn deserialize_kafka_headers() {
        let y = r#"
kafka:
  brokers: ["127.0.0.1:9092"]
  topic: t1
  headers:
    source: logspout
    trace-id: "abc-42"
    empty-value: null
    count: 7
"#;
        let w: Wrap = serde_yaml::from_str(y).unwrap();
        let k = &w.kafka;
        let h = k.headers.as_ref().expect("headers");
        assert_eq!(
            h.get("source").and_then(|v| v.as_deref()),
            Some("logspout")
        );
        assert_eq!(
            h.get("trace-id").and_then(|v| v.as_deref()),
            Some("abc-42")
        );
        assert_eq!(h.get("empty-value"), Some(&None));
        assert_eq!(h.get("count").and_then(|v| v.as_deref()), Some("7"));
    }
}
