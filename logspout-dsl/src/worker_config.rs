//! Worker 模板配置 YAML 中与 Serde 直接对应的形状（不含 `parse_template_config` 或 Handlebars 渲染逻辑）。

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::field_spec::FieldSpec;

/// `sink.kafka:`：已知字段映射到结构体；**未建模的键**在反序列化时由 Serde 忽略（不报错、不保留），便于粘贴 Java client 风格配置。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KafkaConfig {
    /// **`None`** 仅当键在 YAML 中省略时；**有效配置**下须含至少一个非空 broker（见 [`crate::validate_template_sink`] / worker 校验）。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub brokers: Option<Vec<String>>,
    /// **`None`** 仅当键省略；**有效配置**下须为 **trim 后非空** topic。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub topic: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub headers: Option<BTreeMap<String, serde_yaml::Value>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub acks: Option<serde_yaml::Value>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        rename = "timeout-ms"
    )]
    pub timeout_ms: Option<serde_yaml::Value>,
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
}

/// 行日志 sink：**必填** Serde internally-tagged **`type`**（`kafka` | `file` | `stdout`）。
/// - **`output`**：仅 **`type: file`** 有意义；写 **`stdout` / `kafka`** 时多余键由 Serde 忽略。
/// - **`max-size`**：各变体均可携带；截断语义仅 **`file`** sink 使用（他类型可省略或为 `0`）。可为整数（字节）或字符串，如 **`64KiB`**、`10MiB`（底数 1024）。
/// - **`kafka`**：仅 **`type: kafka`** 时需要（`sink.kafka:` 映射块）。
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum SinkConfig {
    Stdout {
        #[serde(
            rename = "max-size",
            default = "default_max_size_bytes",
            deserialize_with = "crate::human_size::deserialize_max_size"
        )]
        max_size_bytes: u64,
    },
    File {
        #[serde(
            rename = "max-size",
            default = "default_max_size_bytes",
            deserialize_with = "crate::human_size::deserialize_max_size"
        )]
        max_size_bytes: u64,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        output: Option<String>,
    },
    Kafka {
        #[serde(
            rename = "max-size",
            default = "default_max_size_bytes",
            deserialize_with = "crate::human_size::deserialize_max_size"
        )]
        max_size_bytes: u64,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        kafka: Option<Box<KafkaConfig>>,
    },
}

impl SinkConfig {
    /// `max-size` 在各变体上的取值（截断语义仅 [`SinkConfig::File`] 使用）。
    pub fn max_size_bytes(&self) -> u64 {
        match self {
            SinkConfig::Stdout { max_size_bytes } => *max_size_bytes,
            SinkConfig::File { max_size_bytes, .. } => *max_size_bytes,
            SinkConfig::Kafka { max_size_bytes, .. } => *max_size_bytes,
        }
    }

    /// 若为 Kafka sink 且配置了 `sink.kafka:`，返回该段。
    pub fn kafka_section(&self) -> Option<&KafkaConfig> {
        match self {
            SinkConfig::Kafka { kafka, .. } => kafka.as_deref(),
            _ => None,
        }
    }
}

/// Worker 模板配置（一份 `.yaml` 对应一棵配置树；序列化后可由 daemon / worker 落盘或经 gRPC 传递）。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TemplateConfig {
    /// Handlebars 源字符串（无须外置文件）。占位符须与 `fields` 键一致；**勿**用 `len` 等名，会与 handlebars 内置 helper（如 `{{len …}}`）冲突。
    pub template: String,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub fields: BTreeMap<String, FieldSpec>,
    /// 每条日志间隔（毫秒），默认 1000。
    #[serde(rename = "min-interval", default = "default_min_interval_ms")]
    pub min_interval_ms: u64,
    /// 行日志写出：**`sink.type`** 及关联项（不可再扁平写在根上）。
    pub sink: SinkConfig,
}

fn default_min_interval_ms() -> u64 {
    1000
}

fn default_max_size_bytes() -> u64 {
    0
}

#[cfg(test)]
mod tests {
    use super::TemplateConfig;

    /// 测试内容：最小 worker 配置 YAML 仅经 Serde 反序列化后的字段。
    /// 输入：`min-interval: 1`、`stdout` sink、模板与 `counter` 字段。
    /// 预期：`min_interval_ms`、`sink.max_size_bytes()`、`template` 正确。
    #[test]
    fn worker_config_yaml_minimal_deserializes() {
        let y = r#"
sink:
  type: stdout
template: "x={{c}}"
min-interval: 1
fields:
  c:
    type: counter
"#;
        let c: TemplateConfig = serde_yaml::from_str(y).unwrap();
        assert_eq!(c.min_interval_ms, 1);
        assert_eq!(c.sink.max_size_bytes(), 0);
        assert_eq!(c.template, "x={{c}}");
    }

    /// 测试内容：`sink.kafka` 仅含 `brokers` 与 `topic` 时能反序列化。
    /// 输入：最小 Kafka 段，无 `acks` / `ssl.*` 等可选键。
    /// 预期：`topic` 与 `brokers` 正确；常见可选字段为 `None`。
    #[test]
    fn deserialize_worker_config_yaml_kafka_section_optional() {
        let y = r#"
sink:
  type: kafka
  kafka:
    brokers: ["127.0.0.1:9092"]
    topic: t1
template: "x"
fields: {}
"#;
        let c: TemplateConfig = serde_yaml::from_str(y).unwrap();
        let k = c.sink.kafka_section().expect("kafka");
        assert_eq!(k.topic.as_deref(), Some("t1"));
        assert_eq!(
            k.brokers,
            Some(vec!["127.0.0.1:9092".to_string()])
        );
        assert!(k.acks.is_none());
        assert!(k.timeout_ms.is_none());
        assert!(k.compression.is_none());
        assert!(k.security_protocol.is_none());
    }

    /// 测试内容：`sink.kafka` 中带点键名（`security.protocol` 等）与别名键能映射到结构体字段。
    /// 输入：`security-protocol`、`ssl-ca-location`、`acks`、`timeout-ms`、`compression`。
    /// 预期：各字段解析为 SSL / acks / 超时 / 压缩的预期值。
    #[test]
    fn deserialize_worker_config_yaml_kafka_options_explicit() {
        let y = r#"
sink:
  type: kafka
  kafka:
    brokers: ["127.0.0.1:9092"]
    topic: t1
    security-protocol: SSL
    ssl-ca-location: /tmp/ca.pem
    acks: all
    timeout-ms: 12000
    compression: gzip
template: "x"
fields: {}
"#;
        let c: TemplateConfig = serde_yaml::from_str(y).unwrap();
        let k = c.sink.kafka_section().unwrap();
        assert_eq!(k.acks.as_ref().and_then(|v| v.as_str()), Some("all"));
        assert_eq!(k.timeout_ms.as_ref().and_then(|v| v.as_u64()), Some(12_000));
        assert_eq!(k.compression.as_deref(), Some("gzip"));
        assert_eq!(k.security_protocol.as_deref(), Some("SSL"));
        assert_eq!(k.ssl_ca_location.as_deref(), Some("/tmp/ca.pem"));
    }

    /// 测试内容：`kafka.acks` 支持整型 YAML 标量。
    /// 输入：`acks: -1`。
    /// 预期：反序列化为整数 `-1`（对应 all）。
    #[test]
    fn deserialize_worker_config_yaml_kafka_acks_integer() {
        let y = r#"
sink:
  type: kafka
  kafka:
    brokers: ["b:9092"]
    topic: t
    acks: -1
template: "x"
fields: {}
"#;
        let c: TemplateConfig = serde_yaml::from_str(y).unwrap();
        let k = c.sink.kafka_section().unwrap();
        assert_eq!(k.acks.as_ref().and_then(|v| v.as_i64()), Some(-1));
    }

    /// 测试内容：Kafka sink 的 `headers` 能从 worker 配置 YAML 反序列化，且与传输相关可选字段分离。
    /// 输入：`sink.kafka` 含 `brokers`、`topic` 及 `headers`（字符串、带引号 trace-id、`null`、整数）。
    /// 预期：`headers` 各键对应 YAML 类型正确；`empty-value` 为 null；未设置 `acks` 等可选字段。
    #[test]
    fn deserialize_worker_config_yaml_kafka_headers() {
        let y = r#"
sink:
  type: kafka
  kafka:
    brokers: ["127.0.0.1:9092"]
    topic: t1
    headers:
      source: logspout
      trace-id: "abc-42"
      empty-value: null
      count: 7
template: "x"
fields: {}
"#;
        let c: TemplateConfig = serde_yaml::from_str(y).unwrap();
        let k = c.sink.kafka_section().unwrap();
        let h = k.headers.as_ref().expect("headers");
        assert_eq!(h.get("source").and_then(|v| v.as_str()), Some("logspout"));
        assert_eq!(h.get("trace-id").and_then(|v| v.as_str()), Some("abc-42"));
        assert!(h.get("empty-value").unwrap().is_null());
        assert_eq!(h.get("count").and_then(|v| v.as_i64()), Some(7));
        assert!(k.acks.is_none());
    }

    /// 测试内容：`sink.kafka` 中含未在 `KafkaConfig` 建模的键时不应导致反序列化失败。
    /// 输入：`client.id`、`metadata.max.age.ms` 等与 Java client 常见键同形的额外键。
    /// 预期：解析成功；`brokers` 与 `topic` 等已知字段仍正确（多余键被 Serde 忽略）。
    #[test]
    fn deserialize_worker_config_yaml_kafka_unknown_keys_ignored() {
        let y = r#"
sink:
  type: kafka
  kafka:
    brokers: ["127.0.0.1:9092"]
    topic: t1
    client.id: logspout-test
    metadata.max.age.ms: 300000
template: "x"
fields: {}
"#;
        let c: TemplateConfig = serde_yaml::from_str(y).unwrap();
        let k = c.sink.kafka_section().unwrap();
        assert_eq!(k.topic.as_deref(), Some("t1"));
        assert_eq!(k.brokers.as_ref().unwrap().as_slice(), &["127.0.0.1:9092".to_string()]);
    }

    /// 测试内容：未写 `max-size` 时 file/stdout sink 的 `max_size_bytes` 默认 0。
    /// 输入：仅 `sink.type: stdout` 与模板、字段的最小 YAML。
    /// 预期：反序列化后 `c.sink.max_size_bytes() == 0`。
    #[test]
    fn deserialize_worker_config_yaml_max_size_defaults_to_zero() {
        let y = r#"
sink:
  type: stdout
template: "x"
fields: {}
"#;
        let c: TemplateConfig = serde_yaml::from_str(y).unwrap();
        assert_eq!(c.sink.max_size_bytes(), 0);
    }

    /// 测试内容：`max-size` 为整数字节标量时原样写入。
    /// 输入：`max-size: 65536`。
    /// 预期：`max_size_bytes == 65536`。
    #[test]
    fn deserialize_worker_config_yaml_max_size_nonzero() {
        let y = r#"
sink:
  type: stdout
  max-size: 65536
template: "x"
fields: {}
"#;
        let c: TemplateConfig = serde_yaml::from_str(y).unwrap();
        assert_eq!(c.sink.max_size_bytes(), 65536);
    }

    /// 测试内容：`max-size` 支持人类可读无引号字符串（KiB）。
    /// 输入：`max-size: 64KiB`。
    /// 预期：`max_size_bytes == 65536`。
    #[test]
    fn deserialize_worker_config_yaml_max_size_human_string() {
        let y = r#"
sink:
  type: stdout
  max-size: 64KiB
template: "x"
fields: {}
"#;
        let c: TemplateConfig = serde_yaml::from_str(y).unwrap();
        assert_eq!(c.sink.max_size_bytes(), 65536);
    }

    /// 测试内容：`max-size` 为带引号的人类可读小数单位时按 MiB 换算并四舍五入。
    /// 输入：`max-size: "1.5MiB"`。
    /// 预期：`max_size_bytes` 等于 `round(1.5 * 1048576)`。
    #[test]
    fn deserialize_worker_config_yaml_max_size_human_quoted() {
        let y = r#"
sink:
  type: stdout
  max-size: "1.5MiB"
template: "x"
fields: {}
"#;
        let c: TemplateConfig = serde_yaml::from_str(y).unwrap();
        assert_eq!(
            c.sink.max_size_bytes(),
            (1.5_f64 * 1048576_f64).round() as u64
        );
    }

    /// 测试内容：YAML 折叠标量 `template: >-` 多行合并为单行模板字符串。
    /// 输入：两行正文 `part2` / `part3` 的 folded `template`。
    /// 预期：反序列化后模板无换行且同时包含 `part2` 与 `part3`。
    #[test]
    fn yaml_folded_template_joins_lines() {
        let y = r#"
sink:
  type: stdout
template: >-
  {{src_ip}} part2
  part3
fields: {}
"#;
        let c: TemplateConfig = serde_yaml::from_str(y).unwrap();
        assert!(
            !c.template.contains('\n'),
            "folded scalar should be one line: {:?}",
            c.template
        );
        assert!(c.template.contains("part2"));
        assert!(c.template.contains("part3"));
    }
}
