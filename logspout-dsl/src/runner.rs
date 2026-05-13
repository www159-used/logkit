use std::collections::BTreeMap;
use std::path::Path;

use handlebars::Handlebars;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use crate::builtins::{slots_from_fields, FieldSpec};
use crate::facade::TemplateSlot;
use crate::{ConfigParseError, Error};

/// 行日志写出方式，见 [`SinkConfig`] 的 **`type`** 字段（YAML：`sink.type`）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum LineSinkType {
    Kafka,
    File,
    Stdout,
}

/// `sink.kafka:`：已知字段映射到结构体；**未建模的键**在反序列化时由 Serde 忽略（不报错、不保留），便于粘贴 Java client 风格配置。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KafkaConfig {
    /// **`None`** 仅当键在 YAML 中省略时；**有效配置**下须含至少一个非空 broker（见 [`validate_template_sink`] / worker 校验）。
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

/// 行日志 sink：**必填** `type`（`kafka` | `file` | `stdout`）。
/// - **`output`**：仅 **`type: file`** 有意义；其它类型**不需要**，[`parse_template_config`] 会丢弃多余或无意义的 `output`。
/// - **`max-size`**：截断仅对 **`file`** 生效（他类型可省略或为 `0`）。可为整数（字节）或字符串，如 **`64KiB`**、**`10MiB`**、`1.5 GiB`（底数 1024）。
/// - **`kafka`**：仅 **`type: kafka`** 时需要。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SinkConfig {
    /// `kafka` | `file` | `stdout`
    #[serde(rename = "type")]
    pub sink_type: LineSinkType,
    /// **`type: file`** 时：单文件超过该字节数则截断；`0` 不限制。YAML 可为整数或带单位字符串（见 crate 说明）。
    #[serde(
        rename = "max-size",
        default = "default_max_size_bytes",
        deserialize_with = "crate::human_size::deserialize_max_size"
    )]
    pub max_size_bytes: u64,
    /// **`type: file`** 时：相对 **`output_base`** 的路径。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output: Option<String>,
    /// **`type: kafka`** 时的集群与透传项。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kafka: Option<KafkaConfig>,
}

/// Producer 配置（一份 YAML 对应一棵配置树；序列化后可由 daemon / worker 落盘或经 gRPC 传递）。
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

fn yaml_extension_ok(path: &Path) -> Result<(), ConfigParseError> {
    let ext = path
        .extension()
        .and_then(|s| s.to_str())
        .map(|e| e.to_ascii_lowercase());
    if !matches!(ext.as_deref(), Some("yaml") | Some("yml")) {
        return Err(ConfigParseError::PathNotYaml(path.display().to_string()));
    }
    Ok(())
}

/// `output` 仅对 [`LineSinkType::File`] 有效；其它类型去掉 `output`，避免误写 `output` 与 `sink.type` 不一致。
fn normalize_sink_output(mut sink: SinkConfig) -> SinkConfig {
    if sink.sink_type != LineSinkType::File {
        sink.output = None;
    }
    sink
}

/// 供 list / stat 等展示的一行 **`sink:`** 摘要（`stdout` / `file:` / `kafka:`）。
pub fn format_sink_summary(sink: &SinkConfig) -> String {
    match sink.sink_type {
        LineSinkType::Stdout => "stdout".into(),
        LineSinkType::File => {
            let path = sink.output.as_deref().unwrap_or("?");
            if sink.max_size_bytes > 0 {
                format!("file: {path} (max-size: {} bytes)", sink.max_size_bytes)
            } else {
                format!("file: {path}")
            }
        }
        LineSinkType::Kafka => {
            let Some(k) = sink.kafka.as_ref() else {
                return "kafka: (missing kafka section)".into();
            };
            let broker = k
                .brokers
                .as_ref()
                .and_then(|b| b.first())
                .map(|b| b.as_str())
                .unwrap_or("?");
            let n = k.brokers.as_ref().map(|b| b.len()).unwrap_or(0);
            let more = n.saturating_sub(1);
            let brokers = if more > 0 {
                format!("{broker} +{more} more")
            } else {
                broker.to_string()
            };
            let topic = k.topic.as_deref().unwrap_or("?");
            let hdr = k.headers.as_ref().map(|h| h.len()).unwrap_or(0);
            if hdr > 0 {
                format!("kafka: topic {topic} @ {brokers} (+{hdr} headers)")
            } else {
                format!("kafka: topic {topic} @ {brokers}")
            }
        }
    }
}

/// 检查 `sink.type` 与 `output` / `kafka` 是否一致。
/// 非 `file` 的 `output` 应在进入此函数前丢弃（[`parse_template_config`] 已处理）。
pub fn validate_template_sink(cfg: &TemplateConfig) -> Result<(), ConfigParseError> {
    match cfg.sink.sink_type {
        LineSinkType::Kafka => {
            let Some(k) = cfg.sink.kafka.as_ref() else {
                return Err(ConfigParseError::Merge(
                    "`sink.type: kafka` requires a non-empty `sink.kafka:` section".into(),
                ));
            };
            let brokers_ok = k
                .brokers
                .as_ref()
                .is_some_and(|b| b.iter().any(|s| !s.trim().is_empty()));
            if !brokers_ok {
                return Err(ConfigParseError::Merge(
                    "`sink.type: kafka` requires `sink.kafka.brokers` with at least one non-empty broker address"
                        .into(),
                ));
            }
            let topic_ok = k
                .topic
                .as_deref()
                .is_some_and(|t| !t.trim().is_empty());
            if !topic_ok {
                return Err(ConfigParseError::Merge(
                    "`sink.type: kafka` requires a non-empty `sink.kafka.topic`".into(),
                ));
            }
        }
        LineSinkType::File => {
            let o = cfg.sink.output.as_deref().unwrap_or("").trim();
            if o.is_empty() {
                return Err(ConfigParseError::Merge(
                    "`sink.type: file` requires a non-empty `sink.output` path".into(),
                ));
            }
        }
        LineSinkType::Stdout => {}
    }
    Ok(())
}

/// 将配置序列化为单份 YAML 字符串（供 gRPC `producer_yaml` / daemon 落盘）。
pub fn template_config_to_yaml(cfg: &TemplateConfig) -> Result<String, serde_yaml::Error> {
    serde_yaml::to_string(cfg)
}

/// 仅接受路径扩展名为 `.yaml` / `.yml`，内容按 YAML 反序列化为 [`TemplateConfig`]。
pub fn parse_template_config(
    config_path: &Path,
    raw: &str,
) -> Result<TemplateConfig, ConfigParseError> {
    yaml_extension_ok(config_path)?;
    let mut cfg: TemplateConfig = serde_yaml::from_str(raw)?;
    cfg.sink = normalize_sink_output(cfg.sink);
    validate_template_sink(&cfg)?;
    Ok(cfg)
}

/// 每轮用门面生成上下文字段，再渲染 `template`。
pub struct TemplateRunner {
    hb: Handlebars<'static>,
    template: String,
    slots: BTreeMap<String, Box<dyn TemplateSlot>>,
}

impl TemplateRunner {
    pub fn try_new(cfg: TemplateConfig) -> Result<Self, Error> {
        if cfg.template.trim().is_empty() {
            return Err(Error::EmptyTemplate);
        }
        let mut hb = Handlebars::new();
        hb.set_strict_mode(false);
        hb.register_escape_fn(handlebars::no_escape);
        hb.register_template_string("inline", &cfg.template)?;
        let slots = slots_from_fields(cfg.fields)?;
        Ok(Self {
            hb,
            template: "inline".to_string(),
            slots,
        })
    }

    /// 生成一行（一条日志）。
    pub fn next_line(&mut self) -> Result<String, Error> {
        let mut map = Map::new();
        for (key, slot) in &mut self.slots {
            map.insert(key.clone(), Value::String(slot.next_value()));
        }
        let s = self.hb.render(&self.template, &Value::Object(map))?;
        Ok(s)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::path::Path;

    use super::*;

    fn sink_stdout() -> SinkConfig {
        SinkConfig {
            sink_type: LineSinkType::Stdout,
            max_size_bytes: 0,
            output: None,
            kafka: None,
        }
    }

    /// 测试内容：多字段模板一次渲染，各 facade 占位符均展开且以 ` | ` 风格串联。
    /// 输入：`TemplateRunner` 含 `Timestamp`/`NameEn`/`Ipv4`/区间整数等字段与对应模板。
    /// 预期：首行含分隔符 ` | `（各段非空拼接）。
    #[test]
    fn render_with_facades() {
        let cfg = TemplateConfig {
            template: "{{ts}} | {{name}} | {{ip}} | {{n}}".to_string(),
            fields: [
                (
                    "ts".to_string(),
                    crate::FieldSpec::Timestamp {
                        format: "%Y".to_string(),
                    },
                ),
                ("name".to_string(), crate::FieldSpec::NameEn),
                ("ip".to_string(), crate::FieldSpec::Ipv4),
                (
                    "n".to_string(),
                    crate::FieldSpec::Integer { min: 1, max: 3 },
                ),
            ]
            .into_iter()
            .collect(),
            min_interval_ms: 1000,
            sink: sink_stdout(),
        };
        let mut r = TemplateRunner::try_new(cfg).unwrap();
        let line = r.next_line().unwrap();
        assert!(line.contains(" | "));
    }

    /// 测试内容：`Counter` 字段从 0 起每行自增。
    /// 输入：模板 `n={{n}}`，字段 `n` 为 `counter`。
    /// 预期：连续三行为 `n=0`、`n=1`、`n=2`。
    #[test]
    fn counter_starts_at_zero_and_increments() {
        let cfg = TemplateConfig {
            template: "n={{n}}".to_string(),
            fields: [("n".to_string(), crate::FieldSpec::Counter)]
                .into_iter()
                .collect(),
            min_interval_ms: 1000,
            sink: sink_stdout(),
        };
        let mut r = TemplateRunner::try_new(cfg).unwrap();
        assert_eq!(r.next_line().unwrap(), "n=0");
        assert_eq!(r.next_line().unwrap(), "n=1");
        assert_eq!(r.next_line().unwrap(), "n=2");
    }

    /// 测试内容：最小 producer YAML 反序列化并与 `TemplateRunner` 联动。
    /// 输入：`min-interval: 1`、`stdout` sink、模板 `x={{c}}`、字段 `counter`。
    /// 预期：`min_interval_ms == 1`；首行渲染为 `x=0`。
    #[test]
    fn deserialize_producer_yaml_minimal_fields() {
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
        assert_eq!(c.sink.max_size_bytes, 0);
        let mut r = TemplateRunner::try_new(c).unwrap();
        assert_eq!(r.next_line().unwrap(), "x=0");
    }

    /// 测试内容：`sink.kafka` 仅含 `brokers` 与 `topic` 时能反序列化。
    /// 输入：最小 Kafka 段，无 `acks` / `ssl.*` 等可选键。
    /// 预期：`topic` 与 `brokers` 正确；常见可选字段为 `None`。
    #[test]
    fn deserialize_producer_yaml_kafka_section_optional() {
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
        let k = c.sink.kafka.as_ref().expect("kafka");
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
    fn deserialize_producer_yaml_kafka_options_explicit() {
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
        let k = c.sink.kafka.as_ref().unwrap();
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
    fn deserialize_producer_yaml_kafka_acks_integer() {
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
        let k = c.sink.kafka.as_ref().unwrap();
        assert_eq!(k.acks.as_ref().and_then(|v| v.as_i64()), Some(-1));
    }

    /// 测试内容：Kafka sink 的 `headers` 能从 producer YAML 反序列化，且与传输相关可选字段分离。
    /// 输入：`sink.kafka` 含 `brokers`、`topic` 及 `headers`（字符串、带引号 trace-id、`null`、整数）。
    /// 预期：`headers` 各键对应 YAML 类型正确；`empty-value` 为 null；未设置 `acks` 等可选字段。
    #[test]
    fn deserialize_producer_yaml_kafka_headers() {
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
        let k = c.sink.kafka.as_ref().unwrap();
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
    fn deserialize_producer_yaml_kafka_unknown_keys_ignored() {
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
        let k = c.sink.kafka.as_ref().unwrap();
        assert_eq!(k.topic.as_deref(), Some("t1"));
        assert_eq!(k.brokers.as_ref().unwrap().as_slice(), &["127.0.0.1:9092".to_string()]);
    }

    /// 测试内容：未写 `max-size` 时 file/stdout sink 的 `max_size_bytes` 默认 0。
    /// 输入：仅 `sink.type: stdout` 与模板、字段的最小 YAML。
    /// 预期：反序列化后 `c.sink.max_size_bytes == 0`。
    #[test]
    fn deserialize_producer_yaml_max_size_defaults_to_zero() {
        let y = r#"
sink:
  type: stdout
template: "x"
fields: {}
"#;
        let c: TemplateConfig = serde_yaml::from_str(y).unwrap();
        assert_eq!(c.sink.max_size_bytes, 0);
    }

    /// 测试内容：`max-size` 为整数字节标量时原样写入。
    /// 输入：`max-size: 65536`。
    /// 预期：`max_size_bytes == 65536`。
    #[test]
    fn deserialize_producer_yaml_max_size_nonzero() {
        let y = r#"
sink:
  type: stdout
  max-size: 65536
template: "x"
fields: {}
"#;
        let c: TemplateConfig = serde_yaml::from_str(y).unwrap();
        assert_eq!(c.sink.max_size_bytes, 65536);
    }

    /// 测试内容：`max-size` 支持人类可读无引号字符串（KiB）。
    /// 输入：`max-size: 64KiB`。
    /// 预期：`max_size_bytes == 65536`。
    #[test]
    fn deserialize_producer_yaml_max_size_human_string() {
        let y = r#"
sink:
  type: stdout
  max-size: 64KiB
template: "x"
fields: {}
"#;
        let c: TemplateConfig = serde_yaml::from_str(y).unwrap();
        assert_eq!(c.sink.max_size_bytes, 65536);
    }

    /// 测试内容：`max-size` 为带引号的人类可读小数单位时按 MiB 换算并四舍五入。
    /// 输入：`max-size: "1.5MiB"`。
    /// 预期：`max_size_bytes` 等于 `round(1.5 * 1048576)`。
    #[test]
    fn deserialize_producer_yaml_max_size_human_quoted() {
        let y = r#"
sink:
  type: stdout
  max-size: "1.5MiB"
template: "x"
fields: {}
"#;
        let c: TemplateConfig = serde_yaml::from_str(y).unwrap();
        assert_eq!(
            c.sink.max_size_bytes,
            (1.5_f64 * 1048576_f64).round() as u64
        );
    }

    /// 测试内容：`parse_template_config` 对非法 `max-size` 单位报错。
    /// 输入：路径 `t.yaml`，`max-size: 12xyz`。
    /// 预期：`unwrap_err()`；错误信息含 `max-size` 或 `unknown`。
    #[test]
    fn parse_template_config_rejects_bad_max_size_unit() {
        let raw = r#"sink:
  type: stdout
  max-size: 12xyz
template: "x"
fields: {}
"#;
        let e = parse_template_config(Path::new("t.yaml"), raw).unwrap_err();
        assert!(
            e.to_string().contains("max-size") || e.to_string().contains("unknown"),
            "{e}"
        );
    }

    /// 测试内容：`parse_template_config` 在 `sink.type: kafka` 时校验 `sink.kafka.topic` 非空。
    /// 输入：含 `brokers` 但省略 `topic` 的最小 producer YAML。
    /// 预期：`unwrap_err()`；错误信息含 `topic`。
    #[test]
    fn parse_template_config_rejects_kafka_missing_topic() {
        let raw = r#"sink:
  type: kafka
  kafka:
    brokers: ["127.0.0.1:9092"]
template: "x"
fields: {}
"#;
        let e = parse_template_config(Path::new("t.yaml"), raw).unwrap_err();
        assert!(e.to_string().to_ascii_lowercase().contains("topic"), "{e}");
    }

    /// 测试内容：`parse_template_config` 在 `sink.type: kafka` 时校验至少一个非空 broker。
    /// 输入：`topic` 有值但 `brokers` 省略。
    /// 预期：`unwrap_err()`；错误信息含 `brokers`。
    #[test]
    fn parse_template_config_rejects_kafka_missing_brokers() {
        let raw = r#"sink:
  type: kafka
  kafka:
    topic: t
template: "x"
fields: {}
"#;
        let e = parse_template_config(Path::new("t.yaml"), raw).unwrap_err();
        assert!(
            e.to_string().to_ascii_lowercase().contains("brokers"),
            "{e}"
        );
    }

    /// 测试内容：扩展名为 `.yaml` 时走完整解析路径（含 `min-interval` 等）。
    /// 输入：`example.yaml` 与合法 producer 片段。
    /// 预期：`min_interval_ms == 2`。
    #[test]
    fn parse_template_config_yaml_by_extension() {
        let raw = r#"sink:
  type: stdout
template: "a={{c}}"
min-interval: 2
fields:
  c: { type: counter }
"#;
        let c = parse_template_config(Path::new("example.yaml"), raw).unwrap();
        assert_eq!(c.min_interval_ms, 2);
    }

    /// 测试内容：非 `.yaml` 扩展名被拒绝。
    /// 输入：路径 `bad.json`。
    /// 预期：错误信息提示需 `.yaml`。
    #[test]
    fn parse_template_config_rejects_non_yaml_extension() {
        let raw = r#"template: "x"
fields: {}
"#;
        let e = parse_template_config(Path::new("bad.json"), raw).unwrap_err();
        assert!(e.to_string().contains(".yaml"), "unexpected error: {e}");
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

    /// 测试内容：`Hostname` 字段生成类 FQDN 形态（含点与连字符）。
    /// 输入：模板 `{{h}}`，字段 `Hostname`。
    /// 预期：渲染行同时包含 `.` 与 `-`。
    #[test]
    fn hostname_slot_contains_two_labels_and_suffix() {
        let cfg = TemplateConfig {
            template: "{{h}}".to_string(),
            fields: [("h".to_string(), crate::FieldSpec::Hostname)]
                .into_iter()
                .collect(),
            min_interval_ms: 1000,
            sink: sink_stdout(),
        };
        let mut r = TemplateRunner::try_new(cfg).unwrap();
        let line = r.next_line().unwrap();
        assert!(line.contains('.'), "{line:?}");
        assert!(line.contains('-'), "{line:?}");
    }

    /// 测试内容：嵌套 `template` 字段类型与子字段组合渲染。
    /// 输入：YAML 中 `sd` 为 `type: template`，内层固定整数与嵌套 `lorem-word` 拼接。
    /// 预期：行以 `[id iut="3" src="` 开头、以 `"]` 结尾，且 `src` 值内含 `.`。
    #[test]
    fn field_type_template_nested_renders_sd_shape() {
        let y = r#"
sink:
  type: stdout
template: "{{sd}}"
min-interval: 1
fields:
  sd:
    type: template
    template: '[id iut="{{iut}}" src="{{src}}"]'
    fields:
      iut:
        type: integer
        min: 3
        max: 3
      src:
        type: template
        template: "{{a}}.{{b}}"
        fields:
          a:
            type: lorem-word
          b:
            type: lorem-word
"#;
        let c: TemplateConfig = serde_yaml::from_str(y).unwrap();
        let mut r = TemplateRunner::try_new(c).unwrap();
        let line = r.next_line().unwrap();
        assert!(
            line.starts_with("[id iut=\"3\" src=\"") && line.ends_with("\"]"),
            "{line:?}"
        );
        assert!(line.contains('.'), "{line:?}");
    }

    /// 测试内容：`Template` 字段可无子字段映射（空 `fields`）。
    /// 输入：内存构造 `FieldSpec::Template` 固定子模板 `fixed`。
    /// 预期：`TemplateRunner::try_new` 成功；首行为 `fixed`。
    #[test]
    fn field_type_template_empty_subfields_ok() {
        let c = TemplateConfig {
            template: "{{x}}".to_string(),
            fields: [(
                "x".to_string(),
                crate::FieldSpec::Template {
                    template: "fixed".to_string(),
                    fields: BTreeMap::new(),
                },
            )]
            .into_iter()
            .collect(),
            min_interval_ms: 1,
            sink: sink_stdout(),
        };
        let mut r = TemplateRunner::try_new(c).unwrap();
        assert_eq!(r.next_line().unwrap(), "fixed");
    }

    /// 测试内容：`one-of` 分支中 counter 仅在选中含 `{{c}}` 的分支时递增。
    /// 输入：`branches: ["-", template+counter]`，循环 800 行。
    /// 预期：非 `-` 行数字严格等于递增计数；至少出现约百次以上模板分支（`next_expected >= 100`）。
    #[test]
    fn field_type_one_of_lazy_counter_only_on_template_branch() {
        let y = r#"
sink:
  type: stdout
template: "{{x}}"
min-interval: 1
fields:
  x:
    type: one-of
    branches:
      - "-"
      - template: "{{c}}"
        fields:
          c:
            type: counter
"#;
        let c: TemplateConfig = serde_yaml::from_str(y).unwrap();
        let mut r = TemplateRunner::try_new(c).unwrap();
        let mut next_expected: u64 = 0;
        for _ in 0..800 {
            let line = r.next_line().unwrap();
            if line == "-" {
                continue;
            }
            let n: u64 = line.parse().expect("non-dash must be counter digits");
            assert_eq!(
                n, next_expected,
                "counter must only advance when template branch is picked"
            );
            next_expected = next_expected.wrapping_add(1);
        }
        assert!(
            next_expected >= 100,
            "expected many template-branch picks in 800 trials"
        );
    }

    /// 测试内容：`one-of` 分支列表为空时配置非法。
    /// 输入：内存构造 `OneOf { branches: vec![] }`。
    /// 预期：`TemplateRunner::try_new` 返回 `Err`。
    #[test]
    fn field_type_one_of_empty_branches_rejected() {
        let c = TemplateConfig {
            template: "{{x}}".to_string(),
            fields: [(
                "x".to_string(),
                crate::FieldSpec::OneOf { branches: vec![] },
            )]
            .into_iter()
            .collect(),
            min_interval_ms: 1,
            sink: sink_stdout(),
        };
        assert!(TemplateRunner::try_new(c).is_err());
    }

    /// 测试内容：`Sentence` 字段词数落在 `[min,max]` 闭区间。
    /// 输入：`min: 2, max: 4`，抽样 20 行。
    /// 预期：每行按空白分词后词数在 2～4 之间。
    #[test]
    fn sentence_word_count_in_range() {
        let cfg = TemplateConfig {
            template: "{{s}}".to_string(),
            fields: [(
                "s".to_string(),
                crate::FieldSpec::Sentence { min: 2, max: 4 },
            )]
            .into_iter()
            .collect(),
            min_interval_ms: 1000,
            sink: sink_stdout(),
        };
        let mut r = TemplateRunner::try_new(cfg).unwrap();
        for _ in 0..20 {
            let line = r.next_line().unwrap();
            let n = line.split_whitespace().count();
            assert!((2..=4).contains(&n), "{line:?}");
        }
    }

    /// 测试内容：`format_sink_summary` 对 stdout / file / kafka（含多 broker 与 headers）的摘要字符串。
    /// 输入：构造 `SinkConfig`：无 kafka；file 有无 max-size；kafka 单/双 broker；kafka 带 1 个 header。
    /// 预期：依次为 `stdout`、`file: a.log`、带 max-size 的 file 行、`kafka: topic t @ h1:9092 +1 more`、`(+1 headers)` 后缀。
    #[test]
    fn format_sink_summary_stdout_file_kafka() {
        assert_eq!(
            format_sink_summary(&SinkConfig {
                sink_type: LineSinkType::Stdout,
                max_size_bytes: 0,
                output: None,
                kafka: None,
            }),
            "stdout"
        );
        assert_eq!(
            format_sink_summary(&SinkConfig {
                sink_type: LineSinkType::File,
                max_size_bytes: 0,
                output: Some("a.log".into()),
                kafka: None,
            }),
            "file: a.log"
        );
        assert_eq!(
            format_sink_summary(&SinkConfig {
                sink_type: LineSinkType::File,
                max_size_bytes: 100,
                output: Some("a.log".into()),
                kafka: None,
            }),
            "file: a.log (max-size: 100 bytes)"
        );
        assert_eq!(
            format_sink_summary(&SinkConfig {
                sink_type: LineSinkType::Kafka,
                max_size_bytes: 0,
                output: None,
                kafka: Some(KafkaConfig {
                    brokers: Some(vec!["h1:9092".into(), "h2:9092".into()]),
                    topic: Some("t".into()),
                    headers: None,
                    acks: None,
                    timeout_ms: None,
                    compression: None,
                    security_protocol: None,
                    ssl_endpoint_identification_algorithm: None,
                    ssl_ca_pem: None,
                    ssl_ca_location: None,
                    ssl_truststore_location: None,
                    ssl_truststore_password: None,
                    ssl_certificate_pem: None,
                    ssl_certificate_location: None,
                    ssl_private_key_pem: None,
                    ssl_key_location: None,
                    ssl_key_pem: None,
                    ssl_keystore_location: None,
                    ssl_keystore_password: None,
                    ssl_keystore_alias: None,
                    ssl_protocol: None,
                    ssl_enabled_protocols: None,
                    sasl_mechanism: None,
                    sasl_jaas_config: None,
                    sasl_username: None,
                    sasl_password: None,
                }),
            }),
            "kafka: topic t @ h1:9092 +1 more"
        );
        assert_eq!(
            format_sink_summary(&SinkConfig {
                sink_type: LineSinkType::Kafka,
                max_size_bytes: 0,
                output: None,
                kafka: Some(KafkaConfig {
                    brokers: Some(vec!["h1:9092".into()]),
                    topic: Some("t".into()),
                    headers: Some(
                        [("a".into(), serde_yaml::Value::String("1".into()))]
                            .into_iter()
                            .collect(),
                    ),
                    acks: None,
                    timeout_ms: None,
                    compression: None,
                    security_protocol: None,
                    ssl_endpoint_identification_algorithm: None,
                    ssl_ca_pem: None,
                    ssl_ca_location: None,
                    ssl_truststore_location: None,
                    ssl_truststore_password: None,
                    ssl_certificate_pem: None,
                    ssl_certificate_location: None,
                    ssl_private_key_pem: None,
                    ssl_key_location: None,
                    ssl_key_pem: None,
                    ssl_keystore_location: None,
                    ssl_keystore_password: None,
                    ssl_keystore_alias: None,
                    ssl_protocol: None,
                    ssl_enabled_protocols: None,
                    sasl_mechanism: None,
                    sasl_jaas_config: None,
                    sasl_username: None,
                    sasl_password: None,
                }),
            }),
            "kafka: topic t @ h1:9092 (+1 headers)"
        );
    }
}
