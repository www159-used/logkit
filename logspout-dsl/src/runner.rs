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

/// `kafka:` 下除 `brokers` / `topic` / `headers` 外的**全部**键（含 `acks`、`timeout-ms`、`compression`、TLS、SASL 等），YAML 原样落在 map 里。
pub type KafkaPassthroughFields = BTreeMap<String, serde_yaml::Value>;

/// 与 Kafka 客户端配置对齐：**`brokers` / `topic` / `headers`** 为显式字段；**其余键**一律经 [`KafkaConfig::extra`] 透传（由 worker / 日后 client 接线解析）。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KafkaConfig {
    /// broker 地址列表，如 `127.0.0.1:9092`。
    pub brokers: Vec<String>,
    pub topic: String,
    /// 每条 produce 的 **record headers**：键与值为 UTF-8 语义；YAML **`null`** 表示 Kafka **空值 header**（`Option::None`）。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub headers: Option<BTreeMap<String, serde_yaml::Value>>,
    #[serde(flatten)]
    pub extra: KafkaPassthroughFields,
}

/// 行日志 sink：**必填** `type`（`kafka` | `file` | `stdout`）。
/// - **`output`**：仅 **`type: file`** 有意义；其它类型**不需要**，合并/解析入口会丢弃多余或前层残留的 `output`。
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

/// 单层 YAML 里可选的 `sink:` 片段（合并时**后者覆盖前者**的各子字段）。
#[derive(Debug, Clone, Default, Deserialize)]
pub struct SinkLayer {
    #[serde(rename = "type", default)]
    pub sink_type: Option<LineSinkType>,
    #[serde(
        rename = "max-size",
        default,
        deserialize_with = "crate::human_size::deserialize_opt_max_size"
    )]
    pub max_size_bytes: Option<u64>,
    #[serde(default)]
    pub output: Option<String>,
    #[serde(default)]
    pub kafka: Option<KafkaConfig>,
}

/// 合并后的 producer 配置（CLI 合并多文件后序列化为**单份 YAML** 传给 daemon / 落盘）。
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

/// 单层 YAML：**模板（造行）** 与 **`sink:`** 可拆在不同文件；[`load_and_merge_producer_paths`] 按路径顺序合并。
#[derive(Debug, Clone, Default, Deserialize)]
pub struct ProducerConfigLayer {
    #[serde(default)]
    pub template: Option<String>,
    #[serde(default)]
    pub fields: Option<BTreeMap<String, FieldSpec>>,
    #[serde(rename = "min-interval", default)]
    pub min_interval_ms: Option<u64>,
    #[serde(default)]
    pub sink: Option<SinkLayer>,
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
        return Err(ConfigParseError::PathNotYaml(
            path.display().to_string(),
        ));
    }
    Ok(())
}

/// `output` 仅对 [`LineSinkType::File`] 有效；其它类型去掉 `output`，避免单层误写或多层合并时残留前层的文件路径。
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
            let broker = k.brokers.first().map(|b| b.as_str()).unwrap_or("?");
            let more = k.brokers.len().saturating_sub(1);
            let brokers = if more > 0 {
                format!("{broker} +{more} more")
            } else {
                broker.to_string()
            };
            let hdr = k.headers.as_ref().map(|h| h.len()).unwrap_or(0);
            if hdr > 0 {
                format!("kafka: topic {} @ {} (+{} headers)", k.topic, brokers, hdr)
            } else {
                format!("kafka: topic {} @ {}", k.topic, brokers)
            }
        }
    }
}

/// 将多层配置合并为一份 [`TemplateConfig`]（后者覆盖前者）。
pub fn merge_producer_layers(layers: Vec<ProducerConfigLayer>) -> Result<TemplateConfig, ConfigParseError> {
    if layers.is_empty() {
        return Err(ConfigParseError::Merge("至少需要 1 个 producer YAML 路径".into()));
    }
    let mut acc = ProducerConfigLayer::default();
    let mut sink_acc = SinkLayer::default();
    for layer in layers {
        if layer.template.is_some() {
            acc.template = layer.template;
        }
        if layer.fields.is_some() {
            acc.fields = layer.fields;
        }
        if layer.min_interval_ms.is_some() {
            acc.min_interval_ms = layer.min_interval_ms;
        }
        if let Some(s) = layer.sink {
            if s.sink_type.is_some() {
                sink_acc.sink_type = s.sink_type;
            }
            if s.max_size_bytes.is_some() {
                sink_acc.max_size_bytes = s.max_size_bytes;
            }
            if s.output.is_some() {
                sink_acc.output = s.output;
            }
            if s.kafka.is_some() {
                sink_acc.kafka = s.kafka;
            }
        }
    }
    let template = acc.template.ok_or_else(|| {
        ConfigParseError::Merge("合并后仍缺少必填字段 `template`（请在某个 YAML 中提供）".into())
    })?;
    if template.trim().is_empty() {
        return Err(ConfigParseError::Merge("`template` 不能为空".into()));
    }
    let sink_type = sink_acc.sink_type.ok_or_else(|| {
        ConfigParseError::Merge(
            "合并后仍缺少 `sink.type`（请在某个 YAML 的 `sink:` 下设置 type: kafka | file | stdout）".into(),
        )
    })?;
    let sink = normalize_sink_output(SinkConfig {
        sink_type,
        max_size_bytes: sink_acc.max_size_bytes.unwrap_or(0),
        output: sink_acc.output,
        kafka: sink_acc.kafka,
    });
    let cfg = TemplateConfig {
        template,
        fields: acc.fields.unwrap_or_default(),
        min_interval_ms: acc.min_interval_ms.unwrap_or(1000),
        sink,
    };
    validate_template_sink(&cfg)?;
    Ok(cfg)
}

/// 检查 `sink.type` 与 `output` / `kafka` 是否一致。
/// 非 `file` 的 `output` 应在进入此函数前丢弃（[`merge_producer_layers`] 与 [`parse_template_config`] 已处理）。
pub fn validate_template_sink(cfg: &TemplateConfig) -> Result<(), ConfigParseError> {
    match cfg.sink.sink_type {
        LineSinkType::Kafka => {
            if cfg.sink.kafka.is_none() {
                return Err(ConfigParseError::Merge(
                    "`sink.type: kafka` 时必须提供 `sink.kafka:` 段".into(),
                ));
            }
        }
        LineSinkType::File => {
            let o = cfg.sink.output.as_deref().unwrap_or("").trim();
            if o.is_empty() {
                return Err(ConfigParseError::Merge(
                    "`sink.type: file` 时必须提供非空的 `sink.output`".into(),
                ));
            }
        }
        LineSinkType::Stdout => {}
    }
    Ok(())
}

/// 读取多个 `.yaml` / `.yml`，按顺序合并（与 `logspout start a.yaml b.yaml` 一致）。
pub fn load_and_merge_producer_paths<P: AsRef<Path>>(paths: &[P]) -> Result<TemplateConfig, ConfigParseError> {
    if paths.is_empty() {
        return Err(ConfigParseError::Merge("至少需要 1 个配置文件路径".into()));
    }
    let mut layers = Vec::with_capacity(paths.len());
    for path in paths {
        let path = path.as_ref();
        yaml_extension_ok(path)?;
        let raw = std::fs::read_to_string(path).map_err(|e| {
            ConfigParseError::Io(path.display().to_string(), e)
        })?;
        let layer: ProducerConfigLayer = serde_yaml::from_str(&raw).map_err(|e| {
            ConfigParseError::Merge(format!("parse {}: {e}", path.display()))
        })?;
        layers.push(layer);
    }
    merge_producer_layers(layers)
}

/// 将合并后的配置序列化为单份 YAML 字符串（供 gRPC `producer_yaml` / daemon 落盘）。
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
        assert_eq!(k.topic, "t1");
        assert_eq!(k.brokers, vec!["127.0.0.1:9092".to_string()]);
        assert!(k.extra.is_empty(), "no extra keys");
    }

    #[test]
    fn deserialize_producer_yaml_kafka_passthrough_extra() {
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
        assert_eq!(
            k.extra.get("acks").and_then(|v| v.as_str()),
            Some("all")
        );
        assert_eq!(
            k.extra.get("timeout-ms").and_then(|v| v.as_u64()),
            Some(12_000)
        );
        assert_eq!(
            k.extra.get("compression").and_then(|v| v.as_str()),
            Some("gzip")
        );
        assert_eq!(
            k.extra.get("security-protocol").and_then(|v| v.as_str()),
            Some("SSL")
        );
        assert_eq!(
            k.extra.get("ssl-ca-location").and_then(|v| v.as_str()),
            Some("/tmp/ca.pem")
        );
    }

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
        assert_eq!(k.extra.get("acks").and_then(|v| v.as_i64()), Some(-1));
    }

    /// 测试内容：Kafka sink 的 `headers` 映射能从 producer YAML 反序列化，且与 `extra` 透传键分离。
    /// 输入：`sink.kafka` 含 `brokers`、`topic` 及 `headers`（字符串、带引号 trace-id、`null`、整数）。
    /// 预期：`headers` 各键对应 YAML 类型正确；`empty-value` 为 null；`extra` 为空。
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
        assert!(k.extra.is_empty());
    }

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

    #[test]
    fn parse_template_config_rejects_non_yaml_extension() {
        let raw = r#"template: "x"
fields: {}
"#;
        let e = parse_template_config(Path::new("bad.json"), raw).unwrap_err();
        assert!(e.to_string().contains(".yaml"), "unexpected error: {e}");
    }

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

    #[test]
    fn merge_two_layers_sink_overrides_output() {
        let a: ProducerConfigLayer = serde_yaml::from_str(
            r#"
sink:
  type: file
  output: first.log
template: "{{x}}"
min-interval: 5
fields:
  x:
    type: counter
"#,
        )
        .unwrap();
        let b: ProducerConfigLayer = serde_yaml::from_str(
            r#"
sink:
  output: second.log
  max-size: 99
"#,
        )
        .unwrap();
        let c = merge_producer_layers(vec![a, b]).unwrap();
        assert_eq!(c.min_interval_ms, 5);
        assert_eq!(c.sink.output.as_deref(), Some("second.log"));
        assert_eq!(c.sink.max_size_bytes, 99);
        let mut r = TemplateRunner::try_new(c).unwrap();
        assert_eq!(r.next_line().unwrap(), "0");
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
                    brokers: vec!["h1:9092".into(), "h2:9092".into()],
                    topic: "t".into(),
                    headers: None,
                    extra: BTreeMap::new(),
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
                    brokers: vec!["h1:9092".into()],
                    topic: "t".into(),
                    headers: Some(
                        [("a".into(), serde_yaml::Value::String("1".into()))]
                            .into_iter()
                            .collect(),
                    ),
                    extra: BTreeMap::new(),
                }),
            }),
            "kafka: topic t @ h1:9092 (+1 headers)"
        );
    }

    #[test]
    fn merge_later_kafka_drops_stale_file_output() {
        let a: ProducerConfigLayer = serde_yaml::from_str(
            r#"
sink:
  type: file
  output: stale.log
template: "{{x}}"
fields:
  x: { type: counter }
"#,
        )
        .unwrap();
        let b: ProducerConfigLayer = serde_yaml::from_str(
            r#"
sink:
  type: kafka
  kafka:
    brokers: ["127.0.0.1:9092"]
    topic: t
"#,
        )
        .unwrap();
        let c = merge_producer_layers(vec![a, b]).unwrap();
        assert_eq!(c.sink.sink_type, LineSinkType::Kafka);
        assert!(c.sink.output.is_none(), "kafka sink must not retain file output");
    }

    #[test]
    fn later_layer_overrides_template() {
        let a: ProducerConfigLayer = serde_yaml::from_str(
            r#"
sink:
  type: stdout
template: "a"
"#,
        )
        .unwrap();
        let b: ProducerConfigLayer = serde_yaml::from_str(r#"template: "b""#).unwrap();
        let c = merge_producer_layers(vec![a, b]).unwrap();
        assert_eq!(c.template, "b");
    }
}

