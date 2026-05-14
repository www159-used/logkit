use std::collections::BTreeMap;
use std::path::Path;

use handlebars::Handlebars;
use serde_json::{Map, Value};

use crate::field_spec::slots_from_fields;
use crate::facade::TemplateSlot;
use crate::worker_config::{
    validate_agent_source_id, KafkaSinkMode, SinkConfig, TemplateConfig,
};
use crate::{ConfigParseError, Error};

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

/// 供 list / stat 等展示的一行 **`sink:`** 摘要（`stdout` / `file:` / `kafka:`）。
pub fn format_sink_summary(sink: &SinkConfig) -> String {
    match sink {
        SinkConfig::Stdout { .. } => "stdout".into(),
        SinkConfig::File {
            max_size_bytes,
            output,
        } => {
            let path = output.as_deref().unwrap_or("?");
            if *max_size_bytes > 0 {
                format!("file: {path} (max-size: {} bytes)", max_size_bytes)
            } else {
                format!("file: {path}")
            }
        }
        SinkConfig::Kafka { kafka, .. } => {
            let Some(k) = kafka.as_deref() else {
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
            let (topic, hdr) = if k.mode == KafkaSinkMode::Agent {
                ("raw_message (agent)", 0usize)
            } else {
                (k.topic.as_deref().unwrap_or("?"), k.headers.as_ref().map(|h| h.len()).unwrap_or(0))
            };
            if hdr > 0 {
                format!("kafka: topic {topic} @ {brokers} (+{hdr} headers)")
            } else {
                format!("kafka: topic {topic} @ {brokers}")
            }
        }
    }
}

/// 检查 `sink.type` 与 `output` / `kafka` 是否一致。
/// 非 `file` 写错的 `output` 键由 Serde 忽略（internally tagged 变体无该字段）。
pub fn validate_template_sink(cfg: &TemplateConfig) -> Result<(), ConfigParseError> {
    match &cfg.sink {
        SinkConfig::Kafka { kafka, .. } => {
            let Some(k) = kafka.as_deref() else {
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
            if k.mode == KafkaSinkMode::Agent {
                let Some(agent) = k.agent.as_ref() else {
                    return Err(ConfigParseError::Merge(
                        "`sink.kafka.mode: agent` requires a `sink.kafka.agent:` mapping（字段均可选，含 `domain`）"
                            .into(),
                    ));
                };
                if let Some(ref sid) = agent.source_id {
                    let t = sid.trim();
                    if !validate_agent_source_id(t) {
                        return Err(ConfigParseError::Merge(
                            "`sink.kafka.agent.source_id` must be a 36-character UUID (8-4-4-4-12 hex with hyphens)"
                                .into(),
                        ));
                    }
                }
            } else {
                let topic_ok = k
                    .topic
                    .as_deref()
                    .is_some_and(|t| !t.trim().is_empty());
                if !topic_ok {
                    return Err(ConfigParseError::Merge(
                        "`sink.type: kafka` requires a non-empty `sink.kafka.topic` when `sink.kafka.mode` is `common` (default)"
                            .into(),
                    ));
                }
            }
        }
        SinkConfig::File { output, .. } => {
            let o = output.as_deref().unwrap_or("").trim();
            if o.is_empty() {
                return Err(ConfigParseError::Merge(
                    "`sink.type: file` requires a non-empty `sink.output` path".into(),
                ));
            }
        }
        SinkConfig::Stdout { .. } => {}
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
    let cfg: TemplateConfig = serde_yaml::from_str(raw)?;
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
    use crate::worker_config::{KafkaConfig, SinkConfig, TemplateConfig};

    fn sink_stdout() -> SinkConfig {
        SinkConfig::Stdout {
            max_size_bytes: 0,
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

    /// 测试内容：`parse_template_config` 后 `TemplateRunner` 对最小 counter 模板渲染首行。
    /// 输入：`min-interval: 1`、`stdout` sink、模板 `x={{c}}`、字段 `counter`。
    /// 预期：首行为 `x=0`。
    #[test]
    fn parse_template_config_minimal_counter_template_renders() {
        let y = r#"
sink:
  type: stdout
template: "x={{c}}"
min-interval: 1
fields:
  c:
    type: counter
"#;
        let c = parse_template_config(Path::new("t.yaml"), y).unwrap();
        let mut r = TemplateRunner::try_new(c).unwrap();
        assert_eq!(r.next_line().unwrap(), "x=0");
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
    /// 输入：含 `brokers` 但省略 `topic` 的最小 worker 配置 YAML。
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
    /// 输入：`example.yaml` 与合法 worker 配置片段。
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
            format_sink_summary(&SinkConfig::Stdout {
                max_size_bytes: 0,
            }),
            "stdout"
        );
        assert_eq!(
            format_sink_summary(&SinkConfig::File {
                max_size_bytes: 0,
                output: Some("a.log".into()),
            }),
            "file: a.log"
        );
        assert_eq!(
            format_sink_summary(&SinkConfig::File {
                max_size_bytes: 100,
                output: Some("a.log".into()),
            }),
            "file: a.log (max-size: 100 bytes)"
        );
        assert_eq!(
            format_sink_summary(&SinkConfig::Kafka {
                max_size_bytes: 0,
                kafka: Some(Box::new(KafkaConfig {
                    brokers: Some(vec!["h1:9092".into(), "h2:9092".into()]),
                    topic: Some("t".into()),
                    ..Default::default()
                })),
            }),
            "kafka: topic t @ h1:9092 +1 more"
        );
        assert_eq!(
            format_sink_summary(&SinkConfig::Kafka {
                max_size_bytes: 0,
                kafka: Some(Box::new(KafkaConfig {
                    brokers: Some(vec!["h1:9092".into()]),
                    topic: Some("t".into()),
                    headers: Some(
                        [("a".into(), Some("1".into()))].into_iter().collect(),
                    ),
                    ..Default::default()
                })),
            }),
            "kafka: topic t @ h1:9092 (+1 headers)"
        );
    }
}
