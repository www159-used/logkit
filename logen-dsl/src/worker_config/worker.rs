//! 根级 [`WorkerConfig`]（`template` / `fields` / `min-interval` / `threads` / `sink`）。

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::field_spec::FieldSpec;

use super::sink::SinkConfig;

/// 一份 worker 实例配置（`.yaml` 对应整棵配置树；序列化后可由 daemon / worker 落盘或经 gRPC 传递）。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerConfig {
    /// Handlebars 源字符串（无须外置文件）。占位符须与 `fields` 键一致；**勿**用 `len` 等名，会与 handlebars 内置 helper（如 `{{len …}}`）冲突。
    pub template: String,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub fields: BTreeMap<String, FieldSpec>,
    /// 每条日志间隔（毫秒），默认 1000。
    #[serde(rename = "min-interval", default = "super::default_min_interval_ms")]
    pub min_interval_ms: u64,
    /// 并发写日志循环数（每个循环独立 `TemplateRunner` 与 sink），默认 1。
    #[serde(default = "super::default_threads")]
    pub threads: u32,
    /// 行日志写出：**`sink.type`** 及关联项（不可再扁平写在根上）。
    pub sink: SinkConfig,
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::super::kafka::KafkaSinkMode;
    use super::WorkerConfig;
    use crate::parse_worker_config;

    /// 测试内容：最小 worker 配置 YAML 仅经 Serde 反序列化后的字段。
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
        let c: WorkerConfig = serde_yaml::from_str(y).unwrap();
        assert_eq!(c.min_interval_ms, 1);
        assert_eq!(c.sink.max_size_bytes(), 0);
        assert_eq!(c.template, "x={{c}}");
    }

    /// 测试内容：`threads` 显式配置反序列化。
    #[test]
    fn worker_config_yaml_threads_deserializes() {
        let y = r#"
sink:
  type: stdout
template: "x"
fields: {}
threads: 4
"#;
        let c: WorkerConfig = serde_yaml::from_str(y).unwrap();
        assert_eq!(c.threads, 4);
    }

    /// 测试内容：整份 YAML 中 `sink.kafka` 最小段反序列化。
    #[test]
    fn worker_yaml_kafka_section_minimal() {
        let y = r#"
sink:
  type: kafka
  kafka:
    brokers: ["127.0.0.1:9092"]
    topic: t1
template: "x"
fields: {}
"#;
        let c: WorkerConfig = serde_yaml::from_str(y).unwrap();
        let k = c.sink.kafka_section().expect("kafka");
        assert_eq!(k.mode, KafkaSinkMode::Common);
        assert_eq!(k.topic.as_deref(), Some("t1"));
    }

    /// 测试内容：未写 `max-size` 时 `type: file` 的 `max_size_bytes` 默认 0。
    #[test]
    fn deserialize_max_size_defaults_to_zero() {
        let y = r#"
sink:
  type: file
  output: out.log
template: "x"
fields: {}
"#;
        let c: WorkerConfig = serde_yaml::from_str(y).unwrap();
        assert_eq!(c.sink.max_size_bytes(), 0);
    }

    /// 测试内容：`max-size` 为整数字节标量（仅 `type: file`）。
    #[test]
    fn deserialize_max_size_nonzero() {
        let y = r#"
sink:
  type: file
  output: out.log
  max-size: 65536
template: "x"
fields: {}
"#;
        let c: WorkerConfig = serde_yaml::from_str(y).unwrap();
        assert_eq!(c.sink.max_size_bytes(), 65536);
    }

    /// 测试内容：`max-size` 支持人类可读无引号字符串（KiB）。
    #[test]
    fn deserialize_max_size_human_string() {
        let y = r#"
sink:
  type: file
  output: out.log
  max-size: 64KiB
template: "x"
fields: {}
"#;
        let c: WorkerConfig = serde_yaml::from_str(y).unwrap();
        assert_eq!(c.sink.max_size_bytes(), 65536);
    }

    /// 测试内容：`max-size` 为带引号的人类可读小数单位时按 MiB 换算并四舍五入。
    #[test]
    fn deserialize_max_size_human_quoted() {
        let y = r#"
sink:
  type: file
  output: out.log
  max-size: "1.5MiB"
template: "x"
fields: {}
"#;
        let c: WorkerConfig = serde_yaml::from_str(y).unwrap();
        assert_eq!(
            c.sink.max_size_bytes(),
            (1.5_f64 * 1048576_f64).round() as u64
        );
    }

    /// 测试内容：YAML 折叠标量 `template: >-` 多行合并为单行模板字符串。
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
        let c: WorkerConfig = serde_yaml::from_str(y).unwrap();
        assert!(
            !c.template.contains('\n'),
            "folded scalar should be one line: {:?}",
            c.template
        );
        assert!(c.template.contains("part2"));
        assert!(c.template.contains("part3"));
    }

    /// 测试内容：`parse_worker_config` 对 `mode: agent` 且无 `topic` 应成功。
    #[test]
    fn parse_worker_config_accepts_kafka_agent_without_topic() {
        let raw = r#"sink:
  type: kafka
  kafka:
    mode: agent
    brokers: ["127.0.0.1:9092"]
    agent:
      domain: acme
template: "{}"
fields: {}
"#;
        let c = parse_worker_config(Path::new("t.yaml"), raw).unwrap();
        let k = c.sink.kafka_section().unwrap();
        assert_eq!(k.mode, KafkaSinkMode::Agent);
        assert!(k.topic.is_none());
    }

    /// 测试内容：`parse_worker_config` 对 `mode: agent` 且 `agent: {}`（无 `domain`）应成功。
    #[test]
    fn parse_worker_config_accepts_kafka_agent_without_domain() {
        let raw = r#"sink:
  type: kafka
  kafka:
    mode: agent
    brokers: ["127.0.0.1:9092"]
    agent: {}
template: "{}"
fields: {}
"#;
        let c = parse_worker_config(Path::new("t.yaml"), raw).unwrap();
        let k = c.sink.kafka_section().unwrap();
        assert_eq!(k.mode, KafkaSinkMode::Agent);
        assert!(k.agent.as_ref().unwrap().domain.is_none());
    }

    /// 测试内容：`mode: agent` 且 `source_id` 非合法 UUID 应失败。
    #[test]
    fn parse_worker_config_rejects_bad_agent_source_id() {
        let raw = r#"sink:
  type: kafka
  kafka:
    mode: agent
    brokers: ["127.0.0.1:9092"]
    agent:
      domain: acme
      source_id: NOTHEX
template: "{}"
fields: {}
"#;
        let e = parse_worker_config(Path::new("t.yaml"), raw).unwrap_err();
        assert!(
            e.to_string().to_ascii_lowercase().contains("source_id")
                || e.to_string().to_ascii_lowercase().contains("source id"),
            "{e}"
        );
    }

    /// 测试内容：`mode: agent` 但省略 `agent:` 块时应失败。
    #[test]
    fn parse_worker_config_rejects_kafka_agent_missing_agent_block() {
        let raw = r#"sink:
  type: kafka
  kafka:
    mode: agent
    brokers: ["127.0.0.1:9092"]
template: "{}"
fields: {}
"#;
        let e = parse_worker_config(Path::new("t.yaml"), raw).unwrap_err();
        assert!(
            e.to_string().to_ascii_lowercase().contains("agent"),
            "{e}"
        );
    }
}
