//! `sink:` 变体（`stdout` | `file` | `kafka`）及校验、摘要。

use std::path::{Path, PathBuf};

use chrono::Local;
use serde::{Deserialize, Serialize};

use crate::ConfigParseError;

use super::kafka::{validate_agent_source_id, KafkaConfig, KafkaSinkMode};

/// 行日志 sink：**必填** Serde internally-tagged **`type`**（`kafka` | `file` | `stdout`）。
/// - **`output`**：仅 **`type: file`** 有意义；可省略，届时由 daemon 生成默认绝对路径；显式填写时必须为绝对路径。写 **`stdout` / `kafka`** 时多余键由 Serde 忽略。
/// - **`max-size`**：仅 **`type: file`** 支持；整数（字节）或字符串，如 **`64KiB`**、`10MiB`（底数 1024）。`stdout` / `kafka` 无此字段；遗留 YAML 若仍写 `max-size`，Serde 默认忽略未知键。
/// - **`kafka`**：仅 **`type: kafka`** 时需要（`sink.kafka:` 映射块）。
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum SinkConfig {
    Stdout,
    File {
        #[serde(
            rename = "max-size",
            default = "super::default_max_size_bytes",
            deserialize_with = "crate::human_size::deserialize_max_size"
        )]
        max_size_bytes: u64,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        output: Option<String>,
    },
    Kafka {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        kafka: Option<Box<KafkaConfig>>,
    },
}

impl SinkConfig {
    /// 仅 [`SinkConfig::File`] 有 **`max-size`**；其它变体恒为 `0`（截断仅 file 使用）。
    pub fn max_size_bytes(&self) -> u64 {
        match self {
            SinkConfig::Stdout => 0,
            SinkConfig::File { max_size_bytes, .. } => *max_size_bytes,
            SinkConfig::Kafka { .. } => 0,
        }
    }

    /// 若为 Kafka sink 且配置了 `sink.kafka:`，返回该段。
    pub fn kafka_section(&self) -> Option<&KafkaConfig> {
        match self {
            SinkConfig::Kafka { kafka, .. } => kafka.as_deref(),
            _ => None,
        }
    }

    /// worker 运行前：对 File sink 补全缺失的 `output`（校验由构造/脚本侧完成）。
    ///
    /// 非 File 变体直接返回；缺省路径为 `{output_base}/{worker_id 前 8 位}-{YYYYMMDD}.log`。
    pub fn fill_default_output(&mut self, output_base: &Path, worker_id: &str) {
        let SinkConfig::File { output, .. } = self else {
            return;
        };

        let missing = output
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .is_none();
        if missing {
            *output = Some(
                default_file_output_path(output_base, worker_id)
                    .to_string_lossy()
                    .into_owned(),
            );
        }
    }
}

/// 供 list / stat 等展示的一行 **`sink:`** 摘要（`stdout` / `file:` / `kafka:`）。
pub fn format_sink_summary(sink: &SinkConfig) -> String {
    match sink {
        SinkConfig::Stdout => "stdout".into(),
        SinkConfig::File {
            max_size_bytes,
            output,
        } => {
            let path = output.as_deref().unwrap_or("(auto)");
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
                (
                    k.topic.as_deref().unwrap_or("?"),
                    k.headers.as_ref().map(|h| h.len()).unwrap_or(0),
                )
            };
            if hdr > 0 {
                format!("kafka: topic {topic} @ {brokers} (+{hdr} headers)")
            } else {
                format!("kafka: topic {topic} @ {brokers}")
            }
        }
    }
}

/// Serde 无法表达的 `sink` 跨字段约束（brokers、topic、output 等）。
pub fn validate_sink(sink: &SinkConfig) -> Result<(), ConfigParseError> {
    match sink {
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
                let topic_ok = k.topic.as_deref().is_some_and(|t| !t.trim().is_empty());
                if !topic_ok {
                    return Err(ConfigParseError::Merge(
                        "`sink.type: kafka` requires a non-empty `sink.kafka.topic` when `sink.kafka.mode` is `common` (default)"
                            .into(),
                    ));
                }
            }
        }
        SinkConfig::File { output, .. } => {
            let Some(o) = output.as_deref().map(str::trim).filter(|s| !s.is_empty()) else {
                return Ok(());
            };
            if !Path::new(o).is_absolute() {
                return Err(ConfigParseError::Merge(
                    "`sink.output` must be an absolute path when `sink.type: file`; omit it to let daemon auto-generate a file under `[logend].worker-output-dir`".into(),
                ));
            }
        }
        SinkConfig::Stdout => {}
    }
    Ok(())
}

fn default_file_output_path(output_base: &Path, worker_id: &str) -> PathBuf {
    let id8 = worker_id.get(..8).unwrap_or(worker_id);
    let date = Local::now().format("%Y%m%d");
    output_base.join(format!("{id8}-{date}.log"))
}

#[cfg(test)]
mod tests {
    use super::super::kafka::{KafkaConfig, KafkaSinkMode};
    use super::*;

    #[test]
    fn kafka_section_returns_some_for_kafka_variant() {
        let k = KafkaConfig {
            mode: KafkaSinkMode::Common,
            brokers: Some(vec!["x".into()]),
            topic: Some("t".into()),
            ..Default::default()
        };
        let sink = SinkConfig::Kafka {
            kafka: Some(Box::new(k)),
        };
        assert!(sink.kafka_section().is_some());
        assert_eq!(sink.max_size_bytes(), 0);
    }

    #[test]
    fn kafka_section_none_for_stdout() {
        let sink = SinkConfig::Stdout;
        assert!(sink.kafka_section().is_none());
    }

    /// 测试内容：`format_sink_summary` 对 stdout / file / kafka（含多 broker 与 headers）的摘要字符串。
    /// 输入：构造 `SinkConfig`：无 kafka；file 有无 max-size；kafka 单/双 broker；kafka 带 1 个 header。
    /// 预期：依次为 `stdout`、`file: (auto)`、`file: /tmp/a.log`、带 max-size 的 file 行、`kafka: topic t @ h1:9092 +1 more`、`(+1 headers)` 后缀。
    #[test]
    fn format_sink_summary_stdout_file_kafka() {
        assert_eq!(format_sink_summary(&SinkConfig::Stdout), "stdout");
        assert_eq!(
            format_sink_summary(&SinkConfig::File {
                max_size_bytes: 0,
                output: None,
            }),
            "file: (auto)"
        );
        assert_eq!(
            format_sink_summary(&SinkConfig::File {
                max_size_bytes: 0,
                output: Some("/tmp/a.log".into()),
            }),
            "file: /tmp/a.log"
        );
        assert_eq!(
            format_sink_summary(&SinkConfig::File {
                max_size_bytes: 100,
                output: Some("/tmp/a.log".into()),
            }),
            "file: /tmp/a.log (max-size: 100 bytes)"
        );
        assert_eq!(
            format_sink_summary(&SinkConfig::Kafka {
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
                kafka: Some(Box::new(KafkaConfig {
                    brokers: Some(vec!["h1:9092".into()]),
                    topic: Some("t".into()),
                    headers: Some([("a".into(), Some("1".into()))].into_iter().collect(),),
                    ..Default::default()
                })),
            }),
            "kafka: topic t @ h1:9092 (+1 headers)"
        );
    }

    /// 测试内容：`sink.type: file` 省略 `output` 时允许由 daemon 自动生成默认文件。
    /// 输入：`SinkConfig::File { output: None }`。
    /// 预期：`validate_sink` 返回 `Ok(())`。
    #[test]
    fn validate_file_sink_allows_missing_output_for_daemon_default() {
        let sink = SinkConfig::File {
            max_size_bytes: 0,
            output: None,
        };
        validate_sink(&sink).expect("daemon should auto-generate output path");
    }

    /// 测试内容：common 模式 kafka 缺 `topic` 被拒绝。
    /// 输入：仅有 brokers、无 topic 的 `SinkConfig::Kafka`。
    /// 预期：`validate_sink` 错误信息含 `topic`。
    #[test]
    fn validate_kafka_rejects_missing_topic() {
        let sink = SinkConfig::Kafka {
            kafka: Some(Box::new(KafkaConfig {
                brokers: Some(vec!["127.0.0.1:9092".into()]),
                topic: None,
                ..Default::default()
            })),
        };
        let e = validate_sink(&sink).unwrap_err();
        assert!(e.to_string().to_ascii_lowercase().contains("topic"), "{e}");
    }

    /// 测试内容：kafka 缺非空 broker 被拒绝。
    /// 输入：有 topic、无 brokers 的 `SinkConfig::Kafka`。
    /// 预期：`validate_sink` 错误信息含 `brokers`。
    #[test]
    fn validate_kafka_rejects_missing_brokers() {
        let sink = SinkConfig::Kafka {
            kafka: Some(Box::new(KafkaConfig {
                brokers: None,
                topic: Some("t".into()),
                ..Default::default()
            })),
        };
        let e = validate_sink(&sink).unwrap_err();
        assert!(
            e.to_string().to_ascii_lowercase().contains("brokers"),
            "{e}"
        );
    }

    /// 测试内容：省略 `sink.output` 时由 `fill_default_output` 生成默认绝对路径。
    /// 输入：`output_base = /var/tmp/logkit`、worker id `12345678-...`、`SinkConfig::File { output: None }`。
    /// 预期：`output` 被补成 `/var/tmp/logkit/12345678-YYYYMMDD.log`。
    #[test]
    fn fill_default_output_generates_default_under_worker_output_dir() {
        let mut sink = SinkConfig::File {
            max_size_bytes: 0,
            output: None,
        };

        sink.fill_default_output(
            Path::new("/var/tmp/logkit"),
            "12345678-aaaa-bbbb-cccc-ddddeeeeffff",
        );

        let SinkConfig::File { output, .. } = sink else {
            panic!("expected file sink");
        };
        let output = output.expect("generated output");
        assert!(output.starts_with("/var/tmp/logkit/12345678-"));
        assert!(output.ends_with(".log"));
    }

    /// 测试内容：显式相对 `sink.output` 在 `validate_sink` 被拒绝。
    /// 输入：`output = "logs/a.log"` 的 file sink。
    /// 预期：错误信息含 absolute path。
    #[test]
    fn validate_file_sink_rejects_relative_output() {
        let sink = SinkConfig::File {
            max_size_bytes: 0,
            output: Some("logs/a.log".into()),
        };
        let err = validate_sink(&sink).unwrap_err();
        assert!(err.to_string().contains("absolute path"), "{err}");
    }
}
