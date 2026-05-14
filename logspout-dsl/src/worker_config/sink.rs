//! `sink:` 变体（`stdout` | `file` | `kafka`）。

use serde::{Deserialize, Serialize};

use super::kafka::KafkaConfig;

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
            default = "super::default_max_size_bytes",
            deserialize_with = "crate::human_size::deserialize_max_size"
        )]
        max_size_bytes: u64,
    },
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
        #[serde(
            rename = "max-size",
            default = "super::default_max_size_bytes",
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
            max_size_bytes: 0,
            kafka: Some(Box::new(k)),
        };
        assert!(sink.kafka_section().is_some());
        assert_eq!(sink.max_size_bytes(), 0);
    }

    #[test]
    fn kafka_section_none_for_stdout() {
        let sink = SinkConfig::Stdout { max_size_bytes: 0 };
        assert!(sink.kafka_section().is_none());
    }
}
