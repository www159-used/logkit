use serde::{Deserialize, Serialize};
use std::path::Path;

/// Console 启动 worker 表单（服务端转为 `.logen` 控制脚本）。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WorkerStartForm {
    pub body_preset: String,
    pub sink_kind: WorkerSinkKind,
    /// 日志速率，如 `1ms`、`100ms`；空或 `0ms` 表示不限速。
    pub rate: String,
    pub threads: Option<u32>,
    pub label: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum WorkerSinkKind {
    Stdout,
    File {
        /// 绝对路径；空则 logend 自动生成。
        output: String,
        /// 如 `64MiB`；空表示不限或默认。
        max_size: String,
    },
    Kafka {
        topic: String,
        brokers: String,
    },
}

/// 与 [`logen_script::preset_names`] 一致。
pub const BODY_PRESETS: &[&str] = &[
    "preset_json",
    "preset_cef",
    "preset_leefv2",
    "preset_cyberark",
    "preset_firewall_winicssec",
    "preset_ips_nsfocus",
    "preset_exchange_tracking",
    "preset_apache_access_xff",
    "preset_apache_middleware",
];

pub fn build_control_script(form: &WorkerStartForm) -> Result<String, String> {
    if !BODY_PRESETS.contains(&form.body_preset.as_str()) {
        return Err(format!("unknown body preset: {}", form.body_preset));
    }
    let sink_expr = match &form.sink_kind {
        WorkerSinkKind::Stdout => "stdout_sink()".into(),
        WorkerSinkKind::File { output, max_size } => {
            let output = output.trim();
            if !output.is_empty() && !Path::new(output).is_absolute() {
                return Err(
                    "file output must be an absolute path, or leave empty for auto".into(),
                );
            }
            let max_size = max_size.trim();
            match (output.is_empty(), max_size.is_empty()) {
                (true, true) => "file_sink()".into(),
                (true, false) => format!("file_sink(max_size: {})", script_string(max_size)),
                (false, true) => format!("file_sink(output: {})", script_string(output)),
                (false, false) => format!(
                    "file_sink(output: {}, max_size: {})",
                    script_string(output),
                    script_string(max_size)
                ),
            }
        }
        WorkerSinkKind::Kafka { topic, brokers } => {
            let topic = topic.trim();
            if topic.is_empty() {
                return Err("kafka topic is required".into());
            }
            let brokers = brokers.trim();
            if brokers.is_empty() {
                format!("kafka_sink(topic: {})", script_string(topic))
            } else {
                format!(
                    "kafka_sink(topic: {}, brokers: {})",
                    script_string(topic),
                    script_string(brokers)
                )
            }
        }
    };

    let mut logen_args = format!("{}(), {sink_expr}", form.body_preset);
    let rate = form.rate.trim();
    if !rate.is_empty() && rate != "0ms" {
        if !rate.ends_with("ms") && !rate.ends_with('s') {
            return Err("rate must be a duration like 1ms or 1s".into());
        }
        logen_args.push_str(&format!(", rate: {rate}"));
    }
    if let Some(threads) = form.threads {
        if threads == 0 {
            return Err("threads must be greater than 0".into());
        }
        logen_args.push_str(&format!(", threads: {threads}"));
    }

    let label = form.label.trim();
    let script = if label.is_empty() {
        format!("start(config: logen({logen_args}))")
    } else {
        format!(
            "start(config: logen({logen_args}), label: {})",
            script_string(label)
        )
    };
    Ok(script)
}

fn script_string(raw: &str) -> String {
    format!(
        "\"{}\"",
        raw.replace('\\', "\\\\").replace('"', "\\\"")
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 测试内容：stdout + preset_json 表单生成控制脚本。
    /// 输入：rate=1ms、threads=2、label=dev。
    /// 预期：脚本含 preset_json、stdout_sink、rate、threads 与 label。
    #[test]
    fn build_stdout_script() {
        let form = WorkerStartForm {
            body_preset: "preset_json".into(),
            sink_kind: WorkerSinkKind::Stdout,
            rate: "1ms".into(),
            threads: Some(2),
            label: "dev".into(),
        };
        let s = build_control_script(&form).unwrap();
        assert!(s.contains("preset_json()"));
        assert!(s.contains("stdout_sink()"));
        assert!(s.contains("rate: 1ms"));
        assert!(s.contains("threads: 2"));
        assert!(s.contains("label: \"dev\""));
    }

    /// 测试内容：file sink 表单生成带 output 与 max_size 的脚本。
    /// 输入：绝对路径 output、max_size=64MiB。
    /// 预期：脚本含 file_sink 命名参数。
    #[test]
    fn build_file_script() {
        let form = WorkerStartForm {
            body_preset: "preset_json".into(),
            sink_kind: WorkerSinkKind::File {
                output: "/tmp/logkit-test.log".into(),
                max_size: "64MiB".into(),
            },
            rate: String::new(),
            threads: None,
            label: String::new(),
        };
        let s = build_control_script(&form).unwrap();
        assert!(s.contains("file_sink(output: \"/tmp/logkit-test.log\", max_size: \"64MiB\")"));
    }

    /// 测试内容：file sink 省略 output 时生成无参 file_sink()。
    /// 输入：output 与 max_size 均为空。
    /// 预期：脚本为 file_sink()。
    #[test]
    fn build_file_auto_output_script() {
        let form = WorkerStartForm {
            body_preset: "preset_json".into(),
            sink_kind: WorkerSinkKind::File {
                output: String::new(),
                max_size: String::new(),
            },
            rate: String::new(),
            threads: None,
            label: String::new(),
        };
        let s = build_control_script(&form).unwrap();
        assert!(s.contains("file_sink()"));
    }

    /// 测试内容：kafka sink 表单生成带 topic 与 brokers 的脚本。
    /// 输入：preset_cef、kafka topic/brokers。
    /// 预期：脚本含 kafka_sink 命名参数。
    #[test]
    fn build_kafka_script() {
        let form = WorkerStartForm {
            body_preset: "preset_cef".into(),
            sink_kind: WorkerSinkKind::Kafka {
                topic: "logs".into(),
                brokers: "127.0.0.1:9092".into(),
            },
            rate: String::new(),
            threads: None,
            label: String::new(),
        };
        let s = build_control_script(&form).unwrap();
        assert!(s.contains("preset_cef()"));
        assert!(s.contains("kafka_sink(topic: \"logs\", brokers: \"127.0.0.1:9092\")"));
    }
}
