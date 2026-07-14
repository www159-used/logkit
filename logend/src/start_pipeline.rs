//! StartWorker：YAML 解析 → Kafka 补全 → file sink 归一化。

use std::path::Path;

use kafka_protocol::KafkaProtocolOptions;
use logen_dsl::{
    finalize_file_sink_output, format_sink_summary, parse_worker_instance_yaml,
    worker_config_to_yaml, WorkerConfig,
};

pub struct PreparedWorkerStart {
    pub id: String,
    pub config_label: String,
    pub instance_yaml: String,
    pub sink_summary: String,
    pub worker_cfg: WorkerConfig,
}

/// 将 RPC 载荷转为可 spawn 的归一化实例（不含注册表写入）。
pub fn prepare_worker_start(
    yaml: &str,
    config_label: String,
    auto_kafka: bool,
    kafka_opts: KafkaProtocolOptions,
    worker_output_dir: &Path,
) -> Result<PreparedWorkerStart, tonic::Status> {
    if yaml.trim().is_empty() {
        return Err(tonic::Status::invalid_argument(
            "instance_yaml required (non-empty instance .yaml / .yml body)",
        ));
    }
    let mut worker_cfg = parse_worker_instance_yaml(yaml, auto_kafka, kafka_opts)
        .map_err(|e| tonic::Status::invalid_argument(format!("实例 YAML: {e}")))?;

    let config_label = if config_label.trim().is_empty() {
        "(no label)".to_string()
    } else {
        config_label
    };

    let id = uuid::Uuid::new_v4().to_string();
    finalize_file_sink_output(&mut worker_cfg.sink, worker_output_dir, &id)
        .map_err(|e| tonic::Status::invalid_argument(format!("实例 YAML: {e}")))?;
    let instance_yaml = worker_config_to_yaml(&worker_cfg)
        .map_err(|e| tonic::Status::internal(format!("实例 YAML 规范化失败: {e}")))?;
    let sink_summary = format_sink_summary(&worker_cfg.sink);

    Ok(PreparedWorkerStart {
        id,
        config_label,
        instance_yaml,
        sink_summary,
        worker_cfg,
    })
}
