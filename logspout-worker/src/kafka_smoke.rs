//! Kafka TLS 探针、一次性写入与 fixture 配置：与 [`crate::sink::KafkaLineSink`] 生产路径分离，仅供 CLI / 集成测试使用。

use std::path::Path;

use logspout_dsl::KafkaConfig;

pub use crate::jks_fixture::{
    FIXTURE_BOOTSTRAP_BROKER, FIXTURE_KEYSTORE_PASSWORD, FIXTURE_TRUSTSTORE_PASSWORD,
};

/// 拉取集群 metadata（broker 数 / topic 数）。
pub fn probe_kafka_ssl_cluster(
    k: &KafkaConfig,
) -> Result<(usize, usize), crate::sink::KafkaLineSinkError> {
    crate::sink::kafka::probe_kafka_ssl_cluster(k)
}

/// 发送一条 UTF-8 记录并 flush。
pub fn produce_one_kafka_ssl_line(
    k: &KafkaConfig,
    payload: &str,
) -> Result<(), crate::sink::KafkaLineSinkError> {
    crate::sink::kafka::produce_one_kafka_ssl_line(k, payload)
}

/// 使用目录下的 `truststore.jks`、`keystore.jks` 与 [`FIXTURE_*`] 口令构造 SSL 客户端配置（JKS 材料，不经 PEM 路径）。
///
/// `skip_hostname_verify`：为 `true` 时写入空的 `ssl.endpoint.identification.algorithm`（与 Java 置空类似），便于连 IP bootstrap；为 `false` 时不设置该字段，保持默认开启主机名校验。
pub fn kafka_config_fixture_jks_dir(
    brokers_one_line: &str,
    topic: &str,
    assets_dir: &Path,
    skip_hostname_verify: bool,
) -> KafkaConfig {
    let a = assets_dir;
    KafkaConfig {
        brokers: Some(vec![brokers_one_line.trim().to_string()]),
        topic: Some(topic.trim().to_string()),
        security_protocol: Some("SSL".into()),
        ssl_endpoint_identification_algorithm: skip_hostname_verify.then(String::new),
        ssl_truststore_location: Some(a.join("truststore.jks").to_string_lossy().into_owned()),
        ssl_truststore_password: Some(FIXTURE_TRUSTSTORE_PASSWORD.into()),
        ssl_keystore_location: Some(a.join("keystore.jks").to_string_lossy().into_owned()),
        ssl_keystore_password: Some(FIXTURE_KEYSTORE_PASSWORD.into()),
        ..Default::default()
    }
}
