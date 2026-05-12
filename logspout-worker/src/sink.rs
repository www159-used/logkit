//! 一行日志的输出目标：统一由 [`LogLineSink`] 约束，便于新增 syslog、gRPC 等实现。

use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{BufWriter, Seek, SeekFrom, Write};
use std::path::Path;

use async_trait::async_trait;
use bytes::Bytes;
use kafka_rs::client::{Acks, Client, Message, MessageHeaders};
use kafka_rs::error::ClientError;
use kafka_rs::indexmap::IndexMap;
use kafka_rs::Compression;
use kafka_rs::StrBytes;
use logspout_dsl::{KafkaConfig, KafkaPassthroughFields, LineSinkType, TemplateConfig};

/// 写入单条渲染后的日志行（UTF-8 文本）。实现可为 stdout、文件、消息队列等。
#[async_trait]
pub trait LogLineSink: Send {
    async fn emit_line(&mut self, line: &str) -> Result<(), String>;
}

/// 标准输出，每条一行。
pub struct StdoutLineSink;

#[async_trait]
impl LogLineSink for StdoutLineSink {
    async fn emit_line(&mut self, line: &str) -> Result<(), String> {
        println!("{line}");
        Ok(())
    }
}

/// 追加写本地文件；`max_size > 0` 时超过则截断为空再继续写（与历史 worker 行为一致）。
pub struct FileLineSink {
    writer: BufWriter<File>,
    max_size: u64,
}

impl FileLineSink {
    /// `rel` 相对 **`output_base`**（独立进程时常为当前工作目录；嵌入 daemon 时为 `[worker].worker_output_dir`）。
    pub fn open(output_base: &Path, rel: &str, max_size: u64) -> Result<Self, String> {
        let path = output_base.join(rel);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(|e| format!("create_dir_all {}: {e}", parent.display()))?;
        }
        let f = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .map_err(|e| format!("open output {}: {e}", path.display()))?;
        Ok(Self {
            writer: BufWriter::new(f),
            max_size,
        })
    }
}

#[async_trait]
impl LogLineSink for FileLineSink {
    async fn emit_line(&mut self, line: &str) -> Result<(), String> {
        writeln!(&mut self.writer, "{line}")
            .and_then(|_| self.writer.flush())
            .map_err(|e| format!("write output: {e}"))?;
        if self.max_size == 0 {
            return Ok(());
        }
        let f = self.writer.get_mut();
        let meta = f.metadata().map_err(|e| format!("metadata: {e}"))?;
        if meta.len() <= self.max_size {
            return Ok(());
        }
        f.set_len(0)
            .and_then(|_| f.seek(SeekFrom::Start(0)))
            .map_err(|e| format!("truncate output: {e}"))?;
        Ok(())
    }
}

pub struct KafkaLineSink {
    #[allow(dead_code)]
    client: Client,
    producer: kafka_rs::client::TopicProducer,
    brokers_display: String,
    topic: String,
    /// 每条记录附带的 Kafka headers（UTF-8 键；值见配置解析）。
    headers: MessageHeaders,
    /// YAML 里写了 `security.protocol`=SSL/SASL_SSL 或 `ssl.*` 等，用于报错时说明协议能力。
    tls_or_ssl_keys_in_yaml: bool,
    /// 与 `kafka-rs` / 将来 client 对齐的透传配置（整表克隆，含 `acks`、`timeout-ms`、`compression`、TLS 等）。
    #[allow(dead_code)]
    pub extra: KafkaPassthroughFields,
}

/// YAML 标量 → record header value：`null` → Kafka 空值；字符串/数字/布尔 → UTF-8 字节。
fn yaml_value_to_header_bytes(v: &serde_yaml::Value) -> Result<Option<Bytes>, String> {
    match v {
        serde_yaml::Value::Null => Ok(None),
        serde_yaml::Value::String(s) => Ok(Some(Bytes::copy_from_slice(s.as_bytes()))),
        serde_yaml::Value::Bool(b) => Ok(Some(Bytes::copy_from_slice(
            if *b { b"true".as_slice() } else { b"false".as_slice() },
        ))),
        serde_yaml::Value::Number(n) => Ok(Some(Bytes::copy_from_slice(n.to_string().as_bytes()))),
        serde_yaml::Value::Tagged(t) => yaml_value_to_header_bytes(&t.value),
        serde_yaml::Value::Sequence(_) | serde_yaml::Value::Mapping(_) => Err(
            "kafka.headers: value must be scalar (string, number, bool) or null".into(),
        ),
    }
}

fn message_headers_from_config(
    headers: Option<&BTreeMap<String, serde_yaml::Value>>,
) -> Result<MessageHeaders, String> {
    let mut out: MessageHeaders = IndexMap::new();
    let Some(map) = headers else {
        return Ok(out);
    };
    for (k, v) in map {
        let key_trim = k.trim();
        if key_trim.is_empty() {
            return Err("kafka.headers: header key must be non-empty".into());
        }
        let key_sb = StrBytes::from_string(key_trim.to_string());
        let val = yaml_value_to_header_bytes(v)?;
        out.insert(key_sb, val);
    }
    Ok(out)
}

fn uses_tls_security_protocol(extra: &KafkaPassthroughFields) -> bool {
    for key in ["security.protocol", "security-protocol"] {
        let Some(v) = extra.get(key) else { continue };
        let serde_yaml::Value::String(s) = v else {
            continue;
        };
        let u = s.trim().to_ascii_uppercase();
        if matches!(u.as_str(), "SSL" | "SASL_SSL") {
            return true;
        }
    }
    false
}

/// YAML 含 TLS 意图（security.protocol 或任意 `ssl.*`），当前 worker 仍用 PLAINTEXT。
fn likely_encrypted_broker_config(extra: &KafkaPassthroughFields) -> bool {
    uses_tls_security_protocol(extra)
        || extra.keys().any(|k| k.starts_with("ssl.") || k.starts_with("sasl."))
}

fn kafka_client_error_hint(e: &ClientError) -> Option<&'static str> {
    match e {
        ClientError::ClusterMetadataTimeout => Some(
            "无法在时限内完成 bootstrap/拉取元数据：请核对 broker 地址与端口、网络、防火墙；集群是否仅开放加密 listener。",
        ),
        ClientError::NoBrokerFound => Some(
            "未发现可用 broker：种子地址是否指向 Kafka（而非其它服务）？端口是否正确？",
        ),
        ClientError::UnknownTopic(_) => Some("集群中不存在该 topic：请先创建 topic 或检查名称拼写。"),
        ClientError::NoPartitionsAvailable(_) => {
            Some("该 topic 当前无可写分区：请确认 topic 已创建且分区就绪。")
        }
        ClientError::NoPartitionLeader(_, _) | ClientError::UnknownPartition(_, _) => Some(
            "分区元数据异常或 leader 未知：集群是否处于维护/扩容、副本是否在选举中？",
        ),
        ClientError::BrokerError(_) => Some(
            "与 broker 通信失败：连接被重置、握手失败或非 Kafka 协议均可能导致；若 broker 要求 TLS/SASL 而客户端为明文，也会出现此类错误。",
        ),
        ClientError::MalformedResponse => {
            Some("收到无法解析的响应：可能连到了非 Kafka 端口，或协议不兼容。")
        }
        ClientError::ResponseError(_, _, _) => Some("broker 返回错误码：请结合集群日志排查 ACL、副本、ISR 等。"),
        ClientError::EncodingError(_) | ClientError::ProducerMessagesEmpty => None,
        ClientError::Other(_) => None,
        ClientError::NoTopicsSpecified | ClientError::NoControllerFound => None,
    }
}

fn format_kafka_emit_error(
    err: ClientError,
    brokers_display: &str,
    topic: &str,
    tls_or_ssl_keys_in_yaml: bool,
) -> String {
    let mut s = format!(
        "kafka produce 失败（brokers=[{}], topic={:?}）: {}",
        brokers_display, topic, err
    );
    if let Some(h) = kafka_client_error_hint(&err) {
        s.push('\n');
        s.push_str(h);
    }
    if tls_or_ssl_keys_in_yaml {
        s.push_str("\n说明：配置里包含 security.protocol=SSL/SASL_SSL 或 ssl.* / sasl.*，但当前 logspout-worker 使用的 kafka-rs 仅按 PLAINTEXT 建连；若 broker 只接受 TLS，连接会失败，需改用明文 listener 或等待后续 TLS 接线。");
    }
    s
}

fn acks_from_value(v: Option<&serde_yaml::Value>) -> Result<Acks, String> {
    let Some(v) = v else {
        return Ok(Acks::Leader);
    };
    match v {
        serde_yaml::Value::Number(n) => {
            let Some(i) = n.as_i64() else {
                return Err("kafka.acks: invalid number".into());
            };
            match i {
                -1 => Ok(Acks::All),
                0 => Ok(Acks::None),
                1 => Ok(Acks::Leader),
                _ => Err(format!(
                    "kafka.acks: unsupported integer {i} (expected -1, 0, or 1)"
                )),
            }
        }
        serde_yaml::Value::String(s) => acks_from_str(s.trim()),
        _ => Err("kafka.acks: unsupported YAML type (use integer or string)".into()),
    }
}

fn acks_from_str(s: &str) -> Result<Acks, String> {
    if s.is_empty() {
        return Ok(Acks::Leader);
    }
    if let Ok(n) = s.parse::<i64>() {
        return match n {
            -1 => Ok(Acks::All),
            0 => Ok(Acks::None),
            1 => Ok(Acks::Leader),
            _ => Err(format!("kafka.acks: unsupported integer {n}")),
        };
    }
    match s.to_ascii_lowercase().as_str() {
        "all" => Ok(Acks::All),
        "none" => Ok(Acks::None),
        "leader" => Ok(Acks::Leader),
        _ => Err(format!("kafka.acks: unknown string {s:?}")),
    }
}

fn compression_from_opt(cs: Option<&str>) -> Result<Option<Compression>, String> {
    let Some(raw) = cs else {
        return Ok(None);
    };
    let s = raw.trim();
    if s.is_empty() {
        return Ok(None);
    }
    match s.to_ascii_lowercase().as_str() {
        "none" | "uncompressed" => Ok(Some(Compression::None)),
        "gzip" => Ok(Some(Compression::Gzip)),
        "snappy" => Ok(Some(Compression::Snappy)),
        "lz4" => Ok(Some(Compression::Lz4)),
        "zstd" => Ok(Some(Compression::Zstd)),
        other => Err(format!("kafka.compression: unknown {other:?}")),
    }
}

fn compression_from_value(v: Option<&serde_yaml::Value>) -> Result<Option<Compression>, String> {
    let Some(v) = v else {
        return Ok(None);
    };
    match v {
        serde_yaml::Value::String(s) => compression_from_opt(Some(s.as_str())),
        serde_yaml::Value::Bool(_) | serde_yaml::Value::Number(_) => {
            Err("kafka.compression: expected string".into())
        }
        _ => Err("kafka.compression: unsupported YAML type".into()),
    }
}

fn parse_timeout_ms(v: &serde_yaml::Value) -> Result<u64, String> {
    match v {
        serde_yaml::Value::Number(n) => n
            .as_u64()
            .or_else(|| n.as_i64().map(|i| i as u64))
            .ok_or_else(|| "kafka.timeout-ms: invalid number".to_string()),
        serde_yaml::Value::String(s) => s
            .trim()
            .parse()
            .map_err(|_| "kafka.timeout-ms: invalid string".to_string()),
        _ => Err("kafka.timeout-ms: unsupported YAML type".into()),
    }
}

fn timeout_ms_from_extra(extra: &KafkaPassthroughFields) -> Result<i32, String> {
    match extra.get("timeout-ms") {
        None => Ok(30_000),
        Some(v) => {
            let ms = parse_timeout_ms(v)?;
            Ok(ms.min(i32::MAX as u64) as i32)
        }
    }
}

impl KafkaLineSink {
    pub fn try_new(k: &KafkaConfig) -> Result<Self, String> {
        let headers = message_headers_from_config(k.headers.as_ref())?;
        let brokers: Vec<String> = k
            .brokers
            .iter()
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();
        let brokers_display = brokers.join(", ");
        let tls_or_ssl_keys_in_yaml = likely_encrypted_broker_config(&k.extra);
        let client = Client::new(brokers);
        let acks = acks_from_value(k.extra.get("acks"))?;
        let timeout_ms = timeout_ms_from_extra(&k.extra)?;
        let compression = compression_from_value(k.extra.get("compression"))?;
        let producer =
            client.topic_producer(k.topic.trim(), acks, Some(timeout_ms), compression);
        Ok(Self {
            client,
            producer,
            brokers_display,
            topic: k.topic.trim().to_string(),
            headers,
            tls_or_ssl_keys_in_yaml,
            extra: k.extra.clone(),
        })
    }
}

#[async_trait]
impl LogLineSink for KafkaLineSink {
    async fn emit_line(&mut self, line: &str) -> Result<(), String> {
        let msg = Message::new(
            None,
            Some(Bytes::copy_from_slice(line.as_bytes())),
            self.headers.clone(),
        );
        self.producer
            .produce(std::slice::from_ref(&msg))
            .await
            .map_err(|e| {
                format_kafka_emit_error(
                    e,
                    &self.brokers_display,
                    &self.topic,
                    self.tls_or_ssl_keys_in_yaml,
                )
            })?;
        Ok(())
    }
}

pub fn validate_kafka_config(k: &KafkaConfig) -> Result<(), String> {
    let brokers: Vec<&str> = k
        .brokers
        .iter()
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .collect();
    if brokers.is_empty() {
        return Err("kafka.brokers must list at least one broker".into());
    }
    if k.topic.trim().is_empty() {
        return Err("kafka.topic must be non-empty".into());
    }
    acks_from_value(k.extra.get("acks"))?;
    timeout_ms_from_extra(&k.extra)?;
    compression_from_value(k.extra.get("compression"))?;
    message_headers_from_config(k.headers.as_ref())?;
    Ok(())
}

/// 按 [`TemplateConfig::sink`] 构造行日志 sink（须已通过 [`validate_template_sink`]）。
pub fn build_line_sink(cfg: &TemplateConfig, output_base: &Path) -> Result<Box<dyn LogLineSink>, String> {
    match cfg.sink.sink_type {
        LineSinkType::Kafka => {
            let k = cfg
                .sink
                .kafka
                .as_ref()
                .expect("validate_template_sink ensures kafka");
            Ok(Box::new(KafkaLineSink::try_new(k)?))
        }
        LineSinkType::File => {
            let rel = cfg
                .sink
                .output
                .as_deref()
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .expect("validate_template_sink ensures output");
            Ok(Box::new(FileLineSink::open(
                output_base,
                rel,
                cfg.sink.max_size_bytes,
            )?))
        }
        LineSinkType::Stdout => Ok(Box::new(StdoutLineSink)),
    }
}
