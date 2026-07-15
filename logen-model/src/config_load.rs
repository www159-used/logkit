//! 解析 worker YAML：`include` 展开、`body` 整包合并、`sink` 深合并；日志体须写在 `body:` 下。

use std::path::{Path, PathBuf};

use kafka_protocol::{document_needs_kafka_transport, kafka_sink_overlay, KafkaProtocolOptions};
use serde_yaml::Value;

use crate::config_merge::{flatten_body_to_root, merge_worker_documents};
use crate::worker_config::{validate_sink, WorkerConfig};
use crate::ConfigParseError;

const MAX_INCLUDE_DEPTH: usize = 16;
const KEY_INCLUDE: &str = "include";

pub(crate) fn yaml_extension_ok(path: &Path) -> Result<(), ConfigParseError> {
    let ext = path
        .extension()
        .and_then(|s| s.to_str())
        .map(|e| e.to_ascii_lowercase());
    if !matches!(ext.as_deref(), Some("yaml") | Some("yml")) {
        return Err(ConfigParseError::PathNotYaml(path.display().to_string()));
    }
    Ok(())
}

fn read_yaml_file(path: &Path) -> Result<Value, ConfigParseError> {
    yaml_extension_ok(path)?;
    let raw = std::fs::read_to_string(path)
        .map_err(|e| ConfigParseError::Io(path.display().to_string(), e))?;
    serde_yaml::from_str(&raw).map_err(ConfigParseError::from)
}

/// 将 `include` 路径解析为规范绝对路径（相对路径相对当前 YAML 所在目录）。
fn resolve_include_path(
    current_file: &Path,
    include_path: &str,
) -> Result<PathBuf, ConfigParseError> {
    let inc = Path::new(include_path);
    let candidate = if inc.is_absolute() {
        inc.to_path_buf()
    } else {
        let base = current_file.parent().unwrap_or_else(|| Path::new("."));
        base.join(inc)
    };
    let canonical =
        std::fs::canonicalize(&candidate).map_err(|e| ConfigParseError::IncludeNotFound {
            from: current_file.display().to_string(),
            path: include_path.into(),
            source: e,
        })?;
    yaml_extension_ok(&canonical)?;
    Ok(canonical)
}

fn load_document(
    path: &Path,
    stack: &mut Vec<PathBuf>,
    depth: usize,
) -> Result<Value, ConfigParseError> {
    if depth > MAX_INCLUDE_DEPTH {
        return Err(ConfigParseError::IncludeDepthExceeded {
            max: MAX_INCLUDE_DEPTH,
        });
    }

    let canonical = std::fs::canonicalize(path)
        .map_err(|e| ConfigParseError::Io(path.display().to_string(), e))?;
    if stack.iter().any(|p| p == &canonical) {
        let mut chain: Vec<String> = stack.iter().map(|p| p.display().to_string()).collect();
        chain.push(canonical.display().to_string());
        return Err(ConfigParseError::IncludeCycle { chain });
    }
    stack.push(canonical.clone());

    let mut doc = read_yaml_file(&canonical)?;

    if let Some(includes) = take_include_list(&mut doc)? {
        let mut acc = Value::Mapping(serde_yaml::Mapping::new());
        for inc in includes {
            let inc_path = resolve_include_path(&canonical, &inc)?;
            let sub = load_document(&inc_path, stack, depth + 1)?;
            merge_worker_documents(&mut acc, sub);
        }
        merge_worker_documents(&mut acc, doc);
        doc = acc;
    }

    stack.pop();
    Ok(doc)
}

fn take_include_list(doc: &mut Value) -> Result<Option<Vec<String>>, ConfigParseError> {
    let Value::Mapping(map) = doc else {
        return Ok(None);
    };
    let Some(inc) = map.remove(Value::String(KEY_INCLUDE.into())) else {
        return Ok(None);
    };

    match inc {
        Value::Sequence(seq) => {
            let mut out = Vec::with_capacity(seq.len());
            for item in seq {
                match item {
                    Value::String(s) => out.push(s),
                    other => {
                        return Err(ConfigParseError::Merge(format!(
                            "`{KEY_INCLUDE}` entries must be strings, got {other:?}"
                        )));
                    }
                }
            }
            Ok(Some(out))
        }
        Value::String(s) => Ok(Some(vec![s])),
        other => Err(ConfigParseError::Merge(format!(
            "`{KEY_INCLUDE}` must be a string or list of strings, got {other:?}"
        ))),
    }
}

fn resolve_config_entry(config_path: &Path) -> Result<PathBuf, ConfigParseError> {
    yaml_extension_ok(config_path)?;
    let anchor = config_path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    let anchor = std::fs::canonicalize(anchor)
        .map_err(|e| ConfigParseError::Io(anchor.display().to_string(), e))?;
    let entry = if config_path.is_absolute() {
        config_path.to_path_buf()
    } else {
        anchor.join(
            config_path
                .file_name()
                .ok_or_else(|| ConfigParseError::Merge("config path has no file name".into()))?,
        )
    };
    std::fs::canonicalize(&entry).map_err(|e| ConfigParseError::Io(entry.display().to_string(), e))
}

/// 生产入口（CLI）：读取本地实例 YAML 并展开 `include` / `body`，**不**反序列化为 [`WorkerConfig`]。
pub fn read_worker_instance_yaml(config_path: &Path) -> Result<String, ConfigParseError> {
    let entry = resolve_config_entry(config_path)?;
    let mut stack = Vec::new();
    let mut doc = load_document(&entry, &mut stack, 0)?;
    flatten_body_to_root(&mut doc).map_err(ConfigParseError::Merge)?;
    Ok(serde_yaml::to_string(&doc)?)
}

/// 从已展平的 worker 配置文档构建 [`WorkerConfig`]（无 `include` / `body` 合并）。
pub fn build_worker_config(
    mut doc: Value,
    auto_kafka_protocol: bool,
    kafka_protocol: KafkaProtocolOptions,
) -> Result<WorkerConfig, ConfigParseError> {
    if auto_kafka_protocol {
        apply_kafka_transport_if_needed(&mut doc, &kafka_protocol)?;
    }
    let cfg: WorkerConfig = serde_yaml::from_value(doc)?;
    validate_sink(&cfg.sink)?;
    Ok(cfg)
}

/// 对已构造的 [`WorkerConfig`] 做 Kafka 自动补全与 sink 校验（脚本 eval 入口）。
pub fn finalize_worker_config(
    cfg: WorkerConfig,
    auto_kafka_protocol: bool,
    kafka_protocol: KafkaProtocolOptions,
) -> Result<WorkerConfig, ConfigParseError> {
    if !auto_kafka_protocol {
        validate_sink(&cfg.sink)?;
        return Ok(cfg);
    }
    let mut doc = serde_yaml::to_value(&cfg)?;
    apply_kafka_transport_if_needed(&mut doc, &kafka_protocol)?;
    let cfg: WorkerConfig = serde_yaml::from_value(doc)?;
    validate_sink(&cfg.sink)?;
    Ok(cfg)
}

/// 生产入口（worker）：从 YAML 文本解析为 [`WorkerConfig`]（含可选 Kafka 自动补全）。
pub fn parse_worker_instance_yaml(
    raw: &str,
    auto_kafka_protocol: bool,
    kafka_protocol: KafkaProtocolOptions,
) -> Result<WorkerConfig, ConfigParseError> {
    let doc: Value = serde_yaml::from_str(raw)?;
    build_worker_config(doc, auto_kafka_protocol, kafka_protocol)
}

/// 单测/本地：路径一站式加载（不自动发现 Kafka 传输）。生产请用 read + parse。
#[doc(hidden)]
pub fn load_worker_config(config_path: &Path) -> Result<WorkerConfig, ConfigParseError> {
    load_worker_config_inner(config_path, None)
}

/// 同 [`load_worker_config`]，并在缺传输配置时自动发现 Kafka。生产请用
/// [`parse_worker_instance_yaml`] 的 `auto_kafka_protocol`。
#[doc(hidden)]
pub fn load_worker_config_with_kafka_protocol(
    config_path: &Path,
    kafka_protocol: KafkaProtocolOptions,
) -> Result<WorkerConfig, ConfigParseError> {
    load_worker_config_inner(config_path, Some(kafka_protocol))
}

fn load_worker_config_inner(
    config_path: &Path,
    kafka_protocol: Option<KafkaProtocolOptions>,
) -> Result<WorkerConfig, ConfigParseError> {
    let entry = resolve_config_entry(config_path)?;
    let mut stack = Vec::new();
    let mut doc = load_document(&entry, &mut stack, 0)?;

    if let Some(opts) = kafka_protocol {
        apply_kafka_transport_if_needed(&mut doc, &opts)?;
    }

    worker_config_from_document(&mut doc)
}

fn apply_kafka_transport_if_needed(
    doc: &mut Value,
    opts: &KafkaProtocolOptions,
) -> Result<(), ConfigParseError> {
    if !document_needs_kafka_transport(doc) {
        return Ok(());
    }
    let existing = doc.get("sink").and_then(|s| s.get("kafka"));
    let overlay = kafka_sink_overlay(opts, existing)?;
    merge_worker_documents(doc, overlay);
    Ok(())
}

/// 单测/fixture：将已合并的 YAML 文档（须含 `body:`）转为 [`WorkerConfig`]。
#[doc(hidden)]
pub fn worker_config_from_document(doc: &mut Value) -> Result<WorkerConfig, ConfigParseError> {
    flatten_body_to_root(doc).map_err(ConfigParseError::Merge)?;
    let cfg: WorkerConfig = serde_yaml::from_value(doc.clone())?;
    validate_sink(&cfg.sink)?;
    Ok(cfg)
}

/// 单测：直接反序列化已展平的 `WorkerConfig` YAML（不走 `include`/`body`）。
/// 生产请用 [`parse_worker_instance_yaml`]。
#[doc(hidden)]
pub fn parse_worker_config(
    config_path: &Path,
    raw: &str,
) -> Result<WorkerConfig, ConfigParseError> {
    yaml_extension_ok(config_path)?;
    let cfg: WorkerConfig = serde_yaml::from_str(raw)?;
    validate_sink(&cfg.sink)?;
    Ok(cfg)
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use kafka_protocol::KafkaProtocolOptions;

    use super::*;

    fn fixture_root() -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/include")
    }

    /// 测试内容：`read_worker_instance_yaml` 展开 include 后仍含 template 与 sink。
    /// 输入：fixtures/include/main.yaml。
    /// 预期：返回 YAML 字符串含 `{{msg}}` 与 kafka topic。
    #[test]
    fn read_worker_instance_yaml_expands_include() {
        let path = fixture_root().join("main.yaml");
        let yaml = read_worker_instance_yaml(&path).expect("read yaml");
        assert!(yaml.contains("{{msg}}"));
        assert!(yaml.contains("app-logs"));
    }

    /// 测试内容：`parse_worker_instance_yaml` 从展开后的 YAML 一次性解析为 WorkerConfig。
    /// 输入：fixtures/include/main.yaml 经 read 得到的 YAML 文本。
    /// 预期：与 `load_worker_config` 等价的 template / kafka topic。
    #[test]
    fn parse_worker_instance_yaml_matches_load_worker_config() {
        let path = fixture_root().join("main.yaml");
        let yaml = read_worker_instance_yaml(&path).expect("read yaml");
        let from_rpc = parse_worker_instance_yaml(&yaml, false, KafkaProtocolOptions::default())
            .expect("parse yaml");
        let direct = load_worker_config(&path).expect("load path");
        assert_eq!(from_rpc.template, direct.template);
        assert_eq!(
            from_rpc.sink.kafka_section().and_then(|k| k.topic.as_deref()),
            direct.sink.kafka_section().and_then(|k| k.topic.as_deref()),
        );
    }

    #[test]
    fn load_merges_body_and_sink() {
        let path = fixture_root().join("main.yaml");
        let cfg = load_worker_config(&path).unwrap();
        assert!(cfg.template.contains("{{msg}}"));
        assert!(cfg.fields.contains_key("msg"));
        let k = cfg.sink.kafka_section().expect("kafka");
        assert_eq!(k.topic.as_deref(), Some("app-logs"));
        assert!(k.brokers.as_ref().is_some_and(|b| !b.is_empty()));
    }

    #[test]
    fn load_detects_include_cycle() {
        let path = fixture_root().join("cycle-a.yaml");
        let e = load_worker_config(&path).unwrap_err();
        assert!(e.to_string().to_lowercase().contains("cycle"), "{e}");
    }

    /// 测试内容：`include` 可用 `..` 引用入口目录外的 YAML。
    /// 输入：`include/../include-shared/body.yaml` + 入口 `sink`。
    /// 预期：合并成功，`template` 来自共享片段。
    #[test]
    fn load_include_parent_relative_path() {
        let path = fixture_root().join("parent-dir.yaml");
        let cfg = load_worker_config(&path).unwrap();
        assert!(cfg.template.contains("shared={{n}}"));
        assert!(matches!(cfg.sink, crate::worker_config::SinkConfig::Stdout));
    }

    /// 测试内容：`include` 可用绝对路径。
    /// 输入：临时入口 YAML，`include` 为 `_base/body.yaml` 的 canonical 路径。
    /// 预期：合并成功并读到 body 模板。
    #[test]
    fn load_include_absolute_path() {
        let root = fixture_root();
        let body = root.join("_base/body.yaml");
        let body_abs = body.canonicalize().expect("body fixture");
        let main = root.join("_abs-main.yaml");
        let yaml = format!(
            "include:\n  - {}\n\nsink:\n  type: stdout\n",
            body_abs.display()
        );
        std::fs::write(&main, yaml).expect("write temp main");
        let result = load_worker_config(&main);
        let _ = std::fs::remove_file(&main);
        let cfg = result.expect("absolute include");
        assert!(cfg.template.contains("{{msg}}"));
    }

    /// 测试内容：已有 security.protocol 时跳过自动合并。
    /// 输入：sink 含 PLAINTEXT 与 brokers。
    /// 预期：`apply_kafka_transport_if_needed` 不修改文档。
    #[test]
    fn apply_skips_when_transport_complete() {
        use kafka_protocol::KafkaProtocolOptions;

        let mut doc: Value = serde_yaml::from_str(
            r#"
sink:
  type: kafka
  kafka:
    security.protocol: PLAINTEXT
    brokers: ["192.168.41.138:9092"]
"#,
        )
        .unwrap();
        let before = doc.clone();
        apply_kafka_transport_if_needed(&mut doc, &KafkaProtocolOptions::default()).unwrap();
        assert_eq!(doc, before);
    }
}
