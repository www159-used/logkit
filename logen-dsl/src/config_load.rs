//! 解析 worker YAML：`include` 展开、`body` 整包合并、`sink` 深合并；日志体须写在 `body:` 下。

use std::path::{Component, Path, PathBuf};

use serde_yaml::Value;

use crate::config_merge::{flatten_body_to_root, merge_worker_documents};
use crate::worker_config::{validate_sink, WorkerConfig};
use crate::ConfigParseError;

const MAX_INCLUDE_DEPTH: usize = 16;
const KEY_INCLUDE: &str = "include";

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

fn read_yaml_file(path: &Path) -> Result<Value, ConfigParseError> {
    yaml_extension_ok(path)?;
    let raw = std::fs::read_to_string(path)
        .map_err(|e| ConfigParseError::Io(path.display().to_string(), e))?;
    serde_yaml::from_str(&raw).map_err(ConfigParseError::from)
}

/// 将 `include` 路径解析为绝对路径，且须落在入口配置所在目录树下。
fn resolve_include_path(
    anchor: &Path,
    current_file: &Path,
    include_path: &str,
) -> Result<PathBuf, ConfigParseError> {
    let inc = Path::new(include_path);
    if inc.is_absolute() {
        return Err(ConfigParseError::IncludePathInvalid {
            path: include_path.into(),
            reason: "absolute paths are not allowed in include".into(),
        });
    }
    for component in inc.components() {
        if matches!(component, Component::ParentDir) {
            return Err(ConfigParseError::IncludePathInvalid {
                path: include_path.into(),
                reason: "`..` is not allowed in include paths".into(),
            });
        }
    }

    let base = current_file.parent().unwrap_or_else(|| Path::new("."));
    let candidate = base.join(inc);
    let canonical = std::fs::canonicalize(&candidate).map_err(|e| {
        ConfigParseError::IncludeNotFound {
            from: current_file.display().to_string(),
            path: include_path.into(),
            source: e,
        }
    })?;

    let anchor_canon = std::fs::canonicalize(anchor).map_err(|e| {
        ConfigParseError::Io(anchor.display().to_string(), e)
    })?;
    if !canonical.starts_with(&anchor_canon) {
        return Err(ConfigParseError::IncludePathInvalid {
            path: include_path.into(),
            reason: format!(
                "resolved path {} escapes config anchor {}",
                canonical.display(),
                anchor_canon.display()
            ),
        });
    }
    yaml_extension_ok(&canonical)?;
    Ok(canonical)
}

fn load_document(
    path: &Path,
    anchor: &Path,
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
            let inc_path = resolve_include_path(anchor, &canonical, &inc)?;
            let sub = load_document(&inc_path, anchor, stack, depth + 1)?;
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

/// 从路径加载 worker 配置：展开 `include`，合并后反序列化为 [`WorkerConfig`]。
pub fn load_worker_config(config_path: &Path) -> Result<WorkerConfig, ConfigParseError> {
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
        anchor.join(config_path.file_name().ok_or_else(|| {
            ConfigParseError::Merge("config path has no file name".into())
        })?)
    };
    let entry = std::fs::canonicalize(&entry)
        .map_err(|e| ConfigParseError::Io(entry.display().to_string(), e))?;

    let mut stack = Vec::new();
    let mut doc = load_document(&entry, &anchor, &mut stack, 0)?;

    worker_config_from_document(&mut doc)
}

/// 将已合并的 YAML 文档（须含 `body:`）转为 [`WorkerConfig`]。
pub fn worker_config_from_document(doc: &mut Value) -> Result<WorkerConfig, ConfigParseError> {
    flatten_body_to_root(doc).map_err(ConfigParseError::Merge)?;
    let cfg: WorkerConfig = serde_yaml::from_value(doc.clone())?;
    validate_sink(&cfg.sink)?;
    Ok(cfg)
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::*;

    fn fixture_root() -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/include")
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
        assert!(
            e.to_string().to_lowercase().contains("cycle"),
            "{e}"
        );
    }
}
