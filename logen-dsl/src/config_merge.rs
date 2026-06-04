//! Worker 配置 YAML 合并：`body` 整包替换，`sink` 等映射深合并。

use serde_yaml::Value;

const KEY_BODY: &str = "body";
const KEY_TEMPLATE: &str = "template";
const KEY_FIELDS: &str = "fields";
const KEY_SINK: &str = "sink";
const KEY_MIN_INTERVAL: &str = "min-interval";
const KEY_THREADS: &str = "threads";
const KEY_TYPE: &str = "type";

/// 将 `over` 合并进 `base`（`base` 会被更新）。
pub fn merge_worker_documents(base: &mut Value, over: Value) {
    let Value::Mapping(over_map) = over else {
        return;
    };

    if let Some(body) = extract_body_fragment(&over_map) {
        set_body_on_document(base, body);
    }

    if let Some(sink) = over_map.get(KEY_SINK).cloned() {
        merge_sink_on_document(base, sink);
    }

    for key in [KEY_MIN_INTERVAL, KEY_THREADS] {
        if let Some(v) = over_map.get(key).cloned() {
            set_mapping_entry(base, key, v);
        }
    }
}

fn extract_body_fragment(map: &serde_yaml::Mapping) -> Option<Value> {
    map.get(KEY_BODY).cloned()
}

fn set_body_on_document(doc: &mut Value, body: Value) {
    let Value::Mapping(doc_map) = doc else {
        *doc = serde_yaml::Mapping::new().into();
        set_body_on_document(doc, body);
        return;
    };
    doc_map.insert(Value::String(KEY_BODY.into()), body);
    doc_map.remove(Value::String(KEY_TEMPLATE.into()));
    doc_map.remove(Value::String(KEY_FIELDS.into()));
}

fn merge_sink_on_document(doc: &mut Value, over_sink: Value) {
    let Value::Mapping(doc_map) = doc else {
        let mut m = serde_yaml::Mapping::new();
        m.insert(Value::String(KEY_SINK.into()), over_sink);
        *doc = Value::Mapping(m);
        return;
    };

    let merged = match doc_map.get(KEY_SINK).cloned() {
        None => over_sink,
        Some(base_sink) => merge_sink_values(base_sink, over_sink),
    };
    doc_map.insert(Value::String(KEY_SINK.into()), merged);
}

fn merge_sink_values(base: Value, over: Value) -> Value {
    let base_type = sink_type_name(&base);
    let over_type = sink_type_name(&over);
    if over_type.is_some() && base_type.is_some() && base_type != over_type {
        return over;
    }
    deep_merge_values(base, over)
}

fn sink_type_name(sink: &Value) -> Option<&str> {
    sink.get(KEY_TYPE)?.as_str()
}

/// 映射递归深合并；标量/序列由 `over` 整体替换。
pub fn deep_merge_values(base: Value, over: Value) -> Value {
    match (base, over) {
        (Value::Mapping(mut bm), Value::Mapping(om)) => {
            for (k, v) in om {
                let merged = if let Some(bv) = bm.get(&k) {
                    deep_merge_values(bv.clone(), v)
                } else {
                    v
                };
                bm.insert(k, merged);
            }
            Value::Mapping(bm)
        }
        (_, o) => o,
    }
}

fn set_mapping_entry(doc: &mut Value, key: &str, value: Value) {
    let map = match doc {
        Value::Mapping(m) => m,
        _ => {
            *doc = Value::Mapping(serde_yaml::Mapping::new());
            match doc {
                Value::Mapping(m) => m,
                _ => unreachable!(),
            }
        }
    };
    map.insert(Value::String(key.into()), value);
}

/// 将 `body` 提升为顶层 `template` / `fields`，供 `WorkerConfig` 反序列化。
pub fn flatten_body_to_root(doc: &mut Value) -> Result<(), String> {
    let Value::Mapping(root) = doc else {
        return Err("worker config root must be a mapping".into());
    };

    if root.contains_key(Value::String(KEY_TEMPLATE.into()))
        || root.contains_key(Value::String(KEY_FIELDS.into()))
    {
        return Err(
            "top-level `template` / `fields` are not supported; wrap them under `body:`".into(),
        );
    }

    let Some(Value::Mapping(body_map)) = root.remove(Value::String(KEY_BODY.into())) else {
        return Err(
            "`body` is required (include a fragment with `body.template` and `body.fields`)".into(),
        );
    };

    let template = body_map
        .get(KEY_TEMPLATE)
        .ok_or_else(|| format!("`{KEY_BODY}` must contain `{KEY_TEMPLATE}`"))?;
    let fields = body_map
        .get(KEY_FIELDS)
        .cloned()
        .unwrap_or(Value::Mapping(serde_yaml::Mapping::new()));

    root.insert(Value::String(KEY_TEMPLATE.into()), template.clone());
    root.insert(Value::String(KEY_FIELDS.into()), fields);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_yaml::Value;

    fn parse(y: &str) -> Value {
        serde_yaml::from_str(y).unwrap()
    }

    #[test]
    fn body_replaces_previous_body() {
        let mut acc = parse(
            r#"
body:
  template: "a={{x}}"
  fields:
    x: { type: counter }
"#,
        );
        merge_worker_documents(
            &mut acc,
            parse(
                r#"
body:
  template: "b={{y}}"
  fields:
    y: { type: counter }
"#,
            ),
        );
        flatten_body_to_root(&mut acc).unwrap();
        let t = acc.get(KEY_TEMPLATE).unwrap().as_str().unwrap();
        assert!(t.contains("b={{y}}"));
        assert!(acc.get(KEY_FIELDS).unwrap().get("y").is_some());
        assert!(acc.get(KEY_FIELDS).unwrap().get("x").is_none());
    }

    #[test]
    fn sink_deep_merges_kafka_topic() {
        let mut acc = parse(
            r#"
sink:
  type: kafka
  kafka:
    brokers: ["127.0.0.1:9092"]
"#,
        );
        merge_worker_documents(
            &mut acc,
            parse(
                r#"
sink:
  kafka:
    topic: t1
"#,
            ),
        );
        let sink = acc.get(KEY_SINK).unwrap();
        assert_eq!(sink.get(KEY_TYPE).unwrap().as_str().unwrap(), "kafka");
        assert_eq!(
            sink.get("kafka")
                .unwrap()
                .get("topic")
                .unwrap()
                .as_str()
                .unwrap(),
            "t1"
        );
        assert!(sink.get("kafka").unwrap().get("brokers").is_some());
    }

    #[test]
    fn sink_type_change_replaces_whole_sink() {
        let mut acc = parse(
            r#"
sink:
  type: kafka
  kafka:
    brokers: ["h:9092"]
"#,
        );
        merge_worker_documents(
            &mut acc,
            parse(
                r#"
sink:
  type: stdout
"#,
            ),
        );
        assert_eq!(
            acc.get(KEY_SINK)
                .unwrap()
                .get(KEY_TYPE)
                .unwrap()
                .as_str()
                .unwrap(),
            "stdout"
        );
    }

    #[test]
    fn threads_overrides_base() {
        let mut acc = parse("threads: 1");
        merge_worker_documents(&mut acc, parse("threads: 8"));
        assert_eq!(acc.get(KEY_THREADS).unwrap().as_u64().unwrap(), 8);
    }

    #[test]
    fn flatten_rejects_top_level_template_fields() {
        let mut doc = parse(
            r#"
template: "x"
fields: {}
sink:
  type: stdout
"#,
        );
        let err = flatten_body_to_root(&mut doc).unwrap_err();
        assert!(err.contains("top-level"));
    }

    #[test]
    fn flatten_requires_body() {
        let mut doc = parse(
            r#"
sink:
  type: stdout
"#,
        );
        let err = flatten_body_to_root(&mut doc).unwrap_err();
        assert!(err.contains("`body` is required"));
    }
}
