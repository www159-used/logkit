use std::collections::BTreeMap;
use std::path::Path;

use handlebars::Handlebars;
use serde::Deserialize;
use serde_json::{Map, Value};

use crate::builtins::{slots_from_fields, FieldSpec};
use crate::facade::TemplateSlot;
use crate::{ConfigParseError, Error};

/// Worker producer 配置：`template` + 可选 `fields`、`min-interval`、`output`（仅 `.yaml` / `.yml`）。
#[derive(Debug, Clone, Deserialize)]
pub struct TemplateConfig {
    /// Handlebars 源字符串（无须外置文件）。占位符须与 `fields` 键一致；**勿**用 `len` 等名，会与 handlebars 内置 helper（如 `{{len …}}`）冲突。
    pub template: String,
    #[serde(default)]
    pub fields: BTreeMap<String, FieldSpec>,
    /// 每条日志间隔（毫秒），默认 1000。
    #[serde(rename = "min-interval", default = "default_min_interval_ms")]
    pub min_interval_ms: u64,
    /// 日志文件相对路径；见 worker 与 `LSPT_WORKER_OUTPUT_DIR` 语义。
    #[serde(default)]
    pub output: Option<String>,
}

fn default_min_interval_ms() -> u64 {
    1000
}

/// 仅接受路径扩展名为 `.yaml` / `.yml`，内容按 YAML 反序列化为 [`TemplateConfig`]。
pub fn parse_template_config(config_path: &Path, raw: &str) -> Result<TemplateConfig, ConfigParseError> {
    let ext = config_path
        .extension()
        .and_then(|s| s.to_str())
        .map(|e| e.to_ascii_lowercase());
    if !matches!(ext.as_deref(), Some("yaml") | Some("yml")) {
        return Err(ConfigParseError::PathNotYaml(
            config_path.display().to_string(),
        ));
    }
    Ok(serde_yaml::from_str(raw)?)
}

/// 每轮用门面生成上下文字段，再渲染 `template`。
pub struct TemplateRunner {
    hb: Handlebars<'static>,
    template: String,
    slots: BTreeMap<String, Box<dyn TemplateSlot>>,
}

impl TemplateRunner {
    pub fn try_new(cfg: TemplateConfig) -> Result<Self, Error> {
        if cfg.template.trim().is_empty() {
            return Err(Error::EmptyTemplate);
        }
        let mut hb = Handlebars::new();
        hb.set_strict_mode(false);
        hb.register_template_string("inline", &cfg.template)?;
        let slots = slots_from_fields(cfg.fields)?;
        Ok(Self {
            hb,
            template: "inline".to_string(),
            slots,
        })
    }

    /// 生成一行（一条日志）。
    pub fn next_line(&mut self) -> Result<String, Error> {
        let mut map = Map::new();
        for (key, slot) in &mut self.slots {
            map.insert(key.clone(), Value::String(slot.next_value()));
        }
        let s = self.hb.render(&self.template, &Value::Object(map))?;
        Ok(s)
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::*;

    #[test]
    fn render_with_facades() {
        let cfg = TemplateConfig {
            template: "{{ts}} | {{name}} | {{ip}} | {{n}}".to_string(),
            fields: [
                (
                    "ts".to_string(),
                    crate::FieldSpec::Timestamp {
                        format: "%Y".to_string(),
                    },
                ),
                ("name".to_string(), crate::FieldSpec::NameEn),
                ("ip".to_string(), crate::FieldSpec::Ipv4),
                (
                    "n".to_string(),
                    crate::FieldSpec::Integer { min: 1, max: 3 },
                ),
            ]
            .into_iter()
            .collect(),
            min_interval_ms: 1000,
            output: None,
        };
        let mut r = TemplateRunner::try_new(cfg).unwrap();
        let line = r.next_line().unwrap();
        assert!(line.contains(" | "));
    }

    #[test]
    fn counter_starts_at_zero_and_increments() {
        let cfg = TemplateConfig {
            template: "n={{n}}".to_string(),
            fields: [("n".to_string(), crate::FieldSpec::Counter)]
                .into_iter()
                .collect(),
            min_interval_ms: 1000,
            output: None,
        };
        let mut r = TemplateRunner::try_new(cfg).unwrap();
        assert_eq!(r.next_line().unwrap(), "n=0");
        assert_eq!(r.next_line().unwrap(), "n=1");
        assert_eq!(r.next_line().unwrap(), "n=2");
    }

    #[test]
    fn deserialize_producer_yaml_minimal_fields() {
        let y = r#"
template: "x={{c}}"
min-interval: 1
fields:
  c:
    type: counter
"#;
        let c: TemplateConfig = serde_yaml::from_str(y).unwrap();
        assert_eq!(c.min_interval_ms, 1);
        let mut r = TemplateRunner::try_new(c).unwrap();
        assert_eq!(r.next_line().unwrap(), "x=0");
    }

    #[test]
    fn parse_template_config_yaml_by_extension() {
        let raw = r#"template: "a={{c}}"
min-interval: 2
fields:
  c: { type: counter }
"#;
        let c = parse_template_config(Path::new("example.yaml"), raw).unwrap();
        assert_eq!(c.min_interval_ms, 2);
    }

    #[test]
    fn parse_template_config_rejects_non_yaml_extension() {
        let raw = r#"template: "x"
fields: {}
"#;
        let e = parse_template_config(Path::new("bad.json"), raw).unwrap_err();
        assert!(
            e.to_string().contains(".yaml"),
            "unexpected error: {e}"
        );
    }

    #[test]
    fn yaml_folded_template_joins_lines() {
        let y = r#"
template: >-
  {{src_ip}} part2
  part3
fields: {}
"#;
        let c: TemplateConfig = serde_yaml::from_str(y).unwrap();
        assert!(
            !c.template.contains('\n'),
            "folded scalar should be one line: {:?}",
            c.template
        );
        assert!(c.template.contains("part2"));
        assert!(c.template.contains("part3"));
    }

    #[test]
    fn sentence_word_count_in_range() {
        let cfg = TemplateConfig {
            template: "{{s}}".to_string(),
            fields: [(
                "s".to_string(),
                crate::FieldSpec::Sentence { min: 2, max: 4 },
            )]
            .into_iter()
            .collect(),
            min_interval_ms: 1000,
            output: None,
        };
        let mut r = TemplateRunner::try_new(cfg).unwrap();
        for _ in 0..20 {
            let line = r.next_line().unwrap();
            let n = line.split_whitespace().count();
            assert!((2..=4).contains(&n), "{line:?}");
        }
    }
}
