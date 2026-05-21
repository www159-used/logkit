use logen_dsl::{FieldSpec, TemplateRunner};
use std::path::PathBuf;

fn etc_body(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../etc/body/apache")
        .join(name)
}

/// Combined+XFF 行末尾应有第三段引号字段（log_parser 的 `ApcXForward` / `x_forward`）。
#[test]
fn apache_access_xff_body_loads_and_renders() {
    let doc: serde_yaml::Value =
        serde_yaml::from_str(&std::fs::read_to_string(etc_body("access-xff.yaml")).unwrap())
            .unwrap();
    let body = doc.get("body").expect("body key");
    let template = body.get("template").unwrap().as_str().unwrap();
    let fields: std::collections::BTreeMap<String, FieldSpec> =
        serde_yaml::from_value(body.get("fields").cloned().unwrap()).unwrap();
    assert!(template.contains("{{x_forward}}"));
    let mut r = TemplateRunner::try_new(template, fields).unwrap();
    for i in 0..200 {
        let line = r.next_line().unwrap();
        let end = line.len();
        assert!(end > 0 && line.ends_with('"'), "line {i}: {line}");
        let before_close = &line[..end - 1];
        let xff_open = before_close.rfind('"').expect("line {i}: {line}");
        assert!(
            line[..xff_open].trim_end().ends_with('"'),
            "line {i} missing trailing quoted XFF after UA: {line}"
        );
    }
}

#[test]
fn middleware_apache_body_loads_and_matches_access1_shape() {
    let doc: serde_yaml::Value =
        serde_yaml::from_str(&std::fs::read_to_string(etc_body("middleware.yaml")).unwrap())
            .unwrap();
    let body = doc.get("body").expect("body key");
    let template = body.get("template").unwrap().as_str().unwrap();
    let fields: std::collections::BTreeMap<String, logen_dsl::FieldSpec> =
        serde_yaml::from_value(body.get("fields").cloned().unwrap()).unwrap();
    assert!(template.contains("{{clientip}}"));
    assert!(template.contains("{{request}}"));
    assert!(fields.contains_key("resp_len"));
    assert!(!fields.contains_key("x_forward"));
    let mut r = TemplateRunner::try_new(template, fields).unwrap();
    for _ in 0..50 {
        let line = r.next_line().unwrap();
        assert!(line.contains(" ["), "missing timestamp bracket: {line}");
        assert!(line.contains("\" "), "missing quoted request: {line}");
    }
}
