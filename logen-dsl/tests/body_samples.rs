use logen_dsl::{FieldSpec, TemplateRunner};
use std::path::PathBuf;

fn body_path(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../etc/body")
        .join(name)
}

fn smoke_body(name: &str, n: usize) {
    let doc: serde_yaml::Value =
        serde_yaml::from_str(&std::fs::read_to_string(body_path(name)).unwrap()).unwrap();
    let body = doc.get("body").expect("body key");
    let template = body.get("template").unwrap().as_str().unwrap();
    let fields: std::collections::BTreeMap<String, FieldSpec> =
        serde_yaml::from_value(body.get("fields").cloned().unwrap()).unwrap();
    let mut r = TemplateRunner::try_new(template, fields).unwrap();
    for i in 0..n {
        let line = r.next_line().unwrap();
        assert!(!line.is_empty(), "{name} line {i} empty");
    }
}

#[test]
fn cyberark_body_renders() {
    smoke_body("cyberark.yaml", 50);
}

#[test]
fn firewall_winicssec_body_renders() {
    smoke_body("firewall-winicssec.yaml", 50);
}

#[test]
fn ips_nsfocus_body_renders() {
    smoke_body("ips-nsfocus.yaml", 50);
}

#[test]
fn exchange_tracking_body_renders() {
    smoke_body("exchange-tracking.yaml", 50);
}
