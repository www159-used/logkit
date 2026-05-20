use logen_dsl::{load_worker_config, TemplateRunner};
use std::path::PathBuf;

fn load_apache_access() -> logen_dsl::WorkerConfig {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../etc/apache.access.yaml");
    load_worker_config(&path).expect("load apache.access.yaml")
}

/// Combined+XFF 行末尾应有第三段引号字段（log_parser 的 `ApcXForward` / `x_forward`）。
#[test]
fn apache_access_yaml_loads_and_renders_trailing_xff() {
    let cfg = load_apache_access();
    let mut r = TemplateRunner::try_new(cfg.template, cfg.fields).unwrap();
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
