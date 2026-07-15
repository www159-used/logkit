//! 命令式 / 函数式脚本：在 **worker** 内解释为 [`logen_model::WorkerConfig`]。
//!
//! 管线：`parse` → `typecheck` → `eval` → 直接构造 `WorkerConfig`。
//! Body preset 为具名内置函数（[`preset_json`]、[`preset_cef`] 等，见 [`preset_names`]），不读 YAML。

mod ast;
mod error;
mod eval;
mod parse;
mod preset;
mod typecheck;
mod types;
mod value;

pub use ast::{Arg, Expr, Program, Stmt, TplPart};
pub use error::ScriptError;
pub use eval::{
    eval, eval_to_worker_config, run_control_script, run_script, run_script_yaml, ControlHost,
    ControlScriptResult, ControlSession, EvalOptions, StatView,
};
pub use parse::parse_program;
pub use preset::{
    preset_apache_access_xff, preset_apache_middleware, preset_by_name, preset_cef,
    preset_cyberark, preset_exchange_tracking, preset_firewall_winicssec, preset_ips_nsfocus,
    preset_json, preset_leefv2, preset_names,
};
pub use typecheck::{builtin_sigs, typecheck, TypedProgram};
pub use types::{BuiltinSig, Param, Type};
pub use value::{ConfigValue, Value};

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use kafka_protocol::KafkaProtocolOptions;

    use super::*;

    #[derive(Default)]
    struct TestControlHost {
        calls: Mutex<Vec<String>>,
    }

    impl ControlHost for TestControlHost {
        fn start(
            &self,
            _config: logen_model::WorkerConfig,
            label: Option<String>,
        ) -> Result<String, ScriptError> {
            self.calls
                .lock()
                .unwrap()
                .push(format!("start:{}", label.unwrap_or_default()));
            Ok("worker-0001".into())
        }

        fn stop(&self, id: &str) -> Result<(), ScriptError> {
            self.calls.lock().unwrap().push(format!("stop:{id}"));
            Ok(())
        }

        fn stat(&self, id_prefix: Option<&str>, _view: StatView) -> Result<String, ScriptError> {
            self.calls
                .lock()
                .unwrap()
                .push(format!("stat:{}", id_prefix.unwrap_or_default()));
            Ok("worker-0001\n".into())
        }
    }

    /// 测试内容：多行 body 模板与字段构造器。
    /// 输入：uuid_v4 + integer + 多行模板 + threads。
    /// 预期：template 保留换行；fields 含 uuid-v4 / integer；threads=2。
    #[test]
    fn eval_multiline_and_field_ctors() {
        let src = "\
let id = uuid_v4()
let n = integer(1, 10)
let body = body(`id=${id}
n=${n}`)
let sink = stdout_sink()
let cfg = logen(body: body, sink: sink, threads: 2)
";
        let cfg = eval_to_worker_config(
            src,
            &EvalOptions::default(),
            false,
            KafkaProtocolOptions::default(),
        )
        .expect("eval");
        assert_eq!(cfg.template, "id={{id}}\nn={{n}}");
        assert_eq!(cfg.threads, 2);
        assert!(matches!(
            cfg.fields.get("id"),
            Some(logen_model::FieldSpec::UuidV4)
        ));
        assert!(matches!(
            cfg.fields.get("n"),
            Some(logen_model::FieldSpec::Integer { min: 1, max: 10 })
        ));
    }

    /// 测试内容：float 字段与 Int 升格为 Float 参数。
    /// 输入：`float(0, 1.5)`。
    /// 预期：FieldSpec::Float。
    #[test]
    fn eval_float_field() {
        let src = r#"
let x = float(0, 1.5)
let body = body(`x=${x}`)
let sink = stdout_sink()
let cfg = logen(body: body, sink: sink)
"#;
        let cfg = eval_to_worker_config(
            src,
            &EvalOptions::default(),
            false,
            KafkaProtocolOptions::default(),
        )
        .expect("eval");
        match cfg.fields.get("x") {
            Some(logen_model::FieldSpec::Float { min, max }) => {
                assert!((*min - 0.0).abs() < 1e-9);
                assert!((*max - 1.5).abs() < 1e-9);
            }
            other => panic!("{other:?}"),
        }
    }

    /// 测试内容：模板插值可直接使用 Field 右值且忽略 `${}` 内的空白。
    /// 输入：`${ uuid_v4() }` 与 `${integer(1, 9)}`。
    /// 预期：生成唯一 `_tpl*` 槽，并将对应 FieldSpec 放入 Body fields。
    #[test]
    fn eval_template_interpolation_field_rvalues() {
        let src = r#"
let b = body(`id=${ uuid_v4() } n=${integer(1, 9)}`)
let cfg = logen(b, stdout_sink())
"#;
        let cfg = eval_to_worker_config(
            src,
            &EvalOptions::default(),
            false,
            KafkaProtocolOptions::default(),
        )
        .expect("eval");
        assert_eq!(cfg.template, "id={{_tpl0}} n={{_tpl1}}");
        assert!(matches!(
            cfg.fields.get("_tpl0"),
            Some(logen_model::FieldSpec::UuidV4)
        ));
    }

    /// 测试内容：直接构造 WorkerConfig 可往返序列化。
    /// 输入：counter + body 插值 + logen。
    /// 预期：`eval_to_worker_config` 与 `run_script_yaml` 反序列化结果字段相同。
    #[test]
    fn eval_direct_matches_yaml_bridge() {
        let src = r#"
let c = counter()
let body = body(`n=${c}`)
let sink = stdout_sink()
let cfg = logen(body: body, sink: sink, rate: 1ms)
"#;
        let direct = eval_to_worker_config(
            src,
            &EvalOptions::default(),
            false,
            KafkaProtocolOptions::default(),
        )
        .expect("direct");
        let yaml = run_script_yaml(src, &EvalOptions::default()).expect("yaml");
        let from_yaml: logen_model::WorkerConfig = serde_yaml::from_str(&yaml).expect("parse yaml");
        assert_eq!(direct.template, from_yaml.template);
        assert_eq!(direct.min_interval, from_yaml.min_interval);
        assert_eq!(direct.threads, from_yaml.threads);
    }

    /// 测试内容：插值 body + stdout → WorkerConfig。
    /// 输入：counter + body(`…`) + logen。
    /// 预期：template 含 `{{c}}`，fields 有 counter。
    #[test]
    fn eval_template_interp_to_worker_config() {
        let src = r#"
let c = counter()
let body = body(`n=${c}`)
let sink = stdout_sink()
let cfg = logen(body: body, sink: sink, rate: 1ms)
"#;
        let cfg = eval_to_worker_config(
            src,
            &EvalOptions::default(),
            false,
            KafkaProtocolOptions::default(),
        )
        .expect("eval");
        assert_eq!(cfg.template, "n={{c}}");
        assert!(cfg.fields.contains_key("c"));
    }

    /// 测试内容：`preset_json()` 内置 Body（宏生成 `_bp*` 槽）。
    /// 输入：preset_json + stdout。
    /// 预期：非空 template；fields 含 `_bp0`。
    #[test]
    fn eval_preset_json() {
        let src = r#"
let body = preset_json()
let sink = stdout_sink()
let cfg = logen(body: body, sink: sink)
"#;
        let cfg = eval_to_worker_config(
            src,
            &EvalOptions::default(),
            false,
            KafkaProtocolOptions::default(),
        )
        .expect("eval");
        assert!(!cfg.template.is_empty());
        assert!(cfg.fields.contains_key("_bp0"));
    }

    /// 测试内容：脚本可调用 `preset_cef()`。
    /// 输入：preset_cef + stdout。
    /// 预期：eval 成功且 fields 非空。
    #[test]
    fn eval_preset_cef() {
        let src = r#"
let body = preset_cef()
let sink = stdout_sink()
let cfg = logen(body: body, sink: sink)
"#;
        let cfg = eval_to_worker_config(
            src,
            &EvalOptions::default(),
            false,
            KafkaProtocolOptions::default(),
        )
        .expect("eval");
        assert!(!cfg.fields.is_empty());
    }

    /// 测试内容：控制脚本通过宿主执行显式 start、stop、stat。
    /// 输入：`start(logen(...))` 返回 id 后传给 stop，最终调用 stat。
    /// 预期：宿主按顺序收到 start/stop/stat；最终值为 Unit 且统计进入输出缓冲。
    #[test]
    fn control_script_emits_lifecycle_operations() {
        let host = std::sync::Arc::new(TestControlHost::default());
        let source = r#"
let id = start(
  logen(preset_json(), stdout_sink()),
  label: "demo",
)
stop(id)
stat()
"#;
        let result = run_control_script(source, host.clone()).expect("control eval");
        assert!(matches!(result.value, Some(Value::Unit)));
        assert_eq!(result.output, "worker-0001\n");
        assert_eq!(
            host.calls.lock().unwrap().as_slice(),
            ["start:demo", "stop:worker-0001", "stat:"]
        );
    }

    /// 测试内容：stat 仅接受 brief 与 full 两种输出视图。
    /// 输入：`stat(view: "verbose")`。
    /// 预期：求值失败并说明允许的视图名称。
    #[test]
    fn stat_rejects_unknown_view() {
        let host = std::sync::Arc::new(TestControlHost::default());
        let err = run_control_script(r#"stat(view: "verbose")"#, host).unwrap_err();
        assert!(err.to_string().contains("brief"), "{err}");
    }
}
