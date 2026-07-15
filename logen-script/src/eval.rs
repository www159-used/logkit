//! Env + eval（仅接受已 typecheck 的程序）。

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use logen_model::{BodyConfig, FieldSpec, KafkaConfig, SinkConfig, WorkerConfig};

use crate::ast::{Arg, Expr, Stmt};
use crate::preset::preset_by_name;
use crate::typecheck::TypedProgram;
use crate::types::Type;
use crate::value::{ConfigValue, Value};
use crate::ScriptError;

/// 控制面副作用的宿主；由 `logend` 实现，脚本 crate 不依赖 daemon。
pub trait ControlHost: Send + Sync {
    fn start(&self, config: WorkerConfig, label: Option<String>) -> Result<String, ScriptError>;
    fn stop(&self, id: &str) -> Result<(), ScriptError>;
    fn stat(&self, id_prefix: Option<&str>, view: StatView) -> Result<String, ScriptError>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StatView {
    Brief,
    Full,
}

#[derive(Clone, Default)]
pub struct EvalOptions {
    pub control: Option<Arc<dyn ControlHost>>,
    pub output: Option<Arc<Mutex<String>>>,
}

impl std::fmt::Debug for EvalOptions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EvalOptions")
            .field("control", &self.control.is_some())
            .field("output", &self.output.is_some())
            .finish()
    }
}

/// 控制脚本的最终值及其显式输出。
#[derive(Debug)]
pub struct ControlScriptResult {
    pub value: Option<Value>,
    pub output: String,
}

/// 求值；返回最后一个表达式语句的值（若有）。
pub fn eval(program: &TypedProgram, opts: &EvalOptions) -> Result<Option<Value>, ScriptError> {
    let mut env = HashMap::new();
    eval_with_env(program, &mut env, opts)
}

/// 在既有值环境中求值，并将成功声明提交到该环境。
pub fn eval_with_env(
    program: &TypedProgram,
    env: &mut HashMap<String, Value>,
    opts: &EvalOptions,
) -> Result<Option<Value>, ScriptError> {
    let mut last = None;
    for stmt in &program.program.stmts {
        match stmt {
            Stmt::Let { name, value } => {
                let v = eval_expr(value, env, opts)?;
                env.insert(name.clone(), v);
            }
            Stmt::Expr(expr) => {
                last = Some(eval_expr(expr, env, opts)?);
            }
        }
    }
    Ok(last)
}

/// 控制脚本的持久变量会话。
pub struct ControlSession {
    environment: ScriptEnvironment,
    options: EvalOptions,
    output: Arc<Mutex<String>>,
}

#[derive(Clone, Default)]
struct ScriptEnvironment {
    types: HashMap<String, Type>,
    values: HashMap<String, Value>,
}

impl ControlSession {
    pub fn new(host: Arc<dyn ControlHost>, output: Arc<Mutex<String>>) -> Self {
        Self {
            environment: ScriptEnvironment::default(),
            options: EvalOptions {
                control: Some(host),
                output: Some(output.clone()),
            },
            output,
        }
    }

    /// 在副本中检查并求值，只有全部成功才提交环境。
    pub fn execute(&mut self, source: &str) -> Result<Option<Value>, ScriptError> {
        self.output
            .lock()
            .map_err(|_| ScriptError::eval_msg("control output buffer poisoned"))?
            .clear();
        let program = crate::parse::parse_program(source)?;
        let mut environment = self.environment.clone();
        let typed = crate::typecheck::typecheck_with_env(program, &mut environment.types)?;
        let value = eval_with_env(&typed, &mut environment.values, &self.options)?;
        self.environment = environment;
        Ok(value)
    }

    pub fn output(&self) -> Result<String, ScriptError> {
        self.output
            .lock()
            .map_err(|_| ScriptError::eval_msg("control output buffer poisoned"))
            .map(|output| output.clone())
    }
}

fn eval_expr(
    expr: &Expr,
    env: &HashMap<String, Value>,
    opts: &EvalOptions,
) -> Result<Value, ScriptError> {
    match expr {
        Expr::Path(path) => get_path(env, path),
        Expr::Str(s) => Ok(Value::Str(s.clone())),
        Expr::Int(n) => Ok(Value::Int(*n)),
        Expr::Float(n) => Ok(Value::Float(*n)),
        Expr::Duration(raw) => {
            let d: Duration = humantime::parse_duration(raw)
                .map_err(|e| ScriptError::eval_msg(format!("duration `{raw}`: {e}")))?;
            Ok(Value::Duration(d))
        }
        Expr::Call { callee, args } => call_builtin(callee, args, env, opts),
        Expr::TemplateLit { parts } => eval_template_lit(parts, env, opts),
    }
}

fn eval_template_lit(
    parts: &[crate::ast::TplPart],
    env: &HashMap<String, Value>,
    opts: &EvalOptions,
) -> Result<Value, ScriptError> {
    use crate::ast::TplPart;
    let mut handlebars = String::new();
    let mut fields = BTreeMap::new();
    for part in parts {
        match part {
            TplPart::Text(t) => handlebars.push_str(t),
            TplPart::Expr(expr) => {
                let Value::Field(field) = eval_expr(expr, env, opts)? else {
                    return Err(ScriptError::eval_msg(
                        "template interpolation: expected Field",
                    ));
                };
                let name = match expr {
                    Expr::Path(path) if path.len() == 1 && !fields.contains_key(&path[0]) => {
                        path[0].clone()
                    }
                    _ => format!("_tpl{}", fields.len()),
                };
                handlebars.push_str("{{");
                handlebars.push_str(&name);
                handlebars.push_str("}}");
                fields.insert(name, field);
            }
        }
    }
    Ok(Value::Template { handlebars, fields })
}

fn get_path(env: &HashMap<String, Value>, path: &[String]) -> Result<Value, ScriptError> {
    if path.is_empty() {
        return Err(ScriptError::eval_msg("empty path"));
    }
    let root = env
        .get(&path[0])
        .ok_or_else(|| ScriptError::UndefinedVar(path[0].clone()))?;
    if path.len() == 1 {
        return Ok(root.clone());
    }
    get_nested(root, &path[1..])
}

fn get_nested(root: &Value, fields: &[String]) -> Result<Value, ScriptError> {
    match root {
        Value::Config(cfg) => match fields[0].as_str() {
            "body" => {
                if fields.len() == 1 {
                    Ok(Value::Body(cfg.body.clone()))
                } else {
                    get_body_path(&cfg.body, &fields[1..])
                }
            }
            "sink" if fields.len() == 1 => Ok(Value::Sink(cfg.sink.clone())),
            "rate" if fields.len() == 1 => Ok(Value::Duration(cfg.rate.unwrap_or(Duration::ZERO))),
            "threads" if fields.len() == 1 => Ok(Value::Int(cfg.threads.unwrap_or(1) as i64)),
            other => Err(ScriptError::eval_msg(format!(
                "Config has no field `{other}`"
            ))),
        },
        Value::Body(b) => get_body_path(b, fields),
        other => Err(ScriptError::eval_msg(format!(
            "cannot index {:?}",
            other.ty()
        ))),
    }
}

fn get_body_path(body: &BodyConfig, fields: &[String]) -> Result<Value, ScriptError> {
    match fields[0].as_str() {
        "template" if fields.len() == 1 => Ok(Value::Str(body.template.clone())),
        other => Err(ScriptError::eval_msg(format!(
            "Body has no field `{other}`"
        ))),
    }
}

fn call_builtin(
    name: &str,
    args: &[Arg],
    env: &HashMap<String, Value>,
    opts: &EvalOptions,
) -> Result<Value, ScriptError> {
    let bound = bind_args(args, env, opts)?;
    if let Some(body) = preset_by_name(name) {
        return Ok(Value::Body(body));
    }
    match name {
        "body" => {
            let (hb, fields) = match bound.get("template").or_else(|| bound.get("__pos_0")) {
                Some(Value::Template { handlebars, fields }) => {
                    (handlebars.clone(), fields.clone())
                }
                Some(Value::Str(s)) => (s.clone(), BTreeMap::new()),
                Some(other) => {
                    return Err(ScriptError::eval_msg(format!(
                        "body.template: expected Template or Str, got {:?}",
                        other.ty()
                    )));
                }
                None => {
                    return Err(ScriptError::eval_msg("body: missing template"));
                }
            };
            Ok(Value::Body(BodyConfig {
                template: hb,
                fields,
            }))
        }
        "counter" => Ok(Value::Field(FieldSpec::Counter)),
        "uuid_v4" => Ok(Value::Field(FieldSpec::UuidV4)),
        "name_en" => Ok(Value::Field(FieldSpec::NameEn)),
        "ipv4" => Ok(Value::Field(FieldSpec::Ipv4)),
        "url" => Ok(Value::Field(FieldSpec::Url)),
        "url_path" => Ok(Value::Field(FieldSpec::UrlPath)),
        "hostname" => Ok(Value::Field(FieldSpec::Hostname)),
        "domain_suffix" => Ok(Value::Field(FieldSpec::DomainSuffix)),
        "lorem_word" => Ok(Value::Field(FieldSpec::LoremWord)),
        "company_name" => Ok(Value::Field(FieldSpec::CompanyName)),
        "user_agent" => Ok(Value::Field(FieldSpec::UserAgent)),
        "username" => Ok(Value::Field(FieldSpec::Username)),
        "timestamp" => {
            let format = optional_str(&bound, "format")
                .or_else(|| match bound.get("__pos_0") {
                    Some(Value::Str(s)) => Some(s.clone()),
                    _ => None,
                })
                .unwrap_or_else(|| "%Y-%m-%d %H:%M:%S".into());
            Ok(Value::Field(FieldSpec::Timestamp { format }))
        }
        "integer" => {
            let min = require_i64_param(&bound, "min", 0)?;
            let max = require_i64_param(&bound, "max", 1)?;
            if min > max {
                return Err(ScriptError::eval_msg(format!(
                    "integer: min ({min}) > max ({max})"
                )));
            }
            Ok(Value::Field(FieldSpec::Integer { min, max }))
        }
        "float" => {
            let min = require_f64_param(&bound, "min", 0)?;
            let max = require_f64_param(&bound, "max", 1)?;
            if min > max {
                return Err(ScriptError::eval_msg(format!(
                    "float: min ({min}) > max ({max})"
                )));
            }
            Ok(Value::Field(FieldSpec::Float { min, max }))
        }
        "sentence" => {
            let min = require_usize_param(&bound, "min", 0)?;
            let max = require_usize_param(&bound, "max", 1)?;
            if min > max {
                return Err(ScriptError::eval_msg(format!(
                    "sentence: min ({min}) > max ({max})"
                )));
            }
            Ok(Value::Field(FieldSpec::Sentence { min, max }))
        }
        "stdout_sink" => Ok(Value::Sink(SinkConfig::Stdout)),
        "kafka_sink" => {
            let topic = require_str(&bound, "topic")?;
            let brokers = optional_str(&bound, "brokers");
            Ok(Value::Sink(build_kafka_sink(&topic, brokers.as_deref())?))
        }
        "start" => {
            let config = require_config(&bound, "config")?.into_worker_config();
            let label = optional_str(&bound, "label").or_else(|| optional_pos_str(&bound, 1));
            let host = opts
                .control
                .as_deref()
                .ok_or_else(|| ScriptError::eval_msg("start: control host is unavailable"))?;
            Ok(Value::Str(host.start(config, label)?))
        }
        "stop" => {
            let id = require_str(&bound, "id")?;
            let host = opts
                .control
                .as_deref()
                .ok_or_else(|| ScriptError::eval_msg("stop: control host is unavailable"))?;
            host.stop(&id)?;
            Ok(Value::Unit)
        }
        "stat" => {
            let id = optional_str(&bound, "id").or_else(|| optional_pos_str(&bound, 0));
            let view = match optional_str(&bound, "view").as_deref().unwrap_or("brief") {
                "brief" => StatView::Brief,
                "full" => StatView::Full,
                other => {
                    return Err(ScriptError::eval_msg(format!(
                        "stat.view: expected `brief` or `full`, got `{other}`"
                    )));
                }
            };
            let host = opts
                .control
                .as_deref()
                .ok_or_else(|| ScriptError::eval_msg("stat: control host is unavailable"))?;
            let rendered = host.stat(id.as_deref(), view)?;
            if let Some(output) = &opts.output {
                output
                    .lock()
                    .map_err(|_| ScriptError::eval_msg("stat: output buffer poisoned"))?
                    .push_str(&rendered);
            }
            Ok(Value::Unit)
        }
        "logen" => {
            let body = require_body(&bound, "body")?;
            let sink = require_sink(&bound, "sink")?;
            let rate = optional_duration(&bound, "rate");
            let threads = match bound.get("threads") {
                None => None,
                Some(Value::Int(n)) => {
                    if *n <= 0 {
                        return Err(ScriptError::eval_msg(format!(
                            "threads: must be positive, got {n}"
                        )));
                    }
                    let n = u32::try_from(*n)
                        .map_err(|_| ScriptError::eval_msg(format!("threads: {n} exceeds u32")))?;
                    Some(n)
                }
                Some(other) => {
                    return Err(ScriptError::eval_msg(format!(
                        "threads: expected Int, got {:?}",
                        other.ty()
                    )));
                }
            };
            Ok(Value::Config(ConfigValue {
                body,
                sink,
                rate,
                threads,
                sealed: true,
            }))
        }
        other => Err(ScriptError::UnknownBuiltin(other.into())),
    }
}

fn bind_args(
    args: &[Arg],
    env: &HashMap<String, Value>,
    opts: &EvalOptions,
) -> Result<HashMap<String, Value>, ScriptError> {
    let mut out = HashMap::new();
    let mut positional = Vec::new();
    for arg in args {
        match arg {
            Arg::Named { name, value } => {
                out.insert(name.clone(), eval_expr(value, env, opts)?);
            }
            Arg::Positional(value) => {
                positional.push(eval_expr(value, env, opts)?);
            }
        }
    }
    for (i, v) in positional.into_iter().enumerate() {
        out.insert(format!("__pos_{i}"), v);
    }
    Ok(out)
}

fn require_i64_param(
    bound: &HashMap<String, Value>,
    key: &str,
    pos: usize,
) -> Result<i64, ScriptError> {
    let v = bound
        .get(key)
        .or_else(|| bound.get(&format!("__pos_{pos}")));
    match v {
        Some(Value::Int(n)) => Ok(*n),
        Some(other) => Err(ScriptError::eval_msg(format!(
            "{key}: expected Int, got {:?}",
            other.ty()
        ))),
        None => Err(ScriptError::eval_msg(format!("missing Int `{key}`"))),
    }
}

fn require_f64_param(
    bound: &HashMap<String, Value>,
    key: &str,
    pos: usize,
) -> Result<f64, ScriptError> {
    let v = bound
        .get(key)
        .or_else(|| bound.get(&format!("__pos_{pos}")));
    match v {
        Some(Value::Float(n)) => Ok(*n),
        Some(Value::Int(n)) => Ok(*n as f64),
        Some(other) => Err(ScriptError::eval_msg(format!(
            "{key}: expected Float, got {:?}",
            other.ty()
        ))),
        None => Err(ScriptError::eval_msg(format!("missing Float `{key}`"))),
    }
}

fn require_usize_param(
    bound: &HashMap<String, Value>,
    key: &str,
    pos: usize,
) -> Result<usize, ScriptError> {
    let n = require_i64_param(bound, key, pos)?;
    if n < 0 {
        return Err(ScriptError::eval_msg(format!(
            "{key}: must be non-negative, got {n}"
        )));
    }
    usize::try_from(n).map_err(|_| ScriptError::eval_msg(format!("{key}: {n} exceeds usize")))
}

fn require_str(bound: &HashMap<String, Value>, key: &str) -> Result<String, ScriptError> {
    if let Some(Value::Str(s)) = bound.get(key) {
        return Ok(s.clone());
    }
    if let Some(Value::Str(s)) = bound.get("__pos_0") {
        return Ok(s.clone());
    }
    Err(ScriptError::eval_msg(format!(
        "missing or invalid string parameter `{key}`"
    )))
}

fn optional_str(bound: &HashMap<String, Value>, key: &str) -> Option<String> {
    match bound.get(key) {
        Some(Value::Str(s)) => Some(s.clone()),
        _ => None,
    }
}

fn optional_pos_str(bound: &HashMap<String, Value>, pos: usize) -> Option<String> {
    match bound.get(&format!("__pos_{pos}")) {
        Some(Value::Str(s)) => Some(s.clone()),
        _ => None,
    }
}

fn optional_duration(bound: &HashMap<String, Value>, key: &str) -> Option<Duration> {
    match bound.get(key) {
        Some(Value::Duration(d)) => Some(*d),
        _ => None,
    }
}

fn require_body(bound: &HashMap<String, Value>, key: &str) -> Result<BodyConfig, ScriptError> {
    match bound.get(key).or_else(|| bound.get("__pos_0")) {
        Some(Value::Body(b)) => Ok(b.clone()),
        _ => Err(ScriptError::eval_msg(format!("missing Body `{key}`"))),
    }
}

fn require_sink(bound: &HashMap<String, Value>, key: &str) -> Result<SinkConfig, ScriptError> {
    match bound.get(key).or_else(|| bound.get("__pos_1")) {
        Some(Value::Sink(s)) => Ok(s.clone()),
        _ => Err(ScriptError::eval_msg(format!("missing Sink `{key}`"))),
    }
}

fn require_config(bound: &HashMap<String, Value>, key: &str) -> Result<ConfigValue, ScriptError> {
    match bound.get(key).or_else(|| bound.get("__pos_0")) {
        Some(Value::Config(c)) => Ok(c.clone()),
        Some(other) => Err(ScriptError::eval_msg(format!(
            "{key}: expected Config, got {:?}",
            other.ty()
        ))),
        None => Err(ScriptError::eval_msg(format!("missing Config `{key}`"))),
    }
}

fn build_kafka_sink(topic: &str, brokers: Option<&str>) -> Result<SinkConfig, ScriptError> {
    let brokers = match brokers {
        None => None,
        Some(b) => {
            let list: Vec<String> = b
                .split(',')
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(str::to_string)
                .collect();
            if list.is_empty() {
                return Err(ScriptError::eval_msg("brokers: empty"));
            }
            Some(list)
        }
    };
    Ok(SinkConfig::Kafka {
        kafka: Some(Box::new(KafkaConfig {
            topic: Some(topic.into()),
            brokers,
            ..Default::default()
        })),
    })
}

fn eval_last_config(
    program: &TypedProgram,
    opts: &EvalOptions,
) -> Result<ConfigValue, ScriptError> {
    let mut env: HashMap<String, Value> = HashMap::new();
    let mut last_cfg: Option<ConfigValue> = None;
    for stmt in &program.program.stmts {
        match stmt {
            Stmt::Let { name, value } => {
                let v = eval_expr(value, &env, opts)?;
                if let Value::Config(c) = &v {
                    last_cfg = Some(c.clone());
                }
                env.insert(name.clone(), v);
            }
            Stmt::Expr(expr) => {
                let v = eval_expr(expr, &env, opts)?;
                if let Value::Config(c) = v {
                    last_cfg = Some(c);
                }
            }
        }
    }
    last_cfg.ok_or_else(|| ScriptError::eval_msg("script produced no Config; end with logen(...)"))
}

/// 一站式：parse → typecheck → eval。
pub fn run_script(source: &str, opts: &EvalOptions) -> Result<Option<Value>, ScriptError> {
    let program = crate::parse::parse_program(source)?;
    let typed = crate::typecheck::typecheck(program)?;
    eval(&typed, opts)
}

/// 执行控制脚本；`start` / `stop` / `stat` 需要 `opts.control`。
pub fn run_control_script(
    source: &str,
    host: Arc<dyn ControlHost>,
) -> Result<ControlScriptResult, ScriptError> {
    let output = Arc::new(Mutex::new(String::new()));
    let mut session = ControlSession::new(host, output.clone());
    let value = session.execute(source)?;
    let output = output
        .lock()
        .map_err(|_| ScriptError::eval_msg("control output buffer poisoned"))?
        .clone();
    Ok(ControlScriptResult { value, output })
}

/// 跑脚本得到 [`WorkerConfig`]（可带 Kafka 自动补全）。
pub fn eval_to_worker_config(
    source: &str,
    opts: &EvalOptions,
    auto_kafka_protocol: bool,
    kafka_opts: kafka_protocol::KafkaProtocolOptions,
) -> Result<logen_model::WorkerConfig, ScriptError> {
    let program = crate::parse::parse_program(source)?;
    let typed = crate::typecheck::typecheck(program)?;
    let cfg = eval_last_config(&typed, opts)?;
    logen_model::finalize_worker_config(cfg.into_worker_config(), auto_kafka_protocol, kafka_opts)
        .map_err(|e| ScriptError::Yaml(e.to_string()))
}

/// 调试：脚本 → YAML 文本（内部序列化，非脚本类型）。
pub fn run_script_yaml(source: &str, opts: &EvalOptions) -> Result<String, ScriptError> {
    let cfg = eval_to_worker_config(
        source,
        opts,
        false,
        kafka_protocol::KafkaProtocolOptions::default(),
    )?;
    serde_yaml::to_string(&cfg).map_err(|e| ScriptError::Yaml(e.to_string()))
}
