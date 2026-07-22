//! 内置签名表与 typecheck。

use std::collections::HashMap;

use crate::ast::{Arg, Expr, Program, Stmt, TplPart};
use crate::types::{BuiltinSig, Param, Type};
use crate::ScriptError;

/// 类型检查通过后的程序（与 AST 同构，仅作能力标记）。
#[derive(Debug, Clone, PartialEq)]
pub struct TypedProgram {
    pub program: Program,
}

pub fn builtin_sigs() -> &'static [BuiltinSig] {
    const fn preset_sig(name: &'static str) -> BuiltinSig {
        BuiltinSig {
            name,
            params: &[],
            ret: Type::Body,
            allow_positional: true,
        }
    }
    const PRESET_JSON: BuiltinSig = preset_sig("preset_json");
    const PRESET_CEF: BuiltinSig = preset_sig("preset_cef");
    const PRESET_LEEFV2: BuiltinSig = preset_sig("preset_leefv2");
    const PRESET_CYBERARK: BuiltinSig = preset_sig("preset_cyberark");
    const PRESET_FIREWALL_WINICSSEC: BuiltinSig = preset_sig("preset_firewall_winicssec");
    const PRESET_IPS_NSFOCUS: BuiltinSig = preset_sig("preset_ips_nsfocus");
    const PRESET_EXCHANGE_TRACKING: BuiltinSig = preset_sig("preset_exchange_tracking");
    const PRESET_APACHE_ACCESS_XFF: BuiltinSig = preset_sig("preset_apache_access_xff");
    const PRESET_APACHE_MIDDLEWARE: BuiltinSig = preset_sig("preset_apache_middleware");
    const BODY: BuiltinSig = BuiltinSig {
        name: "body",
        params: &[Param {
            name: "template",
            ty: Type::Template,
            optional: false,
        }],
        ret: Type::Body,
        allow_positional: true,
    };
    const COUNTER: BuiltinSig = BuiltinSig {
        name: "counter",
        params: &[],
        ret: Type::Field,
        allow_positional: true,
    };
    const UUID_V4: BuiltinSig = BuiltinSig {
        name: "uuid_v4",
        params: &[],
        ret: Type::Field,
        allow_positional: true,
    };
    const NAME_EN: BuiltinSig = BuiltinSig {
        name: "name_en",
        params: &[],
        ret: Type::Field,
        allow_positional: true,
    };
    const IPV4: BuiltinSig = BuiltinSig {
        name: "ipv4",
        params: &[],
        ret: Type::Field,
        allow_positional: true,
    };
    const URL: BuiltinSig = BuiltinSig {
        name: "url",
        params: &[],
        ret: Type::Field,
        allow_positional: true,
    };
    const URL_PATH: BuiltinSig = BuiltinSig {
        name: "url_path",
        params: &[],
        ret: Type::Field,
        allow_positional: true,
    };
    const HOSTNAME: BuiltinSig = BuiltinSig {
        name: "hostname",
        params: &[],
        ret: Type::Field,
        allow_positional: true,
    };
    const DOMAIN_SUFFIX: BuiltinSig = BuiltinSig {
        name: "domain_suffix",
        params: &[],
        ret: Type::Field,
        allow_positional: true,
    };
    const LOREM_WORD: BuiltinSig = BuiltinSig {
        name: "lorem_word",
        params: &[],
        ret: Type::Field,
        allow_positional: true,
    };
    const COMPANY_NAME: BuiltinSig = BuiltinSig {
        name: "company_name",
        params: &[],
        ret: Type::Field,
        allow_positional: true,
    };
    const USER_AGENT: BuiltinSig = BuiltinSig {
        name: "user_agent",
        params: &[],
        ret: Type::Field,
        allow_positional: true,
    };
    const USERNAME: BuiltinSig = BuiltinSig {
        name: "username",
        params: &[],
        ret: Type::Field,
        allow_positional: true,
    };
    const TIMESTAMP: BuiltinSig = BuiltinSig {
        name: "timestamp",
        params: &[Param {
            name: "format",
            ty: Type::Str,
            optional: true,
        }],
        ret: Type::Field,
        allow_positional: true,
    };
    const INTEGER: BuiltinSig = BuiltinSig {
        name: "integer",
        params: &[
            Param {
                name: "min",
                ty: Type::Int,
                optional: false,
            },
            Param {
                name: "max",
                ty: Type::Int,
                optional: false,
            },
        ],
        ret: Type::Field,
        allow_positional: true,
    };
    const FLOAT: BuiltinSig = BuiltinSig {
        name: "float",
        params: &[
            Param {
                name: "min",
                ty: Type::Float,
                optional: false,
            },
            Param {
                name: "max",
                ty: Type::Float,
                optional: false,
            },
        ],
        ret: Type::Field,
        allow_positional: true,
    };
    const SENTENCE: BuiltinSig = BuiltinSig {
        name: "sentence",
        params: &[
            Param {
                name: "min",
                ty: Type::Int,
                optional: false,
            },
            Param {
                name: "max",
                ty: Type::Int,
                optional: false,
            },
        ],
        ret: Type::Field,
        allow_positional: true,
    };
    const KAFKA_SINK: BuiltinSig = BuiltinSig {
        name: "kafka_sink",
        params: &[
            Param {
                name: "topic",
                ty: Type::Str,
                optional: true,
            },
            Param {
                name: "brokers",
                ty: Type::Str,
                optional: true,
            },
            Param {
                name: "mode",
                ty: Type::Str,
                optional: true,
            },
            Param {
                name: "format",
                ty: Type::Str,
                optional: true,
            },
            Param {
                name: "source_id",
                ty: Type::Str,
                optional: true,
            },
            Param {
                name: "appname",
                ty: Type::Str,
                optional: true,
            },
            Param {
                name: "tag",
                ty: Type::Str,
                optional: true,
            },
            Param {
                name: "domain",
                ty: Type::Str,
                optional: true,
            },
        ],
        ret: Type::Sink,
        allow_positional: false,
    };
    const STDOUT_SINK: BuiltinSig = BuiltinSig {
        name: "stdout_sink",
        params: &[],
        ret: Type::Sink,
        allow_positional: true,
    };
    const FILE_SINK: BuiltinSig = BuiltinSig {
        name: "file_sink",
        params: &[
            Param {
                name: "output",
                ty: Type::Str,
                optional: true,
            },
            Param {
                name: "max_size",
                ty: Type::Str,
                optional: true,
            },
        ],
        ret: Type::Sink,
        allow_positional: false,
    };
    const LOGEN: BuiltinSig = BuiltinSig {
        name: "logen",
        params: &[
            Param {
                name: "body",
                ty: Type::Body,
                optional: false,
            },
            Param {
                name: "sink",
                ty: Type::Sink,
                optional: false,
            },
            Param {
                name: "rate",
                ty: Type::Duration,
                optional: true,
            },
            Param {
                name: "threads",
                ty: Type::Int,
                optional: true,
            },
        ],
        ret: Type::Config,
        allow_positional: true,
    };
    const START: BuiltinSig = BuiltinSig {
        name: "start",
        params: &[
            Param {
                name: "config",
                ty: Type::Config,
                optional: false,
            },
            Param {
                name: "label",
                ty: Type::Str,
                optional: true,
            },
        ],
        ret: Type::Str,
        allow_positional: true,
    };
    const STOP: BuiltinSig = BuiltinSig {
        name: "stop",
        params: &[Param {
            name: "id",
            ty: Type::Str,
            optional: false,
        }],
        ret: Type::Unit,
        allow_positional: true,
    };
    const STAT: BuiltinSig = BuiltinSig {
        name: "stat",
        params: &[
            Param {
                name: "id",
                ty: Type::Str,
                optional: true,
            },
            Param {
                name: "view",
                ty: Type::Str,
                optional: true,
            },
        ],
        ret: Type::Unit,
        allow_positional: true,
    };
    &[
        PRESET_JSON,
        PRESET_CEF,
        PRESET_LEEFV2,
        PRESET_CYBERARK,
        PRESET_FIREWALL_WINICSSEC,
        PRESET_IPS_NSFOCUS,
        PRESET_EXCHANGE_TRACKING,
        PRESET_APACHE_ACCESS_XFF,
        PRESET_APACHE_MIDDLEWARE,
        BODY,
        COUNTER,
        UUID_V4,
        NAME_EN,
        IPV4,
        URL,
        URL_PATH,
        HOSTNAME,
        DOMAIN_SUFFIX,
        LOREM_WORD,
        COMPANY_NAME,
        USER_AGENT,
        USERNAME,
        TIMESTAMP,
        INTEGER,
        FLOAT,
        SENTENCE,
        KAFKA_SINK,
        STDOUT_SINK,
        FILE_SINK,
        LOGEN,
        START,
        STOP,
        STAT,
    ]
}

fn sig_map() -> HashMap<&'static str, &'static BuiltinSig> {
    builtin_sigs().iter().map(|s| (s.name, s)).collect()
}

/// `parse → typecheck`：检查 let / 赋值 / 内置调用。
pub fn typecheck(program: Program) -> Result<TypedProgram, ScriptError> {
    let mut env = HashMap::new();
    typecheck_with_env(program, &mut env)
}

/// 在既有类型环境中检查程序，并将成功声明提交到该环境。
pub fn typecheck_with_env(
    program: Program,
    env: &mut HashMap<String, Type>,
) -> Result<TypedProgram, ScriptError> {
    let builtins = sig_map();
    for stmt in &program.stmts {
        match stmt {
            Stmt::Let { name, value } => {
                let ty = type_of_expr(value, env, &builtins)?;
                env.insert(name.clone(), ty);
            }
            Stmt::Expr(expr) => {
                let _ = type_of_expr(expr, env, &builtins)?;
            }
        }
    }
    Ok(TypedProgram { program })
}

fn assignable(expected: Type, got: Type) -> bool {
    expected == got
        || matches!(
            (expected, got),
            (Type::Template, Type::Str)
                | (Type::Template, Type::Template)
                // float 参数允许传整数字面量（如 float(0, 1)）
                | (Type::Float, Type::Int)
        )
}

fn type_of_path_suffix(root: Type, fields: &[String]) -> Result<Type, ScriptError> {
    if fields.is_empty() {
        return Ok(root);
    }
    let mut ty = root;
    for (i, f) in fields.iter().enumerate() {
        ty = match (ty, f.as_str()) {
            (Type::Config, "body") => Type::Body,
            (Type::Config, "sink") => Type::Sink,
            (Type::Config, "rate") => Type::Duration,
            (Type::Config, "threads") => Type::Int,
            (Type::Body, "template") => Type::Str,
            (Type::Sink, "type") => Type::Str,
            (Type::Sink, "output") => Type::Str,
            _ => {
                return Err(ScriptError::type_msg(format!(
                    "cannot access field `{f}` on {ty} (at path segment {})",
                    i + 1
                )));
            }
        };
    }
    Ok(ty)
}

fn type_of_expr(
    expr: &Expr,
    env: &HashMap<String, Type>,
    builtins: &HashMap<&'static str, &'static BuiltinSig>,
) -> Result<Type, ScriptError> {
    match expr {
        Expr::Path(path) => {
            if path.is_empty() {
                return Err(ScriptError::type_msg("empty path"));
            }
            let root = env
                .get(&path[0])
                .copied()
                .ok_or_else(|| ScriptError::UndefinedVar(path[0].clone()))?;
            type_of_path_suffix(root, &path[1..])
        }
        Expr::Str(_) => Ok(Type::Str),
        Expr::Int(_) => Ok(Type::Int),
        Expr::Float(_) => Ok(Type::Float),
        Expr::Duration(_) => Ok(Type::Duration),
        Expr::TemplateLit { parts } => {
            for part in parts {
                if let TplPart::Expr(expr) = part {
                    let ty = type_of_expr(expr, env, builtins)?;
                    if ty != Type::Field {
                        return Err(ScriptError::type_msg(format!(
                            "template interpolation: expected Field, got {ty}"
                        )));
                    }
                }
            }
            Ok(Type::Template)
        }
        Expr::Call { callee, args } => {
            let sig = builtins
                .get(callee.as_str())
                .ok_or_else(|| ScriptError::UnknownBuiltin(callee.clone()))?;
            check_call_args(sig, args, env, builtins)?;
            Ok(sig.ret)
        }
    }
}

fn check_call_args(
    sig: &BuiltinSig,
    args: &[Arg],
    env: &HashMap<String, Type>,
    builtins: &HashMap<&'static str, &'static BuiltinSig>,
) -> Result<(), ScriptError> {
    let mut provided: HashMap<String, Type> = HashMap::new();
    let mut positional_i = 0usize;

    for arg in args {
        match arg {
            Arg::Named { name, value } => {
                let param = sig.params.iter().find(|p| p.name == name).ok_or_else(|| {
                    ScriptError::type_msg(format!("{}: unknown parameter `{name}`", sig.name))
                })?;
                let got = type_of_expr(value, env, builtins)?;
                if !assignable(param.ty, got) {
                    return Err(ScriptError::TypeMismatch {
                        expected: param.ty,
                        got,
                    });
                }
                if provided.insert(name.clone(), got).is_some() {
                    return Err(ScriptError::type_msg(format!(
                        "{}: duplicate parameter `{name}`",
                        sig.name
                    )));
                }
            }
            Arg::Positional(value) => {
                if !sig.allow_positional {
                    return Err(ScriptError::type_msg(format!(
                        "{}: positional args not allowed; use name: value",
                        sig.name
                    )));
                }
                let param = sig.params.get(positional_i).ok_or_else(|| {
                    ScriptError::type_msg(format!("{}: too many positional arguments", sig.name))
                })?;
                let got = type_of_expr(value, env, builtins)?;
                if !assignable(param.ty, got) {
                    return Err(ScriptError::TypeMismatch {
                        expected: param.ty,
                        got,
                    });
                }
                provided.insert(param.name.to_string(), got);
                positional_i += 1;
            }
        }
    }

    for param in sig.params {
        if !param.optional && !provided.contains_key(param.name) {
            return Err(ScriptError::type_msg(format!(
                "{}: missing required parameter `{}`",
                sig.name, param.name
            )));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parse::parse_program;

    /// 测试内容：插值模板要求 Field 绑定。
    /// 输入：`counter` + `` body(`x=${c}`) ``。
    /// 预期：typecheck Ok。
    #[test]
    fn typecheck_template_interp() {
        let src = r#"
let c = counter()
let body = body(`x=${c}`)
let sink = stdout_sink()
let cfg = logen(body: body, sink: sink)
"#;
        typecheck(parse_program(src).unwrap()).expect("typecheck");
    }

    /// 测试内容：控制生命周期 builtin 接受顺序明确的位置参数。
    /// 输入：位置形式的 `logen`、`start`、`stop` 和 `stat` 调用。
    /// 预期：类型检查成功，不要求为必填参数重复书写名称。
    #[test]
    fn typecheck_lifecycle_positional_args() {
        let src = r#"
let id = start(logen(preset_json(), stdout_sink()))
stop(id)
stat(id)
"#;
        typecheck(parse_program(src).expect("parse")).expect("typecheck");
    }

    /// 测试内容：threads 接受 Int。
    /// 输入：`threads: 2`。
    /// 预期：typecheck Ok。
    #[test]
    fn typecheck_threads_int() {
        let src = r#"
let body = preset_json()
let sink = stdout_sink()
let cfg = logen(body: body, sink: sink, threads: 2)
"#;
        typecheck(parse_program(src).unwrap()).expect("typecheck");
    }

    /// 测试内容：kafka_sink 传入 Int topic 应类型失败。
    /// 输入：`kafka_sink(topic: 1)`。
    /// 预期：TypeMismatch。
    #[test]
    fn typecheck_rejects_wrong_kafka_topic_type() {
        let src = r#"
kafka_sink(topic: 1)
"#;
        let err = typecheck(parse_program(src).unwrap()).unwrap_err();
        match err {
            ScriptError::TypeMismatch {
                expected: Type::Str,
                got: Type::Int,
            } => {}
            other => panic!("unexpected {other}"),
        }
    }
}
