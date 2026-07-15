//! pest → AST。

use pest::Parser;
use pest_derive::Parser;

use crate::ast::{Arg, Expr, Program, Stmt, TplPart};
use crate::ScriptError;

#[derive(Parser)]
#[grammar = "grammar.pest"]
pub struct ScriptParser;

pub fn parse_program(source: &str) -> Result<Program, ScriptError> {
    let mut pairs = ScriptParser::parse(Rule::program, source)
        .map_err(|e| ScriptError::Parse(e.to_string()))?;
    let program = pairs
        .next()
        .ok_or_else(|| ScriptError::Parse("empty".into()))?;
    let mut stmts = Vec::new();
    for pair in program.into_inner() {
        match pair.as_rule() {
            Rule::stmt => stmts.push(parse_stmt(pair)?),
            Rule::EOI => {}
            other => {
                return Err(ScriptError::Parse(format!(
                    "unexpected top-level {other:?}"
                )));
            }
        }
    }
    Ok(Program { stmts })
}

fn parse_stmt(pair: pest::iterators::Pair<'_, Rule>) -> Result<Stmt, ScriptError> {
    let inner = pair
        .into_inner()
        .next()
        .ok_or_else(|| ScriptError::Parse("empty stmt".into()))?;
    match inner.as_rule() {
        Rule::let_stmt => {
            let mut it = inner.into_inner();
            let name = it
                .next()
                .ok_or_else(|| ScriptError::Parse("let: missing name".into()))?
                .as_str()
                .to_string();
            let value = parse_expr(
                it.next()
                    .ok_or_else(|| ScriptError::Parse("let: missing value".into()))?,
            )?;
            Ok(Stmt::Let { name, value })
        }
        Rule::expr_stmt => {
            let expr_pair = inner
                .into_inner()
                .next()
                .ok_or_else(|| ScriptError::Parse("expr_stmt empty".into()))?;
            Ok(Stmt::Expr(parse_expr(expr_pair)?))
        }
        other => Err(ScriptError::Parse(format!("unexpected stmt {other:?}"))),
    }
}

fn parse_expr(pair: pest::iterators::Pair<'_, Rule>) -> Result<Expr, ScriptError> {
    match pair.as_rule() {
        Rule::expr => {
            let inner = pair
                .into_inner()
                .next()
                .ok_or_else(|| ScriptError::Parse("empty expr".into()))?;
            parse_expr(inner)
        }
        Rule::call => parse_call(pair),
        Rule::ident => Ok(Expr::Path(vec![pair.as_str().to_string()])),
        Rule::string => Ok(Expr::Str(unescape_string(pair.as_str())?)),
        Rule::integer => {
            let raw = pair.as_str();
            let n: i64 = raw
                .parse()
                .map_err(|e| ScriptError::Parse(format!("integer `{raw}`: {e}")))?;
            Ok(Expr::Int(n))
        }
        Rule::float => {
            let raw = pair.as_str();
            let n: f64 = raw
                .parse()
                .map_err(|e| ScriptError::Parse(format!("float `{raw}`: {e}")))?;
            if !n.is_finite() {
                return Err(ScriptError::Parse(format!(
                    "float `{raw}`: non-finite value"
                )));
            }
            Ok(Expr::Float(n))
        }
        Rule::duration => Ok(Expr::Duration(pair.as_str().to_string())),
        Rule::template_lit => parse_template_lit(pair),
        other => Err(ScriptError::Parse(format!("unexpected expr {other:?}"))),
    }
}

fn parse_template_lit(pair: pest::iterators::Pair<'_, Rule>) -> Result<Expr, ScriptError> {
    let raw = pair.as_str();
    let inner = raw
        .strip_prefix('`')
        .and_then(|s| s.strip_suffix('`'))
        .ok_or_else(|| ScriptError::Parse(format!("bad template literal: {raw}")))?;
    let parts = split_template_source(inner)?;
    Ok(Expr::TemplateLit { parts })
}

/// 将 `` `…${field_expr}…` `` 原文拆成 Text / Expr。
fn split_template_source(s: &str) -> Result<Vec<TplPart>, ScriptError> {
    let mut parts = Vec::new();
    let mut rest = s;
    while !rest.is_empty() {
        if let Some(i) = rest.find("${") {
            if i > 0 {
                parts.push(TplPart::Text(rest[..i].to_string()));
            }
            let after = &rest[i + 2..];
            if let Some(end) = after.find('}') {
                let source = after[..end].trim();
                if source.is_empty() {
                    return Err(ScriptError::Parse("template interpolation is empty".into()));
                }
                let pair = ScriptParser::parse(Rule::expr, source)
                    .map_err(|e| ScriptError::Parse(format!("template interpolation: {e}")))?
                    .next()
                    .ok_or_else(|| ScriptError::Parse("template interpolation is empty".into()))?;
                parts.push(TplPart::Expr(parse_expr(pair)?));
                rest = &after[end + 1..];
                continue;
            }
            return Err(ScriptError::Parse(
                "template interpolation is unclosed".into(),
            ));
        } else {
            parts.push(TplPart::Text(rest.to_string()));
            break;
        }
    }
    if parts.is_empty() {
        parts.push(TplPart::Text(String::new()));
    }
    Ok(parts)
}

fn parse_call(pair: pest::iterators::Pair<'_, Rule>) -> Result<Expr, ScriptError> {
    let mut it = pair.into_inner();
    let callee = it
        .next()
        .ok_or_else(|| ScriptError::Parse("call: missing callee".into()))?
        .as_str()
        .to_string();
    let mut args = Vec::new();
    if let Some(args_pair) = it.next() {
        if args_pair.as_rule() == Rule::args {
            for arg in args_pair.into_inner() {
                args.push(parse_arg(arg)?);
            }
        }
    }
    Ok(Expr::Call { callee, args })
}

fn parse_arg(pair: pest::iterators::Pair<'_, Rule>) -> Result<Arg, ScriptError> {
    let inner = pair
        .into_inner()
        .next()
        .ok_or_else(|| ScriptError::Parse("empty arg".into()))?;
    match inner.as_rule() {
        Rule::named_arg => {
            let mut it = inner.into_inner();
            let name = it
                .next()
                .ok_or_else(|| ScriptError::Parse("named arg: name".into()))?
                .as_str()
                .to_string();
            let value = parse_expr(
                it.next()
                    .ok_or_else(|| ScriptError::Parse("named arg: value".into()))?,
            )?;
            Ok(Arg::Named { name, value })
        }
        _ => Ok(Arg::Positional(parse_expr(inner)?)),
    }
}

fn unescape_string(raw: &str) -> Result<String, ScriptError> {
    let t = raw
        .strip_prefix('"')
        .and_then(|s| s.strip_suffix('"'))
        .ok_or_else(|| ScriptError::Parse(format!("bad string literal: {raw}")))?;
    let mut out = String::with_capacity(t.len());
    let mut chars = t.chars();
    while let Some(c) = chars.next() {
        if c == '\\' {
            match chars.next() {
                Some(n) => out.push(n),
                None => return Err(ScriptError::Parse("trailing backslash in string".into())),
            }
        } else {
            out.push(c);
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::TplPart;

    /// 测试内容：解析模板插值字面量。
    /// 输入：`` `x=${c}` ``。
    /// 预期：Text + Path(c) 表达式。
    #[test]
    fn parse_template_lit() {
        let p = parse_program(r#"let t = `x=${c}`"#).expect("parse");
        match &p.stmts[0] {
            Stmt::Let {
                value: Expr::TemplateLit { parts },
                ..
            } => {
                assert_eq!(
                    parts,
                    &vec![
                        TplPart::Text("x=".into()),
                        TplPart::Expr(Expr::Path(vec!["c".into()])),
                    ]
                );
            }
            other => panic!("{other:?}"),
        }
    }

    /// 测试内容：多行模板保留原文（含换行与缩进）。
    /// 输入：缩进块内的多行反引号模板。
    /// 预期：内容含换行与前导空白。
    #[test]
    fn parse_multiline_template_preserves_whitespace() {
        let p = parse_program("let t = `\n  a=${x}\n    b\n`\n").expect("parse");
        match &p.stmts[0] {
            Stmt::Let {
                value: Expr::TemplateLit { parts },
                ..
            } => {
                let mut s = String::new();
                for part in parts {
                    match part {
                        TplPart::Text(t) => s.push_str(t),
                        TplPart::Expr(Expr::Path(path)) => {
                            s.push_str("${");
                            s.push_str(&path[0]);
                            s.push('}');
                        }
                        TplPart::Expr(other) => panic!("unexpected interpolation {other:?}"),
                    }
                }
                assert_eq!(s, "\n  a=${x}\n    b\n");
            }
            other => panic!("{other:?}"),
        }
    }

    /// 测试内容：解析 i64 / f64 字面量。
    /// 输入：`-42` 与 `3.0`。
    /// 预期：Int / Float。
    #[test]
    fn parse_int_and_float() {
        let p = parse_program("let a = -42\nlet b = 3.0\n").expect("parse");
        match &p.stmts[0] {
            Stmt::Let {
                value: Expr::Int(-42),
                ..
            } => {}
            other => panic!("{other:?}"),
        }
        match &p.stmts[1] {
            Stmt::Let {
                value: Expr::Float(f),
                ..
            } => assert!((*f - 3.0).abs() < 1e-9),
            other => panic!("{other:?}"),
        }
    }

    /// 测试内容：解析 let + 命名参数调用。
    /// 输入：最小脚本片段。
    /// 预期：AST 含 Let 与 Call。
    #[test]
    fn parse_let_and_named_call() {
        let p = parse_program(
            r#"
let sink = kafka_sink(topic: "raw_message")
cfg
"#,
        )
        .expect("parse");
        assert_eq!(p.stmts.len(), 2);
        match &p.stmts[0] {
            Stmt::Let { name, value } => {
                assert_eq!(name, "sink");
                match value {
                    Expr::Call { callee, args } => {
                        assert_eq!(callee, "kafka_sink");
                        assert_eq!(args.len(), 1);
                    }
                    other => panic!("{other:?}"),
                }
            }
            other => panic!("{other:?}"),
        }
    }

    /// 测试内容：duration 与位置参数。
    /// 输入：`preset_json()` 与 `10ms`。
    /// 预期：Call / Duration 正确。
    #[test]
    fn parse_positional_and_duration() {
        let p = parse_program(
            r#"
let body = preset_json()
let cfg = logen(body: body, sink: sink, rate: 10ms)
"#,
        )
        .expect("parse");
        assert_eq!(p.stmts.len(), 2);
        match &p.stmts[0] {
            Stmt::Let {
                value: Expr::Call { callee, args },
                ..
            } => {
                assert_eq!(callee, "preset_json");
                assert!(args.is_empty());
            }
            other => panic!("{other:?}"),
        }
    }
}
