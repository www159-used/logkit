//! [`ScriptError`]：parse / typecheck / eval。

use std::path::PathBuf;

use thiserror::Error;

use crate::types::Type;

#[derive(Debug, Error)]
pub enum ScriptError {
    #[error("parse: {0}")]
    Parse(String),

    #[error("type: {0}")]
    Type(String),

    #[error("type: expected {expected}, got {got}")]
    TypeMismatch { expected: Type, got: Type },

    #[error("undefined variable `{0}`")]
    UndefinedVar(String),

    #[error("unknown builtin `{0}`")]
    UnknownBuiltin(String),

    #[error("eval: {0}")]
    Eval(String),

    #[error("preset `{name}`: {detail}")]
    Preset { name: String, detail: String },

    #[error("io {path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    #[error("yaml: {0}")]
    Yaml(String),
}

impl ScriptError {
    pub(crate) fn type_msg(msg: impl Into<String>) -> Self {
        Self::Type(msg.into())
    }

    pub fn eval_msg(msg: impl Into<String>) -> Self {
        Self::Eval(msg.into())
    }
}
