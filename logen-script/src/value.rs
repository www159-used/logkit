//! 运行时值：直接持有 [`logen_model`] 类型，不再用 `serde_yaml::Value` 充当 Body/Sink/Field。

use std::collections::BTreeMap;
use std::time::Duration;

use logen_model::{BodyConfig, FieldSpec, SinkConfig};

use crate::types::Type;

/// 解释器运行时值（与 [`Type`] 对应）。
#[derive(Debug, Clone)]
pub enum Value {
    Body(BodyConfig),
    Sink(SinkConfig),
    Config(ConfigValue),
    Str(String),
    Int(i64),
    Float(f64),
    Duration(Duration),
    Field(FieldSpec),
    /// 插值模板：已换成唯一 `{{_tplN}}` 槽，并持有字段定义。
    Template {
        handlebars: String,
        fields: BTreeMap<String, FieldSpec>,
    },
    Unit,
}

/// `logen(...)` 产物：分区草稿；创建后密封，不可再改 body/sink。
#[derive(Debug, Clone)]
pub struct ConfigValue {
    pub body: BodyConfig,
    pub sink: SinkConfig,
    pub rate: Option<Duration>,
    pub threads: Option<u32>,
    pub sealed: bool,
}

impl ConfigValue {
    /// 转为 [`logen_model::WorkerConfig`]（不含 Kafka 自动补全）。
    pub fn into_worker_config(self) -> logen_model::WorkerConfig {
        logen_model::WorkerConfig {
            template: self.body.template,
            fields: self.body.fields,
            min_interval: self.rate.unwrap_or(Duration::ZERO),
            threads: self.threads.unwrap_or(1),
            sink: self.sink,
        }
    }
}

impl Value {
    pub fn ty(&self) -> Type {
        match self {
            Value::Body(_) => Type::Body,
            Value::Sink(_) => Type::Sink,
            Value::Config(_) => Type::Config,
            Value::Str(_) => Type::Str,
            Value::Int(_) => Type::Int,
            Value::Float(_) => Type::Float,
            Value::Duration(_) => Type::Duration,
            Value::Field(_) => Type::Field,
            Value::Template { .. } => Type::Template,
            Value::Unit => Type::Unit,
        }
    }
}
