use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("template string must be non-empty")]
    EmptyTemplate,
    #[error("one-of: branches list must be non-empty")]
    EmptyOneOfBranches,
    #[error("one-of: {0}")]
    Branch(#[from] logen_branch::BranchError),
    #[error("integer: min ({min}) > max ({max})")]
    InvalidIntegerRange { min: i64, max: i64 },
    #[error("float: min ({min}) > max ({max})")]
    InvalidFloatRange { min: f64, max: f64 },
    #[error("sentence: min ({min}) > max ({max})")]
    InvalidSentenceRange { min: usize, max: usize },
    #[error("handlebars: {0}")]
    Handlebars(#[from] handlebars::RenderError),
    #[error("handlebars template: {0}")]
    HandlebarsTemplate(#[from] handlebars::TemplateError),
}

/// 解析 worker 模板配置文件（仅 YAML）时的错误。
#[derive(Debug, Error)]
pub enum ConfigParseError {
    #[error("worker config path must end with .yaml or .yml (got {0})")]
    PathNotYaml(String),
    #[error("worker config YAML: {0}")]
    Yaml(#[from] serde_yaml::Error),
    #[error("read {0}: {1}")]
    Io(String, #[source] std::io::Error),
    #[error("merging worker config: {0}")]
    Merge(String),
    #[error("include not found (from {from}): {path}: {source}")]
    IncludeNotFound {
        from: String,
        path: String,
        #[source]
        source: std::io::Error,
    },
    #[error("include cycle: {}", chain.join(" -> "))]
    IncludeCycle { chain: Vec<String> },
    #[error("include depth exceeded (max {max})")]
    IncludeDepthExceeded { max: usize },
    #[error("invalid include path {path}: {reason}")]
    IncludePathInvalid { path: String, reason: String },
    #[error("kafka protocol discovery: {0}")]
    KafkaProtocol(#[from] kafka_protocol::KafkaProtocolError),
}
