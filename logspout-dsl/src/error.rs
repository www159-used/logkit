use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("template string must be non-empty")]
    EmptyTemplate,
    #[error("pick: values list must be non-empty")]
    EmptyPickList,
    #[error("one-of: branches list must be non-empty")]
    EmptyOneOfBranches,
    #[error("integer: min ({min}) > max ({max})")]
    InvalidIntegerRange { min: i64, max: i64 },
    #[error("sentence: min ({min}) > max ({max})")]
    InvalidSentenceRange { min: usize, max: usize },
    #[error("handlebars: {0}")]
    Handlebars(#[from] handlebars::RenderError),
    #[error("handlebars template: {0}")]
    HandlebarsTemplate(#[from] handlebars::TemplateError),
}

/// 解析 producer 配置文件（仅 YAML）时的错误。
#[derive(Debug, Error)]
pub enum ConfigParseError {
    #[error("producer config path must end with .yaml or .yml (got {0})")]
    PathNotYaml(String),
    #[error("producer YAML: {0}")]
    Yaml(#[from] serde_yaml::Error),
    #[error("read {0}: {1}")]
    Io(String, #[source] std::io::Error),
    #[error("merging producer config: {0}")]
    Merge(String),
}
