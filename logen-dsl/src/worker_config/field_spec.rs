//! 字段 **声明**：YAML `fields.<name>` 的 `type` 与参数（无生成状态）。

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use super::branch::OneOfBranch;

/// 配置里 `fields.<name>` 的描述；运行态见 [`super::slot::TemplateSlot`] 与 [`FieldSpec::into_slot`].
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "kebab-case")]
pub enum FieldSpec {
    /// 随机 UUID v4 字符串
    UuidV4,
    /// `fake` 英文**人名**（`name::Name`），多为「名 + 姓」展示用，如 `John Smith`
    NameEn,
    /// `fake` IPv4
    Ipv4,
    /// 当前时间按 [`chrono`] 格式化（`format` 为 `strftime`，如 `%Y-%m-%d %H:%M:%S`）
    Timestamp {
        #[serde(default = "default_ts_format")]
        format: String,
    },
    /// 闭区间 `[min, max]` 内随机整数（`fake`）
    Integer { min: i64, max: i64 },
    /// [`fake`] lorem：空格分隔的随机英文词，词数在 `[min, max]`（含）之间均匀随机
    Sentence { min: usize, max: usize },
    /// 随机绝对 URL（`fake` + `url` crate，形如 `https://example.com/fruit/...`）
    Url,
    /// 随机 URL 的请求路径部分（含 query、fragment），用于 `\"GET {{dst}} HTTP/1.1\"` 等
    UrlPath,
    /// 随机 **FQDN 形**主机标签：`{lorem-word}-{lorem-word}.{domain-suffix}`（小写），适合 syslog HOSTNAME
    Hostname,
    /// `fake` 顶级域后缀（`internet::DomainSuffix`），如 `com`、`org`
    DomainSuffix,
    /// 单个随机英文词（`lorem::Word`），小写；适合短小 APP-NAME、标记等
    LoremWord,
    /// `fake` 公司名（`company::CompanyName`），可含空格；注意 syslog APP-NAME 语义上通常为无空单词
    CompanyName,
    /// 随机 User-Agent 字符串（`fake` internet）
    UserAgent,
    /// `fake` **登录名/句柄**（`internet::Username`），与 `name-en` 的全名风格不同；适合 HTTP `%u`、账号 id
    Username,
    /// 从 0 起每轮递增 1（`u64`，溢出后按环绕继续）
    Counter,
    /// 子模板：用 Handlebars 渲染 **`template`**，占位符仅来自本节点的 **`fields`**（可再嵌套 `template`，形成树）。
    /// 适合 RFC 5424 `STRUCTURED-DATA` 等需拼多段、多层的场景。
    Template {
        template: String,
        #[serde(default)]
        fields: BTreeMap<String, FieldSpec>,
    },
    /// 多选一：字面量、`{ w, v }` 或预编译 `template` 子树；权重在 `into_slot` 时编入 [`logen_branch::BranchPicker`]。
    OneOf { branches: Vec<OneOfBranch> },
}

fn default_ts_format() -> String {
    "%Y-%m-%d %H:%M:%S".to_string()
}
