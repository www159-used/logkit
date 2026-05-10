use std::collections::BTreeMap;

use chrono::Local;
use fake::{Fake, Faker};
use fake::faker::company::raw::CompanyName;
use fake::faker::internet::raw::{DomainSuffix, IPv4, UserAgent, Username};
use fake::faker::lorem::raw::Word;
use fake::faker::lorem::en::Words;
use fake::faker::name::raw::Name;
use fake::locales::EN;
use fake::uuid::UUIDv4;
use handlebars::Handlebars;
use serde::Deserialize;
use serde_json::{Map, Value};
use url::Url;

use crate::facade::TemplateSlot;
use crate::Error;

/// [`FieldSpec::OneOf`] 的单个分支：YAML 中可写 **字符串字面量**，或 **`template` + `fields`** 子树（与 `type: template` 同形）。
/// 每轮 **均匀** 随机选一翼；**仅被选翼会求值**（未选中的 `template` 子树及其 `counter` 等不会跑）。
#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum OneOfBranch {
    Literal(String),
    Template(OneOfTemplateBranch),
}

/// `one-of` 中带 `template` / `fields` 的分支（映射 YAML 里含 `template` 键的映射表）。
#[derive(Debug, Clone, Deserialize)]
pub struct OneOfTemplateBranch {
    pub template: String,
    #[serde(default)]
    pub fields: BTreeMap<String, FieldSpec>,
}

/// 配置里 `fields.<name>` 的描述：内置 `type` 门面，由 [`into_slot`] 转成 [`TemplateSlot`]。
#[derive(Debug, Clone, Deserialize)]
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
    /// 从给定列表均匀随机（仅用 [`fake`] 抽下标）
    Pick {
        values: Vec<String>,
    },
    /// 闭区间 `[min, max]` 内随机整数（`fake`）
    Integer {
        min: i64,
        max: i64,
    },
    /// [`fake`] lorem：空格分隔的随机英文词，词数在 `[min, max]`（含）之间均匀随机
    Sentence {
        min: usize,
        max: usize,
    },
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
    /// 多选一：分支为 **字面量字符串** 或 **内联 template 子树**；仅选中分支参与本行生成（lazy）。
    OneOf {
        branches: Vec<OneOfBranch>,
    },
}

fn default_ts_format() -> String {
    "%Y-%m-%d %H:%M:%S".to_string()
}

impl FieldSpec {
    pub fn into_slot(self) -> Result<Box<dyn TemplateSlot>, Error> {
        match self {
            FieldSpec::UuidV4 => Ok(Box::new(UuidV4Slot)),
            FieldSpec::NameEn => Ok(Box::new(NameEnSlot)),
            FieldSpec::Ipv4 => Ok(Box::new(Ipv4Slot)),
            FieldSpec::Timestamp { format } => Ok(Box::new(TimestampSlot { format })),
            FieldSpec::Pick { values } => {
                if values.is_empty() {
                    return Err(Error::EmptyPickList);
                }
                Ok(Box::new(PickSlot { values }))
            }
            FieldSpec::Integer { min, max } => {
                if min > max {
                    return Err(Error::InvalidIntegerRange { min, max });
                }
                Ok(Box::new(IntegerSlot { min, max }))
            }
            FieldSpec::Sentence { min, max } => {
                if min > max {
                    return Err(Error::InvalidSentenceRange { min, max });
                }
                Ok(Box::new(SentenceSlot { min, max }))
            }
            FieldSpec::Hostname => Ok(Box::new(HostnameSlot)),
            FieldSpec::DomainSuffix => Ok(Box::new(DomainSuffixSlot)),
            FieldSpec::LoremWord => Ok(Box::new(LoremWordSlot)),
            FieldSpec::CompanyName => Ok(Box::new(CompanyNameSlot)),
            FieldSpec::UserAgent => Ok(Box::new(UserAgentSlot)),
            FieldSpec::Username => Ok(Box::new(UsernameSlot)),
            FieldSpec::Url => Ok(Box::new(UrlSlot)),
            FieldSpec::UrlPath => Ok(Box::new(UrlPathSlot)),
            FieldSpec::Counter => Ok(Box::new(CounterSlot { n: 0 })),
            FieldSpec::Template { template, fields } => Ok(Box::new(make_composite_template_slot(
                template,
                fields,
            )?)),
            FieldSpec::OneOf { branches } => {
                if branches.is_empty() {
                    return Err(Error::EmptyOneOfBranches);
                }
                let mut arms = Vec::with_capacity(branches.len());
                for b in branches {
                    arms.push(match b {
                        OneOfBranch::Literal(s) => OneOfArm::Literal(s),
                        OneOfBranch::Template(OneOfTemplateBranch { template, fields }) => {
                            OneOfArm::Nested(Box::new(make_composite_template_slot(
                                template,
                                fields,
                            )?))
                        }
                    });
                }
                Ok(Box::new(OneOfSlot { arms }))
            }
        }
    }
}

fn make_composite_template_slot(
    template: String,
    fields: BTreeMap<String, FieldSpec>,
) -> Result<CompositeTemplateSlot, Error> {
    if template.trim().is_empty() {
        return Err(Error::EmptyTemplate);
    }
    let mut hb = Handlebars::new();
    hb.set_strict_mode(false);
    hb.register_escape_fn(handlebars::no_escape);
    hb.register_template_string("slot", template.as_str())?;
    let slots = slots_from_fields(fields)?;
    Ok(CompositeTemplateSlot { hb, slots })
}

enum OneOfArm {
    Literal(String),
    Nested(Box<CompositeTemplateSlot>),
}

struct OneOfSlot {
    arms: Vec<OneOfArm>,
}

impl TemplateSlot for OneOfSlot {
    fn next_value(&mut self) -> String {
        let i = fake_index(self.arms.len());
        match &mut self.arms[i] {
            OneOfArm::Literal(s) => s.clone(),
            OneOfArm::Nested(c) => c.next_value(),
        }
    }
}

struct UuidV4Slot;

impl TemplateSlot for UuidV4Slot {
    fn next_value(&mut self) -> String {
        UUIDv4.fake()
    }
}

struct NameEnSlot;

impl TemplateSlot for NameEnSlot {
    fn next_value(&mut self) -> String {
        Name(EN).fake()
    }
}

struct Ipv4Slot;

impl TemplateSlot for Ipv4Slot {
    fn next_value(&mut self) -> String {
        IPv4(EN).fake()
    }
}

struct TimestampSlot {
    format: String,
}

impl TemplateSlot for TimestampSlot {
    fn next_value(&mut self) -> String {
        Local::now().format(&self.format).to_string()
    }
}

struct PickSlot {
    values: Vec<String>,
}

impl TemplateSlot for PickSlot {
    fn next_value(&mut self) -> String {
        let i = fake_index(self.values.len());
        self.values[i].clone()
    }
}

struct IntegerSlot {
    min: i64,
    max: i64,
}

impl TemplateSlot for IntegerSlot {
    fn next_value(&mut self) -> String {
        let v: i64 = (self.min..=self.max).fake();
        v.to_string()
    }
}

struct SentenceSlot {
    min: usize,
    max: usize,
}

impl TemplateSlot for SentenceSlot {
    fn next_value(&mut self) -> String {
        let n: usize = (self.min..=self.max).fake();
        if n == 0 {
            return String::new();
        }
        Words(n..n + 1).fake::<Vec<String>>().join(" ")
    }
}

struct UserAgentSlot;

impl TemplateSlot for UserAgentSlot {
    fn next_value(&mut self) -> String {
        UserAgent(EN).fake()
    }
}

struct UsernameSlot;

impl TemplateSlot for UsernameSlot {
    fn next_value(&mut self) -> String {
        Username(EN).fake()
    }
}

struct UrlSlot;

impl TemplateSlot for UrlSlot {
    fn next_value(&mut self) -> String {
        Faker.fake::<Url>().as_str().to_string()
    }
}

struct UrlPathSlot;

impl TemplateSlot for UrlPathSlot {
    fn next_value(&mut self) -> String {
        let u: Url = Faker.fake();
        let mut out = u.path().to_string();
        if out.is_empty() {
            out.push('/');
        }
        if let Some(q) = u.query() {
            out.push('?');
            out.push_str(q);
        }
        if let Some(f) = u.fragment() {
            out.push('#');
            out.push_str(f);
        }
        out
    }
}

struct HostnameSlot;

impl TemplateSlot for HostnameSlot {
    fn next_value(&mut self) -> String {
        let a: String = Word(EN).fake();
        let b: String = Word(EN).fake();
        let suf: String = DomainSuffix(EN).fake();
        format!(
            "{}-{}.{}",
            a.to_lowercase(),
            b.to_lowercase(),
            suf.to_lowercase()
        )
    }
}

struct DomainSuffixSlot;

impl TemplateSlot for DomainSuffixSlot {
    fn next_value(&mut self) -> String {
        DomainSuffix(EN).fake()
    }
}

struct LoremWordSlot;

impl TemplateSlot for LoremWordSlot {
    fn next_value(&mut self) -> String {
        Word(EN).fake::<String>().to_lowercase()
    }
}

struct CompanyNameSlot;

impl TemplateSlot for CompanyNameSlot {
    fn next_value(&mut self) -> String {
        CompanyName(EN).fake()
    }
}

struct CounterSlot {
    n: u64,
}

impl TemplateSlot for CounterSlot {
    fn next_value(&mut self) -> String {
        let out = self.n;
        self.n = self.n.wrapping_add(1);
        out.to_string()
    }
}

/// 嵌套模板槽：每轮先递归取子字段字符串，再渲染为本字段的一条字符串。
struct CompositeTemplateSlot {
    hb: Handlebars<'static>,
    slots: BTreeMap<String, Box<dyn TemplateSlot>>,
}

impl TemplateSlot for CompositeTemplateSlot {
    fn next_value(&mut self) -> String {
        let mut map = Map::new();
        for (key, slot) in &mut self.slots {
            map.insert(key.clone(), Value::String(slot.next_value()));
        }
        self.hb
            .render("slot", &Value::Object(map))
            .unwrap_or_else(|e| format!("{{{{nested render: {e}}}}}"))
    }
}

fn fake_index(len: usize) -> usize {
    debug_assert!(len > 0);
    (0..len).fake::<usize>()
}

/// 将配置中的 `fields` 转成有序插槽（[`BTreeMap`] 保证键稳定顺序，便于测试）。
pub(crate) fn slots_from_fields(
    fields: BTreeMap<String, FieldSpec>,
) -> Result<BTreeMap<String, Box<dyn TemplateSlot>>, Error> {
    let mut out = BTreeMap::new();
    for (k, spec) in fields {
        out.insert(k, spec.into_slot()?);
    }
    Ok(out)
}
