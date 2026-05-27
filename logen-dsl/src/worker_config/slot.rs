//! 字段 **运行态**：由 [`super::field_spec::FieldSpec`] 编译为 [`TemplateSlot`]，供 [`crate::TemplateRunner`] 每轮调用。

use std::collections::BTreeMap;

use chrono::Local;
use fake::faker::company::raw::CompanyName;
use fake::faker::internet::raw::{DomainSuffix, IPv4, UserAgent, Username};
use fake::faker::lorem::en::Words;
use fake::faker::lorem::raw::Word;
use fake::faker::name::raw::Name;
use fake::locales::EN;
use fake::uuid::UUIDv4;
use fake::{Fake, Faker};
use handlebars::Handlebars;
use serde_json::{Map, Value};
use url::Url;

use crate::Error;

use super::field_spec::FieldSpec;
use super::branch::OneOfSlot;

/// 模板字段插槽：每轮渲染前调用 [`TemplateSlot::next_value`]。
pub trait TemplateSlot: Send {
    fn next_value(&mut self) -> String;
}

impl FieldSpec {
    pub fn into_slot(self) -> Result<Box<dyn TemplateSlot>, Error> {
        match self {
            FieldSpec::UuidV4 => Ok(Box::new(UuidV4Slot)),
            FieldSpec::NameEn => Ok(Box::new(NameEnSlot)),
            FieldSpec::Ipv4 => Ok(Box::new(Ipv4Slot)),
            FieldSpec::Timestamp { format } => Ok(Box::new(TimestampSlot { format })),
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
            FieldSpec::Template { template, fields } => {
                Ok(Box::new(make_composite_template_slot(template, fields)?))
            }
            FieldSpec::OneOf { branches } => Ok(Box::new(OneOfSlot::from_branches(branches)?)),
        }
    }
}

pub(crate) fn make_composite_template_slot(
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

pub(crate) struct CompositeTemplateSlot {
    hb: Handlebars<'static>,
    slots: BTreeMap<String, Box<dyn TemplateSlot>>,
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