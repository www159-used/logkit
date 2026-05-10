use std::collections::BTreeMap;

use chrono::Local;
use fake::{Fake, Faker};
use fake::faker::internet::raw::{IPv4, UserAgent, Username};
use fake::faker::lorem::en::Words;
use fake::faker::name::raw::Name;
use fake::locales::EN;
use fake::uuid::UUIDv4;
use serde::Deserialize;
use url::Url;

use crate::facade::TemplateSlot;
use crate::Error;

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
    /// 随机 User-Agent 字符串（`fake` internet）
    UserAgent,
    /// `fake` **登录名/句柄**（`internet::Username`），与 `name-en` 的全名风格不同；适合 HTTP `%u`、账号 id
    Username,
    /// 从 0 起每轮递增 1（`u64`，溢出后按环绕继续）
    Counter,
}

fn default_ts_format() -> String {
    "%Y-%m-%d %H:%M:%S".to_string()
}

impl FieldSpec {
    pub fn into_slot(self) -> Result<Box<dyn TemplateSlot>, Error> {
        Ok(match self {
            FieldSpec::UuidV4 => Box::new(UuidV4Slot),
            FieldSpec::NameEn => Box::new(NameEnSlot),
            FieldSpec::Ipv4 => Box::new(Ipv4Slot),
            FieldSpec::Timestamp { format } => Box::new(TimestampSlot { format }),
            FieldSpec::Pick { values } => {
                if values.is_empty() {
                    return Err(Error::EmptyPickList);
                }
                Box::new(PickSlot { values })
            }
            FieldSpec::Integer { min, max } => {
                if min > max {
                    return Err(Error::InvalidIntegerRange { min, max });
                }
                Box::new(IntegerSlot { min, max })
            }
            FieldSpec::Sentence { min, max } => {
                if min > max {
                    return Err(Error::InvalidSentenceRange { min, max });
                }
                Box::new(SentenceSlot { min, max })
            }
            FieldSpec::UserAgent => Box::new(UserAgentSlot),
            FieldSpec::Username => Box::new(UsernameSlot),
            FieldSpec::Url => Box::new(UrlSlot),
            FieldSpec::UrlPath => Box::new(UrlPathSlot),
            FieldSpec::Counter => Box::new(CounterSlot { n: 0 }),
        })
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
