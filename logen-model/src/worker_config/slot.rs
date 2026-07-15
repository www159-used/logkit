//! 字段运行态：[`FieldSpec`] → [`TemplateSlot`]，以及顶层 [`TemplateRunner`]。

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
use logen_branch::BranchPicker;
use serde_json::{Map, Value};
use url::Url;

use crate::Error;

use super::branch::{OneOfBranch, OneOfTemplateBranch};
use super::field_spec::FieldSpec;

const INLINE_TMPL: &str = "inline";
const COMPOSITE_TMPL: &str = "slot";

/// 模板字段插槽：每轮渲染前调用 [`TemplateSlot::next_value`]。
pub trait TemplateSlot: Send {
    fn next_value(&mut self) -> String;
}

/// 每轮用字段插槽生成上下文字段，再渲染 `template`。
pub struct TemplateRunner {
    hb: Handlebars<'static>,
    slots: BTreeMap<String, Box<dyn TemplateSlot>>,
}

impl TemplateRunner {
    /// 仅依赖渲染所需的 `template` 与 `fields`（与 `sink` / `min-interval` 无关）。
    pub fn try_new(
        template: impl AsRef<str>,
        fields: BTreeMap<String, FieldSpec>,
    ) -> Result<Self, Error> {
        let source = template.as_ref();
        if source.trim().is_empty() {
            return Err(Error::EmptyTemplate);
        }
        let mut hb = Handlebars::new();
        hb.set_strict_mode(false);
        hb.register_escape_fn(handlebars::no_escape);
        hb.register_template_string(INLINE_TMPL, source)?;
        let slots = slots_from_fields(fields)?;
        Ok(Self { hb, slots })
    }

    /// 生成一行（一条日志）。
    pub fn next_line(&mut self) -> Result<String, Error> {
        let mut map = Map::new();
        for (key, slot) in &mut self.slots {
            map.insert(key.clone(), Value::String(slot.next_value()));
        }
        Ok(self.hb.render(INLINE_TMPL, &Value::Object(map))?)
    }
}

impl FieldSpec {
    pub(crate) fn into_slot(self) -> Result<Box<dyn TemplateSlot>, Error> {
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
            FieldSpec::Float { min, max } => {
                if min > max {
                    return Err(Error::InvalidFloatRange { min, max });
                }
                if !min.is_finite() || !max.is_finite() {
                    return Err(Error::InvalidFloatRange { min, max });
                }
                Ok(Box::new(FloatSlot { min, max }))
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
    hb.register_template_string(COMPOSITE_TMPL, template.as_str())?;
    let slots = slots_from_fields(fields)?;
    Ok(CompositeTemplateSlot { hb, slots })
}

enum OneOfArm {
    Literal(String),
    Nested(Box<CompositeTemplateSlot>),
}

struct OneOfSlot {
    picker: BranchPicker,
    arms: Vec<OneOfArm>,
}

impl OneOfSlot {
    fn from_branches(branches: Vec<OneOfBranch>) -> Result<Self, Error> {
        if branches.is_empty() {
            return Err(Error::EmptyOneOfBranches);
        }
        let default_w = super::default_one_of_weight();
        let mut weights = Vec::with_capacity(branches.len());
        let mut arms = Vec::with_capacity(branches.len());
        for b in branches {
            let (w, arm) = match b {
                OneOfBranch::Literal(s) => (default_w, OneOfArm::Literal(s)),
                OneOfBranch::WeightedLiteral { w, v } => (w, OneOfArm::Literal(v)),
                OneOfBranch::Template(OneOfTemplateBranch {
                    w,
                    template,
                    fields,
                }) => (
                    w,
                    OneOfArm::Nested(Box::new(make_composite_template_slot(template, fields)?)),
                ),
            };
            weights.push(w);
            arms.push(arm);
        }
        let picker = BranchPicker::new(&weights)?;
        Ok(Self { picker, arms })
    }
}

impl TemplateSlot for OneOfSlot {
    fn next_value(&mut self) -> String {
        let i = self.picker.choose();
        match &mut self.arms[i] {
            OneOfArm::Literal(s) => s.clone(),
            OneOfArm::Nested(c) => c.next_value(),
        }
    }
}

struct CompositeTemplateSlot {
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

struct FloatSlot {
    min: f64,
    max: f64,
}

impl TemplateSlot for FloatSlot {
    fn next_value(&mut self) -> String {
        let v = if (self.max - self.min).abs() < f64::EPSILON {
            self.min
        } else {
            let t: f64 = (0.0f64..1.0).fake();
            self.min + t * (self.max - self.min)
        };
        format!("{v}")
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
            .render(COMPOSITE_TMPL, &Value::Object(map))
            .unwrap_or_else(|e| format!("{{{{nested render: {e}}}}}"))
    }
}

/// 将配置中的 `fields` 转成有序插槽（[`BTreeMap`] 保证键稳定顺序，便于测试）。
fn slots_from_fields(
    fields: BTreeMap<String, FieldSpec>,
) -> Result<BTreeMap<String, Box<dyn TemplateSlot>>, Error> {
    let mut out = BTreeMap::new();
    for (k, spec) in fields {
        out.insert(k, spec.into_slot()?);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 测试内容：多字段模板一次渲染，各字段占位符均展开。
    /// 输入：含 `Timestamp`/`NameEn`/`Ipv4`/区间整数的模板。
    /// 预期：首行含分隔符 ` | `。
    #[test]
    fn render_with_slots_smoke() {
        let mut r = TemplateRunner::try_new(
            "{{ts}} | {{name}} | {{ip}} | {{n}}",
            [
                (
                    "ts".to_string(),
                    FieldSpec::Timestamp {
                        format: "%Y".to_string(),
                    },
                ),
                ("name".to_string(), FieldSpec::NameEn),
                ("ip".to_string(), FieldSpec::Ipv4),
                ("n".to_string(), FieldSpec::Integer { min: 1, max: 3 }),
            ]
            .into_iter()
            .collect(),
        )
        .unwrap();
        let line = r.next_line().unwrap();
        assert!(line.contains(" | "));
    }

    /// 测试内容：`Counter` 字段从 0 起每行自增。
    /// 输入：模板 `n={{n}}`，字段 `n` 为 `counter`。
    /// 预期：连续三行为 `n=0`、`n=1`、`n=2`。
    #[test]
    fn counter_starts_at_zero_and_increments() {
        let mut r = TemplateRunner::try_new(
            "n={{n}}",
            [("n".to_string(), FieldSpec::Counter)].into_iter().collect(),
        )
        .unwrap();
        assert_eq!(r.next_line().unwrap(), "n=0");
        assert_eq!(r.next_line().unwrap(), "n=1");
        assert_eq!(r.next_line().unwrap(), "n=2");
    }

    /// 测试内容：`Hostname` 字段生成类 FQDN 形态（含点与连字符）。
    /// 输入：模板 `{{h}}`，字段 `Hostname`。
    /// 预期：渲染行同时包含 `.` 与 `-`。
    #[test]
    fn hostname_slot_contains_two_labels_and_suffix() {
        let mut r = TemplateRunner::try_new(
            "{{h}}",
            [("h".to_string(), FieldSpec::Hostname)].into_iter().collect(),
        )
        .unwrap();
        let line = r.next_line().unwrap();
        assert!(line.contains('.'), "{line:?}");
        assert!(line.contains('-'), "{line:?}");
    }

    /// 测试内容：嵌套 `template` 字段类型与子字段组合渲染。
    /// 输入：内存构造两层 `FieldSpec::Template`。
    /// 预期：行以 `[id iut="3" src="` 开头、以 `"]` 结尾，且 `src` 值内含 `.`。
    #[test]
    fn field_type_template_nested_renders_sd_shape() {
        let mut fields = BTreeMap::new();
        fields.insert(
            "sd".into(),
            FieldSpec::Template {
                template: r#"[id iut="{{iut}}" src="{{src}}"]"#.into(),
                fields: BTreeMap::from([
                    ("iut".into(), FieldSpec::Integer { min: 3, max: 3 }),
                    (
                        "src".into(),
                        FieldSpec::Template {
                            template: "{{a}}.{{b}}".into(),
                            fields: BTreeMap::from([
                                ("a".into(), FieldSpec::LoremWord),
                                ("b".into(), FieldSpec::LoremWord),
                            ]),
                        },
                    ),
                ]),
            },
        );
        let mut r = TemplateRunner::try_new("{{sd}}", fields).unwrap();
        let line = r.next_line().unwrap();
        assert!(
            line.starts_with("[id iut=\"3\" src=\"") && line.ends_with("\"]"),
            "{line:?}"
        );
        assert!(line.contains('.'), "{line:?}");
    }

    /// 测试内容：`Template` 字段可无子字段映射（空 `fields`）。
    /// 输入：内存构造 `FieldSpec::Template` 固定子模板 `fixed`。
    /// 预期：`TemplateRunner::try_new` 成功；首行为 `fixed`。
    #[test]
    fn field_type_template_empty_subfields_ok() {
        let mut r = TemplateRunner::try_new(
            "{{x}}",
            [(
                "x".to_string(),
                FieldSpec::Template {
                    template: "fixed".to_string(),
                    fields: BTreeMap::new(),
                },
            )]
            .into_iter()
            .collect(),
        )
        .unwrap();
        assert_eq!(r.next_line().unwrap(), "fixed");
    }

    /// 测试内容：`one-of` 分支中 counter 仅在选中含 `{{c}}` 的分支时递增。
    /// 输入：`branches: [Literal("-"), Template+counter]`，循环 800 行。
    /// 预期：非 `-` 行数字严格等于递增计数；至少出现约百次以上模板分支。
    #[test]
    fn field_type_one_of_lazy_counter_only_on_template_branch() {
        let fields = BTreeMap::from([(
            "x".to_string(),
            FieldSpec::OneOf {
                branches: vec![
                    OneOfBranch::Literal("-".into()),
                    OneOfBranch::Template(OneOfTemplateBranch {
                        w: 1,
                        template: "{{c}}".into(),
                        fields: BTreeMap::from([("c".into(), FieldSpec::Counter)]),
                    }),
                ],
            },
        )]);
        let mut r = TemplateRunner::try_new("{{x}}", fields).unwrap();
        let mut next_expected: u64 = 0;
        for _ in 0..800 {
            let line = r.next_line().unwrap();
            if line == "-" {
                continue;
            }
            let n: u64 = line.parse().expect("non-dash must be counter digits");
            assert_eq!(
                n, next_expected,
                "counter must only advance when template branch is picked"
            );
            next_expected = next_expected.wrapping_add(1);
        }
        assert!(
            next_expected >= 100,
            "expected many template-branch picks in 800 trials"
        );
    }

    /// 测试内容：`one-of` 分支列表为空时配置非法。
    /// 输入：内存构造 `OneOf { branches: vec![] }`。
    /// 预期：`TemplateRunner::try_new` 返回 `Err`。
    #[test]
    fn field_type_one_of_empty_branches_rejected() {
        assert!(TemplateRunner::try_new(
            "{{x}}",
            [("x".to_string(), FieldSpec::OneOf { branches: vec![] })]
                .into_iter()
                .collect(),
        )
        .is_err());
    }

    /// 测试内容：`Sentence` 字段词数落在 `[min,max]` 闭区间。
    /// 输入：`min: 2, max: 4`，抽样 20 行。
    /// 预期：每行按空白分词后词数在 2～4 之间。
    #[test]
    fn sentence_word_count_in_range() {
        let mut r = TemplateRunner::try_new(
            "{{s}}",
            [("s".to_string(), FieldSpec::Sentence { min: 2, max: 4 })]
                .into_iter()
                .collect(),
        )
        .unwrap();
        for _ in 0..20 {
            let line = r.next_line().unwrap();
            let n = line.split_whitespace().count();
            assert!((2..=4).contains(&n), "{line:?}");
        }
    }

    /// 测试内容：`Float` 字段在固定区间时输出可解析浮点。
    /// 输入：`min=max=1.0`。
    /// 预期：渲染值为 1.0。
    #[test]
    fn field_type_float_fixed_range() {
        let mut r = TemplateRunner::try_new(
            "{{f}}",
            [("f".to_string(), FieldSpec::Float { min: 1.0, max: 1.0 })]
                .into_iter()
                .collect(),
        )
        .unwrap();
        let line = r.next_line().unwrap();
        let v: f64 = line.parse().expect("float");
        assert!((v - 1.0).abs() < 1e-9, "{line:?}");
    }
}
