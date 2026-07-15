//! [`body_preset!`]：`"a: {}", field_v` → 模板 `a: {{id}}`，fields `id → field_v`。

use heck::ToPascalCase;
use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::{format_ident, quote};
use syn::parse::{Parse, ParseStream};
use syn::punctuated::Punctuated;
use syn::{braced, bracketed, parenthesized, parse_macro_input, Expr, LitInt, LitStr, Result, Token};

/// 无参 [`logen_model::FieldSpec`] 变体对应的 snake_case 名（`uuid_v4` → `UuidV4`）。
const UNIT_FIELDS: &[&str] = &[
    "uuid_v4",
    "name_en",
    "ipv4",
    "url",
    "url_path",
    "hostname",
    "domain_suffix",
    "lorem_word",
    "company_name",
    "user_agent",
    "username",
    "counter",
];

/// 构造 [`logen_model::BodyConfig`]。
///
/// ```ignore
/// body_preset!(
///     r#"{"t":"{}","n":{}}"#,
///     timestamp("%Y"),
///     counter,
/// )
/// ```
///
/// 每个空 `{}` 按序换成 `{{_bpN}}`，并与后继 field 一一对应写入 map。
/// `{{` / `}}` 按 `format!` 规则转义为字面 `{` / `}`。
#[proc_macro]
pub fn body_preset(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as BodyPresetInput);
    match expand_body_preset(input) {
        Ok(ts) => ts.into(),
        Err(e) => e.to_compile_error().into(),
    }
}

struct BodyPresetInput {
    template: LitStr,
    fields: Vec<FieldAst>,
}

impl Parse for BodyPresetInput {
    fn parse(input: ParseStream<'_>) -> Result<Self> {
        let template: LitStr = input.parse()?;
        let mut fields = Vec::new();
        while !input.is_empty() {
            input.parse::<Token![,]>()?;
            if input.is_empty() {
                break;
            }
            fields.push(input.parse()?);
        }
        Ok(Self { template, fields })
    }
}

enum FieldAst {
    Unit(syn::Ident),
    Timestamp(Option<LitStr>),
    Integer { min: LitInt, max: LitInt },
    Float { min: Expr, max: Expr },
    Sentence { min: LitInt, max: LitInt },
    OneOfLiterals(Vec<LitStr>),
    OneOfArms(Vec<OneOfArmAst>),
    Template {
        template: LitStr,
        fields: Vec<FieldAst>,
    },
}

enum OneOfArmAst {
    /// `w => "lit"`
    WeightedLit { w: LitInt, v: LitStr },
    /// `w => template("{}", …)`
    WeightedTemplate {
        w: LitInt,
        template: LitStr,
        fields: Vec<FieldAst>,
    },
}

impl Parse for FieldAst {
    fn parse(input: ParseStream<'_>) -> Result<Self> {
        let name: syn::Ident = input.parse()?;
        let key = name.to_string();
        if input.peek(syn::token::Paren) {
            let content;
            parenthesized!(content in input);
            return parse_field_call(&key, &name, &content);
        }
        if key == "one_of" {
            if input.peek(syn::token::Bracket) {
                let content;
                bracketed!(content in input);
                let lits = Punctuated::<LitStr, Token![,]>::parse_terminated(&content)?;
                return Ok(FieldAst::OneOfLiterals(lits.into_iter().collect()));
            }
            if input.peek(syn::token::Brace) {
                let content;
                braced!(content in input);
                let mut arms = Vec::new();
                while !content.is_empty() {
                    let w: LitInt = content.parse()?;
                    content.parse::<Token![=>]>()?;
                    arms.push(parse_one_of_arm_rhs(w, &content)?);
                    if content.peek(Token![,]) {
                        content.parse::<Token![,]>()?;
                    }
                }
                return Ok(FieldAst::OneOfArms(arms));
            }
            return Err(syn::Error::new(
                name.span(),
                "one_of: expected `[\"a\", …]` or `{ w => \"v\" | template(…), … }`",
            ));
        }
        if key == "timestamp" {
            return Ok(FieldAst::Timestamp(None));
        }
        if UNIT_FIELDS.contains(&key.as_str()) {
            return Ok(FieldAst::Unit(name));
        }
        Err(syn::Error::new(
            name.span(),
            format!("unknown field type `{key}`"),
        ))
    }
}

fn parse_one_of_arm_rhs(w: LitInt, content: ParseStream<'_>) -> Result<OneOfArmAst> {
    if content.peek(syn::Ident) {
        let fork = content.fork();
        let ident: syn::Ident = fork.parse()?;
        if ident == "template" && fork.peek(syn::token::Paren) {
            let _ = content.parse::<syn::Ident>()?;
            let inner;
            parenthesized!(inner in content);
            let FieldAst::Template { template, fields } = parse_template_call(&inner)? else {
                unreachable!("parse_template_call returns Template");
            };
            return Ok(OneOfArmAst::WeightedTemplate { w, template, fields });
        }
    }
    let v: LitStr = content.parse()?;
    Ok(OneOfArmAst::WeightedLit { w, v })
}

fn parse_template_call(content: ParseStream<'_>) -> Result<FieldAst> {
    let template: LitStr = content.parse()?;
    let mut fields = Vec::new();
    while !content.is_empty() {
        content.parse::<Token![,]>()?;
        if content.is_empty() {
            break;
        }
        fields.push(content.parse()?);
    }
    Ok(FieldAst::Template { template, fields })
}

fn parse_field_call(key: &str, name: &syn::Ident, content: ParseStream<'_>) -> Result<FieldAst> {
    match key {
        "timestamp" => {
            if content.is_empty() {
                Ok(FieldAst::Timestamp(None))
            } else {
                let fmt: LitStr = content.parse()?;
                Ok(FieldAst::Timestamp(Some(fmt)))
            }
        }
        "integer" => {
            let min: LitInt = content.parse()?;
            content.parse::<Token![,]>()?;
            let max: LitInt = content.parse()?;
            Ok(FieldAst::Integer { min, max })
        }
        "float" => {
            let min: Expr = content.parse()?;
            content.parse::<Token![,]>()?;
            let max: Expr = content.parse()?;
            Ok(FieldAst::Float { min, max })
        }
        "sentence" => {
            let min: LitInt = content.parse()?;
            content.parse::<Token![,]>()?;
            let max: LitInt = content.parse()?;
            Ok(FieldAst::Sentence { min, max })
        }
        "template" => parse_template_call(content),
        "one_of" => Err(syn::Error::new(
            name.span(),
            "one_of: use `one_of […]` or `one_of { … }`, not parentheses",
        )),
        _ => Err(syn::Error::new(
            name.span(),
            format!("`{key}(…)` is not a known field constructor"),
        )),
    }
}

/// 将模板改写为 Handlebars，并返回占位个数。
///
/// - `{}` → `{{_bpN}}`（槽）
/// - `{{` / `}}` → 字面 `{` / `}`（转义，同 `format!`）
/// - 其余单独的 `{` / `}` 原样保留（便于 JSON 等）
fn rewrite_template(src: &str) -> syn::Result<(String, usize)> {
    let bytes = src.as_bytes();
    let mut out = String::with_capacity(src.len() + 8);
    let mut i = 0;
    let mut n = 0usize;
    while i < bytes.len() {
        if i + 1 < bytes.len() && bytes[i] == b'{' && bytes[i + 1] == b'{' {
            out.push('{');
            i += 2;
            continue;
        }
        if i + 1 < bytes.len() && bytes[i] == b'}' && bytes[i + 1] == b'}' {
            out.push('}');
            i += 2;
            continue;
        }
        if i + 1 < bytes.len() && bytes[i] == b'{' && bytes[i + 1] == b'}' {
            let id = format!("_bp{n}");
            out.push_str("{{");
            out.push_str(&id);
            out.push_str("}}");
            n += 1;
            i += 2;
            continue;
        }
        out.push(bytes[i] as char);
        i += 1;
    }
    Ok((out, n))
}

fn expand_body_preset(input: BodyPresetInput) -> syn::Result<TokenStream2> {
    let (rewritten, nslots) = rewrite_template(&input.template.value())?;
    if nslots != input.fields.len() {
        return Err(syn::Error::new(
            input.template.span(),
            format!(
                "body_preset!: template has {nslots} `{{}}` slot(s) but {} field(s) were provided",
                input.fields.len()
            ),
        ));
    }

    let mut inserts = Vec::with_capacity(input.fields.len());
    for (i, field) in input.fields.iter().enumerate() {
        let id = format!("_bp{i}");
        let spec = field_to_tokens(field)?;
        inserts.push(quote! {
            __fields.insert(::std::string::String::from(#id), #spec);
        });
    }

    Ok(quote! {
        {
            let mut __fields = ::std::collections::BTreeMap::new();
            #(#inserts)*
            ::logen_model::BodyConfig {
                template: ::std::string::String::from(#rewritten),
                fields: __fields,
            }
        }
    })
}

fn field_to_tokens(field: &FieldAst) -> syn::Result<TokenStream2> {
    match field {
        FieldAst::Unit(name) => {
            let variant = unit_variant(name)?;
            Ok(quote! { ::logen_model::FieldSpec::#variant })
        }
        FieldAst::Timestamp(None) => Ok(quote! {
            ::logen_model::FieldSpec::Timestamp {
                format: ::std::string::String::from("%Y-%m-%d %H:%M:%S"),
            }
        }),
        FieldAst::Timestamp(Some(fmt)) => Ok(quote! {
            ::logen_model::FieldSpec::Timestamp {
                format: ::std::string::String::from(#fmt),
            }
        }),
        FieldAst::Integer { min, max } => Ok(quote! {
            ::logen_model::FieldSpec::Integer {
                min: #min,
                max: #max,
            }
        }),
        FieldAst::Float { min, max } => Ok(quote! {
            ::logen_model::FieldSpec::Float {
                min: #min,
                max: #max,
            }
        }),
        FieldAst::Sentence { min, max } => Ok(quote! {
            ::logen_model::FieldSpec::Sentence {
                min: #min,
                max: #max,
            }
        }),
        FieldAst::OneOfLiterals(lits) => {
            let branches = lits.iter().map(|v| {
                quote! {
                    ::logen_model::OneOfBranch::Literal(::std::string::String::from(#v))
                }
            });
            Ok(quote! {
                ::logen_model::FieldSpec::OneOf {
                    branches: ::std::vec![#(#branches),*],
                }
            })
        }
        FieldAst::OneOfArms(arms) => {
            let mut branches = Vec::with_capacity(arms.len());
            for arm in arms {
                branches.push(one_of_arm_to_tokens(arm)?);
            }
            Ok(quote! {
                ::logen_model::FieldSpec::OneOf {
                    branches: ::std::vec![#(#branches),*],
                }
            })
        }
        FieldAst::Template {
            template,
            fields: nested,
        } => template_field_to_tokens(template, nested),
    }
}

fn one_of_arm_to_tokens(arm: &OneOfArmAst) -> syn::Result<TokenStream2> {
    match arm {
        OneOfArmAst::WeightedLit { w, v } => Ok(quote! {
            ::logen_model::OneOfBranch::WeightedLiteral {
                w: #w,
                v: ::std::string::String::from(#v),
            }
        }),
        OneOfArmAst::WeightedTemplate {
            w,
            template,
            fields,
        } => {
            let (rewritten, nslots) = rewrite_template(&template.value())?;
            if nslots != fields.len() {
                return Err(syn::Error::new(
                    template.span(),
                    format!(
                        "one_of template: has {nslots} `{{}}` slot(s) but {} field(s)",
                        fields.len()
                    ),
                ));
            }
            let mut inserts = Vec::new();
            for (i, f) in fields.iter().enumerate() {
                let id = format!("_bp{i}");
                let spec = field_to_tokens(f)?;
                inserts.push(quote! {
                    __nested.insert(::std::string::String::from(#id), #spec);
                });
            }
            Ok(quote! {
                ::logen_model::OneOfBranch::Template(::logen_model::OneOfTemplateBranch {
                    w: #w,
                    template: ::std::string::String::from(#rewritten),
                    fields: {
                        let mut __nested = ::std::collections::BTreeMap::new();
                        #(#inserts)*
                        __nested
                    },
                })
            })
        }
    }
}

fn template_field_to_tokens(template: &LitStr, nested: &[FieldAst]) -> syn::Result<TokenStream2> {
    let (rewritten, nslots) = rewrite_template(&template.value())?;
    if nslots != nested.len() {
        return Err(syn::Error::new(
            template.span(),
            format!(
                "template(…): has {nslots} `{{}}` slot(s) but {} field(s)",
                nested.len()
            ),
        ));
    }
    let mut inserts = Vec::new();
    for (i, f) in nested.iter().enumerate() {
        let id = format!("_bp{i}");
        let spec = field_to_tokens(f)?;
        inserts.push(quote! {
            __nested.insert(::std::string::String::from(#id), #spec);
        });
    }
    Ok(quote! {
        {
            let mut __nested = ::std::collections::BTreeMap::new();
            #(#inserts)*
            ::logen_model::FieldSpec::Template {
                template: ::std::string::String::from(#rewritten),
                fields: __nested,
            }
        }
    })
}

fn unit_variant(name: &syn::Ident) -> syn::Result<syn::Ident> {
    let key = name.to_string();
    if !UNIT_FIELDS.contains(&key.as_str()) {
        return Err(syn::Error::new(
            name.span(),
            format!("unknown field type `{key}`"),
        ));
    }
    Ok(format_ident!("{}", key.to_pascal_case()))
}
