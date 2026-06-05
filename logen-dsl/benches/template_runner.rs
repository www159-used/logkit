use std::collections::BTreeMap;
use std::hint::black_box;

use criterion::{criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion, Throughput};
use logen_dsl::{FieldSpec, OneOfBranch, OneOfTemplateBranch, TemplateRunner};

struct TemplateCase {
    name: &'static str,
    template: &'static str,
    fields: BTreeMap<String, FieldSpec>,
}

fn field_map(entries: Vec<(&str, FieldSpec)>) -> BTreeMap<String, FieldSpec> {
    entries
        .into_iter()
        .map(|(key, spec)| (key.to_string(), spec))
        .collect()
}

fn template_cases() -> Vec<TemplateCase> {
    vec![
        TemplateCase {
            name: "simple_scalars",
            template: "{{ts}} level={{level}} user={{user}} status={{status}} n={{n}}",
            fields: field_map(vec![
                (
                    "ts",
                    FieldSpec::Timestamp {
                        format: "%Y-%m-%dT%H:%M:%S%.3f%:z".to_string(),
                    },
                ),
                (
                    "level",
                    FieldSpec::OneOf {
                        branches: vec![
                            OneOfBranch::Literal("INFO".to_string()),
                            OneOfBranch::Literal("WARN".to_string()),
                            OneOfBranch::Literal("ERROR".to_string()),
                        ],
                    },
                ),
                ("user", FieldSpec::Username),
                ("status", FieldSpec::Integer { min: 200, max: 599 }),
                ("n", FieldSpec::Counter),
            ]),
        },
        TemplateCase {
            name: "agent_style_nested",
            template: "{{ts}} {{host}} {{request}} trace={{trace}} {{detail}}",
            fields: field_map(vec![
                (
                    "ts",
                    FieldSpec::Timestamp {
                        format: "%d/%b/%Y:%H:%M:%S %z".to_string(),
                    },
                ),
                ("host", FieldSpec::Hostname),
                (
                    "request",
                    FieldSpec::Template {
                        template: "{{method}} {{path}} HTTP/1.1".to_string(),
                        fields: field_map(vec![
                            (
                                "method",
                                FieldSpec::OneOf {
                                    branches: vec![
                                        OneOfBranch::WeightedLiteral {
                                            w: 5,
                                            v: "GET".to_string(),
                                        },
                                        OneOfBranch::WeightedLiteral {
                                            w: 3,
                                            v: "POST".to_string(),
                                        },
                                        OneOfBranch::WeightedLiteral {
                                            w: 1,
                                            v: "DELETE".to_string(),
                                        },
                                    ],
                                },
                            ),
                            ("path", FieldSpec::UrlPath),
                        ]),
                    },
                ),
                ("trace", FieldSpec::UuidV4),
                (
                    "detail",
                    FieldSpec::OneOf {
                        branches: vec![
                            OneOfBranch::Literal("upstream=cache".to_string()),
                            OneOfBranch::Template(OneOfTemplateBranch {
                                w: 2,
                                template: "upstream={{upstream}} cost={{cost}}ms".to_string(),
                                fields: field_map(vec![
                                    ("upstream", FieldSpec::Hostname),
                                    ("cost", FieldSpec::Integer { min: 1, max: 1200 }),
                                ]),
                            }),
                        ],
                    },
                ),
            ]),
        },
        TemplateCase {
            name: "heavy_nested_jsonish",
            template: "{{ts}} {{host}} app={{app}} env={{env}} user={{user}} trace={{trace}} req={{request}} ctx={{ctx}} body={{body}}",
            fields: field_map(vec![
                (
                    "ts",
                    FieldSpec::Timestamp {
                        format: "%Y-%m-%dT%H:%M:%S%.6f%:z".to_string(),
                    },
                ),
                ("host", FieldSpec::Hostname),
                (
                    "app",
                    FieldSpec::OneOf {
                        branches: vec![
                            OneOfBranch::Literal("gateway".to_string()),
                            OneOfBranch::Literal("middleware".to_string()),
                            OneOfBranch::Literal("ingest".to_string()),
                        ],
                    },
                ),
                (
                    "env",
                    FieldSpec::OneOf {
                        branches: vec![
                            OneOfBranch::Literal("prod".to_string()),
                            OneOfBranch::Literal("staging".to_string()),
                        ],
                    },
                ),
                ("user", FieldSpec::Username),
                ("trace", FieldSpec::UuidV4),
                (
                    "request",
                    FieldSpec::Template {
                        template: "{{method}} {{path}} status={{status}} size={{size}}".to_string(),
                        fields: field_map(vec![
                            (
                                "method",
                                FieldSpec::OneOf {
                                    branches: vec![
                                        OneOfBranch::Literal("GET".to_string()),
                                        OneOfBranch::Literal("POST".to_string()),
                                        OneOfBranch::Literal("PUT".to_string()),
                                        OneOfBranch::Literal("DELETE".to_string()),
                                    ],
                                },
                            ),
                            ("path", FieldSpec::UrlPath),
                            ("status", FieldSpec::Integer { min: 200, max: 599 }),
                            ("size", FieldSpec::Integer { min: 128, max: 65535 }),
                        ]),
                    },
                ),
                (
                    "ctx",
                    FieldSpec::Template {
                        template: "[user={{user_id}} region={{region}} shard={{shard}}]".to_string(),
                        fields: field_map(vec![
                            ("user_id", FieldSpec::Counter),
                            (
                                "region",
                                FieldSpec::OneOf {
                                    branches: vec![
                                        OneOfBranch::Literal("cn-north-1".to_string()),
                                        OneOfBranch::Literal("cn-east-1".to_string()),
                                        OneOfBranch::Literal("eu-west-1".to_string()),
                                    ],
                                },
                            ),
                            ("shard", FieldSpec::Integer { min: 0, max: 63 }),
                        ]),
                    },
                ),
                (
                    "body",
                    FieldSpec::OneOf {
                        branches: vec![
                            OneOfBranch::Template(OneOfTemplateBranch {
                                w: 3,
                                template:
                                    "{\"op\":\"{{op}}\",\"elapsed\":{{elapsed}},\"peer\":\"{{peer}}\"}"
                                        .to_string(),
                                fields: field_map(vec![
                                    (
                                        "op",
                                        FieldSpec::OneOf {
                                            branches: vec![
                                                OneOfBranch::Literal("lookup".to_string()),
                                                OneOfBranch::Literal("insert".to_string()),
                                                OneOfBranch::Literal("delete".to_string()),
                                            ],
                                        },
                                    ),
                                    ("elapsed", FieldSpec::Integer { min: 1, max: 3000 }),
                                    ("peer", FieldSpec::Hostname),
                                ]),
                            }),
                            OneOfBranch::Template(OneOfTemplateBranch {
                                w: 1,
                                template:
                                    "{\"error\":\"{{error}}\",\"retry\":{{retry}},\"ua\":\"{{ua}}\"}"
                                        .to_string(),
                                fields: field_map(vec![
                                    (
                                        "error",
                                        FieldSpec::OneOf {
                                            branches: vec![
                                                OneOfBranch::Literal("timeout".to_string()),
                                                OneOfBranch::Literal("queue_full".to_string()),
                                                OneOfBranch::Literal("backend_5xx".to_string()),
                                            ],
                                        },
                                    ),
                                    ("retry", FieldSpec::Integer { min: 0, max: 8 }),
                                    ("ua", FieldSpec::UserAgent),
                                ]),
                            }),
                        ],
                    },
                ),
            ]),
        },
    ]
}

fn bench_template_runner_init(c: &mut Criterion) {
    let cases = template_cases();
    let mut group = c.benchmark_group("template_runner_init");
    for case in &cases {
        group.bench_with_input(BenchmarkId::from_parameter(case.name), case, |b, case| {
            b.iter_batched(
                || case.fields.clone(),
                |fields| {
                    let runner = TemplateRunner::try_new(black_box(case.template), fields).unwrap();
                    black_box(runner);
                },
                BatchSize::SmallInput,
            );
        });
    }
    group.finish();
}

fn bench_template_runner_next_line(c: &mut Criterion) {
    let cases = template_cases();
    let mut group = c.benchmark_group("template_runner_next_line");
    group.throughput(Throughput::Elements(1));
    for case in &cases {
        let mut runner = TemplateRunner::try_new(case.template, case.fields.clone()).unwrap();
        group.bench_with_input(BenchmarkId::from_parameter(case.name), case, |b, _case| {
            b.iter(|| black_box(runner.next_line().unwrap()));
        });
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_template_runner_init,
    bench_template_runner_next_line
);
criterion_main!(benches);
