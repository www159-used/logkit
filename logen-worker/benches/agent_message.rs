use std::hint::black_box;

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use logen_model::KafkaAgentFormat;
use logen_worker::{agent_fixtures, build_agent_message};

struct PayloadCase {
    name: &'static str,
    raw_message: String,
}

fn payload_cases() -> Vec<PayloadCase> {
    let medium = r#"{"remote_addr":"10.0.0.7","method":"GET","path":"/api/v1/orders?id=123&expand=items","status":200,"elapsed_ms":37,"trace_id":"7c25d0f5-85cb-4c47-8244-7e6de7e236f8","user":"alice","upstream":"cache-03"}"#;
    let large = format!(
        "{{\"remote_addr\":\"10.0.0.7\",\"method\":\"POST\",\"path\":\"/api/v1/submit\",\"status\":502,\"elapsed_ms\":1837,\"trace_id\":\"7c25d0f5-85cb-4c47-8244-7e6de7e236f8\",\"body\":\"{}\"}}",
        "x".repeat(4096)
    );
    vec![
        PayloadCase {
            name: "tiny_json",
            raw_message: "{}".to_string(),
        },
        PayloadCase {
            name: "medium_access_log",
            raw_message: medium.to_string(),
        },
        PayloadCase {
            name: "large_json_body",
            raw_message: large,
        },
    ]
}

fn bench_build_agent_message(c: &mut Criterion) {
    let cases = payload_cases();
    for (label, format) in [
        ("json", KafkaAgentFormat::Json),
        ("pb", KafkaAgentFormat::Pb),
    ] {
        let runtime_config =
            agent_fixtures::agent_runtime_config(agent_fixtures::BENCH_YAML, format)
                .expect("bench agent fixture");
        let mut group = c.benchmark_group(format!("build_agent_message_{label}"));
        for case in &cases {
            group.throughput(Throughput::Bytes(case.raw_message.len() as u64));
            group.bench_with_input(BenchmarkId::from_parameter(case.name), case, |b, case| {
                b.iter(|| {
                    black_box(build_agent_message(
                        &runtime_config,
                        black_box(case.raw_message.as_str()),
                        black_box(123_i64),
                        black_box(1_700_000_000_000_i64),
                    ))
                });
            });
        }
        group.finish();
    }
}

criterion_group!(benches, bench_build_agent_message);
criterion_main!(benches);
