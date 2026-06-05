use std::hint::black_box;

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use logen_dsl::{KafkaAgentConfig, KafkaConfig, KafkaSinkMode};
use logen_worker::{build_agent_message, build_runtime_agent_config, RuntimeAgentConfig};

struct PayloadCase {
    name: &'static str,
    raw_message: String,
}

fn runtime_agent_config() -> RuntimeAgentConfig {
    let kafka = KafkaConfig {
        mode: KafkaSinkMode::Agent,
        agent: Some(KafkaAgentConfig {
            domain: Some("dom1".to_string()),
            domain_token: Some("dom-token-123456".to_string()),
            appname: Some("apache_middleware".to_string()),
            source: Some("middleware".to_string()),
            token: Some("token-1234567890".to_string()),
            tag: Some("root_60".to_string()),
            hostname: Some("bench-host-01".to_string()),
            ip: Some("192.168.1.60".to_string()),
            source_id: Some("3d4cc8d3-4acf-4eb2-9b8b-f24da54be340".to_string()),
            flag: Some(0),
            fields: Some(r#"{"cluster":"bench","role":"middleware"}"#.to_string()),
            ..Default::default()
        }),
        brokers: Some(vec!["127.0.0.1:9092".to_string()]),
        ..Default::default()
    };
    build_runtime_agent_config(&kafka).unwrap()
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
    let runtime_config = runtime_agent_config();
    let cases = payload_cases();
    let mut group = c.benchmark_group("build_agent_message");
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

criterion_group!(benches, bench_build_agent_message);
criterion_main!(benches);
