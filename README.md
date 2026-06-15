# logkit

[![Clippy](https://github.com/www159-used/logkit/actions/workflows/clippy.yml/badge.svg)](https://github.com/www159-used/logkit/actions/workflows/clippy.yml)
[![Test](https://github.com/www159-used/logkit/actions/workflows/test.yml/badge.svg)](https://github.com/www159-used/logkit/actions/workflows/test.yml)
[![Coverage](https://github.com/www159-used/logkit/actions/workflows/coverage.yml/badge.svg)](https://github.com/www159-used/logkit/actions/workflows/coverage.yml)
[![Docs](https://github.com/www159-used/logkit/actions/workflows/docs.yml/badge.svg)](https://www159-used.github.io/logkit/)

许可：[AGPL-3.0](LICENSE)

## 文档

**<https://www159-used.github.io/logkit/>** — mdbook（logen-dsl、CLI 等）。本地：`cd guide && mdbook build`。

## 编译

**依赖**（需 [protoc](https://grpc.io/docs/protoc-installation/)；编 Kafka sink 时还需 **CMake**）：

```bash
cargo build --release
# target/release/logen、logend，以及 tools/* 二进制
```

**Linux 发行包**（glibc 2.17，与 Release CI 一致；含 `pullout` dlopen 等）：

打包脚本依赖 **Zig**、`cargo-zigbuild`

```bash
./scripts/logkit-pack.sh              # x86_64 / aarch64 → dist/*.tar.gz
./scripts/logkit-pack.sh native       # 本机 target/release → dist/logkit/
```

## Benchmark

当前仓库提供两组可长期维护的 `cargo bench` 微基准，用于量化热点链路的单次成本与吞吐：

- `logen-dsl`：模板初始化与单条渲染
- `logen-worker`：agent 模式下 `raw_message -> payload` 的 JSON 或 PB（`sink.kafka.agent.format`）打包

运行方式：

```bash
cargo bench -p logen-dsl --bench template_runner
cargo bench -p logen-worker --bench agent_message
```

结果解读建议：

- `template_runner_init/*`：看模板与字段 fixture 的初始化成本，适合比较编译期/预热期变化。
- `template_runner_next_line/*`：看单条渲染吞吐，适合比较 `handlebars`、slot 组合与字段生成开销。
- `build_agent_message/*`：看不同 `raw_message` 大小下的 agent JSON/PB 打包成本。

输出位于 `target/criterion/`。做提交间对比时，建议保持同一台机器、同一编译 profile、同一组 fixture，只比较同名 benchmark 的变化。
