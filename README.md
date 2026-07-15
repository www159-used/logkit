# logkit

[![Clippy](https://github.com/www159-used/logkit/actions/workflows/clippy.yml/badge.svg)](https://github.com/www159-used/logkit/actions/workflows/clippy.yml)
[![Test](https://github.com/www159-used/logkit/actions/workflows/test.yml/badge.svg)](https://github.com/www159-used/logkit/actions/workflows/test.yml)
[![Coverage](https://github.com/www159-used/logkit/actions/workflows/coverage.yml/badge.svg)](https://github.com/www159-used/logkit/actions/workflows/coverage.yml)
[![Docs](https://github.com/www159-used/logkit/actions/workflows/docs.yml/badge.svg)](https://www159-used.github.io/logkit/)

许可：[AGPL-3.0](LICENSE)

使用方法和介绍查看：[logkit book](https://www159-used.github.io/logkit/)

## 编译

```bash
cargo build --release
```

link依赖 **Zig**、`cargo-zigbuild`

```bash
./scripts/logkit-pack.sh              # x86_64 / aarch64 → dist/*.tar.gz
./scripts/logkit-pack.sh native       # 本机 target/release → dist/logkit/
```

## Benchmark

```bash
cargo bench -p logen-model --bench template_runner
cargo bench -p logen-worker --bench agent_message
```