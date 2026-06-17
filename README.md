# logkit

[![Clippy](https://codeberg.org/www159/logkit/badges/workflows/clippy.yaml/badge.svg)](https://codeberg.org/www159/logkit/actions)
[![Test](https://codeberg.org/www159/logkit/badges/workflows/test.yaml/badge.svg)](https://codeberg.org/www159/logkit/actions)
[![Coverage](https://codeberg.org/www159/logkit/badges/workflows/coverage.yaml/badge.svg)](https://codeberg.org/www159/logkit/actions)
[![Docs](https://codeberg.org/www159/logkit/badges/workflows/docs.yaml/badge.svg)](https://www159.codeberg.page/logkit/)

许可：[AGPL-3.0](LICENSE)

使用方法和介绍查看：[logkit book](https://www159.codeberg.page/logkit/)

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
cargo bench -p logen-dsl --bench template_runner
cargo bench -p logen-worker --bench agent_message
```