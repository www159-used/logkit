# logkit

[![Clippy](https://github.com/951mmm/logkit/actions/workflows/clippy.yml/badge.svg)](https://github.com/951mmm/logkit/actions/workflows/clippy.yml)
[![Test](https://github.com/951mmm/logkit/actions/workflows/test.yml/badge.svg)](https://github.com/951mmm/logkit/actions/workflows/test.yml)
[![Coverage](https://github.com/951mmm/logkit/actions/workflows/coverage.yml/badge.svg)](https://github.com/951mmm/logkit/actions/workflows/coverage.yml)

许可：[GPL-3.0](LICENSE)

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
