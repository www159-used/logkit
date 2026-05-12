# logkit

Rust 实现的日志造数 / 灌流工具链（CS 架构）：**Unix 套接字上的 gRPC 控制面** + 进程内 **`logspout-worker`** 执行 producer。起因包含 ARM 产物、交叉编译与避免裸露 TCP 端口等考量。

许可：**GPL-3.0**（见仓库根目录 [`LICENSE`](LICENSE)）。

## 各 crate 说明（文档入口）

| Crate | 说明 |
|--------|------|
| [`logspout`](logspout/README.md) | CLI：`ping` / `start` / `list` / `stop` / `stat` / `cat` |
| [`logspout-daemon`](logspout-daemon/README.md) | 守护进程：监听 UDS，调度造日志任务 |
| [`logspout-worker`](logspout-worker/README.md) | 造日志库与独立二进制 `logspout-worker` |
| [`logspout-dsl`](logspout-dsl/README.md) | Producer YAML：`template` / `fields` / `sink` |
| [`logspout-config`](logspout-config/README.md) | 共用 TOML：合并默认与 `--defaults-file` |
| [`logspout-proto`](logspout-proto/README.md) | `protobuf` + `tonic` 生成代码 |

示例 YAML 在 **[`etc/`](etc/)**（Apache、RFC 5424、LEEF 等）。

## 构建

```bash
cargo build --release
```

产物：`target/release/logspout`、`logspout-daemon`、`logspout-worker`。

### Linux musl（交叉编译，推荐 Zig）

含 **Kafka（rdkafka：vendored OpenSSL + static libcurl，供 bundled librdkafka 编译）** 时，目标三需要可用的 **C 工具链**；在 **macOS** 或未安装 **`x86_64-linux-musl-gcc`** 一类 musl 交叉 GCC 的环境下，请用 **Zig + cargo-zigbuild**（与 Release CI、打包脚本一致）：

1. 安装 [Zig](https://ziglang.org/download/) 并加入 `PATH`。
2. `cargo install cargo-zigbuild`
3. `rustup target add x86_64-unknown-linux-musl`（若还要 aarch64：`rustup target add aarch64-unknown-linux-musl`）
4. 全工作区 release：`cargo zigbuild --release --target x86_64-unknown-linux-musl`  
   或只编 worker：`cargo zigbuild --release -p logspout-worker --target x86_64-unknown-linux-musl`

仍需本机有 **CMake**（供 `rdkafka-sys` 编 librdkafka）。详见 [`logspout-worker/README.md`](logspout-worker/README.md)。

## 打包

本地 musl / native 目录包见 **`scripts/logkit-pack.sh`**（macOS 下默认 **`auto` 即走 Zig**；环境变量 **`LOGKIT_PACK_LINKER`** 等见脚本注释）。Release 标签推送时的矩阵见 **`.github/workflows/pack.yml`**。

## 建议阅读顺序

1. [`logspout-config/README.md`](logspout-config/README.md)（默认值与路径）
2. [`logspout-daemon/README.md`](logspout-daemon/README.md)→ 启动 daemon  
3. [`logspout/README.md`](logspout/README.md)→ 客户端命令  
4. [`logspout-dsl/README.md`](logspout-dsl/README.md)→ 编写 producer YAML  
