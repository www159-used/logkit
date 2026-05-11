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

## 打包

本地 musl / native 目录包见 **`scripts/logkit-pack.sh`**（环境变量 **`LOGKIT_PACK_LINKER`** 等见脚本注释）。

## 建议阅读顺序

1. [`logspout-config/README.md`](logspout-config/README.md)（默认值与路径）
2. [`logspout-daemon/README.md`](logspout-daemon/README.md)→ 启动 daemon  
3. [`logspout/README.md`](logspout/README.md)→ 客户端命令  
4. [`logspout-dsl/README.md`](logspout-dsl/README.md)→ 编写 producer YAML  
