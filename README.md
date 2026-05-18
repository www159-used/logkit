# logkit

Rust 实现的日志造数 / 灌流工具链（CS 架构）：**Unix 套接字上的 gRPC 控制面** + 进程内 **`logen-worker`** 执行造数。起因包含 ARM 产物、交叉编译与避免裸露 TCP 端口等考量。

**总览与架构（mdBook）**：仓库根目录 [`guide/`](guide/)，`cd guide && mdbook build`（可选 `--open`），生成物在 `guide/book/`（已 `.gitignore`）。含 Mermaid 的子书需先执行 **`./scripts/fetch-mdbook-mermaid-assets.sh`**，再 `mdbook build`。书中链到 [**logen CLI**](logen/guide/book/index.html)（`cd logen/guide && mdbook build`）、[**logen-dsl**](logen-dsl/guide/book/index.html)（`cd logen-dsl/guide && mdbook build`）及各 crate `README`。

许可：**GPL-3.0**（见仓库根目录 [`LICENSE`](LICENSE)）。

## 各 crate 说明（文档入口）

| Crate | 说明 |
|--------|------|
| [`logen`](logen/README.md) | CLI：`ping` / `start` / `list` / `stop` / `stat` / `cat`（[mdBook](logen/guide)：`cd logen/guide && mdbook build`） |
| [`logend`](logend/README.md) | 守护进程：监听 UDS，调度造日志任务 |
| [`logen-worker`](logen-worker/README.md) | 造日志库（由 daemon 进程内嵌入） |
| [`logen-dsl`](logen-dsl/guide/src/intro.md) | Worker 模板 YAML：`template` / `fields` / `sink`（[mdBook 规范](logen-dsl/guide)：`cd logen-dsl/guide && mdbook build`） |
| [`logen-config`](logen-config/README.md) | 共用 TOML：合并默认与 `--defaults-file` |
| [`logen-proto`](logen-proto/README.md) | `protobuf` + `tonic` 生成代码 |

示例 YAML 在 **[`etc/`](etc/)**（Apache、RFC 5424、LEEF 等）。

## 版本与发版

工作区版本在根目录 **`Cargo.toml`** 的 **`[workspace.package].version`**；各 crate 仅写 `version.workspace = true`（与 `edition` / `license` 一并继承）。

使用 **[cargo-release](https://github.com/crate-ci/cargo-release)** 统一 bump、提交、打 tag（配置见 [`release.toml`](release.toml)）：

```bash
cargo install cargo-release --locked
cargo release patch --execute   # 或 minor / major / 具体版本如 0.2.0
git push --follow-tags
```

推送 **`v*`** 标签会触发 [`.github/workflows/pack.yml`](.github/workflows/pack.yml) 构建并发布 GitHub Release。

## 构建

```bash
cargo build --release
```

产物：`target/release/logen`、`logend`。

### Linux musl（交叉编译，推荐 Zig）

含 **Kafka（rdkafka：vendored OpenSSL + static libcurl，供 bundled librdkafka 编译）** 时，目标三需要可用的 **C 工具链**；在 **macOS** 或未安装 **`x86_64-linux-musl-gcc`** 一类 musl 交叉 GCC 的环境下，请用 **Zig + cargo-zigbuild**（与 Release CI、打包脚本一致）：

1. 安装 [Zig](https://ziglang.org/download/) 并加入 `PATH`。
2. `cargo install cargo-zigbuild`
3. `rustup target add x86_64-unknown-linux-musl`（若还要 aarch64：`rustup target add aarch64-unknown-linux-musl`）
4. 全工作区 release：`cargo zigbuild --release --target x86_64-unknown-linux-musl`

仍需本机有 **CMake**（供 `rdkafka-sys` 编 librdkafka）。详见 [`logen-worker/README.md`](logen-worker/README.md)。

## 打包

本地 musl / native 目录包见 **`scripts/logkit-pack.sh`**（macOS 下默认 **`auto` 即走 Zig**；环境变量 **`LOGKIT_PACK_LINKER`** 等见脚本注释）。Release 标签推送时的矩阵见 **`.github/workflows/pack.yml`**。

## 建议阅读顺序

1. 根目录 **[`guide/`](guide/)**（总览与架构示意，并链到 DSL 子书）
2. [`logen-config/README.md`](logen-config/README.md)（默认值与路径）
3. [`logend/README.md`](logend/README.md)→ 启动 daemon  
4. [`logen/README.md`](logen/README.md)→ 客户端命令  
5. [`logen-dsl/guide`](logen-dsl/guide/src/intro.md)（`mdbook build`）→ 编写 worker 模板 YAML  
