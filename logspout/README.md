# logspout（CLI）

面向 **`logspout-daemon`** 的 **gRPC 客户端**：通过 **Unix 域套接字**连接，下发 `start`、查询 `list` / `stat`、停止 `stop` 等。**仅支持 Unix**。

配置与 daemon **共用**同一套 TOML 合并逻辑，见 [`logspout-config`](../logspout-config/README.md)。

## 依赖前提

- 已启动 **`logspout-daemon`**，且套接字文件存在（默认 `{tmp_dir}/logspout-daemon.sock`，由 [`logspout-config`](../logspout-config/README.md) 中的 `[common].tmp_dir` 决定）。
- 若 CLI 与 daemon 使用不同的 `tmp_dir`，须用 **`-S` / `--sock`** 指向 daemon 实际使用的 `.sock`。

## 常用选项

| 选项 | 说明 |
|------|------|
| `--defaults-file PATH` | TOML 路径；与内嵌默认深度合并 |
| 环境变量 `LOGSPOUT_DEFAULTS_FILE` | 同上（等价于自动前置 `--defaults-file`） |
| `-S`, `--sock PATH` | 覆盖默认 Unix 套接字路径 |

## 子命令

| 子命令 | 作用 |
|--------|------|
| `ping` | 探测 daemon；打印配置中的 `ping_reply_text` |
| `echo [TEXT...]` | 回显，用于链路测试 |
| `list` | 列出运行中的实例：id、alive、healthy、sink 摘要 |
| `start CONFIG.yaml` | 读取**单份** producer YAML，校验后启动；`config_label` 为该路径 |
| `stop <id>` | 停止实例；`id` 支持完整 UUID，也支持**唯一前缀** |
| `stat [前缀]` | 无参数：全部实例；有参数：按 id **前缀**筛选 |
| `cat <id>` | 打印该实例内存中的 producer YAML（`id` 规则同 `stop`） |

## 快速示例（在仓库根目录）

终端 A：启动 daemon（见 [`logspout-daemon`](../logspout-daemon/README.md)）。

终端 B：

```bash
./target/release/logspout ping
./target/release/logspout start etc/apache.combined.file.yaml
./target/release/logspout list
./target/release/logspout stat
./target/release/logspout stop <id>
```

Producer 语法见 [`logspout-dsl`](../logspout-dsl/README.md)；示例文件在 [**`etc/`**](../etc/)。

## 常见问题

**报错 unix socket 不存在**

- 先启动 `logspout-daemon`。
- 对齐 **`LOGSPOUT_DEFAULTS_FILE` / `--defaults-file`** 与 `[common].tmp_dir`，或使用 `-S` 指向正确 `.sock`。

**`start` 失败或消息过大**

- producer YAML 作为单次 RPC 载荷；必要时调大 TOML 里 `[protocol.grpc]` 的 **`max_encoding_message_size_bytes`** 等（见 [`logspout-config`](../logspout-config/README.md)）。
