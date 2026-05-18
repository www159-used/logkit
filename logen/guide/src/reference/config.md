# 配置与套接字

**logen** 与 **logend** 通过 **`logen-config`** 加载同一套 TOML。CLI 在连接前会检查套接字文件是否存在。

详细键说明见 [`logen-config` README](../../../logen-config/README.md) 与嵌入模板 **`assets/conf.ref.toml`**。

## 套接字路径

| 来源 | 路径 |
|------|------|
| 默认 | `{common.tmp_dir}/logend.sock` |
| 覆盖 | `logen -S /path/to/logend.sock` |

**`tmp_dir`** 在 TOML **`[common]`** 段。多 daemon 实例（极少见）须使用不同 `tmp_dir`，避免套接字与 pid 冲突。

## 与 start 相关的段

### `[protocol.grpc]`

| 键 | 与 logen 的关系 |
|----|-----------------|
| `max_encoding_message_size_bytes` | `start` 上传实例 YAML 的上限 |
| `max_decoding_message_size_bytes` | 接收 `stat` / `cat` 等较大响应的上限 |
| `ping_reply_text` | `ping` 子命令输出 |
| `client_connect_uri` | tonic 所需的形式 URI；**传输仍为 Unix 套接字**，不建 TCP |

### `[worker]`

主要由 **logend** 消费；与 CLI 间接相关：

| 键 | 说明 |
|----|------|
| `worker_output_dir` | **必填**；`sink.type: file` 的 `output` 相对此目录 |
| `heartbeat_timeout_secs` | 影响 `list` / `stat` 的 **healthy** |
| `heartbeat_interval_secs` | worker 向 daemon 上报心跳的周期 |

## 日志

**logen** 自身几乎不写日志；错误信息走 **stderr**，成功输出走 **stdout**。

daemon 日志由 **logend** 配置（如 `[daemon].log_level`、`RUST_LOG`），见 logend 文档。

## 实例 YAML

实例内容**不在** TOML 里，而在 **`start`** 指定的 YAML 文件中，规范见 **[logen-dsl](../../logen-dsl/guide/book/index.html)**。
