# 配置与套接字

**logen** 与 **logend** 通过 **`logen-config`** 加载 TOML，仅 **2 段**：**`[client]`**、**`[logend]`**。

详细说明见 [`logen-config` README](../../../logen-config/README.md) 与 **`assets/conf.ref.toml`**。

## 段一览

| 段 | 使用者 | 说明 |
|----|--------|------|
| `[client]` | logen | 连接方式（unix / tcp） |
| `[logend]` | logend | tmp_dir、监听、worker、grpc 限制等 |

## 连接方式

### 本地 Unix（默认）

| 来源 | 路径 |
|------|------|
| 默认 | `{logend.tmp_dir}/logend.sock` 或 `[logend].socket` |
| 覆盖 | `logen -S /path/to/logend.sock` |

### 远端 TCP

**logend** 配置 `[logend].listen`，**logen** 配置 `[client]` 或 CLI：

```bash
logen -H 10.0.0.5 -P 19407 list
```

## 与 start 相关

| 键（在 `[logend]`） | 说明 |
|---------------------|------|
| `max_encoding_message_size_bytes` | `start` 上传实例 YAML 上限 |
| `max_decoding_message_size_bytes` | `stat` / `cat` 等大响应上限 |
| `worker_output_dir` | **必填**；file sink 的 `output` 相对此目录 |
| `heartbeat_timeout_secs` / `heartbeat_interval_secs` | 影响 `list` / `stat` 的 **healthy** |

## 实例 YAML

实例内容在 **`start`** 指定的 YAML 文件中，见 **[logen-dsl](../logen-dsl/intro.md)**。
