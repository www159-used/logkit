# 输出

**`sink:`** 决定日志往哪送。

| `type` | 说明 |
|--------|------|
| `stdout` | 写到 worker 控制台 |
| `file` | 追加到文件 |
| [`kafka`](kafka.md) | 发到 kafka |

<a id="type-stdout"></a>

## `type: stdout`

```yaml
sink:
  type: stdout
```

<a id="type-file"></a>

## `type: file`

```yaml
sink:
  type: file
  output: apache.log
  max-size: 10mb
```

| 字段 | 必填 |
|------|------|
| [`output`](#file-output) | 是 |
| [`max-size`](#file-max-size) | 否 |

<a id="file-output"></a>

### `output`

是**相对路径**，最终会拼到 daemon 配置里的 `[worker].worker_output_dir`

```yaml
sink:
  type: file
  output: apache/access.log
```

若 `[worker].worker_output_dir = "./output"`，最终路径类似 `./output/apache/access.log`。

<a id="file-max-size"></a>

### `max-size`

可写整数（字节）或人类可读大小：`65536`、`64KiB`、`10mb`、`"1.5MiB"`

`0` 或不写 = 不限制；超过上限则截断为 0 字节后继续写