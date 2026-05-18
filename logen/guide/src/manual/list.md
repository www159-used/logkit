# list

列出当前由 **`logend`** 托管的运行中 worker 实例。

## 用法

```bash
logen list
```

**别名**：`logen ls`（与 `list` 完全等价）。

## 输出

制表符分隔表头一行，随后每个实例一行：

```text
id      alive   healthy sink
```

| 列 | 含义 |
|----|------|
| **id** | 实例 UUID（`start` 时分配） |
| **alive** | 任务是否仍在运行 |
| **healthy** | 是否在心跳超时内收到心跳（启用 worker 心跳时才有意义） |
| **sink** | sink 摘要（如 `stdout`、`file: path`、`kafka: broker…`） |

## 示例

```bash
$ logen list
id                                      alive   healthy sink
a1b2c3d4-e5f6-7890-abcd-ef1234567890    true    true    stdout
```

## 说明

- 仅包含 daemon **已知**的实例；已 `stop` 或异常退出的不会长期保留（以 daemon 实现为准）。
- 需要更细的吞吐、心跳间隔等，请用 [stat](stat.md)。

## 相关

- 启动实例：[start](start.md)
- 停止实例：[stop](stop.md)
