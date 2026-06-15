# stop

停止指定 worker 实例。

## 用法

```bash
logen stop <id>
```

| 参数 | 说明 |
|------|------|
| **id** | 完整 UUID，或能**唯一匹配**某一运行中实例的前缀 |

## 行为

- 调用 **`StopWorker`** RPC。
- 标准输出打印 daemon 返回的 **status** 字符串（如 `stopped`）。

## id 与前缀

与 **`cat`** 相同：若前缀匹配到**多个**实例，daemon 会拒绝或报错（以避免误停）；仅匹配一个时成功。

建议：日常从 [list](list.md) 复制完整 id；调试时可用前 8 位等**唯一**前缀。

## 示例

```bash
$ logen list
a1b2c3d4-e5f6-7890-abcd-ef1234567890    true    true    file: logs/app.log

$ logen stop a1b2c3d4
stopped
```
