# cat

打印**运行中** worker 实例在 daemon 内存里保存的实例 YAML（与 `start` 时发送的内容一致，而非磁盘上你本地的原文件）。

## 用法

```bash
logen cat <id>
```

| 参数 | 说明 |
|------|------|
| **id** | 完整 UUID 或**唯一**前缀（规则同 [stop](stop.md)） |

## 行为

- 调用 **`CatWorker`** RPC。
- 将返回的 **`yaml`** 原样写到**标准输出**（不额外加文件名或换行修饰）。

## 示例

```bash
$ logen cat a1b2c3d4-e5f6-7890-abcd-ef1234567890
template: "{{remote_addr}} - ..."
fields:
  ...
sink:
  type: file
  output: logs/apache.log
  ...
```

## 说明

- 用于确认 daemon 侧实际运行的配置（合并/校验后的视图）。
- 若本地 YAML 已改但未重新 `start`，**cat** 看到的仍是**当前运行实例**的配置，不是你可能已编辑的磁盘文件。
