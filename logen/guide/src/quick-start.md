# 快速上手

下列步骤假设已在仓库根目录完成 **`cargo build --release`**，且 **`logend`**、**`logen`** 在 `target/release/`。

## 1. 准备实例 YAML

新建 `example.yaml`（stdout 模式，便于直接看到输出）：

```yaml
template: "{{ts}} {{ip}} {{status}} {{path}}"
fields:
  ts:
    type: timestamp
    format: "%Y-%m-%dT%H:%M:%S%z"
  ip:
    type: ipv4
  status:
    type: pick
    values: [200, 200, 404, 500]
  path:
    type: url-path
sink:
  type: stdout
```

字段与 sink 的完整说明见 **[logen-dsl](../../logen-dsl/guide/book/index.html)**。

## 2. 启动 logend

终端 A：

```bash
./target/release/logend
# 或后台：./target/release/logend &
```

默认套接字为 **`{tmp_dir}/logend.sock`**，由 TOML 的 `[common].tmp_dir` 决定（见 [配置与套接字](reference/config.md)）。

## 3. 用 logen 操作

终端 B：

```bash
# 探活
./target/release/logen ping

# 启动 worker（打印：id<TAB>status）
./target/release/logen start ./example.yaml

# 列出实例
./target/release/logen list
# 别名：logen ls

# 查看统计（EPS、心跳等）
./target/release/logen stat

# 停止（id 可用前缀，见 stop 章节）
./target/release/logen stop <id>
```

`start` 使用 **stdout** sink 时，渲染出的日志行会出现在 **logend 所在终端的标准输出**（worker 与 daemon 同进程）。

## 4. 使用仓库示例

[`etc/`](../../../etc/) 下有 Apache Combined、JSON、LEEF、Kafka 等示例，例如：

```bash
./target/release/logen start etc/apache.file.yaml
./target/release/logen start etc/json.kafka.yaml
```

Kafka 类示例需可达的 broker 与 TLS 材料；见 [logen-dsl · Kafka 模式](../../logen-dsl/guide/src/manual/kafka.md)。
