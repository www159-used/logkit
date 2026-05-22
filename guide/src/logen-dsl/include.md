# include

支持将 **`body`**、**`sink`**、速率等拆到多个 YAML，入口文件用 **`include`** 按顺序拼成一份 worker 配置。

## 配置示例

### 入口 + 两个片段

`main.yaml`（入口，优先级最高）：

```yaml
include:
  - _base/body.yaml
  - _base/sink-kafka.yaml

min-interval: 200ms

sink:
  kafka:
    topic: app-logs
```

`_base/body.yaml`（只提供日志体）：

```yaml
body:
  template: '{"msg":"{{msg}}"}'
  fields:
    msg:
      type: sentence
      min: 2
      max: 5
```

`_base/sink-kafka.yaml`（只提供输出）：

```yaml
sink:
  type: kafka
  kafka:
    brokers: ["127.0.0.1:9092"]
```

合并后：模板与 `fields` 来自 `body.yaml`，`brokers` 来自 `sink-kafka.yaml`，`topic` 与 `min-interval` 由入口覆盖。

### 单文件 `include`

```yaml
include: _base/body.yaml

body:
  template: "override"
  fields: {}
sink:
  type: stdout
```

`include` 可以是**一个字符串**或**字符串列表**。

合并完成后须仍有一份有效的 **`body:`**（含 **`template`**），否则解析失败。

## 合并规则

| 键 / 块 | 行为 |
|---------|------|
| **展开顺序** | 先按 `include` **从左到右** 合并；**再**合并**当前文件**（当前文件覆盖 include） |
| **`body:`** | **整包替换**；新的 `body` 完全取代旧的，**不会**把 `fields` 拼在一起 |
| **`sink:`** | **深合并**；同 `type` 时合并子映射（例如只写 `kafka.topic` 可保留先前的 `brokers`） |
| **`sink.type` 变化** | 换类型时 **整段 `sink` 由后者替换** |
| **`min-interval` / `threads`** | **后写覆盖** |

`body` 相当于换一整段模板；`sink` 相当于在同类输出上打补丁。

**路径**：相对**当前 YAML 所在目录**解析；也支持**绝对路径**与 **`..`**（由你自行保证目标存在、可读）。仅 `.yaml` / `.yml`。被 include 的文件里可再写 `include`（嵌套展开，深度上限 **16**）；**循环引用**会报错。
