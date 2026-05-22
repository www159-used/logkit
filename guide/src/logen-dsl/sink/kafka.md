# 输出kafka

传输、TLS（JKS/PEM）、SASL 见 [security](kafka-security.md)。`mode: agent` 见 [agent](kafka-agent.md)。

## `sink.kafka` 基础字段

| 字段 | 必填 |
|------|------|
| [`mode`](#kafka-mode) | 否 |
| [`brokers`](#kafka-brokers) | 是 |
| [`topic`](#kafka-topic) | `mode`为`common` 是 | 
| [`headers`](#kafka-headers) | 否 | 
| [`request.required.acks`](#kafka-request-required-acks) | 否 |
| [`message.timeout.ms`](#kafka-message-timeout-ms) | 否 |
| [`delivery.timeout.ms`](#kafka-delivery-timeout-ms) | 否 |
| [`security.protocol`](kafka-security.md#securityprotocol) | 否 |

## producer透传配置（librdkafka）

worker 启动时会先写入下列默认值，可以在yaml中直接配置

| librdkafka 键 | 默认值 |
|---------------|--------|
| `queue.buffering.max.kbytes` | `65536` |
| `batch.size` | `65536` |
| `queue.buffering.max.ms` | `10` |
| `message.max.bytes` | `10485760` |
| `compression.type` | `lz4` |
| `socket.timeout.ms` | `60000` |

未在一等字段中建模的 librdkafka 键，可直接写在 `sink.kafka:` 下（键名即 librdkafka 键名），由 worker 落入 `extras` 并覆盖内置值。


## 字段说明

<a id="kafka-mode"></a>

### `mode`

- `common`：如果没有写`mode`，默认为`common`；直接发送渲染出的文本行；需要配置 `topic`
- `agent`：由 worker 组装 agent 风格 envelope；要求存在 `sink.kafka.agent`；`topic` 可省略；参考 [agent模式](kafka-agent.md)

<a id="kafka-brokers"></a>

### `brokers`

至少要有一个非空 broker。

```yaml
brokers:
  - "192.168.1.60:9092"
  - "192.168.1.61:9092"
```

<a id="kafka-topic"></a>

### `topic`

- `mode: common` 时必填
- `mode: agent` 时可省略

<a id="kafka-headers"></a>

### `headers`

支持kafka record的headers协议
(为此我尝试了好几个kafka依赖)

支持字符串、整数/浮点、布尔、`null`；

```yaml
headers:
  env: prod
  retry: 3
  sampled: true
  note: null
```

<a id="kafka-request-required-acks"></a>

### `request.required.acks`

写入确认级别，作为字符串映射给 rdkafka。例如 `request.required.acks: all` 或 `request.required.acks: 1`；省略时 worker 默认 `1`。

<a id="kafka-message-timeout-ms"></a>

### `message.timeout.ms`

单条消息在客户端侧等待投递结果的上限（毫秒），例如 `message.timeout.ms: 30000`；省略时默认 `30000`。

<a id="kafka-delivery-timeout-ms"></a>

### `delivery.timeout.ms`

端到端投递超时（毫秒）。省略且未在 `extras` 中覆盖时，worker 取 `message.timeout.ms + 5000`（至少 `10000`）。
