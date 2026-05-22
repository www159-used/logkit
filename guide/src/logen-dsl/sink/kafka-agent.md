# kafka agent模式字段

## 例子

```yaml
sink:
  type: kafka
  kafka:
    mode: agent
    brokers:
      - "192.168.1.60:9092"
    agent:
      source_id: 43983bfc-2db3-47a5-a3a8-d832b2855d51
      domain: acme
```

## agent.*字段列表

| 字段 | 必填 |
|------|------|
| [`source_id`](#source_id) | 否 |
| [`domain`](#domain) | 否 |
| [`domain_token`](#others) | 否 |
| [`appname`](#others) | 否 |
| [`source`](#others) | 否 |
| [`token`](#others) | 否 |
| [`tag`](#others) | 否 |
| [`hostname`](#others) | 否 |
| [`ip`](#others) | 否 |
| [`flag`](#others) | 否 |
| [`fields`](#others) | 否 |

## 字段说明

<a id="source_id"></a>

### `source_id`

必须是 36 字符标准 UUID。

```yaml
agent:
  source_id: 43983bfc-2db3-47a5-a3a8-d832b2855d51
```

<a id="domain"></a>

### `domain`

可选；为空时不构造 `domain` 字段。

<a id="others"></a>

### 其它 `agent.*`

`domain_token`、`appname`、`source`、`token`、`tag`、`hostname`、`ip`、`flag`、`fields` 均为可选；不写时由 worker 运行时生成。
