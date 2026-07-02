# kafka security相关字段


## 配置示例

### JKS（TLS / mTLS）

```yaml
security.protocol: SSL
ssl.truststore.location: /opt/yotta/cert/truststore.jks
ssl.truststore.password: xxx
ssl.keystore.location: /opt/yotta/cert/keystore.jks
ssl.keystore.password: xxx
ssl.endpoint.identification.algorithm: ""
```

仅需校验 broker、不配客户端证书时，可省略 `ssl.keystore.*`。

### PEM：仅 TLS

```yaml
security.protocol: SSL
ssl.ca.location: /opt/yotta/cert/ca.crt
ssl.endpoint.identification.algorithm: ""
```

### PEM：mTLS

```yaml
security.protocol: SSL
ssl.ca.location: /opt/yotta/cert/ca.crt
ssl.certificate.location: /opt/yotta/cert/client.crt
ssl.key.location: /opt/yotta/cert/client.key
ssl.endpoint.identification.algorithm: ""
```

## 字段说明

### `security.protocol`

目前仅支持 **`PLAINTEXT`**、**`SSL`**。

### JKS 相关

| 键 | 说明 |
|----|------|
| `ssl.truststore.location` | 信任库；`.jks`、`.p12` / `.pfx` |
| `ssl.truststore.password` | 信任库密码 |
| `ssl.keystore.location` | 客户端身份库；mTLS 时使用 |
| `ssl.keystore.password` | 身份库密码 |
| `ssl.keystore.alias` | 多私钥 JKS 时选别名；省略则取升序第一条 |

### PEM 相关

| 键 | 说明 |
|----|------|
| `ssl.ca.pem` / `ssl.ca.location` | CA |
| `ssl.certificate.pem` / `ssl.certificate.location` | 客户端证书 |
| `ssl.private.key.pem` / `ssl.key.pem` / `ssl.key.location` | 私钥 |

### `ssl.endpoint.identification.algorithm`

空串表示关闭主机名校验；不写则默认开启。

### `ssl.protocol` / `ssl.enabled.protocols`

仅为兼容而接受，**不会**传给 rdkafka。

### `sasl.*`

支持通过配置 `security.protocol: SASL_PLAINTEXT` 或 `SASL_SSL` 来启用 SASL 认证。

| 键 | 说明 |
|----|------|
| `sasl.mechanism` | 认证机制，例如 `PLAIN`、`SCRAM-SHA-256`、`SCRAM-SHA-512` |
| `sasl.username` | 用户名 |
| `sasl.password` | 密码 |
| `sasl.jaas.config` | 仅为兼容 Java 客户端而接受，**不会**传给 rdkafka。请直接使用 `sasl.username` 和 `sasl.password`。 |

#### SASL 配置示例

**SCRAM-SHA-256 (SASL_SSL)**

```yaml
sink:
  type: kafka
  kafka:
    brokers:
      - "kafka.example.com:9093"
    topic: raw_message
    security.protocol: SASL_SSL
    sasl.mechanism: SCRAM-SHA-256
    sasl.username: myuser
    sasl.password: mypass
    ssl.ca.location: /path/to/ca.crt
    ssl.endpoint.identification.algorithm: ""
```

**PLAIN (SASL_PLAINTEXT)**

```yaml
sink:
  type: kafka
  kafka:
    brokers:
      - "kafka.example.com:9092"
    topic: raw_message
    security.protocol: SASL_PLAINTEXT
    sasl.mechanism: PLAIN
    sasl.username: myuser
    sasl.password: mypass
```