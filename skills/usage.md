# logkit 使用方法

## 组件说明

| 组件 | 路径 | 职责 |
|------|------|------|
| `logend` | `logkit/bin/logend` | daemon 进程，gRPC 服务端 |
| `logen` | `logkit/bin/logen` | CLI 控制面（start/stop/stat） |
| `kafka-ssl-gen` | `logkit/tools/bin/kafka-ssl-gen` | 从 client.conf 生成 Kafka TLS 配置 |
| `mysql_local` | `logkit/tools/bin/mysql_local` | MySQL 本地客户端 |
| `pullout` | `logkit/tools/bin/pullout` | 解密配置密文 |
| `jumpserver` | `logkit/tools/bin/jumpserver` | 堡垒机工具 |

## 快速开始

```bash
cd /root/wwhm
./logkit/bin/logend &
./logkit/tools/bin/kafka-ssl-gen
./logkit/bin/logen start my-config.yaml
./logkit/bin/logen stat
./logkit/bin/logend --stop
```

---

## include — 配置复用（重要）

**YAML 支持 `include` 引入其他文件，实现配置拼接。**

### 语法

```yaml
# 单文件
include: etc/common.yaml

# 多文件（按顺序合并）
include:
  - etc/common.yaml
  - etc/body/apache/access-xff.yaml
```

### 路径规则

- 相对路径：相对于 **当前 YAML 所在目录**
- include 深度上限：16 层
- 循环引用会报错

### 合并规则

| 键 | 合并方式 |
|-----|----------|
| `body` | **整包替换**（取最后一个） |
| `sink` | **深度合并**（同 key 覆盖） |
| `min-interval` | 后 include 的覆盖前面的 |
| `threads` | 后 include 的覆盖前面的 |

### 写法：拼装配置

```yaml
# 先引入现成的 body 和公共参数
include:
  - etc/common.yaml                       # threads + min-interval
  - etc/body/apache/access-xff.yaml       # body template + fields

# 再写自身 sink（覆盖/补充）
sink:
  type: kafka
  kafka:
    <<: include kafka.ssl.yaml            # 复用 ssl-gen 输出
    source_id: apache-access-log
    app_name: apache-access
    tag: prod
    topic: log_river                      # 覆盖 topic
```

### etc/ 下现成可 include 的文件

| 文件 | 作用 |
|------|------|
| `etc/common.yaml` | threads=2, min-interval=10ms |
| `etc/body/apache/access-xff.yaml` | Apache access + XFF |
| `etc/body/apache/middleware.yaml` | 中间件 JSON |
| `etc/body/cef.yaml` | CEF 格式 |
| `etc/body/leefv2.yaml` | LEEF v2 |
| `etc/body/json.yaml` | JSON |
| `etc/body/ips-nsfocus.yaml` | IPS |
| `etc/body/firewall-winicssec.yaml` | 防火墙 |
| `etc/body/exchange-tracking.yaml` | Exchange 追踪 |
| `etc/body/cyberark.yaml` | CyberArk |

---

## DSL 完整语法

### 顶层结构

```yaml
# --- 日志体 ---
template: '{{src_ip}} - {{user}} [{{timestamp}}] "{{method}} {{dst}}"'
fields:
  src_ip:   { type: ipv4 }
  user:     { type: username }
  timestamp:
    type: timestamp
    format: "%d/%b/%Y:%H:%M:%S %z"
  method:
    type: one-of
    branches:
      - { v: GET }
      - { v: POST }
      - { v: HEAD }
  dst:
    type: url-path

# --- 并发控制 ---
min-interval: 10ms      # 每条最小间隔（省略=不限速）
threads: 8              # 并发线程数（默认 1）

# --- 输出 ---
sink:
  type: kafka
  kafka:
    brokers: ["192.168.1.132:9092"]
    topic: log_river
```

### 字段类型速查

| 类型 | YAML | 说明 |
|------|------|------|
| **uuid-v4** | `{ type: uuid-v4 }` | 随机 UUID v4 |
| **ipv4** | `{ type: ipv4 }` | 随机 IPv4 |
| **name-en** | `{ type: name-en }` | 英文人名 (John Smith) |
| **username** | `{ type: username }` | 登录名/句柄 |
| **hostname** | `{ type: hostname }` | FQDN 形主机标签 |
| **domain-suffix** | `{ type: domain-suffix }` | 顶级域名 (com, org) |
| **company-name** | `{ type: company-name }` | 公司名 |
| **user-agent** | `{ type: user-agent }` | 随机 User-Agent |
| **url** | `{ type: url }` | 随机绝对 URL |
| **url-path** | `{ type: url-path }` | URL 路径部分 |
| **lorem-word** | `{ type: lorem-word }` | 单个随机英文词 |
| **sentence** | `{ type: sentence, min: 3, max: 8 }` | lorem ipsum 词组 |
| **integer** | `{ type: integer, min: 0, max: 100 }` | 闭区间随机整数 |
| **timestamp** | `{ type: timestamp, format: "%Y-%m-%d %H:%M:%S" }` | 当前时间 |
| **counter** | `{ type: counter }` | 从 0 自增 |
| **template** | `{ type: template, template: "…", fields: {…} }` | 嵌套子模板 |
| **one-of** | `{ type: one-of, branches: […] }` | 多选一 |

### one-of 用法

```yaml
dst:
  type: one-of
  branches:
    # 字面量（等权重）
    - v: GET

    # 加权
    - w: 3
      v: POST

    # 嵌套模板分支
    - w: 1
      template: "/fig?type={{t}}"
      fields:
        t:
          type: one-of
          branches:
            - { v: jpg }
            - { v: png }
```

### template（嵌套模板）

```yaml
sd:
  type: template
  template: '[id="{{id}}" msg="{{msg}}"]'
  fields:
    id:
      type: uuid-v4
    msg:
      type: sentence
      min: 2
      max: 5
```

### min-interval 格式

| 值 | 含义 |
|----|------|
| `10ms` | 每条间隔 10 毫秒 |
| `1s` | 每条间隔 1 秒 |
| `100us` | 每条间隔 100 微秒 |
| 省略 / `0` | 不限速 |

---

## kafka-ssl-gen

```bash
./logkit/tools/bin/kafka-ssl-gen
./logkit/tools/bin/kafka-ssl-gen /path/to/client.conf
```

在当前目录生成 `kafka.ssl.yaml`。

### Agent Mode 额外字段（可自定义）

| 字段 | 说明 | 示例 |
|------|------|------|
| `source_id` | 数据源标识 | `apache-access-log` |
| `app_name` | 应用名称 | `apache-access` |
| `tag` | 标签 | `prod`、`test` |

> `source_id` 对接 Agent Mode 时需从页面「采集项→数据源」获取；否则可自定义。

## mysql_local

```bash
./logkit/tools/bin/pullout 'config:yb:v1:<密文>'
./logkit/tools/bin/mysql_local -u<用户名> -p'<密码>' <database>
```

## pullout

```bash
./logkit/tools/bin/pullout 'config:yb:v1:xxxxxxxxxxxx'
```

## sink.kafka 完整配置

```yaml
sink:
  type: kafka
  kafka:
    brokers: ["192.168.1.132:9092"]
    topic: your_topic
    producer:
      acks: -1              # 0 / 1 / -1
      compression: lz4       # lz4 / snappy / zstd / gzip
    security: ssl
    ssl:
      ca: assets/ca.cert
      cert: assets/agent.pem
      key: assets/agent.key
    headers:
      trace-id: "{{ uuid }}"
```

## 常见问题

| 问题 | 排查 |
|------|------|
| macOS `ProcessFdQuotaExceeded` | `ulimit -n 65536` |
| Kafka 连接失败 | 检查 `brokers`、防火墙、TLS |
| TLS handshake 失败 | 用 `kafka-ssl-gen` 重新生成 |
| include 找不到文件 | 路径相对于当前 YAML，检查大小写 |

## 相关

- 打包：`./scripts/logkit-pack.sh gnu`
- 部署：rsync + tar 到远程机器
