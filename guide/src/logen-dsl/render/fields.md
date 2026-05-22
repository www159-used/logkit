# fields

fields的类型盘点

| type | 说明 |
|------|------|
| [`uuid-v4`](#uuid-v4) | 随机 UUID |
| [`name-en`](#name-en) | 英文人名风格 |
| [`ipv4`](#ipv4) | 随机 IPv4 |
| [`timestamp`](#timestamp) | 当前时间；`format` 为 strftime |
| [`integer`](#integer) | `min`–`max` 闭区间随机整数 |
| [`sentence`](#sentence) | 随机英文词组；`min` / `max` 控制词数 |
| [`url`](#url) | 随机绝对 URL |
| [`url-path`](#url-path) | 随机 path/query，适合 HTTP 请求行 |
| [`hostname`](#hostname) | FQDN 风格主机名 |
| [`domain-suffix`](#domain-suffix) | TLD |
| [`lorem-word`](#lorem-word) | 单随机小写词 |
| [`company-name`](#company-name) | 公司名 |
| [`user-agent`](#user-agent) | UA 字符串 |
| [`username`](#username) | 登录名风格 |
| [`counter`](#counter) | 从 0 递增，`u64` 环绕 |
| [`template`](#template-type) | 子模板：嵌套 `template` + `fields` |
| [`one-of`](#one-of) | 多分支；仅选中分支求值 |

## 字段说明

<a id="uuid-v4"></a>

### `uuid-v4`

生成随机 UUID v4 字符串

例子：

```yaml
req_id:
  type: uuid-v4
```

<a id="name-en"></a>

### `name-en`

生成英文人名风格字符串

<a id="ipv4"></a>

### `ipv4`

生成随机 IPv4 地址

<a id="timestamp"></a>

### `timestamp`

- 生成当前时间
- `format` 使用 `strftime` 风格

例子：

```yaml
ts:
  type: timestamp
  format: "%Y-%m-%dT%H:%M:%S%z"
```

<a id="integer"></a>

### `integer`

在 `min` 到 `max` 闭区间内随机取整数

例子：

```yaml
status:
  type: integer
  min: 200
  max: 599
```

<a id="sentence"></a>

### `sentence`

- 生成随机英文词组
- `min` / `max` 控制词数范围

例子：

```yaml
msg:
  type: sentence
  min: 3
  max: 8
```

<a id="url"></a>

### `url`

生成随机绝对 URL

<a id="url-path"></a>

### `url-path`

- 生成随机 path/query
- 适合 HTTP 请求行里的路径部分

<a id="hostname"></a>

### `hostname`

生成 FQDN 风格主机名

<a id="domain-suffix"></a>

### `domain-suffix`

生成顶级域后缀，例如 `com`、`org`

<a id="lorem-word"></a>

### `lorem-word`

生成单个随机英文小写词

<a id="company-name"></a>

### `company-name`

生成公司名风格字符串

<a id="user-agent"></a>

### `user-agent`

生成 User-Agent 字符串

<a id="username"></a>

### `username`

生成登录名 / 句柄风格字符串

<a id="counter"></a>

### `counter`

一般用来调试

从 `0` 开始，每生成一行递增 `1`，
到 `u64` 上限后归`0`。（没那么容易！）

<a id="template-type"></a>

### `template`

引入链式模板特性

在字段内部再嵌一层小模板，
适合需要拼复杂字段时使用

例子：

```yaml
sd:
  type: template
  template: '[id user="{{user}}" src="{{src}}"]'
  fields:
    user:
      type: username
    src:
      type: ipv4
```

<a id="one-of"></a>

### `one-of`

随机的多分支，可以自定义权重

不写权重默认为1

例子：

```yaml
level:
  type: one-of
  branches:
    - v: info
    - { w: 3, v: warn }
    - { w: 1, v: error }
user:
  type: one-of
  branches:
    - { w: 2, v: "-" }
    - w: 1
      template: "{{u}}"
      fields:
        u:
          type: username
```
