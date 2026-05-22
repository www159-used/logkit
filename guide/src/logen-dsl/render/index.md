# 渲染

写在 **`body:`** 下，可以渲染任何格式的日志

| 键 | 必填 | 默认 |
|----|------|------|
| [`template`](#template) | 是 | — |
| [`fields`](fields.md) | 否 | 空映射 |

## 配置示例

### 例：文本行（类 Apache Combined）

```yaml
body:
  template: '{{client_ip}} - {{user}} [{{ts}}] "{{method}} {{path}} HTTP/1.1" {{status}} {{size}}'
  fields:
    client_ip:
      type: ipv4
    user:
      type: username
    ts:
      type: timestamp
      format: "%d/%b/%Y:%H:%M:%S %z"
    method:
      type: one-of
      branches:
        - { w: 3, v: GET }
        - { v: POST }
    path:
      type: url-path
    status:
      type: one-of
      branches:
        - { w: 4, v: "200" }
        - { v: "404" }
    size:
      type: integer
      min: 200
      max: 8000
```

### 例：单行 JSON

```yaml
body:
  template: '{"ts":"{{ts}}","level":"{{level}}","msg":"{{msg}}","trace_id":"{{trace_id}}"}'
  fields:
    ts:
      type: timestamp
      format: "%Y-%m-%dT%H:%M:%S%z"
    level:
      type: one-of
      branches:
        - { w: 3, v: info }
        - { v: error }
    msg:
      type: sentence
      min: 3
      max: 10
    trace_id:
      type: uuid-v4
```

<a id="template"></a>

## `template`

一行日志的 **Handlebars** 模板字符串。

- 占位符写成 `{{字段名}}`，名字须与 `fields` 里的键一致。
- 未在 `fields` 中声明的占位符，渲染结果为空。
- **不要**用 `len` 等可能与 Handlebars 内置 helper 冲突的名字。
