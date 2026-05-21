# 快速上手

## 1 最小 YAML

请编辑一个yaml，简单起见我们使用stdout模式

```yaml
body:
  template: "{{ts}} {{ip}} {{status}} {{path}}"
  fields:
    ts:
      type: timestamp
      format: "%Y-%m-%dT%H:%M:%S%z"
    ip:
      type: ipv4
    status:
      type: one-of
      branches:
        - { w: 2, v: "200" }
        - { w: 1, v: "404" }
        - { w: 1, v: "500" }
    path:
      type: url-path
sink:
  type: stdout
```

## 2 启动

首先启动`logend`。`logend`是所有`worker`的管理者

``` bash
logend &
```

然后请使用上面的配置，启动一个`worker`

```bash
logen start ./example.yaml
```

## 3 结果

控制台会持续打出类似下面这样的行：

```text
2026-05-16T10:02:41+0800 192.168.1.24 404 /api/v1/users?id=8
```

恭喜你成功启动了一个`logen worker`