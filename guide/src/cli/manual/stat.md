# stat

查询 worker 实例的运行统计与健康信息。

## 用法

```bash
logen stat
logen stat <id_prefix>
```

| 形式 | 说明 |
|------|------|
| **无参数** | 返回**全部**实例的统计 |
| **id_prefix** | 仅返回 id **前缀匹配**的实例（可匹配多条） |

## 输出字段

每个实例一块，键值对形式（空行分隔）：

| 字段 | 含义 |
|------|------|
| **id** | 实例 UUID |
| **config_path** | 启动时的 `config_label`（一般为 `start` 传入的 YAML 路径） |
| **sink** | sink 摘要 |
| **alive** | 任务是否仍在运行 |
| **healthy** | 心跳是否在超时内 |
| **eps** | 估算吞吐（事件/秒，由累计事件与运行时间外推） |
| **eps_interval** | 最近心跳窗口内的 Δ事件/Δt |
| **events_total** | 累计渲染并 emit 的事件数 |
| **events_est** | 估算总事件（含外推） |
| **sec_since_hb** | 距上次心跳秒数 |
| **hb_timeout_s** / **hb_interval_s** | TOML `[worker]` 中的心跳配置 |

## 示例

```bash
$ logen stat
id:             a1b2c3d4-e5f6-7890-abcd-ef1234567890
config_path:    etc/apache.file.yaml
sink:           file: logs/apache.log (max-size: 1048576 bytes)
alive:          true
healthy:        true
eps:            12.345  (realtime est.: extrapolated total / uptime)
eps_interval:   11.800  (last heartbeat window Δ/Δt)
events_total:   12345
events_est:     12345.0
sec_since_hb:   0.500
hb_timeout_s:   30
hb_interval_s:  5

$ logen stat a1b2
(no matching workers)   # 若无匹配
```

## 何时使用

- 压测或灌流时观察 **EPS** 是否稳定。
- 排查「实例在跑但不健康」：对比 **healthy** 与 **sec_since_hb**。
