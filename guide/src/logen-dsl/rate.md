# 速率


| 键 | 必填 | 默认 |
|----|------|------|
| [`min-interval`](#min-interval) | 否 |  |
| [`threads`](#threads) | 否 | `1` |


<a id="min-interval"></a>

## `min-interval`

**省略**或 **`0`**：不做限制。
否则按照间时间隔控制速率

支持人类可读的时间单位([humantime](https://github.com/chronotope/humantime))

例子：

```yaml
min-interval: 0
min-interval: 1s
min-interval: 100ms
min-interval: 1d
```

<a id="threads"></a>

## `threads`

为了解决单个循环的上限问题，
worker支持启动多个写日志循环

每个循环独立生成日志，并有自己的sink

例子：

```yaml
threads: 4
min-interval: 100
```