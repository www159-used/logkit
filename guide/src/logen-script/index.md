# logen-script

欢迎来到logen的dsl。

v1.11.0及之前版本依赖yaml做声明式配置。
但是使用yaml意味着我的ast就是yaml。
首先这是一层不必要的依赖，
其次定义一些自定义的结构比较麻烦。

yaml另一个严重的问题就是没法很好的处理组合，复用。
当我想要组合不同的sink，body，config，于是不得不引入include语法。

在声明式配置中引入include语法就像java的clone一样具有二义性。
尤其是列表的patch，你将没法决定patch的粒度和深度。

于是便萌生了dsl的想法。其实yaml的包名就是logen-dsl，只不过第一版为了快速迭代上线没有经过复杂的设计和开发（当时也没啥时间。。）。

## 类型系统

在定义一个dsl前，请着重考虑类型系统。严格的类型系统约束和映射（最好是满射到native）能减少许多不必要的麻烦。
logen-dsl定义了以下几种类型：

| 类型 | 说明 |
|-----|-----|
| Str | 字符串，"a" |
| Int | 整数，范围i64 |
| Float | 浮点数，范围f64 |
| Duration | 人类可读时间，1s，20d |
| Sink | 内置类型，表示yaml的sink段|
| Body | 内置类型 |
| Field | Body的字段类型，这是超类 |
| Template | Body的template类型，模板字符串 | 
| Unit | void类型，空元组 |

特殊的，只定义了所有能用到的类型，没有引入型变等高阶操作

## 项目结构

纯gpt生成，我自己都没研究明白。

```modern-dot
digraph {
  rankdir=TB;
  nodesep=0.65;
  ranksep=0.42;
  graph [bgcolor="transparent", fontcolor="{{ text }}", fontname="Helvetica"];
  node [shape=box, style="rounded,filled", fillcolor="{{ node_fill }}", color="{{ node_stroke }}", fontcolor="{{ text }}", fontname="Helvetica"];
  edge [color="{{ edge }}", fontcolor="{{ text }}", fontname="Helvetica"];

  { rank=same;
    cli [label="logen CLI", fillcolor="{{ green_fill }}", color="{{ green_stroke }}"];
    store [label="ControlSessionStore", fillcolor="{{ blue_fill }}", color="{{ blue_stroke }}"];
    session [label="ControlSession\nparse + typecheck + eval", fillcolor="{{ blue_fill }}", color="{{ blue_stroke }}"];
    host [label="LogendControlHost\nstart / stop / stat", fillcolor="{{ purple_fill }}", color="{{ purple_stroke }}"];
    registry [label="WorkerRegistry", fillcolor="{{ amber_fill }}", color="{{ amber_stroke }}"];
    worker [label="logen-worker", fillcolor="{{ green_fill }}", color="{{ green_stroke }}"];
  }
  edge [style=invis, weight=100];
  cli -> store -> session -> host -> registry -> worker;

  node [shape=point, width=0.01, height=0.01, label="", color="{{ edge_muted }}", fillcolor="{{ edge_muted }}"];
  { rank=same; cli_1; store_1; session_1; host_1; registry_1; worker_1; }
  { rank=same; cli_2; store_2; session_2; host_2; registry_2; worker_2; }
  { rank=same; cli_3; store_3; session_3; host_3; registry_3; worker_3; }
  { rank=same; cli_4; store_4; session_4; host_4; registry_4; worker_4; }
  { rank=same; cli_5; store_5; session_5; host_5; registry_5; worker_5; }
  { rank=same; cli_6; store_6; session_6; host_6; registry_6; worker_6; }
  { rank=same; cli_7; store_7; session_7; host_7; registry_7; worker_7; }

  edge [style=dashed, color="{{ edge_muted }}", arrowhead=none];
  cli -> cli_1 -> cli_2 -> cli_3 -> cli_4 -> cli_5 -> cli_6 -> cli_7;
  store -> store_1 -> store_2 -> store_3 -> store_4 -> store_5 -> store_6 -> store_7;
  session -> session_1 -> session_2 -> session_3 -> session_4 -> session_5 -> session_6 -> session_7;
  host -> host_1 -> host_2 -> host_3 -> host_4 -> host_5 -> host_6 -> host_7;
  registry -> registry_1 -> registry_2 -> registry_3 -> registry_4 -> registry_5 -> registry_6 -> registry_7;
  worker -> worker_1 -> worker_2 -> worker_3 -> worker_4 -> worker_5 -> worker_6 -> worker_7;

  edge [style=solid, color="{{ edge }}", arrowhead=normal, constraint=false];
  cli_1 -> store_1 [label="OpenControlSession"];
  store_2 -> session_2 [label="创建持久环境"];
  cli_3 -> store_3 [label="EvalControlSession(source)"];
  store_4 -> session_4 [label="按 session_id 串行执行"];
  session_5 -> host_5 [label="start(config)"];
  host_6 -> registry_6 [label="finalize + register"];
  registry_7 -> worker_7 [label="spawn(WorkerConfig)"];

  edge [color="{{ edge_muted }}", constraint=false];
  worker_7 -> registry_7 [label="heartbeat"];
}
```

## 例子