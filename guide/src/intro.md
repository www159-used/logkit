# logkit介绍

本项目旨在解放heka的奴役，以及对旧工具的改造

**thanks to: [logspout](https://github.com/jiwen624/logspout)**（本仓库已更名为 logen，与上游项目无隶属关系）

许可：**GPL-3.0**（见仓库根目录 [`LICENSE`](../../LICENSE)）。

## 架构示意

### 图 1：协议和通信

```mermaid
flowchart TB
  subgraph d1_proto["logen-proto"]
    IDL["gRPC"]
  end
  subgraph d1_cfg["logen-config"]
    CFG["unix文件，log路径约定"]
  end
  subgraph d1_cli["logen"]
    CLI["CLI"]
  end
  subgraph d1_dm["logend"]
    DA["daemon"]
  end
  IDL -.->|IDL| CLI
  IDL -.->|IDL| DA
  CFG -.->|启动加载| DA
  CLI -->|gRPC / Unix 套接字| DA
  style d1_proto fill:#EDE7F6,stroke:#5E35B1,color:#1a1a1a
  style d1_cfg fill:#E8F5E9,stroke:#2E7D32,color:#1a1a1a
  style d1_cli fill:#E3F2FD,stroke:#1565C0,color:#1a1a1a
  style d1_dm fill:#FFF3E0,stroke:#EF6C00,color:#1a1a1a
```

### 图 2：启动流程

先起 **`logend`**，再用 **`logen start`** 下发任务；造数在 daemon **进程内**由 `logen-worker` 执行（非子进程）。

#### 2a 启动 logend

```mermaid
sequenceDiagram
  autonumber
  actor U as 用户
  participant CFG as logen-config
  participant D as logend

  U->>D: logend [--defaults-file]
  D->>CFG: load_merged(TOML)
  CFG-->>D: LogenConfig
  D->>D: 创建 tmp_dir、worker_output_dir
  D->>D: 绑定 {tmp_dir}/logend.sock
  D->>D: 写入 logend.pid
  Note over D: gRPC 监听 UDS，等待 logen 连接
```

#### 2b 启动造数（logen start）

```mermaid
sequenceDiagram
  autonumber
  actor U as 用户
  participant CLI as logen
  participant DSL as logen-dsl
  participant D as logend
  participant WK as logen-worker
  participant OUT as 输出

  U->>CLI: logen start producer.yaml
  CLI->>CLI: 读取 YAML
  CLI->>DSL: parse_template_config（本地校验）
  DSL-->>CLI: TemplateConfig
  CLI->>DSL: template_config_to_yaml
  CLI->>D: gRPC StartWorker(producer_yaml)
  D->>DSL: parse_template_config（再次校验）
  DSL-->>D: TemplateConfig
  D->>WK: spawn_producer_task（同进程 Tokio 任务）
  D-->>CLI: StartWorkerReply(id)

  par 造数循环
    loop 按 min-interval
      WK->>DSL: TemplateRunner 渲染一行
      WK->>OUT: sink 写出
    end
  and 心跳
    loop 按 heartbeat_interval
      WK->>D: gRPC Heartbeat(events_total)
    end
  end
```

## 模块文档

| 模块 | mdBook |
|------|--------|
| **logen-dsl** | [`logen-dsl/guide/book/index.html`](../../logen-dsl/guide/book/index.html) |