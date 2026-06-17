# 全局选项

所有子命令共享下列选项（由 **clap** 解析，放在子命令**之前**）：

```bash
logen [OPTIONS] <COMMAND>
```

## `--defaults-file PATH`

指定与 **`logend`** 共用的 TOML 配置文件；与内嵌默认配置**深度合并**，文件中的值覆盖默认值。

```bash
logen --defaults-file /path/to/logen.toml ping
logen --defaults-file /path/to/logen.toml start etc/json.file.yaml
```

## 环境变量 `LOGEN_DEFAULTS_FILE`

若设置且非空，等价于在命令行最前插入 `--defaults-file <PATH>`（由 **`logen-config`** 在进程启动时注入）。

```bash
export LOGEN_DEFAULTS_FILE=/etc/logkit/logen.toml
logen ping
```

## `-S` / `--sock PATH`

覆盖由 **`[logend].tmp_dir`** 推导的 Unix 套接字路径（默认 **`{tmp_dir}/logend.sock`**）。

当 CLI 与 daemon 使用不同 `tmp_dir`、或套接字不在默认位置时必须指定：

```bash
logen -S /var/run/logkit/logend.sock list
```

## 版本与帮助

```bash
logen --version
logen --help
logen start --help
```

本仓库 CLI **未启用** `help` 子命令（`disable_help_subcommand`）；请用 `--help`。

## 平台

**logen** 仅在 **Unix** 上编译运行（依赖 Unix domain socket）。非 Unix 平台构建时仅打印错误并退出。
