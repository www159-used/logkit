# logkit

Rust 工作区：**logspout-daemon**（gRPC over Unix 套接字的守护进程）与 **logspout**（命令行客户端），用于按模板生成日志并由 worker 子进程输出；含 **logspout-config**、**logspout-proto** 与 **logspout-dsl**（模板运行时）。

## 构建

```bash
cargo build --release
```

产物：`target/release/logspout`、`target/release/logspout-daemon`。

## 配置

通过 `--defaults-file` 或环境变量 **`LOGSPOUT_DEFAULTS_FILE`** 指定 TOML；与内嵌参考配置深度合并。常用项包括 `[common].tmp_dir`（单实例运行目录，内含 `logspout-daemon.sock` / `logspout-daemon.pid` / `logspout-daemon.log`）、`[log_server]` 等。详见 `logspout-config/assets/conf.ref.toml`。

## 打包

本地 musl / native 目录包见 `scripts/logkit-pack.sh`（环境变量 **`LOGKIT_PACK_LINKER`** 等）。

## 许可

本项目以 **GNU General Public License v3.0** 发布，全文见仓库根目录 [`LICENSE`](LICENSE)。
