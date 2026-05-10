# lspt

Rust 工作区：**lsptd**（gRPC over Unix 套接字的守护进程）与 **lspt**（命令行客户端），用于按模板生成日志并由 worker 子进程输出；含共享配置、proto 与模板运行时库。

## 构建

```bash
cargo build --release
```

产物：`target/release/lspt`、`target/release/lsptd`。

## 配置

通过 `--defaults-file` 或环境变量 `LSPT_DEFAULTS_FILE` 指定 TOML；与内嵌参考配置深度合并。常用项包括 `[common].tmp_dir`（单实例运行目录，内含 `lsptd.sock` / `lsptd.pid` / `lsptd.log`）、`[log_server]` 等。详见 `lspt-config/assets/conf.ref.toml`。

## 许可

本项目以 **GNU General Public License v3.0** 发布，全文见仓库根目录 [`LICENSE`](LICENSE)。
