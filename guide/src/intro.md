# logkit 介绍

本项目旨在解放 heka 的奴役，以及对旧工具的改造。

增加一些趣味性功能和实验，研究工程领域的惯性。

**thanks to: [logspout](https://github.com/jiwen624/logspout)**（本仓库已更名为 logen，与上游项目无隶属关系）

许可：**AGPL-3.0**（见 [`LICENSE`](../../LICENSE)）。

## 文档

在线阅读：**<https://www159-used.github.io/logkit/>**（推送到 `master` 后由 GitHub Actions 自动发布）

本地构建与预览：

```bash
cd guide && mdbook build
# 或：./scripts/serve-guide.sh
```

含架构图时需安装 **[mdbook-modern-dot](https://github.com/www159-used/mdbook-modern-dot)** 与系统 **Graphviz**（`dot`）。

- [logen-model 配置规范](logen-model/intro.md)