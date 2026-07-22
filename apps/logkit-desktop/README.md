# Logkit Desktop（Tauri）

## Icons

仓库里**只跟踪**源图：

- `src-tauri/icons/icon-legacy.png` — 原始素材
- `src-tauri/icons/icon-source.png` — 由脚本裁切/圆角后的 1024×1024 输入

其余 `icons/**`（`.icns`、`.ico`、`ios/`、`android/` 等）在 bundle 前生成，已写入 `.gitignore`。

换图标时：

```bash
# 1. 替换 icon-legacy.png
# 2. 再生各平台尺寸
bash apps/logkit-desktop/scripts/prepare-icons.sh
```

`cargo tauri build` 的 `beforeBuildCommand` 会在打 release 包前自动执行上述脚本（在 leptos build 之前）。

## gen/schemas

`src-tauri/gen/` 为 Tauri 构建产物（capabilities / ACL JSON Schema），本地 build 自动生成，**不提交**。`capabilities/default.json` 里的 `$schema` 仍指向该目录，仅供编辑器补全。
