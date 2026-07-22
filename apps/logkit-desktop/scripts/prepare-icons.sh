#!/usr/bin/env bash
# 从 icon-legacy.png 生成 icon-source.png，再调用 tauri icon 写出各平台尺寸。
set -euo pipefail

ROOT="$(git rev-parse --show-toplevel)"
cd "$ROOT"

python3 apps/logkit-desktop/scripts/generate-macos-icon.py

cd apps/logkit-desktop/src-tauri
cargo tauri icon icons/icon-source.png
