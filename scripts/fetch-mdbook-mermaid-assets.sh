#!/usr/bin/env bash
# 在 mdbook build 之前拉取 Mermaid 运行时（不提交进 Git）：
#   ./scripts/fetch-mdbook-mermaid-assets.sh
#   cd logen-dsl/guide && mdbook build
#
# 写入各子书 guide/ 目录：
#   mermaid.min.js、mermaid-init.js（与 mdbook-mermaid 发布包内 assets 一致）
#
# 环境变量：
#   MDBOOK_MERMAID_TAG — 默认 v0.17.0（对应 https://github.com/badboy/mdbook-mermaid 的 tag）
#   MDBOOK_MERMAID_USE_INSTALL=1 — 若已安装 mdbook-mermaid，改用其 install 子命令（会改 book.toml 时仅补缺）
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

TAG="${MDBOOK_MERMAID_TAG:-v0.17.0}"
ASSETS_BASE="https://raw.githubusercontent.com/badboy/mdbook-mermaid/${TAG}/src/bin/assets"
GUIDES=(logen-dsl/guide logen/guide)

need_cmd() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "fetch-mdbook-mermaid-assets: 需要命令: $1" >&2
    exit 1
  }
}

fetch_via_curl() {
  local dir="$1"
  need_cmd curl
  mkdir -p "$dir"
  echo "fetch-mdbook-mermaid-assets: ${dir}/ ← ${TAG} (curl)"
  curl -fsSL -o "${dir}/mermaid.min.js" "${ASSETS_BASE}/mermaid.min.js"
  curl -fsSL -o "${dir}/mermaid-init.js" "${ASSETS_BASE}/mermaid-init.js"
}

fetch_via_install() {
  local dir="$1"
  need_cmd mdbook-mermaid
  echo "fetch-mdbook-mermaid-assets: ${dir}/ ← mdbook-mermaid install"
  mdbook-mermaid install "$dir"
}

for guide in "${GUIDES[@]}"; do
  if [[ ! -f "${guide}/book.toml" ]]; then
    echo "fetch-mdbook-mermaid-assets: 跳过（无 book.toml）: ${guide}" >&2
    continue
  fi
  if [[ "${MDBOOK_MERMAID_USE_INSTALL:-}" == 1 ]] && command -v mdbook-mermaid >/dev/null 2>&1; then
    fetch_via_install "$guide"
  else
    fetch_via_curl "$guide"
  fi
done

echo "fetch-mdbook-mermaid-assets: 完成（${#GUIDES[@]} 个子书）"
