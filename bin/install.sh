#!/usr/bin/env bash
# 解压发行包得到 **logkit/** 目录后，进入该目录执行：**./install.sh**
# 默认在 **~/.bashrc** 末尾追加 **export PATH="<本目录>/bin:<本目录>/tools/bin:${PATH}"**。
#
# 环境变量（可选）：
#   LOGKIT_RC  — 要修改的配置文件，默认 ~/.bashrc
#   LOGKIT_DRY — 设为 1 时只打印将要追加的内容，不写文件

set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]:-$0}")" && pwd)"
RC="${LOGKIT_RC:-$HOME/.bashrc}"
MARKER="# logkit PATH (bin/install.sh)"

if [[ ! -d "$ROOT/bin" ]] || [[ ! -x "$ROOT/bin/logspout" ]]; then
  echo "error: 未找到 $ROOT/bin/logspout（请在解压后的 logkit 根目录执行 ./install.sh）" >&2
  exit 1
fi

block=$(
  printf '\n%s\n' "$MARKER"
  printf 'export PATH="%s/bin:%s/tools/bin:${PATH}"\n' "$ROOT" "$ROOT"
)

# 已有主标记但缺 tools/bin（旧版 install）：只补一行。
if [[ -f "$RC" ]] && grep -qF "$MARKER" "$RC" 2>/dev/null; then
  if ! grep -qF "$ROOT/tools/bin" "$RC" 2>/dev/null; then
    {
      printf '\n# logkit tools/bin (supplement)\n'
      printf 'export PATH="%s/tools/bin:${PATH}"\n' "$ROOT"
    } >>"$RC"
    echo "已在 $RC 中补充 tools/bin；请执行: source $RC"
  else
    echo "已配置过（$RC 中已有 $MARKER 与 tools/bin），跳过。"
  fi
  exit 0
fi

if [[ "${LOGKIT_DRY:-}" == "1" ]]; then
  printf 'would append to %s:\n%s' "$RC" "$block"
  exit 0
fi
[[ -f "$RC" ]] || touch "$RC"
printf '%s' "$block" >>"$RC"
echo "已追加到 $RC；请执行: source $RC 或重新打开终端。"
