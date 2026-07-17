#!/usr/bin/env bash
# 打包 editors/vscode-logen → dist/logkit-logen-<ver>.vsix
# 版本须与根 Cargo.toml [workspace.package].version 一致（cargo release 会同步）。
#
#   ./scripts/vscode-logen-pack.sh
#   ./scripts/vscode-logen-pack.sh --install          # 打包后安装到 Cursor/VS Code
#   ./scripts/vscode-logen-pack.sh --out /tmp         # 指定输出目录（默认 $ROOT/dist）
#   ./scripts/vscode-logen-pack.sh --install --app cursor

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]:-$0}")/.." && pwd)"
EXT="$ROOT/editors/vscode-logen"
OUT_DIR="$ROOT/dist"
DO_INSTALL=0
APP="" # cursor | code；空则自动探测

die() {
  echo "error: $*" >&2
  exit 1
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --out)
      OUT_DIR="${2:-}"
      [[ -n "$OUT_DIR" ]] || die "--out 需要目录参数"
      shift 2
      ;;
    --install)
      DO_INSTALL=1
      shift
      ;;
    --app)
      APP="${2:-}"
      [[ "$APP" == "cursor" || "$APP" == "code" ]] || die "--app 应为 cursor 或 code"
      shift 2
      ;;
    -h | --help)
      sed -n '2,9p' "$0" | sed 's/^# \{0,1\}//'
      exit 0
      ;;
    *)
      die "未知参数: $1"
      ;;
  esac
done

[[ -f "$EXT/package.json" ]] || die "未找到 $EXT/package.json"
[[ -f "$ROOT/Cargo.toml" ]] || die "未找到 $ROOT/Cargo.toml"

ws=$(sed -n 's/^version = "\([^"]*\)"/\1/p' "$ROOT/Cargo.toml" | head -1)
ext=$(sed -n 's/^[[:space:]]*"version": "\([^"]*\)".*/\1/p' "$EXT/package.json" | head -1)
[[ -n "$ws" && -n "$ext" ]] || die "无法读取 workspace / extension 版本"
[[ "$ws" == "$ext" ]] || die "版本不一致：workspace=$ws extension=$ext（先 cargo release 或手动对齐）"

if ! command -v vsce >/dev/null 2>&1; then
  if command -v npx >/dev/null 2>&1; then
    VSCE=(npx --yes @vscode/vsce)
  else
    die "需要 vsce 或 npx（npm i -g @vscode/vsce）"
  fi
else
  VSCE=(vsce)
fi

mkdir -p "$OUT_DIR"
echo "packing vscode-logen $ext …"
(
  cd "$EXT"
  rm -f "logen-${ext}.vsix"
  "${VSCE[@]}" package --no-dependencies
  [[ -f "logen-${ext}.vsix" ]] || die "vsce 未产出 logen-${ext}.vsix"
)

dest="$OUT_DIR/logkit-logen-${ext}.vsix"
mv -f "$EXT/logen-${ext}.vsix" "$dest"
echo "packed -> $dest"

if [[ "$DO_INSTALL" -eq 1 ]]; then
  if [[ -z "$APP" ]]; then
    if command -v cursor >/dev/null 2>&1; then
      APP=cursor
    elif command -v code >/dev/null 2>&1; then
      APP=code
    else
      die "未找到 cursor / code CLI（Cursor: Command Palette → Install 'cursor' command）"
    fi
  fi
  command -v "$APP" >/dev/null 2>&1 || die "未找到命令: $APP"
  # 同版本覆盖需 --force；装完后 Reload Window 使语法生效
  echo "installing into $APP …"
  "$APP" --install-extension "$dest" --force
  echo "installed logkit.logen@$ext — 在编辑器里执行 Developer: Reload Window"
fi
