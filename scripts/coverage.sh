#!/usr/bin/env bash
# 工作区覆盖率：生成 LCOV + 终端摘要；可选 HTML 报告。
#
#   ./scripts/coverage.sh              # lcov.info + summary
#   ./scripts/coverage.sh --html       # 另开 target/llvm-cov/html/index.html
#   ./scripts/coverage.sh --open-html  # 生成后用系统默认浏览器打开 HTML
#
# 依赖：rustup component add llvm-tools-preview
#       cargo install cargo-llvm-cov --locked

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

need() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "coverage.sh: 需要命令: $1" >&2
    exit 1
  }
}

need cargo
need rustup

if ! rustup component list --installed | grep -q '^llvm-tools'; then
  echo "coverage.sh: 安装 llvm-tools-preview …" >&2
  rustup component add llvm-tools-preview
fi

if ! cargo llvm-cov --version >/dev/null 2>&1; then
  echo "coverage.sh: 安装 cargo-llvm-cov …" >&2
  cargo install cargo-llvm-cov --locked
fi

IGNORE='(^|/)(tests|benches|examples)/|/target/|\.cargo/registry/'
COMMON=(
  --workspace
  --all-targets
  --ignore-filename-regex
  "$IGNORE"
)

HTML=0
OPEN_HTML=0
for arg in "$@"; do
  case "$arg" in
    --html) HTML=1 ;;
    --open-html) HTML=1; OPEN_HTML=1 ;;
    -h|--help)
      sed -n '2,12p' "$0"
      exit 0
      ;;
    *)
      echo "coverage.sh: 未知参数: $arg（支持 --html、--open-html）" >&2
      exit 1
      ;;
  esac
done

echo "coverage.sh: 生成 lcov.info …" >&2
cargo llvm-cov "${COMMON[@]}" --lcov --output-path lcov.info

echo "coverage.sh: 摘要 …" >&2
cargo llvm-cov report --summary-only --ignore-filename-regex "$IGNORE"

if [[ "$HTML" -eq 1 ]]; then
  echo "coverage.sh: HTML 报告 …" >&2
  cargo llvm-cov "${COMMON[@]}" --html
  REPORT="$ROOT/target/llvm-cov/html/index.html"
  echo "coverage.sh: $REPORT" >&2
  if [[ "$OPEN_HTML" -eq 1 ]] && command -v open >/dev/null 2>&1; then
    open "$REPORT"
  fi
fi

echo "coverage.sh: 完成 → lcov.info" >&2
