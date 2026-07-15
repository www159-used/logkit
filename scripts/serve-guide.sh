#!/usr/bin/env bash
# 构建并本地预览仓库根 mdbook（含 logen-model 等章节）。
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT}"

echo "serve-guide: 构建 guide …"
(cd guide && mdbook build)

echo "serve-guide: http://127.0.0.1:3000"
exec bash -c 'cd guide && mdbook serve --hostname 127.0.0.1 --port 3000 "$@"' _ "$@"
