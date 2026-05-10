#!/usr/bin/env bash
# 打包发布：
#   ./scripts/pack.sh           # 默认：linux x86_64 / aarch64 musl → dist/lspt-bundle-<triple>.tar.gz
#   ./scripts/pack.sh native    # 本机 target/release → dist/lspt-bundle/（不压缩）
#
# musl 交叉（默认不依赖 Docker）：
# - macOS：默认用 Zig + cargo-zigbuild（本机 Apple ld 无法链 linux-musl）。
#     1) 安装 Zig https://ziglang.org/download/ 并加入 PATH
#     2) cargo install cargo-zigbuild
# - Linux：默认直接 cargo build（建议安装 musl 工具链，如 musl-tools / 发行版对应包）
#
# 环境变量 LSPT_PACK_LINKER（可选）：
#   auto   — 默认；Darwin 用 zigbuild，Linux 用 cargo
#   zig    — 强制 cargo zigbuild（全平台）
#   cargo  — 强制 cargo build；在 macOS 上通常会链接失败，除非自配 .cargo 链接器
#   cross  — 强制 cross build（需 Docker；仅在你显式选择时使用）
#
# macOS + 已自配 musl 链接器：LSPT_PACK_LINKER=cargo LSPT_PACK_ALLOW_HOST_MUSL=1 ./scripts/pack.sh musl
#
# 打包机在 macOS 时建议安装 **gtar**（brew install gnu-tar），脚本优先用它打 tar.gz，避免 bsdtar 写入
# xattr / AppleDouble（._*），Linux 解压就不会有 LIBARCHIVE.xattr… 告警。
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

MODE="${1:-musl}"

pack_native_dir() {
  cargo build --release
  local dist="$ROOT/dist/lspt-bundle"
  rm -rf "$dist"
  mkdir -p "$dist/bin" "$dist/etc"
  if [[ "$(uname -s)" == "Darwin" ]]; then
    export COPYFILE_DISABLE=1
  fi
  cp "$ROOT/target/release/lspt" "$ROOT/target/release/lsptd" "$dist/bin/"
  cp "$ROOT/etc/"* "$dist/etc/"
  chmod +x "$dist/bin/lspt" "$dist/bin/lsptd"
  echo "packed -> $dist"
}

need_zigbuild() {
  command -v zig >/dev/null 2>&1 || {
    cat >&2 <<'EOF'
error: 需要 Zig（无需 Docker）: https://ziglang.org/download/
  安装后将 zig 加入 PATH，再执行: cargo install cargo-zigbuild
EOF
    exit 1
  }
  if ! cargo zigbuild --help &>/dev/null; then
    echo "error: 安装 cargo-zigbuild: cargo install cargo-zigbuild" >&2
    exit 1
  fi
}

preflight_musl_linker() {
  local mode="${LSPT_PACK_LINKER:-auto}"
  case "${mode}" in
    cross)
      command -v cross >/dev/null 2>&1 || {
        echo "error: LSPT_PACK_LINKER=cross 但未找到 cross（cargo install cross --git https://github.com/cross-rs/cross）" >&2
        exit 1
      }
      ;;
    zig)
      need_zigbuild
      ;;
    cargo)
      case "$(uname -s)" in
        Darwin)
          if [[ "${LSPT_PACK_ALLOW_HOST_MUSL:-}" != "1" ]]; then
            echo "error: macOS 上 LSPT_PACK_LINKER=cargo 几乎必失败；改用默认（zig）或设 LSPT_PACK_ALLOW_HOST_MUSL=1（已自配链接器）" >&2
            exit 1
          fi
          ;;
      esac
      ;;
    auto)
      case "$(uname -s)" in
        Darwin)
          need_zigbuild
          ;;
      esac
      ;;
    *)
      echo "error: LSPT_PACK_LINKER 须为 auto|zig|cross|cargo" >&2
      exit 2
      ;;
  esac
}

run_build() {
  local target="$1"
  local mode="${LSPT_PACK_LINKER:-auto}"
  case "${mode}" in
    cross)
      cross build --release --target "$target"
      ;;
    zig)
      cargo zigbuild --release --target "$target"
      ;;
    cargo)
      cargo build --release --target "$target"
      ;;
    auto)
      case "$(uname -s)" in
        Darwin)
          cargo zigbuild --release --target "$target"
          ;;
        *)
          cargo build --release --target "$target"
          ;;
      esac
      ;;
  esac
}

# 优先 gtar（GNU）；带 --numeric-owner 便于 Linux 解压无归属告警。
make_targz() {
  local out="$1"
  local parent="$2"
  local inner="$3"
  local tarbin=tar
  if command -v gtar >/dev/null 2>&1; then
    tarbin=gtar
  fi
  if "$tarbin" --version 2>&1 | head -1 | grep -qi gnu; then
    "$tarbin" -czf "$out" -C "$parent" --owner=0 --group=0 --numeric-owner "$inner"
  else
    COPYFILE_DISABLE=1 "$tarbin" -czf "$out" -C "$parent" "$inner"
  fi
}

pack_musl_tarball() {
  local target="$1"
  local parent="$ROOT/dist/.stage-$target"
  local stage="$parent/lspt-bundle"
  rm -rf "$parent"
  mkdir -p "$stage/bin" "$stage/etc"
  if [[ "$(uname -s)" == "Darwin" ]]; then
    export COPYFILE_DISABLE=1
  fi
  cp "$ROOT/target/$target/release/lspt" "$ROOT/target/$target/release/lsptd" "$stage/bin/"
  cp "$ROOT/etc/"* "$stage/etc/"
  chmod +x "$stage/bin/lspt" "$stage/bin/lsptd"
  find "$parent" -name '._*' -delete 2>/dev/null || true
  local out="$ROOT/dist/lspt-bundle-${target}.tar.gz"
  mkdir -p "$ROOT/dist"
  make_targz "$out" "$parent" "lspt-bundle"
  rm -rf "$parent"
  echo "$out"
}

ensure_rust_std() {
  local target="$1"
  if ! command -v rustup >/dev/null 2>&1; then
    echo "error: 未找到 rustup，无法安装目标 $target 的 rust-std。" >&2
    exit 1
  fi
  echo "rustup target add $target ..."
  rustup target add "$target"
}

case "$MODE" in
  native)
    pack_native_dir
    ;;
  musl)
    mkdir -p "$ROOT/dist"
    preflight_musl_linker
    for target in x86_64-unknown-linux-musl aarch64-unknown-linux-musl; do
      ensure_rust_std "$target"
      echo "building $target ..."
      run_build "$target"
      pack_musl_tarball "$target"
    done
    echo "done."
    ;;
  *)
    echo "usage: $0 [musl|native]" >&2
    exit 2
    ;;
esac
