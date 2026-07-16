#!/usr/bin/env bash
# logkit 工作区打包（logen + logend、tools/*/ 二进制 → logkit/tools/bin/，以及根目录 install.sh）：
#   ./scripts/logkit-pack.sh           # 默认：linux x86_64 / aarch64 glibc 2.17 → dist/logkit-<triple>.tar.gz
#   ./scripts/logkit-pack.sh native    # 本机 target/release → dist/logkit/（不压缩）
#   ./scripts/logkit-pack.sh musl      # 旧 musl 静态链（与 CI 不一致；pullout dlopen 可能不可用）
#
# Linux 交叉（默认不依赖 Docker）：
# - 需 Zig + cargo-zigbuild，目标为 *.gnu.2.17（与 GitHub Release CI 一致，兼容 CentOS 7）。
#     1) 安装 Zig https://ziglang.org/download/ 并加入 PATH
#     2) cargo install cargo-zigbuild
#     3) rustup target add x86_64-unknown-linux-gnu aarch64-unknown-linux-gnu
# - macOS / Linux 均用 zigbuild 编 .2.17（Linux 上勿用系统 gcc，否则会链到新 glibc）。
#
# 环境变量 LOGKIT_PACK_LINKER（可选）：
#   auto   — 默认；linux-gnu / musl 均 zigbuild（Darwin 亦同）
#   zig    — 强制 cargo zigbuild
#   cargo  — 强制 cargo build（仅 native 或已自配链接器时合理）
#   cross  — 强制 cross build（需 Docker）
#
# 环境变量 LOGKIT_PACK_PROFILE（可选，默认 release）：
#   release       — 发行默认（strip、无 debug）
#   release-perf  — perf/BCC 火焰图：保留符号 + 自动追加 force-frame-pointers
#   dev           — 调试构建
#
# 示例：
#   LOGKIT_PACK_PROFILE=release-perf ./scripts/logkit-pack.sh gnu
#
# macOS 上链 logend 若 ProcessFdQuotaExceeded：ulimit -n 65536 后再打包。
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

MODE="${1:-gnu}"
LOGKIT_PACK_PROFILE="${LOGKIT_PACK_PROFILE:-release}"

validate_pack_profile() {
  case "$LOGKIT_PACK_PROFILE" in
    release|release-perf|dev) ;;
    *)
      echo "error: LOGKIT_PACK_PROFILE 须为 release|release-perf|dev（当前: $LOGKIT_PACK_PROFILE）" >&2
      exit 2
      ;;
  esac
}

prepare_pack_profile() {
  validate_pack_profile
  apply_pack_profile_rustflags
}

apply_pack_profile_rustflags() {
  [[ "$LOGKIT_PACK_PROFILE" == "release-perf" ]] || return 0
  if [[ -z "${RUSTFLAGS:-}" ]]; then
    export RUSTFLAGS="-C force-frame-pointers=yes"
    return 0
  fi
  export RUSTFLAGS="$RUSTFLAGS -C force-frame-pointers=yes"
}

cargo_profile_name() {
  echo "$LOGKIT_PACK_PROFILE"
}

artifact_profile_dir_name() {
  if [[ "$LOGKIT_PACK_PROFILE" == "dev" ]]; then
    echo "debug"
  else
    echo "$LOGKIT_PACK_PROFILE"
  fi
}

# 将仓库 bin/install.sh 拷到发行包 **logkit/** 根目录（sudo ./install.sh → /opt + systemd）。
copy_pack_install() {
  local dest_root="$1"
  cp "$ROOT/bin/install.sh" "$dest_root/install.sh"
  chmod +x "$dest_root/install.sh"
}

copy_pack_etc() {
  local dest_root="$1"
  mkdir -p "$dest_root/etc"
  cp -R "$ROOT/etc/." "$dest_root/etc/"
}

copy_pack_license() {
  local dest_root="$1"
  if [[ -f "$ROOT/LICENSE" ]]; then
    cp "$ROOT/LICENSE" "$dest_root/LICENSE"
  else
    echo "warning: 未找到 $ROOT/LICENSE（跳过拷入发行包）" >&2
  fi
}

TOOL_BINS=(kafka-protocol mysql_local pullout jumpserver)

copy_pack_tools() {
  local dest_root="$1"
  local release_dir="$2"
  local td="$dest_root/tools/bin"
  mkdir -p "$td"
  local b
  for b in "${TOOL_BINS[@]}"; do
    if [[ -f "$release_dir/$b" ]]; then
      cp "$release_dir/$b" "$td/"
      chmod +x "$td/$b"
    else
      echo "warning: 未找到 $release_dir/$b（跳过拷入 tools/bin）" >&2
    fi
  done
}
pack_native_dir() {
  prepare_pack_profile
  local profile
  profile="$(cargo_profile_name)"
  local artifact_profile
  artifact_profile="$(artifact_profile_dir_name)"
  cargo build --profile "$profile"
  local artifact_dir="$ROOT/target/$artifact_profile"
  local dist="$ROOT/dist/logkit"
  rm -rf "$dist"
  mkdir -p "$dist/bin" "$dist/etc"
  if [[ "$(uname -s)" == "Darwin" ]]; then
    export COPYFILE_DISABLE=1
  fi
  cp "$artifact_dir/logen" "$artifact_dir/logend" "$dist/bin/"
  copy_pack_tools "$dist" "$artifact_dir"
  copy_pack_etc "$dist"
  copy_pack_install "$dist"
  copy_pack_license "$dist"
  chmod +x "$dist/bin/logen" "$dist/bin/logend"
  echo "packed (profile=$LOGKIT_PACK_PROFILE) -> $dist"
}

raise_open_files_limit() {
  [[ "$(uname -s)" == "Darwin" ]] || return 0
  local cur want="${LOGKIT_PACK_NOFILE:-65536}"
  cur="$(ulimit -n 2>/dev/null || echo 256)"
  if [[ "$cur" == "unlimited" ]]; then
    return 0
  fi
  if [[ "$cur" -ge "$want" ]]; then
    return 0
  fi
  if ulimit -n "$want" 2>/dev/null; then
    echo "ulimit -n: $cur -> $(ulimit -n)"
    return 0
  fi
  for fallback in 16384 8192 4096; do
    if [[ "$cur" -lt "$fallback" ]] && ulimit -n "$fallback" 2>/dev/null; then
      echo "warning: ulimit -n: $cur -> $(ulimit -n)（未达 $want）" >&2
      return 0
    fi
  done
  echo "warning: 无法提高 ulimit -n（当前 $cur）；链 logend 可能失败 ProcessFdQuotaExceeded" >&2
}

need_zigbuild() {
  command -v zig >/dev/null 2>&1 || {
    cat >&2 <<'EOF'
error: 需要 Zig: https://ziglang.org/download/
  安装后加入 PATH，再执行: cargo install cargo-zigbuild
EOF
    exit 1
  }
  if ! cargo zigbuild --help &>/dev/null; then
    echo "error: 安装 cargo-zigbuild: cargo install cargo-zigbuild" >&2
    exit 1
  fi
}

preflight_linux_linker() {
  raise_open_files_limit
  local mode="${LOGKIT_PACK_LINKER:-auto}"
  case "${mode}" in
    cross)
      command -v cross >/dev/null 2>&1 || {
        echo "error: LOGKIT_PACK_LINKER=cross 但未找到 cross" >&2
        exit 1
      }
      ;;
    cargo)
      ;;
    zig|auto)
      need_zigbuild
      ;;
    *)
      echo "error: LOGKIT_PACK_LINKER 须为 auto|zig|cross|cargo" >&2
      exit 2
      ;;
  esac
}

run_build() {
  local target="$1"
  local mode="${LOGKIT_PACK_LINKER:-auto}"
  local profile
  profile="$(cargo_profile_name)"
  local -a jobs=()
  if [[ -n "${LOGKIT_PACK_JOBS:-}" ]]; then
    jobs=(-j "$LOGKIT_PACK_JOBS")
  fi
  case "${mode}" in
    cross)
      cross build --profile "$profile" --target "$target" "${jobs[@]}"
      ;;
    zig)
      cargo zigbuild --profile "$profile" --target "$target" "${jobs[@]}"
      ;;
    cargo)
      cargo build --profile "$profile" --target "$target" "${jobs[@]}"
      ;;
    auto)
      # gnu.2.17 / musl 交叉统一 zigbuild，避免链到宿主新 glibc
      cargo zigbuild --profile "$profile" --target "$target" "${jobs[@]}"
      ;;
  esac
}

# zigbuild 的 *.gnu.2.17 链的是老 glibc，但产物目录仍是 target/<base-triple>/<profile>/
target_artifact_dir() {
  local target="$1"
  local artifact_profile
  artifact_profile="$(artifact_profile_dir_name)"
  local dir="$ROOT/target/$target/$artifact_profile"
  if [[ -f "$dir/logen" ]]; then
    echo "$dir"
    return 0
  fi
  local base="${target%.2.*}"
  if [[ "$base" != "$target" ]]; then
    dir="$ROOT/target/$base/$artifact_profile"
    if [[ -f "$dir/logen" ]]; then
      echo "$dir"
      return 0
    fi
  fi
  echo "error: 未找到 $target profile=$LOGKIT_PACK_PROFILE 的产物（已查 target/$target/$artifact_profile 与 target/$base/$artifact_profile）" >&2
  exit 1
}

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

pack_linux_tarball() {
  local target="$1"
  local artifact_dir
  artifact_dir="$(target_artifact_dir "$target")"
  local parent="$ROOT/dist/.stage-$target"
  local stage="$parent/logkit"
  rm -rf "$parent"
  mkdir -p "$stage/bin" "$stage/etc"
  if [[ "$(uname -s)" == "Darwin" ]]; then
    export COPYFILE_DISABLE=1
  fi
  cp "$artifact_dir/logen" "$artifact_dir/logend" "$stage/bin/"
  copy_pack_tools "$stage" "$artifact_dir"
  copy_pack_etc "$stage"
  copy_pack_install "$stage"
  copy_pack_license "$stage"
  chmod +x "$stage/bin/logen" "$stage/bin/logend"
  find "$parent" -name '._*' -delete 2>/dev/null || true
  local out="$ROOT/dist/logkit-${target}.tar.gz"
  if [[ "$LOGKIT_PACK_PROFILE" != "release" ]]; then
    out="$ROOT/dist/logkit-${target}.${LOGKIT_PACK_PROFILE}.tar.gz"
  fi
  mkdir -p "$ROOT/dist"
  make_targz "$out" "$parent" "logkit"
  rm -rf "$parent"
  echo "$out"
}

ensure_rust_std() {
  local target="$1"
  # zig 的 .2.17 后缀目标仍依赖 rustup 的 gnu 三元组 std
  local rust_target="${target%%.2.*}"
  if ! command -v rustup >/dev/null 2>&1; then
    echo "error: 未找到 rustup，无法安装目标 $rust_target 的 rust-std。" >&2
    exit 1
  fi
  echo "rustup target add $rust_target ..."
  rustup target add "$rust_target"
}

pack_linux_targets() {
  local -a targets=("$@")
  prepare_pack_profile
  mkdir -p "$ROOT/dist"
  preflight_linux_linker
  echo "pack profile: $LOGKIT_PACK_PROFILE"
  local target
  for target in "${targets[@]}"; do
    ensure_rust_std "$target"
    echo "building $target (profile=$LOGKIT_PACK_PROFILE) ..."
    run_build "$target"
    pack_linux_tarball "$target"
  done
  echo "done."
}

case "$MODE" in
  native)
    pack_native_dir
    ;;
  gnu|glibc|linux)
    pack_linux_targets \
      x86_64-unknown-linux-gnu.2.17 \
      aarch64-unknown-linux-gnu.2.17
    ;;
  musl)
    echo "warning: musl 与 CI 不一致；pullout 可能无法 dlopen。建议: ./scripts/logkit-pack.sh gnu" >&2
    pack_linux_targets \
      x86_64-unknown-linux-musl \
      aarch64-unknown-linux-musl
    ;;
  *)
    echo "usage: $0 [gnu|musl|native]" >&2
    echo "  gnu   — glibc 2.17（默认，与 CI 一致）" >&2
    echo "  musl  — 旧 musl 静态链" >&2
    echo "  native — 本机构建（见 LOGKIT_PACK_PROFILE）" >&2
    echo "  LOGKIT_PACK_PROFILE=release|release-perf|dev（默认 release）" >&2
    exit 2
    ;;
esac
