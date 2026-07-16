#!/usr/bin/env bash
# 系统安装 / 升级：将发行包装到 **/opt/logkit**，并安装 logend / jumpserver 的 systemd unit。
# 解压后在 logkit 根目录以 root 执行：**sudo ./install.sh**
#
# 环境变量（可选）：
#   LOGKIT_PREFIX  — 安装根目录，默认 /opt/logkit（会改写 unit 中的路径）
#   LOGKIT_DRY     — 设为 1 时只打印将要执行的操作，不写文件

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]:-$0}")" && pwd)"
PREFIX="${LOGKIT_PREFIX:-/opt/logkit}"
DRY="${LOGKIT_DRY:-0}"
UNITS=(logend.service jumpserver.service)

die() {
  echo "error: $*" >&2
  exit 1
}

run() {
  if [[ "$DRY" == "1" ]]; then
    printf '+'
    printf ' %q' "$@"
    printf '\n'
    return 0
  fi
  "$@"
}

write_file() {
  local path="$1"
  local content="$2"
  if [[ "$DRY" == "1" ]]; then
    printf '+ write %q <<EOF\n%sEOF\n' "$path" "$content"
    return 0
  fi
  printf '%s' "$content" >"$path"
}

service_active() {
  command -v systemctl >/dev/null 2>&1 || return 1
  systemctl is-active --quiet "$1" 2>/dev/null
}

# 从 etc/systemd 模板生成 unit：替换 /opt/logkit → PREFIX；无 yotta 时去掉 User/Group。
render_unit() {
  local src="$1"
  local out
  out=$(sed "s|/opt/logkit|${PREFIX}|g" "$src")
  if [[ "$src" == *jumpserver.service ]] && ! id -u yotta >/dev/null 2>&1; then
    out=$(printf '%s\n' "$out" | sed -e '/^User=/d' -e '/^Group=/d')
  fi
  printf '%s\n' "$out"
}

[[ -x "$ROOT/bin/logen" ]] || die "未找到 $ROOT/bin/logen（请在解压后的 logkit 根目录执行）"
[[ -x "$ROOT/bin/logend" ]] || die "未找到 $ROOT/bin/logend"
[[ -f "$ROOT/etc/systemd/logend.service" ]] || die "未找到 $ROOT/etc/systemd/logend.service"
[[ -f "$ROOT/etc/systemd/jumpserver.service" ]] || die "未找到 $ROOT/etc/systemd/jumpserver.service"

if [[ "$(uname -s)" != "Linux" ]]; then
  die "仅支持 Linux（当前 $(uname -s)）"
fi
if [[ "$DRY" != "1" ]] && [[ "$(id -u)" -ne 0 ]]; then
  die "需要 root（请使用 sudo ./install.sh）"
fi

if ! id -u yotta >/dev/null 2>&1; then
  echo "warning: 本机无 yotta 用户，jumpserver.service 将以 root 运行" >&2
fi

restart_units=()
for u in "${UNITS[@]}"; do
  if service_active "$u"; then
    restart_units+=("$u")
    echo "检测到 $u 正在运行，先停止以便替换二进制 …"
    run systemctl stop "$u"
  fi
done

echo "安装到 $PREFIX …"
run mkdir -p "$PREFIX/bin" "$PREFIX/etc" "$PREFIX/tools/bin"
run cp -f "$ROOT/bin/logen" "$ROOT/bin/logend" "$PREFIX/bin/"
run chmod 755 "$PREFIX/bin/logen" "$PREFIX/bin/logend"

if [[ -d "$ROOT/etc" ]]; then
  run cp -R "$ROOT/etc/." "$PREFIX/etc/"
fi

if [[ -d "$ROOT/tools/bin" ]]; then
  for f in "$ROOT/tools/bin"/*; do
    [[ -f "$f" ]] || continue
    run cp -f "$f" "$PREFIX/tools/bin/"
    run chmod 755 "$PREFIX/tools/bin/$(basename "$f")"
  done
fi

if [[ ! -x "$PREFIX/tools/bin/jumpserver" ]] && [[ "$DRY" != "1" ]]; then
  echo "warning: 未找到 $PREFIX/tools/bin/jumpserver，仍会安装 jumpserver.service" >&2
fi

for u in "${UNITS[@]}"; do
  echo "安装 systemd unit → /etc/systemd/system/$u"
  write_file "/etc/systemd/system/$u" "$(render_unit "$ROOT/etc/systemd/$u")"
  run chmod 644 "/etc/systemd/system/$u"
done

if command -v systemctl >/dev/null 2>&1; then
  run systemctl daemon-reload
else
  echo "warning: 未找到 systemctl，跳过 daemon-reload" >&2
fi

if [[ ${#restart_units[@]} -gt 0 ]]; then
  for u in "${restart_units[@]}"; do
    echo "重新启动 $u …"
    run systemctl start "$u"
  done
  cat <<EOF
升级完成（已重启: ${restart_units[*]}）。

  systemctl status logend jumpserver
  journalctl -u logend -u jumpserver -f
  tail -f /root/.logkit/logend.log

注意：重启会中断当前 worker / 控制会话，需按需重新 start。
EOF
else
  cat <<EOF
安装完成。

  sudo systemctl enable --now logend jumpserver
  systemctl status logend jumpserver
  $PREFIX/bin/logen

程序：$PREFIX/{bin,tools/bin}
运行时默认：\$HOME/.logkit（root 服务通常为 /root/.logkit）
  sock / logend.log / output/

升级：解压新包后再次执行 sudo ./install.sh
EOF
fi
