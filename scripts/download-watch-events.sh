#!/usr/bin/env bash
#
# 管理 GH Archive WatchEvent 的长期 BigQuery 下载任务。
#
# 任务使用 screen 脱离当前终端，并由 caffeinate 阻止 macOS 在下载期间自动休眠。
# 启动前必须确认 T0 是真实挂载卷，避免移动硬盘掉线后误把数据写入本机 /Volumes/T0 目录。

set -euo pipefail

readonly SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
readonly REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
readonly SCRIPT_PATH="$SCRIPT_DIR/$(basename "${BASH_SOURCE[0]}")"
readonly SESSION_NAME="${STARCAT_WATCH_SESSION_NAME:-starcat-watch-events}"
readonly VOLUME_ROOT="${STARCAT_WATCH_VOLUME_ROOT:-/Volumes/T0}"
readonly WORKSPACE="${STARCAT_WATCH_WORKSPACE:-$VOLUME_ROOT/Starcat/bigquery/watch-events-2016-2026}"
readonly LOG_FILE="${STARCAT_WATCH_LOG_FILE:-$VOLUME_ROOT/Starcat/bigquery/watch-events-2016-2026.log}"
readonly BILLING_PROJECT="${STARCAT_BQ_BILLING_PROJECT:-glassy-landing-454512-p9}"
readonly START_DATE="${STARCAT_WATCH_START_DATE:-2016-01-01}"
readonly END_DATE="${STARCAT_WATCH_END_DATE:-2026-08-25}"
readonly MAXIMUM_BYTES_PER_DAY="${STARCAT_WATCH_MAXIMUM_BYTES_PER_DAY:-1073741824}"
readonly MAXIMUM_TOTAL_BYTES="${STARCAT_WATCH_MAXIMUM_TOTAL_BYTES:-600000000000}"
readonly RUN_ID="${STARCAT_WATCH_RUN_ID:-gharchive-watch-2016-2026}"
readonly CLI="$REPO_ROOT/.venv/bin/starcat-recsys"
readonly STATE_FILE="$WORKSPACE/raw/gh_archive/download-state.json"
readonly RUNTIME_DIRECTORY="${TMPDIR:-/tmp}"
readonly PID_FILE="${STARCAT_WATCH_PID_FILE:-${RUNTIME_DIRECTORY%/}/starcat-watch-events.pid}"

usage() {
  cat <<'EOF'
用法：
  ./scripts/download-watch-events.sh start    启动后台下载
  ./scripts/download-watch-events.sh stop     停止后台下载
  ./scripts/download-watch-events.sh restart  重新启动后台下载
  ./scripts/download-watch-events.sh status   查看进程和 checkpoint
  ./scripts/download-watch-events.sh logs     持续查看日志

可通过 STARCAT_BQ_BILLING_PROJECT、STARCAT_WATCH_WORKSPACE 等环境变量覆盖默认配置。
EOF
}

has_screen_session() {
  local sessions
  # macOS 自带的 screen 即使列出了会话也可能返回非零状态，因此不能在 pipefail 下
  # 直接把 `screen -ls` 接给 grep，否则会把真实运行中的任务误判为已停止。
  sessions="$(/usr/bin/screen -ls 2>/dev/null || true)"
  /usr/bin/grep -Eq \
    "[0-9]+\.${SESSION_NAME}[[:space:]]+\((Detached|Attached)\)" \
    <<<"$sessions"
}

worker_pid() {
  [[ -f "$PID_FILE" ]] || return 1
  local pid
  pid="$(<"$PID_FILE")"
  [[ "$pid" =~ ^[0-9]+$ ]] || return 1
  /bin/kill -0 "$pid" 2>/dev/null || return 1

  # PID 文件可能因异常退出而残留。校验命令行后才允许把该 PID 当作下载任务，
  # 防止 PID 被系统复用时误伤无关进程。
  /bin/ps -p "$pid" -o command= 2>/dev/null \
    | /usr/bin/grep -Fq "$CLI bigquery download-watch-events --workspace $WORKSPACE" \
    || return 1
  printf '%s\n' "$pid"
}

is_running() {
  has_screen_session || worker_pid >/dev/null
}

require_t0_mount() {
  if [[ ! -d "$VOLUME_ROOT" ]] \
    || ! /sbin/mount | /usr/bin/grep -Fq " on $VOLUME_ROOT "; then
    echo "错误：$VOLUME_ROOT 未作为独立磁盘挂载，拒绝启动，避免写满本机磁盘。" >&2
    exit 1
  fi
}

require_runtime() {
  if [[ ! -x "$CLI" ]]; then
    echo "错误：未找到 $CLI，请先在项目根目录执行 uv sync --all-groups。" >&2
    exit 1
  fi
}

start_download() {
  require_t0_mount
  require_runtime

  if is_running; then
    echo "下载任务已在运行：$SESSION_NAME"
    return
  fi

  /bin/mkdir -p "$WORKSPACE" "$(dirname "$LOG_FILE")"

  /bin/rm -f "$PID_FILE"
  /usr/bin/screen -dmS "$SESSION_NAME" "$SCRIPT_PATH" _run
  /bin/sleep 1

  if ! is_running; then
    echo "错误：下载任务启动失败，请检查日志：$LOG_FILE" >&2
    /usr/bin/tail -n 20 "$LOG_FILE" 2>/dev/null || true
    exit 1
  fi

  echo "下载任务已启动：$SESSION_NAME"
  echo "数据目录：$WORKSPACE"
  echo "日志文件：$LOG_FILE"
}

stop_download() {
  if ! is_running; then
    echo "下载任务未运行：$SESSION_NAME"
    return
  fi

  local pid=""
  pid="$(worker_pid || true)"
  if [[ -n "$pid" ]]; then
    /bin/kill -TERM "$pid" 2>/dev/null || true
  fi
  if has_screen_session; then
    /usr/bin/screen -S "$SESSION_NAME" -X quit 2>/dev/null || true
  fi

  # 给 BigQuery 客户端释放连接和临时资源的时间；超时后只终止 PID 文件中经过
  # 命令行校验的下载进程，不使用宽泛 pkill。
  local attempt
  for attempt in 1 2 3 4 5; do
    worker_pid >/dev/null || break
    /bin/sleep 1
  done
  pid="$(worker_pid || true)"
  if [[ -n "$pid" ]]; then
    /bin/kill -KILL "$pid" 2>/dev/null || true
  fi
  /bin/rm -f "$PID_FILE"
  echo "下载任务已停止：$SESSION_NAME"
}

show_status() {
  if is_running; then
    echo "运行状态：运行中（screen: ${SESSION_NAME}）"
  else
    echo "运行状态：已停止（screen: ${SESSION_NAME}）"
  fi

  echo "数据目录：$WORKSPACE"
  echo "日志文件：$LOG_FILE"
  local pid=""
  pid="$(worker_pid || true)"
  [[ -n "$pid" ]] && echo "下载进程 PID：$pid"

  if [[ -f "$STATE_FILE" ]]; then
    "$REPO_ROOT/.venv/bin/python" - "$STATE_FILE" <<'PY'
import json
import sys
from pathlib import Path

state = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
partitions = state.get("completed_partitions", {})
last_partition = max(partitions) if partitions else "无"
print(f"已完成分区：{len(partitions)}")
print(f"最后分区：{last_partition}")
print(f"累计预计扫描字节：{state.get('estimated_total_bytes', 0)}")
PY
  else
    echo "checkpoint：尚未生成"
  fi
}

follow_logs() {
  require_t0_mount
  /usr/bin/touch "$LOG_FILE"
  exec /usr/bin/tail -n 100 -F "$LOG_FILE"
}

run_worker() {
  require_t0_mount
  require_runtime

  local child_pid
  /usr/bin/env PYTHONUNBUFFERED=1 \
    /usr/bin/caffeinate -ims \
    "$CLI" bigquery download-watch-events \
    --workspace "$WORKSPACE" \
    --billing-project "$BILLING_PROJECT" \
    --start-date "$START_DATE" \
    --end-date "$END_DATE" \
    --maximum-bytes-billed-per-day "$MAXIMUM_BYTES_PER_DAY" \
    --maximum-total-bytes-billed "$MAXIMUM_TOTAL_BYTES" \
    --run-id "$RUN_ID" \
    >>"$LOG_FILE" 2>&1 &
  child_pid=$!
  printf '%s\n' "$child_pid" >"$PID_FILE"

  terminate_child() {
    /bin/kill -TERM "$child_pid" 2>/dev/null || true
  }
  cleanup_pid_file() {
    /bin/rm -f "$PID_FILE"
  }
  trap terminate_child HUP INT TERM
  trap cleanup_pid_file EXIT

  set +e
  wait "$child_pid"
  local exit_code=$?
  set -e
  exit "$exit_code"
}

case "${1:-}" in
  start)
    start_download
    ;;
  stop)
    stop_download
    ;;
  restart)
    stop_download
    start_download
    ;;
  status)
    show_status
    ;;
  logs)
    follow_logs
    ;;
  _run)
    run_worker
    ;;
  -h|--help|help)
    usage
    ;;
  *)
    usage >&2
    exit 2
    ;;
esac
