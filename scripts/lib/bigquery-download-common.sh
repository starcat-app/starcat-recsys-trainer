#!/usr/bin/env bash
# BigQuery 长期下载脚本的共享生命周期、额度监控和磁盘保护。
# 本文件由 WatchEvent / PushEvent 入口 source，不应直接执行。

readonly REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
readonly SCRIPT_PATH="$SCRIPT_DIR/$(basename "${BASH_SOURCE[1]}")"
readonly BILLING_PROJECT="${STARCAT_BQ_BILLING_PROJECT:-glassy-landing-454512-p9}"
readonly LOCATION="${STARCAT_BQ_LOCATION:-US}"
readonly QUOTA_WARNING_PERCENT="${STARCAT_BQ_QUOTA_WARNING_PERCENT:-80}"
readonly QUOTA_STOP_PERCENT="${STARCAT_BQ_QUOTA_STOP_PERCENT:-90}"
readonly QUOTA_LOG_INTERVAL_SECONDS="${STARCAT_BQ_QUOTA_LOG_INTERVAL_SECONDS:-1800}"
readonly CLI="$REPO_ROOT/.venv/bin/starcat-recsys"
readonly STATE_FILE="$WORKSPACE/$STATE_RELATIVE_PATH"
readonly RUNTIME_DIRECTORY="${TMPDIR:-/tmp}"
readonly PID_FILE="${RUNTIME_DIRECTORY%/}/${PID_BASENAME}.pid"
readonly MONITOR_PID_FILE="${RUNTIME_DIRECTORY%/}/${PID_BASENAME}-quota.pid"

usage() {
  cat <<EOF
用法：
  ./scripts/$(basename "$SCRIPT_PATH") start    启动后台下载
  ./scripts/$(basename "$SCRIPT_PATH") stop     停止后台下载
  ./scripts/$(basename "$SCRIPT_PATH") restart  重新启动后台下载
  ./scripts/$(basename "$SCRIPT_PATH") status   查看进程、checkpoint 和额度
  ./scripts/$(basename "$SCRIPT_PATH") logs     持续查看日志

默认每 ${QUOTA_LOG_INTERVAL_SECONDS} 秒记录一次本月 Project 额度；达到
${QUOTA_WARNING_PERCENT}% 警告，达到 ${QUOTA_STOP_PERCENT}% 停止当前下载任务。
EOF
}

has_screen_session() {
  local sessions
  # macOS screen 列出会话时仍可能返回非零，先捕获文本再匹配，避免 pipefail 误判。
  sessions="$(/usr/bin/screen -ls 2>/dev/null || true)"
  /usr/bin/grep -Eq \
    "[0-9]+\.${SESSION_NAME}[[:space:]]+\((Detached|Attached)\)" \
    <<<"$sessions"
}

validated_pid() {
  local file="$1"
  local command_fragment="$2"
  [[ -f "$file" ]] || return 1
  local pid
  pid="$(<"$file")"
  [[ "$pid" =~ ^[0-9]+$ ]] || return 1
  /bin/kill -0 "$pid" 2>/dev/null || return 1
  /bin/ps -p "$pid" -o command= 2>/dev/null \
    | /usr/bin/grep -Fq "$command_fragment" \
    || return 1
  printf '%s\n' "$pid"
}

worker_pid() {
  validated_pid "$PID_FILE" "$CLI bigquery $COMMAND_NAME --workspace $WORKSPACE"
}

monitor_pid() {
  validated_pid "$MONITOR_PID_FILE" "$SCRIPT_PATH _quota_monitor"
}

is_running() {
  has_screen_session || worker_pid >/dev/null
}

require_external_mount() {
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

quota_check() {
  local output
  local exit_code
  set +e
  output="$(
    /usr/bin/env GOOGLE_CLOUD_PROJECT="$BILLING_PROJECT" \
      "$CLI" bigquery quota-usage \
      --billing-project "$BILLING_PROJECT" \
      --location "$LOCATION" \
      --warning-percent "$QUOTA_WARNING_PERCENT" \
      --stop-percent "$QUOTA_STOP_PERCENT" 2>&1
  )"
  exit_code=$?
  set -e
  printf '%s INFO [%s] %s\n' "$(date '+%F %T')" "$EVENT_LABEL" "$output" \
    | /usr/bin/tee -a "$LOG_FILE"
  return "$exit_code"
}

start_download() {
  require_external_mount
  require_runtime
  /bin/mkdir -p "$WORKSPACE" "$(dirname "$LOG_FILE")"

  if is_running; then
    echo "下载任务已在运行：$SESSION_NAME"
    return
  fi

  local quota_exit_code
  set +e
  quota_check
  quota_exit_code=$?
  set -e
  if [[ "$quota_exit_code" -eq 3 ]]; then
    echo "错误：BigQuery 月度用量已达到停止阈值，拒绝启动 $EVENT_LABEL。" >&2
    exit 1
  elif [[ "$quota_exit_code" -ne 0 ]]; then
    echo "错误：无法读取 BigQuery 月度额度，拒绝启动 $EVENT_LABEL。" >&2
    exit 1
  fi

  /bin/rm -f "$PID_FILE" "$MONITOR_PID_FILE"
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

terminate_validated_pid() {
  local pid="$1"
  [[ -n "$pid" ]] && /bin/kill -TERM "$pid" 2>/dev/null || true
}

stop_download() {
  if ! is_running && ! monitor_pid >/dev/null; then
    echo "下载任务未运行：$SESSION_NAME"
    return
  fi

  terminate_validated_pid "$(monitor_pid || true)"
  terminate_validated_pid "$(worker_pid || true)"
  if has_screen_session; then
    /usr/bin/screen -S "$SESSION_NAME" -X quit 2>/dev/null || true
  fi

  local attempt
  for attempt in 1 2 3 4 5; do
    if ! worker_pid >/dev/null && ! monitor_pid >/dev/null; then
      break
    fi
    /bin/sleep 1
  done
  local pid
  pid="$(worker_pid || true)"
  [[ -n "$pid" ]] && /bin/kill -KILL "$pid" 2>/dev/null || true
  pid="$(monitor_pid || true)"
  [[ -n "$pid" ]] && /bin/kill -KILL "$pid" 2>/dev/null || true
  /bin/rm -f "$PID_FILE" "$MONITOR_PID_FILE"
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
  local pid
  pid="$(worker_pid || true)"
  [[ -n "$pid" ]] && echo "下载进程 PID：$pid"
  pid="$(monitor_pid || true)"
  [[ -n "$pid" ]] && echo "额度监控 PID：$pid"

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
  quota_check || true
}

follow_logs() {
  require_external_mount
  /usr/bin/touch "$LOG_FILE"
  exec /usr/bin/tail -n 100 -F "$LOG_FILE"
}

run_quota_monitor() {
  local watched_pid="${1:?缺少被监控的下载 PID}"
  printf '%s\n' "$$" >"$MONITOR_PID_FILE"
  trap '/bin/rm -f "$MONITOR_PID_FILE"' EXIT

  while /bin/kill -0 "$watched_pid" 2>/dev/null; do
    /bin/sleep "$QUOTA_LOG_INTERVAL_SECONDS"
    /bin/kill -0 "$watched_pid" 2>/dev/null || break
    local quota_exit_code
    set +e
    quota_check
    quota_exit_code=$?
    set -e
    if [[ "$quota_exit_code" -eq 3 ]]; then
      printf '%s WARNING [%s] 已达到 BigQuery 额度停止阈值，停止提交新分区。\n' \
        "$(date '+%F %T')" "$EVENT_LABEL" | /usr/bin/tee -a "$LOG_FILE"
      /bin/kill -TERM "$watched_pid" 2>/dev/null || true
      break
    elif [[ "$quota_exit_code" -ne 0 ]]; then
      printf '%s WARNING [%s] 额度查询失败，将在下一周期重试。\n' \
        "$(date '+%F %T')" "$EVENT_LABEL" | /usr/bin/tee -a "$LOG_FILE"
    fi
  done
}

run_worker() {
  require_external_mount
  require_runtime

  local child_pid
  local quota_pid
  /usr/bin/env PYTHONUNBUFFERED=1 GOOGLE_CLOUD_PROJECT="$BILLING_PROJECT" \
    /usr/bin/caffeinate -ims \
    "$CLI" bigquery "$COMMAND_NAME" \
    --workspace "$WORKSPACE" \
    --billing-project "$BILLING_PROJECT" \
    --start-date "$START_DATE" \
    --end-date "$END_DATE" \
    --maximum-bytes-billed-per-day "$MAXIMUM_BYTES_PER_DAY" \
    --maximum-total-bytes-billed "$MAXIMUM_TOTAL_BYTES" \
    --location "$LOCATION" \
    --run-id "$RUN_ID" \
    >>"$LOG_FILE" 2>&1 &
  child_pid=$!
  printf '%s\n' "$child_pid" >"$PID_FILE"
  "$SCRIPT_PATH" _quota_monitor "$child_pid" >>"$LOG_FILE" 2>&1 &
  quota_pid=$!

  terminate_children() {
    /bin/kill -TERM "$quota_pid" 2>/dev/null || true
    /bin/kill -TERM "$child_pid" 2>/dev/null || true
  }
  cleanup_pid_files() {
    /bin/rm -f "$PID_FILE" "$MONITOR_PID_FILE"
  }
  trap terminate_children HUP INT TERM
  trap cleanup_pid_files EXIT

  set +e
  wait "$child_pid"
  local exit_code=$?
  /bin/kill -TERM "$quota_pid" 2>/dev/null || true
  wait "$quota_pid" 2>/dev/null || true
  set -e
  exit "$exit_code"
}

bigquery_download_main() {
  case "${1:-}" in
    start) start_download ;;
    stop) stop_download ;;
    restart)
      stop_download
      start_download
      ;;
    status) show_status ;;
    logs) follow_logs ;;
    _run) run_worker ;;
    _quota_monitor) run_quota_monitor "${2:-}" ;;
    -h|--help|help) usage ;;
    *)
      usage >&2
      exit 2
      ;;
  esac
}
