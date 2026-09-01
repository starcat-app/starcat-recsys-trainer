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
readonly LAST_EXIT_FILE="${STARCAT_DOWNLOAD_LAST_EXIT_FILE:-${LOG_FILE}.last-exit.json}"
readonly STARTUP_READY_ATTEMPTS="${STARCAT_DOWNLOAD_STARTUP_READY_ATTEMPTS:-10}"
readonly STARTUP_READY_INTERVAL_SECONDS="${STARCAT_DOWNLOAD_STARTUP_READY_INTERVAL_SECONDS:-1}"

usage() {
  cat <<EOF
用法：
  ./scripts/$(basename "$SCRIPT_PATH") start    启动后台下载
  ./scripts/$(basename "$SCRIPT_PATH") stop     停止后台下载
  ./scripts/$(basename "$SCRIPT_PATH") restart  重新启动后台下载
  ./scripts/$(basename "$SCRIPT_PATH") catch-up 连续补齐到最新完整 UTC 日
  ./scripts/$(basename "$SCRIPT_PATH") status   查看进程、checkpoint 和额度
  ./scripts/$(basename "$SCRIPT_PATH") logs     持续查看日志

start / stop / restart / status 可追加 --json，供本机 Admin Console 的固定
Action Adapter 消费；JSON 不包含真实数据目录或日志路径。控制台批量读取状态时，
可对第二个任务使用 status --json --skip-quota，避免重复发起同一 Project 额度查询。

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
  # screen 只负责承载 wrapper；只有命令行和 PID 都通过校验的 worker 才代表真实下载。
  worker_pid >/dev/null
}

wait_for_worker_ready() {
  local attempt
  for ((attempt = 1; attempt <= STARTUP_READY_ATTEMPTS; attempt++)); do
    if worker_pid >/dev/null; then
      return 0
    fi
    # screen 已消失说明 wrapper 提前退出，无需继续等待完整超时。
    has_screen_session || return 1
    /bin/sleep "$STARTUP_READY_INTERVAL_SECONDS"
  done
  return 1
}

launch_screen_session() {
  /usr/bin/screen -dmS "$SESSION_NAME" "$SCRIPT_PATH" _run
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
  if has_screen_session; then
    echo "错误：发现没有有效 worker 的残留 screen 会话：$SESSION_NAME" >&2
    echo "请先使用 stop 清理残留会话，再重新启动。" >&2
    return 1
  fi

  local quota_exit_code
  echo "正在检查 BigQuery 月度额度，完成后将启动 $EVENT_LABEL 下载任务..."
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
  launch_screen_session
  if ! wait_for_worker_ready; then
    echo "错误：下载 worker 未能在启动窗口内就绪，请检查最近退出状态和日志。" >&2
    show_last_exit_status >&2 || true
    /usr/bin/tail -n 20 "$LOG_FILE" 2>/dev/null || true
    return 1
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
  if ! is_running && ! monitor_pid >/dev/null && ! has_screen_session; then
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
    echo "运行状态：运行中（worker 已校验，screen: ${SESSION_NAME}）"
  else
    echo "运行状态：已停止（没有有效 worker）"
  fi
  has_screen_session && echo "screen 状态：存在（${SESSION_NAME}）"
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
  show_last_exit_status
  quota_check || true
}

show_last_exit_status() {
  [[ -f "$LAST_EXIT_FILE" ]] || return 0
  "$REPO_ROOT/.venv/bin/python" - "$LAST_EXIT_FILE" <<'PY'
import json
import sys
from pathlib import Path

try:
    document = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
    exit_code = int(document["exit_code"])
    ended_at = str(document["ended_at"])
    signal_number = document.get("signal")
except (KeyError, OSError, TypeError, ValueError, json.JSONDecodeError) as error:
    print(f"最近退出状态：无法读取（{type(error).__name__}）")
else:
    signal_text = f"，signal={signal_number}" if signal_number is not None else ""
    print(f"最近退出状态：{ended_at}，exit_code={exit_code}{signal_text}")
PY
}

show_status_json() {
  local skip_quota="${1:-false}"
  local running="false"
  local worker=""
  local monitor=""
  local quota_json="null"
  local screen_alive="false"
  local quota_exit_code
  is_running && running="true"
  has_screen_session && screen_alive="true"
  worker="$(worker_pid || true)"
  monitor="$(monitor_pid || true)"
  quota_exit_code=0
  if [[ "$skip_quota" != "true" ]]; then
    set +e
    quota_json="$(
      /usr/bin/env GOOGLE_CLOUD_PROJECT="$BILLING_PROJECT" \
        "$CLI" bigquery quota-usage \
        --billing-project "$BILLING_PROJECT" \
        --location "$LOCATION" \
        --warning-percent "$QUOTA_WARNING_PERCENT" \
        --stop-percent "$QUOTA_STOP_PERCENT" \
        --format json 2>/dev/null
    )"
    quota_exit_code=$?
    set -e
  fi
  [[ -n "$quota_json" ]] || quota_json="null"

  "$REPO_ROOT/.venv/bin/python" - \
    "$STATE_FILE" "$LAST_EXIT_FILE" "$EVENT_LABEL" "$COMMAND_NAME" "$running" \
    "$screen_alive" "$SESSION_NAME" \
    "$worker" "$monitor" "$START_DATE" "$END_DATE" "$quota_json" \
    "$quota_exit_code" <<'PY'
import json
import sys
from datetime import date
from pathlib import Path

(
    state_name,
    last_exit_name,
    event_label,
    command_name,
    running_text,
    screen_alive_text,
    session_name,
    worker_pid,
    monitor_pid,
    start_date,
    end_date,
    quota_text,
    quota_exit_code,
) = sys.argv[1:]
checkpoint_error = None
last_exit = None
last_exit_error = None
partitions = {}
estimated_total_bytes = 0
state_path = Path(state_name)
if state_path.is_file():
    try:
        state = json.loads(state_path.read_text(encoding="utf-8"))
        partitions = state.get("completed_partitions", {})
        if not isinstance(partitions, dict):
            raise ValueError("completed_partitions 不是对象")
        estimated_total_bytes = int(state.get("estimated_total_bytes", 0))
    except (OSError, ValueError, TypeError, json.JSONDecodeError) as error:
        checkpoint_error = type(error).__name__
        partitions = {}
last_exit_path = Path(last_exit_name)
if last_exit_path.is_file():
    try:
        candidate = json.loads(last_exit_path.read_text(encoding="utf-8"))
        if not isinstance(candidate, dict) or candidate.get("schema_version") != 1:
            raise ValueError("last exit schema 无效")
        last_exit = candidate
    except (OSError, ValueError, TypeError, json.JSONDecodeError) as error:
        last_exit_error = type(error).__name__
try:
    quota = json.loads(quota_text)
except json.JSONDecodeError:
    quota = None
start = date.fromisoformat(start_date)
end = date.fromisoformat(end_date)
document = {
    "schema_version": 1,
    "event": event_label,
    "command": command_name,
    "state": "running" if running_text == "true" else "stopped",
    "screen_alive": screen_alive_text == "true",
    "screen_session": session_name,
    "worker_pid": int(worker_pid) if worker_pid else None,
    "quota_monitor_pid": int(monitor_pid) if monitor_pid else None,
    "start_date": start_date,
    "end_date": end_date,
    "total_partitions": (end - start).days + 1,
    "completed_partitions": len(partitions),
    "last_partition": max(partitions) if partitions else None,
    "estimated_total_bytes": estimated_total_bytes,
    "checkpoint_error": checkpoint_error,
    "last_exit": last_exit,
    "last_exit_error": last_exit_error,
    "quota": quota,
    "quota_error": None if int(quota_exit_code) in {0, 3} else "QUOTA_QUERY_FAILED",
}
print(json.dumps(document, ensure_ascii=False, separators=(",", ":")))
PY
}

run_action_json() {
  local action="$1"
  local callback="$2"
  local exit_code
  set +e
  "$callback" >/dev/null 2>&1
  exit_code=$?
  set -e
  local state="stopped"
  is_running && state="running"
  "$REPO_ROOT/.venv/bin/python" - "$action" "$EVENT_LABEL" "$state" "$exit_code" <<'PY'
import json
import sys

action, event_label, state, exit_code = sys.argv[1:]
code = int(exit_code)
print(json.dumps({
    "schema_version": 1,
    "action": action,
    "event": event_label,
    "ok": code == 0,
    "state": state,
    "exit_code": code,
    "error_code": None if code == 0 else "DOWNLOAD_ACTION_FAILED",
}, ensure_ascii=False, separators=(",", ":")))
raise SystemExit(code)
PY
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
  local caffeinate_pid
  local quota_pid
  local wrapper_signal=""
  local run_start_date="${1:-$START_DATE}"
  local run_end_date="${2:-$END_DATE}"
  local run_append_existing="${3:-$APPEND_EXISTING}"
  # macOS 自带 Bash 3.2 在 nounset 模式下展开空数组会直接退出；这里只需一个
  # 可选 flag，使用已初始化标量可避免错误发生在日志重定向建立之前。
  local append_existing_arg=""
  local started_at
  started_at="$(date -u '+%Y-%m-%dT%H:%M:%SZ')"
  if [[ "$run_append_existing" == "true" ]]; then
    append_existing_arg="--append-existing"
  fi
  printf '%s INFO [%s] 下载 wrapper 已启动，准备校验 checkpoint 并恢复任务。\n' \
    "$(date '+%F %T')" "$EVENT_LABEL" >>"$LOG_FILE"
  /usr/bin/env PYTHONUNBUFFERED=1 GOOGLE_CLOUD_PROJECT="$BILLING_PROJECT" \
    "$CLI" bigquery "$COMMAND_NAME" \
    --workspace "$WORKSPACE" \
    --billing-project "$BILLING_PROJECT" \
    --start-date "$run_start_date" \
    --end-date "$run_end_date" \
    --maximum-bytes-billed-per-day "$MAXIMUM_BYTES_PER_DAY" \
    --maximum-total-bytes-billed "$MAXIMUM_TOTAL_BYTES" \
    --location "$LOCATION" \
    --run-id "$RUN_ID" \
    ${append_existing_arg:+"$append_existing_arg"} \
    >>"$LOG_FILE" 2>&1 &
  child_pid=$!
  printf '%s\n' "$child_pid" >"$PID_FILE"
  printf '%s INFO [%s] 下载 worker 已就绪（PID %s），开始校验既有分区。\n' \
    "$(date '+%F %T')" "$EVENT_LABEL" "$child_pid" >>"$LOG_FILE"
  # caffeinate 只跟随真实 CLI PID 保持系统唤醒，不能再包裹 CLI；否则它可能把
  # Python 的退出码或终止信号折叠成自己的 exit_code=1，导致故障原因不可见。
  /usr/bin/caffeinate -ims -w "$child_pid" >>"$LOG_FILE" 2>&1 &
  caffeinate_pid=$!
  "$SCRIPT_PATH" _quota_monitor "$child_pid" >>"$LOG_FILE" 2>&1 &
  quota_pid=$!

  handle_wrapper_signal() {
    wrapper_signal="$1"
    printf '%s WARNING [%s] 下载 wrapper 收到 %s，正在终止 worker（PID %s）。\n' \
      "$(date '+%F %T')" "$EVENT_LABEL" "$wrapper_signal" "$child_pid" >>"$LOG_FILE"
    /bin/kill -TERM "$quota_pid" 2>/dev/null || true
    /bin/kill -TERM "$child_pid" 2>/dev/null || true
    /bin/kill -TERM "$caffeinate_pid" 2>/dev/null || true
  }
  cleanup_pid_files() {
    /bin/rm -f "$PID_FILE" "$MONITOR_PID_FILE"
  }
  trap 'handle_wrapper_signal HUP' HUP
  trap 'handle_wrapper_signal INT' INT
  trap 'handle_wrapper_signal TERM' TERM
  trap cleanup_pid_files EXIT

  set +e
  wait "$child_pid"
  local exit_code=$?
  # wait 被 wrapper 信号中断时会先返回 128+signal；再次 wait 可取得被 TERM 的
  # Python worker 真实退出码。若首个 wait 已完成回收，127 表示保留原结果。
  if [[ -n "$wrapper_signal" ]]; then
    wait "$child_pid" 2>/dev/null
    local final_wait_exit_code=$?
    if [[ "$final_wait_exit_code" -ne 127 ]]; then
      exit_code="$final_wait_exit_code"
    fi
  fi
  /bin/kill -TERM "$quota_pid" 2>/dev/null || true
  wait "$quota_pid" 2>/dev/null || true
  /bin/kill -TERM "$caffeinate_pid" 2>/dev/null || true
  wait "$caffeinate_pid" 2>/dev/null || true
  set -e
  record_worker_exit "$started_at" "$exit_code" "$wrapper_signal"
  return "$exit_code"
}

record_worker_exit() {
  local started_at="$1"
  local exit_code="$2"
  local wrapper_signal="${3:-}"
  local ended_at
  local signal_number=""
  ended_at="$(date -u '+%Y-%m-%dT%H:%M:%SZ')"
  if [[ "$exit_code" -gt 128 ]]; then
    signal_number="$((exit_code - 128))"
  fi
  if [[ "$exit_code" -eq 0 ]]; then
    printf '%s INFO [%s] 下载 worker 正常退出（exit_code=0）。\n' \
      "$(date '+%F %T')" "$EVENT_LABEL" >>"$LOG_FILE"
  else
    printf '%s ERROR [%s] 下载 worker 异常退出（exit_code=%s%s）。\n' \
      "$(date '+%F %T')" "$EVENT_LABEL" "$exit_code" \
      "${signal_number:+, signal=$signal_number}" >>"$LOG_FILE"
  fi

  # 最近退出状态与大日志分离，status 不需要扫描日志，也不暴露真实路径或错误正文。
  "$REPO_ROOT/.venv/bin/python" - \
    "$LAST_EXIT_FILE" "$EVENT_LABEL" "$COMMAND_NAME" "$started_at" "$ended_at" \
    "$exit_code" "$signal_number" "$wrapper_signal" <<'PY' || true
import json
import os
import sys
from pathlib import Path

path = Path(sys.argv[1])
path.parent.mkdir(parents=True, exist_ok=True)
signal_number = int(sys.argv[7]) if sys.argv[7] else None
wrapper_signal = sys.argv[8] or None
document = {
    "schema_version": 1,
    "event": sys.argv[2],
    "command": sys.argv[3],
    "started_at": sys.argv[4],
    "ended_at": sys.argv[5],
    "exit_code": int(sys.argv[6]),
    "signal": signal_number,
    "wrapper_signal": wrapper_signal,
}
temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
temporary.write_text(json.dumps(document, ensure_ascii=False), encoding="utf-8")
os.replace(temporary, path)
PY
}

catch_up_download() {
  if [[ "${CATCH_UP_ENABLED:-false}" != "true" ]]; then
    echo "错误：$EVENT_LABEL 尚未开放每日追赶入口。" >&2
    return 2
  fi
  require_external_mount
  require_runtime
  /bin/mkdir -p "$WORKSPACE" "$(dirname "$LOG_FILE")"
  if is_running; then
    echo "错误：下载任务正在运行，不能同时执行每日追赶：$SESSION_NAME" >&2
    return 1
  fi

  local target_args=()
  if [[ -n "${CATCH_UP_TARGET_DATE:-}" ]]; then
    target_args+=(--target-date "$CATCH_UP_TARGET_DATE")
  fi
  local plan
  plan="$(
    "$CLI" bigquery plan-watch-events-catch-up \
      --workspace "$WORKSPACE" \
      "${target_args[@]}"
  )"
  local status start_date end_date pending_days
  IFS=$'\t' read -r status start_date end_date pending_days <<<"$(
    "$REPO_ROOT/.venv/bin/python" - "$plan" <<'PY'
import json
import sys

document = json.loads(sys.argv[1])
print("\t".join([
    str(document.get("status", "")),
    str(document.get("start_date", "")),
    str(document.get("end_date", "")),
    str(document.get("pending_days", 0)),
]))
PY
  )"
  if [[ "$status" == "no_work" ]]; then
    printf '%s INFO [%s] 已是最新完整 UTC 日，无需下载。\n' \
      "$(date '+%F %T')" "$EVENT_LABEL" | /usr/bin/tee -a "$LOG_FILE"
    return 0
  fi
  if [[ "$status" != "pending" || -z "$start_date" || -z "$end_date" ]]; then
    echo "错误：WatchEvent 追赶计划无效。" >&2
    return 1
  fi

  local quota_exit_code
  set +e
  quota_check
  quota_exit_code=$?
  set -e
  if [[ "$quota_exit_code" -eq 3 ]]; then
    echo "错误：BigQuery 月度用量已达到停止阈值，拒绝追赶 $EVENT_LABEL。" >&2
    return 1
  elif [[ "$quota_exit_code" -ne 0 ]]; then
    echo "错误：无法读取 BigQuery 月度额度，拒绝追赶 $EVENT_LABEL。" >&2
    return 1
  fi

  printf '%s INFO [%s] 开始追赶 %s 至 %s，共 %s 天。\n' \
    "$(date '+%F %T')" "$EVENT_LABEL" "$start_date" "$end_date" "$pending_days" \
    | /usr/bin/tee -a "$LOG_FILE"
  run_worker "$start_date" "$end_date" true
}

restart_download() {
  stop_download
  start_download
}

bigquery_download_main() {
  case "${1:-}" in
    start)
      if [[ "${2:-}" == "--json" ]]; then
        run_action_json "start" start_download
      else
        start_download
      fi
      ;;
    stop)
      if [[ "${2:-}" == "--json" ]]; then
        run_action_json "stop" stop_download
      else
        stop_download
      fi
      ;;
    restart)
      if [[ "${2:-}" == "--json" ]]; then
        run_action_json "restart" restart_download
      else
        restart_download
      fi
      ;;
    catch-up)
      catch_up_download
      ;;
    status)
      if [[ "${2:-}" == "--json" ]]; then
        if [[ "${3:-}" == "--skip-quota" ]]; then
          show_status_json true
        else
          show_status_json false
        fi
      else
        show_status
      fi
      ;;
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
