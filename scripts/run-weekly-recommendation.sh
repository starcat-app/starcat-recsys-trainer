#!/usr/bin/env bash
# launchd 与人工共用每周入口；凭据由 Python 从私有文件读取，不经 shell 展开。
set -euo pipefail
export PATH="/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin:${PATH:-}"
repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
config="${STARCAT_WEEKLY_RECOMMENDATION_CONFIG:-${HOME}/.config/starcat/weekly-recommendation.yaml}"
command="${1:-run}"
if [[ $# -gt 0 ]]; then shift; fi
export PYTHONPATH="${repo_root}/src"
cd "${repo_root}"
if [[ "${command}" == "status" ]]; then
  exec "${repo_root}/.venv/bin/python" -m starcat_recsys.weekly status --config "${config}"
fi
"${repo_root}/.venv/bin/python" -m starcat_recsys.weekly "${command}" --config "${config}" "$@" &
worker=$!
# caffeinate 只跟随真实 worker；shell 收到停止信号时必须转发，避免遗留训练进程。
/usr/bin/caffeinate -ims -w "${worker}" &
awake=$!
stop_worker() {
  kill -TERM "${worker}" 2>/dev/null || true
}
trap stop_worker HUP INT TERM
set +e
wait "${worker}"
result=$?
if kill -0 "${worker}" 2>/dev/null; then
  wait "${worker}"
  result=$?
fi
kill -TERM "${awake}" 2>/dev/null || true
wait "${awake}" 2>/dev/null || true
exit "${result}"
