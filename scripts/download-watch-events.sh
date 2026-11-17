#!/usr/bin/env bash
# 管理独立的 GH Archive WatchEvent 长期下载任务。

set -euo pipefail

readonly SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
readonly EVENT_LABEL="WatchEvent"
readonly COMMAND_NAME="download-watch-events"
readonly SESSION_NAME="${STARCAT_WATCH_SESSION_NAME:-starcat-watch-events}"
# 维护者磁盘位置只保存在被 Git 忽略的纯文本配置中；环境变量优先。
local_volume_root=""
if [[ -f "$SCRIPT_DIR/../configs/data-volume.local" ]]; then
  IFS= read -r local_volume_root <"$SCRIPT_DIR/../configs/data-volume.local" || true
fi
readonly VOLUME_ROOT="${STARCAT_WATCH_VOLUME_ROOT:-${local_volume_root:-/Volumes/StarcatData}}"
readonly WORKSPACE="${STARCAT_WATCH_WORKSPACE:-$VOLUME_ROOT/Starcat/bigquery/watch-events-2016-2026}"
readonly LOG_FILE="${STARCAT_WATCH_LOG_FILE:-$VOLUME_ROOT/Starcat/bigquery/watch-events-2016-2026.log}"
readonly START_DATE="${STARCAT_WATCH_START_DATE:-2016-01-01}"
readonly END_DATE="${STARCAT_WATCH_END_DATE:-2026-08-25}"
readonly MAXIMUM_BYTES_PER_DAY="${STARCAT_WATCH_MAXIMUM_BYTES_PER_DAY:-1073741824}"
readonly MAXIMUM_TOTAL_BYTES="${STARCAT_WATCH_MAXIMUM_TOTAL_BYTES:-600000000000}"
readonly RUN_ID="${STARCAT_WATCH_RUN_ID:-gharchive-watch-2016-2026}"
readonly APPEND_EXISTING="${STARCAT_WATCH_APPEND_EXISTING:-false}"
readonly CATCH_UP_ENABLED="true"
readonly CATCH_UP_TARGET_DATE="${STARCAT_WATCH_TARGET_DATE:-}"
readonly STATE_RELATIVE_PATH="raw/gh_archive/download-state.json"
readonly PID_BASENAME="starcat-watch-events"

# 两个事件脚本只声明各自的数据契约；生命周期、额度保护和防休眠逻辑共用同一实现。
source "$SCRIPT_DIR/lib/bigquery-download-common.sh"
bigquery_download_main "$@"
