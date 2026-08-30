#!/usr/bin/env bash
# 管理独立的 GH Archive PushEvent 长期下载任务。

set -euo pipefail

readonly SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
readonly EVENT_LABEL="PushEvent"
readonly COMMAND_NAME="download-push-events"
readonly SESSION_NAME="${STARCAT_PUSH_SESSION_NAME:-starcat-push-events}"
readonly VOLUME_ROOT="${STARCAT_PUSH_VOLUME_ROOT:-/Volumes/T0}"
readonly WORKSPACE="${STARCAT_PUSH_WORKSPACE:-$VOLUME_ROOT/Starcat/bigquery/push-events-2016-2026}"
readonly LOG_FILE="${STARCAT_PUSH_LOG_FILE:-$VOLUME_ROOT/Starcat/bigquery/push-events-2016-2026.log}"
readonly START_DATE="${STARCAT_PUSH_START_DATE:-2016-01-01}"
readonly END_DATE="${STARCAT_PUSH_END_DATE:-2026-08-25}"
readonly MAXIMUM_BYTES_PER_DAY="${STARCAT_PUSH_MAXIMUM_BYTES_PER_DAY:-2147483648}"
readonly MAXIMUM_TOTAL_BYTES="${STARCAT_PUSH_MAXIMUM_TOTAL_BYTES:-800000000000}"
readonly RUN_ID="${STARCAT_PUSH_RUN_ID:-gharchive-push-2016-2026}"
readonly APPEND_EXISTING="${STARCAT_PUSH_APPEND_EXISTING:-false}"
readonly STATE_RELATIVE_PATH="raw/gh_archive_push_events/download-state.json"
readonly PID_BASENAME="starcat-push-events"

source "$SCRIPT_DIR/lib/bigquery-download-common.sh"
bigquery_download_main "$@"
