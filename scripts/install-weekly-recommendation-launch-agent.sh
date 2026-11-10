#!/usr/bin/env bash
# 安装每周日 11:00 的用户 LaunchAgent；RunAtLoad 由 Python 判断是否有漏跑周次。
set -euo pipefail
label="com.starcat.weekly-recommendation"
script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
plist="${HOME}/Library/LaunchAgents/${label}.plist"
config="${STARCAT_WEEKLY_RECOMMENDATION_CONFIG:-${HOME}/.config/starcat/weekly-recommendation.yaml}"
log_file="${HOME}/Library/Logs/Starcat/weekly-recommendation.log"
domain="gui/$(id -u)"

xml_escape() {
  # 路径可能含空格或 XML 保留字符，不能直接插入 plist。
  printf '%s' "$1" | sed -e 's/\&/\&amp;/g' -e 's/</\&lt;/g' -e 's/>/\&gt;/g'
}

render() {
  cat <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0"><dict>
  <key>Label</key><string>${label}</string>
  <key>ProgramArguments</key><array>
    <string>/bin/bash</string><string>$(xml_escape "${script_dir}/run-weekly-recommendation.sh")</string>
    <string>run</string><string>--scheduled</string>
  </array>
  <key>EnvironmentVariables</key><dict>
    <key>STARCAT_WEEKLY_RECOMMENDATION_CONFIG</key><string>$(xml_escape "${config}")</string>
    <key>TZ</key><string>Asia/Shanghai</string>
  </dict>
  <key>StartCalendarInterval</key><dict>
    <key>Weekday</key><integer>0</integer>
    <key>Hour</key><integer>11</integer>
    <key>Minute</key><integer>0</integer>
  </dict>
  <key>RunAtLoad</key><true/>
  <key>ProcessType</key><string>Background</string>
  <key>StandardOutPath</key><string>$(xml_escape "${log_file}")</string>
  <key>StandardErrorPath</key><string>$(xml_escape "${log_file}")</string>
</dict></plist>
EOF
}

case "${1:-}" in
  render) render ;;
  install)
    [[ -f "${config}" && "$(stat -f '%Lp' "${config}")" == "600" ]] || {
      echo "请先创建权限为 600 的每周训练配置" >&2; exit 2;
    }
    mkdir -p "$(dirname "${plist}")" "$(dirname "${log_file}")"
    render >"${plist}"
    plutil -lint "${plist}" >/dev/null
    launchctl bootout "${domain}" "${plist}" 2>/dev/null || true
    launchctl bootstrap "${domain}" "${plist}"
    echo "已安装 ${label}：每周日 11:00，登录后补跑漏掉的周次"
    ;;
  status) launchctl print "${domain}/${label}" ;;
  uninstall)
    launchctl bootout "${domain}" "${plist}" 2>/dev/null || true
    rm -f "${plist}"
    ;;
  *) echo "用法: $0 {render|install|status|uninstall}" >&2; exit 2 ;;
esac
