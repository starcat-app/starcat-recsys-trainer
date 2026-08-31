"""WatchEvent Raw 每日追赶计划。

下载器本身负责 BigQuery 查询、预算和 checkpoint 写入；本模块只读取 checkpoint，
计算从最后一个已完成分区到最新完整 UTC 日之间的连续范围。把日期决策放在可测试的
Python 模块中，避免 LaunchAgent 或 shell 因本地时区、断点状态而跳日。
"""

from __future__ import annotations

import json
from datetime import date, timedelta
from pathlib import Path
from typing import Any

_STATE_RELATIVE_PATH = Path("raw/gh_archive/download-state.json")


def plan_watch_catch_up(
    workspace: Path,
    target_date: date | None = None,
    *,
    today_utc: date | None = None,
) -> dict[str, Any]:
    """返回 WatchEvent 连续追加范围，默认目标是已经完整结束的 UTC 昨日。"""

    state_path = workspace.resolve() / _STATE_RELATIVE_PATH
    if not state_path.is_file():
        raise RuntimeError("WatchEvent checkpoint 不存在，必须先完成首次下载")
    try:
        state = json.loads(state_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise RuntimeError("WatchEvent checkpoint 无法读取") from error
    if state.get("schema_version") != 1:
        raise RuntimeError("WatchEvent checkpoint schema_version 不受支持")
    scope = state.get("scope")
    completed = state.get("completed_partitions")
    if not isinstance(scope, dict) or not isinstance(completed, dict) or not completed:
        raise RuntimeError("WatchEvent checkpoint 缺少已完成分区")

    try:
        initial_date = date.fromisoformat(str(scope["start_date"]))
        completed_dates = sorted(date.fromisoformat(str(value)) for value in completed)
    except (KeyError, ValueError) as error:
        raise RuntimeError("WatchEvent checkpoint 日期无效") from error

    # 下载器按日期串行写 checkpoint。这里再次验证连续性，防止损坏的状态把每日任务
    # 直接推进到缺口之后，进而让 History 服务永远无法应用相邻 Delta。
    expected_count = (completed_dates[-1] - initial_date).days + 1
    if completed_dates[0] != initial_date or len(completed_dates) != expected_count:
        raise RuntimeError("WatchEvent checkpoint 已完成分区不连续")

    latest_complete = (today_utc or date.today()) - timedelta(days=1)
    target = target_date or latest_complete
    if target > latest_complete:
        raise RuntimeError("WatchEvent 追赶目标不能晚于最新完整 UTC 日")

    current = completed_dates[-1]
    document: dict[str, Any] = {
        "schema_version": 1,
        "current_watermark": current.isoformat(),
        "target_watermark": target.isoformat(),
    }
    if current >= target:
        return {**document, "status": "no_work", "pending_days": 0}
    start = current + timedelta(days=1)
    return {
        **document,
        "status": "pending",
        "start_date": start.isoformat(),
        "end_date": target.isoformat(),
        "pending_days": (target - current).days,
    }
