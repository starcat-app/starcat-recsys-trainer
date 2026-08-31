"""WatchEvent 每日追赶范围回归测试。"""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import pytest

from starcat_recsys.watch_catchup import plan_watch_catch_up


def _checkpoint(workspace: Path, partitions: list[str]) -> None:
    target = workspace / "raw/gh_archive/download-state.json"
    target.parent.mkdir(parents=True)
    target.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "scope": {"start_date": "2026-08-25", "end_date": "2026-08-30"},
                "completed_partitions": {value: {} for value in partitions},
            }
        ),
        encoding="utf-8",
    )


def test_plans_contiguous_range_through_latest_complete_utc_day(tmp_path: Path) -> None:
    _checkpoint(tmp_path, ["20260825", "20260826"])

    plan = plan_watch_catch_up(tmp_path, today_utc=date(2026, 8, 31))

    assert plan == {
        "schema_version": 1,
        "current_watermark": "2026-08-26",
        "target_watermark": "2026-08-30",
        "status": "pending",
        "start_date": "2026-08-27",
        "end_date": "2026-08-30",
        "pending_days": 4,
    }


def test_returns_no_work_when_checkpoint_reaches_target(tmp_path: Path) -> None:
    _checkpoint(tmp_path, ["20260825", "20260826"])

    plan = plan_watch_catch_up(
        tmp_path,
        target_date=date(2026, 8, 26),
        today_utc=date(2026, 8, 31),
    )

    assert plan["status"] == "no_work"
    assert plan["pending_days"] == 0


def test_rejects_checkpoint_gap(tmp_path: Path) -> None:
    _checkpoint(tmp_path, ["20260825", "20260827"])

    with pytest.raises(RuntimeError, match="不连续"):
        plan_watch_catch_up(tmp_path, today_utc=date(2026, 8, 31))


def test_rejects_current_or_future_utc_target(tmp_path: Path) -> None:
    _checkpoint(tmp_path, ["20260825", "20260826"])

    with pytest.raises(RuntimeError, match="最新完整 UTC 日"):
        plan_watch_catch_up(
            tmp_path,
            target_date=date(2026, 8, 31),
            today_utc=date(2026, 8, 31),
        )
