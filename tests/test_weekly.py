"""每周训练的门槛、时间窗口和恢复回归；只使用隔离临时目录。"""

import hashlib
import json
import math
import plistlib
import subprocess
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from starcat_recsys.artifacts import artifact_for, sha256_file, write_json_atomic
from starcat_recsys.interfaces import TaskContext
from starcat_recsys.monthly_roster import propose_roster
from starcat_recsys.recommendations import artifact_path
from starcat_recsys.weekly import WeeklyRun, due_slot
from starcat_recsys.weekly_data import prepare_datasets, verify_raw
from starcat_recsys.weekly_training import quality_decision


def metrics(subjects: int = 5000) -> dict[str, Any]:
    """相同评估人群上的聚合数据，供每条独立门槛测试使用。"""
    value = {
        "evaluated_subjects": subjects,
        "recall@20": 0.04,
        "ndcg@20": 0.02,
        "empty_result_rate": 0.01,
    }
    return {"models": {"hybrid_25": value.copy(), "incumbent": value.copy()}}


@pytest.mark.parametrize(
    "field,value",
    [
        ("evaluated_subjects", 4999),
        ("recall@20", 0.039),
        ("ndcg@20", 0.019),
        ("empty_result_rate", 0.02),
        ("recall@20", float("nan")),
    ],
)
def test_quality_rejects_each_regression(field: str, value: float) -> None:
    """训练成功不等于质量放行；人数不足和微小质量回退都必须拦截。"""
    document = metrics()
    document["models"]["hybrid_25"][field] = value
    assert not quality_decision(document)["passed"]


def test_quality_accepts_equal_or_better_model() -> None:
    """固定算法的周期重训允许持平，不要求每周重新证明算法显著优于旧基线。"""
    assert quality_decision(metrics())["passed"]
    document = metrics()
    for model in document["models"].values():
        model["empty_result_rate"] = 0.11
    assert not quality_decision(document)["passed"]


@pytest.mark.parametrize(
    "now,expected",
    [
        ("2026-11-15T02:59:00+00:00", "2026-11-08"),
        ("2026-11-15T03:00:00+00:00", "2026-11-15"),
        ("2026-11-16T00:00:00+00:00", "2026-11-15"),
    ],
)
def test_sunday_due_slot(now: str, expected: str) -> None:
    """周日 11:00 前不提前触发，周一登录仍能补上周日任务。"""
    assert due_slot(datetime.fromisoformat(now)) == expected


def test_installed_schedule_is_sunday_and_load_checks_due() -> None:
    """检查实际安装脚本渲染的 plist，避免文档时间与调度参数不一致。"""
    script = Path(__file__).parents[1] / "scripts/install-weekly-recommendation-launch-agent.sh"
    output = subprocess.check_output(["bash", str(script), "render"])
    config = plistlib.loads(output)
    assert config["StartCalendarInterval"] == {"Weekday": 0, "Hour": 11, "Minute": 0}
    assert config["RunAtLoad"] is True
    assert config["ProgramArguments"][-1] == "--scheduled"


def test_raw_and_dataset_preserve_holdout_and_first_star(tmp_path: Path) -> None:
    """重复 Star 不能移进 test；留出主体不能进入 train，目录外真值必须保留。"""
    raw = tmp_path / "raw"
    raw.mkdir()
    parts = {}
    for day in [1, 2, 3]:
        rows = []
        for actor in [1, 2, 3]:
            repos = range(1, 6) if day == 1 else ([6 if actor == 1 else 7] if day == 2 else [2])
            for repo in repos:
                rows.append(
                    {
                        "source_record_id": str(repo),
                        "actor_id": actor,
                        "repo_id": repo,
                        "created_at": datetime(2026, 11, day, tzinfo=UTC),
                    }
                )
        path = raw / f"watch-events-2026110{day}.parquet"
        pq.write_table(pa.Table.from_pylist(rows), path)
        parts[f"2026110{day}"] = {
            "checksum": sha256_file(path),
            "metadata": {"row_count": len(rows)},
        }
    write_json_atomic(
        raw / "download-state.json",
        {"scope": {"start_date": "2026-11-01"}, "completed_partitions": parts},
    )
    paths = verify_raw(raw, date(2026, 11, 3))
    frozen = tmp_path / "frozen.parquet"
    subject = hashlib.sha256(b"gh_archive:1").hexdigest()
    pq.write_table(
        pa.Table.from_pylist(
            [
                {
                    "source": "gh_archive",
                    "source_subject_id": subject,
                    "sample_weight": 0.7,
                    "split": "train",
                }
            ]
        ),
        frozen,
    )
    core = tmp_path / "core.parquet"
    expanded = tmp_path / "expanded.parquet"
    pq.write_table(pa.table({"repo_id": list(range(1, 7))}), core)
    pq.write_table(pa.table({"repo_id": list(range(1, 8))}), expanded)
    context = TaskContext("weekly-test", tmp_path / "run", "sha256:test", 7)
    artifacts = prepare_datasets(
        context,
        {
            "frozen_training_dataset": str(frozen),
            "core_catalog": str(core),
            "expanded_catalog": str(expanded),
        },
        paths,
        datetime(2026, 11, 2, tzinfo=UTC),
        datetime(2026, 11, 4, tzinfo=UTC),
    )
    by_kind = {item.kind: pq.read_table(Path(item.uri[7:])).to_pylist() for item in artifacts}
    assert len(by_kind["training_dataset"]) == 5
    assert len(by_kind["refit_dataset"]) == 6
    assert {row["source_subject_id"] for row in by_kind["training_dataset"]} == {subject}
    evaluation = by_kind["evaluation_dataset"]
    assert subject not in {row["source_subject_id"] for row in evaluation}
    assert {row["repo_id"] for row in evaluation if row["split"] == "test"} == {7}
    assert all(not row["training_eligible"] for row in evaluation if row["split"] == "test")
    parts.pop("20261102")
    write_json_atomic(
        raw / "download-state.json",
        {"scope": {"start_date": "2026-11-01"}, "completed_partitions": parts},
    )
    with pytest.raises(ValueError, match="2026-11-02"):
        verify_raw(raw, date(2026, 11, 3))


def test_resume_reuses_only_verified_completed_stages(tmp_path: Path) -> None:
    """模拟掉电：已完成阶段可恢复，完成记录对应的文件被损坏则必须停止。"""
    config = {"workspace": str(tmp_path), "initial_bundle": str(tmp_path / "baseline")}
    state_file = tmp_path / "state.json"
    state: dict[str, Any] = {}
    task = WeeklyRun(config, state, state_file, date(2026, 11, 9), "2026-11-08")

    def action(context: TaskContext) -> list[Any]:
        path = context.workspace / "result.json"
        write_json_atomic(path, {"done": True})
        return [artifact_for(path, "weekly_report", {})]

    artifact = task.step("example", action)[0]
    resumed = WeeklyRun(config, state, state_file, date(2026, 11, 9), "2026-11-08")
    assert resumed.root == task.root
    assert resumed.step("example", lambda _: pytest.fail("completed stage ran twice")) == [artifact]
    Path(artifact.uri[7:]).write_text(json.dumps({"corrupt": True}))
    with pytest.raises(ValueError, match="摘要"):
        resumed.step("example", action)


def test_monthly_roster_only_uses_past_and_excludes_both_cohorts(tmp_path: Path) -> None:
    """换出者也不能当考生；留出周才凑足五个历史仓库的新人不能入选。"""

    def subject(actor: int) -> str:
        return hashlib.sha256(f"gh_archive:{actor}".encode()).hexdigest()

    cutoff = datetime(2026, 11, 1, tzinfo=UTC)
    frozen = tmp_path / "old.parquet"
    core = tmp_path / "core.parquet"
    pq.write_table(
        pa.Table.from_pylist(
            [
                {
                    "source": "gh_archive",
                    "split": "train",
                    "source_subject_id": subject(n),
                    "sample_weight": 0.73,
                }
                for n in range(1, 21)
            ]
        ),
        frozen,
    )
    pq.write_table(pa.table({"repo_id": list(range(1, 205))}), core)
    paths = []
    for day, entries in [
        ("2026-09-15", [(20, range(1, 6)), (21, range(1, 5)), (24, range(1, 6))]),
        (
            "2026-10-31",
            [(n, [1]) for n in range(1, 20)] + [(21, [5]), (22, range(1, 5)), (23, range(1, 202))],
        ),
        ("2026-11-01", [(20, [9]), (21, [9]), (22, [5]), (24, [9])]),
    ]:
        path = tmp_path / f"watch-events-{day.replace('-', '')}.parquet"
        pq.write_table(
            pa.Table.from_pylist(
                [
                    {
                        "actor_id": actor,
                        "repo_id": repo,
                        "created_at": datetime.fromisoformat(day).replace(tzinfo=UTC),
                    }
                    for actor, repos in entries
                    for repo in repos
                ]
            ),
            path,
        )
        paths.append(path)
    context = TaskContext("monthly", tmp_path, "test", 7)
    roster = propose_roster(context, frozen, core, paths, cutoff)[0]
    assert roster.metadata["replaced"] == 1  # 不足两个合格名额时不补凑
    members = {
        r["source_subject_id"]: r["sample_weight"]
        for r in pq.read_table(artifact_path(roster)).to_pylist()
    }
    assert set(members) == {subject(i) for i in [*range(1, 20), 21]}
    assert members[subject(1)] == pytest.approx(0.73)
    assert members[subject(21)] == pytest.approx(1 / math.log2(7))
    data = prepare_datasets(
        context,
        {
            "frozen_training_dataset": str(frozen),
            "core_catalog": str(core),
            "expanded_catalog": str(core),
        },
        paths,
        cutoff,
        datetime(2026, 11, 2, tzinfo=UTC),
        artifact_path(roster),
    )
    evaluation = next(item for item in data if item.kind == "evaluation_dataset")
    assert set(
        pq.read_table(artifact_path(evaluation)).column("source_subject_id").to_pylist()
    ) == {subject(24)}


def test_cli_preserves_safe_missing_partition_reason(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """操作日志须保留缺失日期；不能只剩无法定位的 ValueError，也不输出底层正文。"""
    import starcat_recsys.weekly as weekly
    from starcat_recsys.weekly_data import RawValidationError

    config = tmp_path / "config.yaml"
    config.write_text("{}")

    def failed_run(*_: Any, **__: Any) -> Any:
        raise RawValidationError("Raw 缺失日期 2026-11-02")

    monkeypatch.setattr(weekly, "run", failed_run)
    monkeypatch.setattr("sys.argv", ["weekly", "run", "--config", str(config)])
    assert weekly.main() == 1
    assert json.loads(capsys.readouterr().out)["reason"] == "Raw 缺失日期 2026-11-02"
