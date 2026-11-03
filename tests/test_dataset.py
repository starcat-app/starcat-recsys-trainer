from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import cast

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from starcat_recsys.artifacts import artifact_for
from starcat_recsys.canonical import INTERACTION_SCHEMA, REPOSITORY_SCHEMA
from starcat_recsys.dataset import DatasetConfig, DuckDBDatasetBuilder
from starcat_recsys.interfaces import TaskContext


def test_builds_latest_public_deduplicated_time_split_dataset(tmp_path: Path) -> None:
    interactions_path = tmp_path / "interactions.parquet"
    repositories_path = tmp_path / "repositories.parquet"
    rows = [
        _interaction("old:1", "starcat", 1, "2025-01-01", "current_star_membership"),
        _interaction("old:2", "starcat", 2, "2025-01-01", "current_star_membership"),
        _interaction("new:1", "starcat", 1, "2026-01-01", "current_star_membership"),
        _interaction("new:2", "starcat", 2, "2026-01-01", "current_star_membership"),
        _interaction("gh:1", "gh", 1, "2025-01-01", "star_event", "2025-01-01"),
        _interaction("gh:2", "gh", 2, "2025-01-02", "star_event", "2025-05-01"),
        _interaction("gh:3", "gh", 3, "2025-01-03", "star_event", "2025-09-01"),
        _interaction("gh:4", "gh", 4, "2025-01-04", "star_event", "2025-10-01"),
        # 相同 source_record_id 只能保留一条。
        _interaction("gh:4", "gh", 4, "2025-01-04", "star_event", "2025-10-01"),
        # 未进入仓库元数据目录的互动必须在快照选择和窗口去重前被过滤。
        _interaction("gh:orphan", "gh", 999, "2025-01-05", "star_event", "2025-01-05"),
    ]
    pq.write_table(pa.Table.from_pylist(rows, schema=INTERACTION_SCHEMA), interactions_path)
    repo_rows = [
        _repository(1),
        _repository(2),
        _repository(3, archived=True),
        _repository(4),
    ]
    pq.write_table(pa.Table.from_pylist(repo_rows, schema=REPOSITORY_SCHEMA), repositories_path)
    inputs = [
        artifact_for(interactions_path, "canonical_interactions", {}),
        artifact_for(repositories_path, "canonical_repositories", {}),
    ]

    artifacts = DuckDBDatasetBuilder(
        DatasetConfig(
            train_end=datetime(2025, 4, 1, tzinfo=UTC),
            validation_end=datetime(2025, 8, 1, tzinfo=UTC),
        )
    ).build(TaskContext("run", tmp_path / "workspace", "sha256:test", 7), inputs)

    dataset = pq.read_table(
        Path(next(item for item in artifacts if item.kind == "training_dataset").uri[7:])
    ).to_pylist()
    # repo 4 只在 test 窗口出现，不能借未来互动进入训练 catalog。
    assert len(dataset) == 4
    assert {row["repo_id"] for row in dataset} == {1, 2}
    assert {row["split"] for row in dataset} == {"train", "validation"}
    assert all(row["sample_weight"] > 0 for row in dataset)
    assert {row["training_eligible"] for row in dataset if row["source"] == "starcat_snapshot"} == {
        True
    }
    assert {row["training_eligible"] for row in dataset if row["source"] == "gh_archive"} == {False}
    report = json.loads((tmp_path / "workspace/dataset/quality-report.json").read_text())
    assert report["input_interaction_rows"] == 10
    assert report["latest_snapshot_rows"] == 6
    assert report["deduplicated_rows"] == 5
    assert report["public_repository_rows"] == 4
    assert report["unique_subject_repository_rows"] == 4
    assert report["training_eligible_subject_count"] == 1
    assert report["event_identity_policy"] == "gh_archive_watch_event_v2"
    assert artifacts[0].metadata["event_identity_policy"] == report["event_identity_policy"]


def test_train_weight_is_invariant_to_future_interactions(tmp_path: Path) -> None:
    base = [
        _interaction("gh:1", "gh", 1, "2025-01-01", "star_event", "2025-01-01"),
        _interaction("gh:2", "gh", 2, "2025-01-02", "star_event", "2025-01-02"),
    ]
    with_future = [
        *base,
        _interaction("gh:3", "gh", 3, "2025-07-01", "star_event", "2025-07-01"),
        _interaction("gh:4", "gh", 4, "2026-01-01", "star_event", "2026-01-01"),
    ]

    base_dataset = _build_dataset(tmp_path / "base", base)
    future_dataset = _build_dataset(tmp_path / "future", with_future)

    base_weight = next(row["sample_weight"] for row in base_dataset if row["repo_id"] == 1)
    future_weight = next(row["sample_weight"] for row in future_dataset if row["repo_id"] == 1)
    assert base_weight == future_weight == 0.5


@pytest.mark.parametrize("changed_field", ["occurred_at", "source_subject_id", "repo_id"])
def test_watch_event_identity_preserves_id_reuse(tmp_path: Path, changed_field: str) -> None:
    """原始 ID 不是全局唯一；不同时间、主体或仓库的事件不能相互覆盖。"""
    original = _interaction("reused", "a", 1, "2025-01-01", "star_event", "2025-01-01")
    collision = {**original, "observed_at": _time("2026-01-01")}
    collision[changed_field] = {
        "occurred_at": _time("2026-01-01"),
        "source_subject_id": "b",
        "repo_id": 3,
    }[changed_field]
    rows = [
        original,
        _interaction("a2", "a", 2, "2025-01-01", "star_event", "2025-01-01"),
        _interaction("b1", "b", 1, "2025-01-01", "star_event", "2025-01-01"),
        _interaction("b2", "b", 2, "2025-01-01", "star_event", "2025-01-01"),
        collision,
    ]

    dataset = _build_dataset(tmp_path, rows)

    report = json.loads((tmp_path / "workspace/dataset/quality-report.json").read_text())
    assert report["deduplicated_rows"] == 5
    # 后续“同主体仓库首次 Star”仍独立生效，不能用保留所有事件来膨胀训练关系。
    first = next(row for row in dataset if row["source_subject_id"] == "a" and row["repo_id"] == 1)
    assert first["occurred_at"] == original["occurred_at"]
    assert first["observed_at"] == original["observed_at"]


def test_duplicate_watch_event_keeps_latest_observation(tmp_path: Path) -> None:
    """下载重试应合并同一事件，重复观测只能更新该事件自身。"""
    original = _interaction("same", "a", 1, "2025-01-01", "star_event", "2025-01-01")
    latest = {**original, "observed_at": _time("2025-02-01")}
    dataset = _build_dataset(
        tmp_path,
        [
            original,
            original,
            latest,
            _interaction("a2", "a", 2, "2025-01-01", "star_event", "2025-01-01"),
        ],
    )

    assert len(dataset) == 2
    assert (
        next(row for row in dataset if row["repo_id"] == 1)["observed_at"] == latest["observed_at"]
    )
    report = json.loads((tmp_path / "workspace/dataset/quality-report.json").read_text())
    assert report["deduplicated_rows"] == 2


def test_future_reused_event_id_cannot_change_training_history(tmp_path: Path) -> None:
    """未来无关事件不得删除训练记录或改变目录支持度、主体准入和权重。"""
    base = [
        _interaction("reused", "a", 1, "2025-01-01", "star_event", "2025-01-01"),
        _interaction("a2", "a", 2, "2025-01-02", "star_event", "2025-01-02"),
        _interaction("b1", "b", 1, "2025-01-01", "star_event", "2025-01-01"),
        _interaction("b2", "b", 2, "2025-01-02", "star_event", "2025-01-02"),
    ]
    config = DatasetConfig(
        train_end=datetime(2025, 4, 1, tzinfo=UTC),
        validation_end=datetime(2025, 8, 1, tzinfo=UTC),
        minimum_repository_interactions=2,
        freeze_cohort=True,
    )
    original, original_repositories = _build_dataset_with_config(
        tmp_path / "base", base, config, cohort_ids=[1, 2]
    )
    with_future, future_repositories = _build_dataset_with_config(
        tmp_path / "future",
        [
            *base,
            _interaction("reused", "future", 2, "2026-01-01", "star_event", "2026-01-01"),
        ],
        config,
        cohort_ids=[1, 2],
    )

    assert original_repositories == future_repositories
    assert original == [row for row in with_future if row["split"] == "train"]
    assert len(original) == 4


def test_snapshot_id_deduplication_and_latest_snapshot_are_unchanged(tmp_path: Path) -> None:
    """WatchEvent 修复不能顺带改变其他来源按 ID 去重和仅用最新快照的规则。"""
    rows = [
        _interaction("old", "starcat", 4, "2025-01-01", "current_star_membership"),
        _interaction("reused", "starcat", 1, "2026-01-01", "current_star_membership"),
        _interaction("reused", "starcat", 2, "2026-01-01", "current_star_membership"),
        _interaction("new", "starcat", 3, "2026-01-01", "current_star_membership"),
    ]

    dataset = _build_dataset(tmp_path, rows)

    assert len(dataset) == 2
    assert {row["repo_id"] for row in dataset} in ({1, 3}, {2, 3})
    report = json.loads((tmp_path / "workspace/dataset/quality-report.json").read_text())
    assert report["latest_snapshot_rows"] == 3
    assert report["deduplicated_rows"] == 2


def test_non_archive_events_keep_source_id_deduplication(tmp_path: Path) -> None:
    """即使其他来源也产生 Star 关系，本次修复也不扩展它的事件身份。"""
    dataset = _build_dataset(
        tmp_path,
        [
            _interaction("reused", "starcat", 1, "2025-01-01", "star_event", "2025-01-01"),
            _interaction("reused", "starcat", 2, "2025-02-01", "star_event", "2025-02-01"),
            _interaction("other", "starcat", 3, "2025-01-01", "star_event", "2025-01-01"),
        ],
    )

    assert {row["repo_id"] for row in dataset} == {2, 3}


def test_frozen_cohort_ignores_expansion_and_future_for_eligibility(tmp_path: Path) -> None:
    """新候选和未来行为不能稀释权重或触发主体上限；新主体也不能混入。"""
    rows = [
        _interaction("a1", "a", 1, "2025-01-01", "star_event", "2025-01-01"),
        _interaction("a2", "a", 2, "2025-01-01", "star_event", "2025-01-01"),
        _interaction("a3", "a", 3, "2025-01-01", "star_event", "2025-01-01"),
        _interaction("a4", "a", 4, "2025-09-01", "star_event", "2025-09-01"),
        _interaction("b3", "b", 3, "2025-01-01", "star_event", "2025-01-01"),
        _interaction("b4", "b", 4, "2025-01-01", "star_event", "2025-01-01"),
    ]
    data, _ = _build_dataset_with_config(
        tmp_path,
        rows,
        DatasetConfig(
            train_end=datetime(2025, 4, 1, tzinfo=UTC),
            validation_end=datetime(2025, 8, 1, tzinfo=UTC),
            maximum_repositories_per_subject=2,
            freeze_cohort=True,
        ),
        cohort_ids=[1, 2],
    )
    assert {row["source_subject_id"] for row in data} == {"a"}
    assert len(data) == 4
    assert all(row["sample_weight"] == 0.5 for row in data)
    assert next(row for row in data if row["repo_id"] == 4)["split"] == "test"


def test_frozen_cohort_requires_reference_catalog(tmp_path: Path) -> None:
    """缺参考目录必须阻断，不能悄悄退回动态人群。"""
    with pytest.raises(ValueError, match="cohort_repositories"):
        _build_dataset_with_config(
            tmp_path,
            [],
            DatasetConfig(
                train_end=datetime(2025, 4, 1, tzinfo=UTC),
                validation_end=datetime(2025, 8, 1, tzinfo=UTC),
                freeze_cohort=True,
            ),
        )


def test_collapses_repeated_subject_repo_to_first_star_event(tmp_path: Path) -> None:
    dataset = _build_dataset(
        tmp_path,
        [
            _interaction("gh:later", "gh", 1, "2025-03-01", "star_event", "2025-03-01"),
            _interaction("gh:first", "gh", 1, "2025-01-01", "star_event", "2025-01-01"),
            _interaction("gh:other", "gh", 2, "2025-02-01", "star_event", "2025-02-01"),
        ],
    )

    repo_one = [row for row in dataset if row["repo_id"] == 1]
    assert len(repo_one) == 1
    assert repo_one[0]["source_record_id"] == "gh:first"
    assert repo_one[0]["occurred_at"] == _time("2025-01-01")


def test_filters_repositories_by_distinct_observed_stargazers(tmp_path: Path) -> None:
    rows = [
        _interaction("u1:1", "u1", 1, "2025-01-01", "star_event", "2025-01-01"),
        _interaction("u1:3", "u1", 3, "2025-01-01", "star_event", "2025-01-01"),
        _interaction("u2:1", "u2", 1, "2025-01-01", "star_event", "2025-01-01"),
        _interaction("u2:3", "u2", 3, "2025-01-01", "star_event", "2025-01-01"),
        _interaction("u3:2", "u3", 2, "2025-01-01", "star_event", "2025-01-01"),
        _interaction("u3:3", "u3", 3, "2025-01-01", "star_event", "2025-01-01"),
    ]

    dataset, repositories = _build_dataset_with_config(
        tmp_path,
        rows,
        DatasetConfig(
            train_end=datetime(2025, 4, 1, tzinfo=UTC),
            validation_end=datetime(2025, 8, 1, tzinfo=UTC),
            minimum_repository_interactions=2,
        ),
    )

    assert {row["repo_id"] for row in dataset} == {1, 3}
    assert {row["repo_id"] for row in repositories} == {1, 3}
    counts = {row["repo_id"]: row["training_stargazer_count"] for row in repositories}
    assert counts == {1: 2, 3: 3}


def test_future_stars_do_not_satisfy_repository_support_threshold(tmp_path: Path) -> None:
    rows = [
        _interaction("u1:1", "u1", 1, "2025-01-01", "star_event", "2025-01-01"),
        _interaction("u1:3", "u1", 3, "2025-01-01", "star_event", "2025-01-01"),
        _interaction("u1:4", "u1", 4, "2025-01-01", "star_event", "2025-01-01"),
        _interaction("u2:1", "u2", 1, "2025-07-01", "star_event", "2025-07-01"),
        _interaction("u2:3", "u2", 3, "2025-01-01", "star_event", "2025-01-01"),
        _interaction("u2:4", "u2", 4, "2025-01-01", "star_event", "2025-01-01"),
    ]

    dataset, repositories = _build_dataset_with_config(
        tmp_path,
        rows,
        DatasetConfig(
            train_end=datetime(2025, 4, 1, tzinfo=UTC),
            validation_end=datetime(2025, 8, 1, tzinfo=UTC),
            minimum_repository_interactions=2,
        ),
    )

    assert {row["repo_id"] for row in repositories} == {3, 4}
    assert {row["repo_id"] for row in dataset} == {3, 4}


def test_filters_overactive_subjects_with_configured_upper_bound(tmp_path: Path) -> None:
    rows = [
        _interaction("u1:1", "u1", 1, "2025-01-01", "star_event", "2025-01-01"),
        _interaction("u1:2", "u1", 2, "2025-01-01", "star_event", "2025-01-01"),
        _interaction("u1:3", "u1", 3, "2025-01-01", "star_event", "2025-01-01"),
        _interaction("u2:1", "u2", 1, "2025-01-01", "star_event", "2025-01-01"),
        _interaction("u2:2", "u2", 2, "2025-01-01", "star_event", "2025-01-01"),
    ]

    dataset, _ = _build_dataset_with_config(
        tmp_path,
        rows,
        DatasetConfig(
            train_end=datetime(2025, 4, 1, tzinfo=UTC),
            validation_end=datetime(2025, 8, 1, tzinfo=UTC),
            maximum_repositories_per_subject=2,
        ),
    )

    assert {row["source_subject_id"] for row in dataset} == {"u2"}
    report = json.loads((tmp_path / "workspace/dataset/quality-report.json").read_text())
    assert report["eligible_subject_count"] == 1
    assert report["training_eligible_subject_count"] == 1


def test_limits_repository_catalog_by_train_support(tmp_path: Path) -> None:
    rows = [
        _interaction("u1:1", "u1", 1, "2025-01-01", "star_event", "2025-01-01"),
        _interaction("u1:2", "u1", 2, "2025-01-01", "star_event", "2025-01-01"),
        _interaction("u2:1", "u2", 1, "2025-01-01", "star_event", "2025-01-01"),
        _interaction("u2:3", "u2", 3, "2025-01-01", "star_event", "2025-01-01"),
        _interaction("u3:1", "u3", 1, "2025-01-01", "star_event", "2025-01-01"),
        _interaction("u3:2", "u3", 2, "2025-01-01", "star_event", "2025-01-01"),
    ]

    dataset, repositories = _build_dataset_with_config(
        tmp_path,
        rows,
        DatasetConfig(
            train_end=datetime(2025, 4, 1, tzinfo=UTC),
            validation_end=datetime(2025, 8, 1, tzinfo=UTC),
            maximum_repository_count=2,
        ),
    )

    assert {row["repo_id"] for row in repositories} == {1, 2}
    assert {row["repo_id"] for row in dataset} == {1, 2}
    report = json.loads((tmp_path / "workspace/dataset/quality-report.json").read_text())
    assert report["candidate_repository_count"] == 3
    assert report["public_repository_count"] == 2


def test_deterministically_samples_subjects(tmp_path: Path) -> None:
    rows = [
        _interaction(f"{subject}:1", subject, 1, "2025-01-01", "star_event", "2025-01-01")
        for subject in [f"u{index}" for index in range(100)]
    ] + [
        _interaction(f"{subject}:2", subject, 2, "2025-01-01", "star_event", "2025-01-01")
        for subject in [f"u{index}" for index in range(100)]
    ]
    config = DatasetConfig(
        train_end=datetime(2025, 4, 1, tzinfo=UTC),
        validation_end=datetime(2025, 8, 1, tzinfo=UTC),
        subject_sample_rate=0.1,
    )

    first, first_repositories = _build_dataset_with_config(tmp_path / "first", rows, config)
    second, _ = _build_dataset_with_config(tmp_path / "second", rows, config)

    first_subjects = {row["source_subject_id"] for row in first}
    second_subjects = {row["source_subject_id"] for row in second}
    assert first_subjects == second_subjects
    assert 0 < len(first_subjects) < 100
    # 仓库支持数仍必须使用抽样前的全量主体，抽样只裁剪昂贵的主体-仓库聚合。
    assert {row["training_stargazer_count"] for row in first_repositories} == {100}


def test_rejects_inconsistent_dataset_activity_filters() -> None:
    with pytest.raises(ValueError, match="maximum_repositories_per_subject"):
        DatasetConfig(
            train_end=datetime(2025, 4, 1, tzinfo=UTC),
            validation_end=datetime(2025, 8, 1, tzinfo=UTC),
            minimum_repositories_per_subject=10,
            maximum_repositories_per_subject=9,
        )
    with pytest.raises(ValueError, match="minimum_repository_interactions"):
        DatasetConfig(
            train_end=datetime(2025, 4, 1, tzinfo=UTC),
            validation_end=datetime(2025, 8, 1, tzinfo=UTC),
            minimum_repository_interactions=0,
        )
    with pytest.raises(ValueError, match="maximum_repository_count"):
        DatasetConfig(
            train_end=datetime(2025, 4, 1, tzinfo=UTC),
            validation_end=datetime(2025, 8, 1, tzinfo=UTC),
            maximum_repository_count=1,
        )
    with pytest.raises(ValueError, match="subject_sample_rate"):
        DatasetConfig(
            train_end=datetime(2025, 4, 1, tzinfo=UTC),
            validation_end=datetime(2025, 8, 1, tzinfo=UTC),
            subject_sample_rate=0,
        )


def _build_dataset(tmp_path: Path, rows: list[dict[str, object]]) -> list[dict[str, object]]:
    dataset, _ = _build_dataset_with_config(
        tmp_path,
        rows,
        DatasetConfig(
            train_end=datetime(2025, 4, 1, tzinfo=UTC),
            validation_end=datetime(2025, 8, 1, tzinfo=UTC),
        ),
    )
    return dataset


def _build_dataset_with_config(
    tmp_path: Path,
    rows: list[dict[str, object]],
    config: DatasetConfig,
    cohort_ids: list[int] | None = None,
) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    tmp_path.mkdir(parents=True, exist_ok=True)
    interactions_path = tmp_path / "interactions.parquet"
    repositories_path = tmp_path / "repositories.parquet"
    pq.write_table(pa.Table.from_pylist(rows, schema=INTERACTION_SCHEMA), interactions_path)
    repo_ids = sorted({int(str(row["repo_id"])) for row in rows})
    pq.write_table(
        pa.Table.from_pylist(
            [_repository(repo_id) for repo_id in repo_ids], schema=REPOSITORY_SCHEMA
        ),
        repositories_path,
    )
    inputs = [
        artifact_for(interactions_path, "canonical_interactions", {}),
        artifact_for(repositories_path, "canonical_repositories", {}),
    ]
    if cohort_ids is not None:
        reference = tmp_path / "reference.parquet"
        pq.write_table(pa.table({"repo_id": cohort_ids}), reference)
        inputs.append(artifact_for(reference, "cohort_repositories", {}))
    artifacts = DuckDBDatasetBuilder(config).build(
        TaskContext("run", tmp_path / "workspace", "sha256:test", 7),
        inputs,
    )
    dataset_path = Path(
        next(item for item in artifacts if item.kind == "training_dataset").uri.removeprefix(
            "file://"
        )
    )
    repository_path = Path(
        next(item for item in artifacts if item.kind == "dataset_repositories").uri.removeprefix(
            "file://"
        )
    )
    return (
        cast(list[dict[str, object]], pq.read_table(dataset_path).to_pylist()),
        cast(list[dict[str, object]], pq.read_table(repository_path).to_pylist()),
    )


def _interaction(
    record_id: str,
    subject: str,
    repo_id: int,
    observed_date: str,
    relation_type: str,
    occurred_date: str | None = None,
) -> dict[str, object]:
    return {
        "source": "starcat_snapshot" if subject == "starcat" else "gh_archive",
        "source_record_id": record_id,
        "source_subject_id": subject,
        "repo_id": repo_id,
        "relation_type": relation_type,
        "occurred_at": _time(occurred_date) if occurred_date else None,
        "observed_at": _time(observed_date),
        "source_weight": 1.0,
        "snapshot_id": observed_date if subject == "starcat" else None,
        "schema_version": 1,
    }


def _repository(repo_id: int, *, archived: bool = False) -> dict[str, object]:
    return {
        "repo_id": repo_id,
        "full_name": f"owner/repo-{repo_id}",
        "description": None,
        "topics": [],
        "primary_language": None,
        "license_key": None,
        "stars": 1,
        "forks": 0,
        "archived": archived,
        "disabled": False,
        "visibility": "public",
        "pushed_at": None,
        "fetched_at": _time("2026-01-01"),
        "metadata_version": 1,
    }


def _time(value: str | None) -> datetime | None:
    return datetime.fromisoformat(f"{value}T00:00:00+00:00") if value else None
