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
    artifacts = DuckDBDatasetBuilder(config).build(
        TaskContext("run", tmp_path / "workspace", "sha256:test", 7),
        [
            artifact_for(interactions_path, "canonical_interactions", {}),
            artifact_for(repositories_path, "canonical_repositories", {}),
        ],
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
