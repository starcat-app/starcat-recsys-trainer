from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

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
    assert len(dataset) == 5
    assert {row["repo_id"] for row in dataset} == {1, 2, 4}
    assert {row["split"] for row in dataset} == {"train", "validation", "test"}
    assert all(row["sample_weight"] > 0 for row in dataset)
    report = json.loads((tmp_path / "workspace/dataset/quality-report.json").read_text())
    assert report["input_interaction_rows"] == 9
    assert report["latest_snapshot_rows"] == 7
    assert report["deduplicated_rows"] == 6
    assert report["public_repository_rows"] == 5


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
