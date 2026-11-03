from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from starcat_recsys.artifacts import artifact_for
from starcat_recsys.interfaces import TaskContext
from starcat_recsys.trainers.costar import CoStarConfig, CoStarTrainer


def test_costar_ranks_stronger_shared_support_first(tmp_path: Path) -> None:
    path = tmp_path / "dataset.parquet"
    rows: list[dict[str, object]] = []
    for subject, repo_ids in {
        "u1": [1, 2],
        "u2": [1, 2],
        "u3": [1, 3],
    }.items():
        rows.extend(_row(subject, repo_id) for repo_id in repo_ids)
    pq.write_table(pa.Table.from_pylist(rows), path)

    artifact = CoStarTrainer(
        CoStarConfig(
            reference_time=datetime(2026, 1, 1, tzinfo=UTC),
            half_life_days=365,
            shrinkage=1,
            top_k=2,
        )
    ).train(
        TaskContext("run", tmp_path / "workspace", "sha256:test", 7),
        [artifact_for(path, "training_dataset", {})],
    )[0]

    recommendations = pq.read_table(Path(artifact.uri[7:])).to_pylist()
    repo_one = [row for row in recommendations if row["source_repo_id"] == 1]
    assert [row["target_repo_id"] for row in repo_one] == [2, 3]
    first_signals = json.loads(repo_one[0]["signals"])
    assert first_signals["kind"] == "time_decayed_costar"
    assert first_signals["support"] > json.loads(repo_one[1]["signals"])["support"]


def test_costar_applies_legacy_weight_when_timestamp_is_missing(tmp_path: Path) -> None:
    path = tmp_path / "dataset.parquet"
    pq.write_table(pa.Table.from_pylist([_row("u1", 1, occurred_at=None), _row("u1", 2)]), path)
    artifact = CoStarTrainer(
        CoStarConfig(
            reference_time=datetime(2026, 1, 1, tzinfo=UTC),
            legacy_time_weight=0.25,
            shrinkage=0,
            top_k=1,
        )
    ).train(
        TaskContext("run", tmp_path / "workspace", "sha256:test", 7),
        [artifact_for(path, "training_dataset", {})],
    )[0]

    rows = pq.read_table(Path(artifact.uri[7:])).to_pylist()
    assert len(rows) == 2
    assert json.loads(rows[0]["signals"])["support"] == 0.25


def test_costar_bounds_subject_pair_expansion(tmp_path: Path) -> None:
    path = tmp_path / "dataset.parquet"
    pq.write_table(
        pa.Table.from_pylist(
            [_row(subject, repo_id) for subject in ["u1", "u2"] for repo_id in [1, 2, 3]]
        ),
        path,
    )

    artifact = CoStarTrainer(
        CoStarConfig(
            reference_time=datetime(2026, 1, 1, tzinfo=UTC),
            shrinkage=0,
            maximum_repositories_per_subject=3,
            maximum_pair_observations=3,
            top_k=2,
        )
    ).train(
        TaskContext("run", tmp_path / "workspace", "sha256:test", 7),
        [artifact_for(path, "training_dataset", {})],
    )[0]

    rows = pq.read_table(Path(artifact.uri[7:])).to_pylist()
    assert len(rows) == 6
    assert {json.loads(row["signals"])["support"] for row in rows} == {0.5}


def test_strict_budget_rejects_changing_the_training_cohort(tmp_path: Path) -> None:
    """严格候选模式超预算应失败，而非悄悄训练部分主体。"""
    path = tmp_path / "dataset.parquet"
    pq.write_table(
        pa.Table.from_pylist(
            [_row(subject, repo) for subject in ["u1", "u2"] for repo in [1, 2, 3]]
        ),
        path,
    )
    with pytest.raises(ValueError, match="禁止隐式截断主体"):
        CoStarTrainer(
            CoStarConfig(
                reference_time=datetime(2026, 1, 1, tzinfo=UTC),
                maximum_repositories_per_subject=3,
                maximum_pair_observations=3,
                require_full_pair_budget=True,
            )
        ).train(
            TaskContext("strict", tmp_path / "workspace", "test", 42),
            [artifact_for(path, "training_dataset", {})],
        )


def _row(
    subject: str,
    repo_id: int,
    occurred_at: datetime | None = datetime(2025, 1, 1, tzinfo=UTC),
) -> dict[str, object]:
    return {
        "source_subject_id": subject,
        "repo_id": repo_id,
        "sample_weight": 1.0,
        "training_eligible": True,
        "split": "train",
        "occurred_at": occurred_at,
        "observed_at": datetime(2025, 1, 1, tzinfo=UTC),
    }
