from __future__ import annotations

from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from starcat_recsys.artifacts import artifact_for
from starcat_recsys.interfaces import TaskContext
from starcat_recsys.trainers.svd import SVDConfig, SVDTrainer


def test_svd_finds_repositories_with_shared_subjects(tmp_path: Path) -> None:
    path = tmp_path / "dataset.parquet"
    rows: list[dict[str, object]] = []
    for subject, repo_ids in {
        "u1": [1, 2],
        "u2": [1, 2],
        "u3": [3, 4],
        "u4": [3, 4],
    }.items():
        rows.extend(
            {
                "source_subject_id": subject,
                "repo_id": repo_id,
                "sample_weight": 1.0,
                "training_eligible": True,
                "split": "train",
            }
            for repo_id in repo_ids
        )
    pq.write_table(pa.Table.from_pylist(rows), path)

    artifact = SVDTrainer(SVDConfig(components=2, top_k=1)).train(
        TaskContext("run", tmp_path / "workspace", "sha256:test", 17),
        [artifact_for(path, "training_dataset", {})],
    )[0]

    recommendations = pq.read_table(Path(artifact.uri[7:])).to_pylist()
    nearest = {row["source_repo_id"]: row["target_repo_id"] for row in recommendations}
    assert nearest[1] == 2
    assert nearest[2] == 1
    assert nearest[3] == 4
    assert nearest[4] == 3


def test_svd_rejects_homogeneous_matrix_without_runtime_warning(tmp_path: Path) -> None:
    path = tmp_path / "dataset.parquet"
    pq.write_table(
        pa.Table.from_pylist(
            [
                {
                    "source_subject_id": subject,
                    "repo_id": repo_id,
                    "sample_weight": 1.0,
                    "training_eligible": True,
                    "split": "train",
                }
                for subject in ["u1", "u2"]
                for repo_id in [1, 2]
            ]
        ),
        path,
    )

    with pytest.raises(ValueError, match="列方差"):
        SVDTrainer(SVDConfig(components=1, top_k=1)).train(
            TaskContext("run", tmp_path / "workspace", "sha256:test", 7),
            [artifact_for(path, "training_dataset", {})],
        )


def test_svd_preprocessing_enforces_all_memory_bounds(tmp_path: Path) -> None:
    path = tmp_path / "dataset.parquet"
    pq.write_table(
        pa.Table.from_pylist(
            [
                {
                    "source_subject_id": subject,
                    "repo_id": repo_id,
                    "sample_weight": 1.0,
                    "training_eligible": True,
                    "split": "train",
                }
                for subject in [f"u{index}" for index in range(10)]
                for repo_id in range(1, 6)
            ]
        ),
        path,
    )
    trainer = SVDTrainer(
        SVDConfig(
            components=2,
            top_k=1,
            maximum_interactions=10,
            maximum_subjects=3,
            maximum_repositories=3,
            neighbor_batch_size=2,
        )
    )
    connection = duckdb.connect()
    try:
        connection.read_parquet(path.as_posix()).create_view("dataset")
        trainer._prepare_values(connection)
        interaction_count = connection.execute("SELECT count(*) FROM svd_values").fetchone()
        subject_count = connection.execute(
            "SELECT count(DISTINCT source_subject_id) FROM svd_values"
        ).fetchone()
        repository_count = connection.execute(
            "SELECT count(DISTINCT repo_id) FROM svd_values"
        ).fetchone()
    finally:
        connection.close()

    assert interaction_count is not None and interaction_count[0] <= 10
    assert subject_count is not None and subject_count[0] <= 3
    assert repository_count is not None and repository_count[0] <= 3
