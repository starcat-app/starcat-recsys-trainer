from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from starcat_recsys.artifacts import artifact_for
from starcat_recsys.interfaces import TaskContext
from starcat_recsys.trainers.popular import PopularConfig, PopularTrainer


def test_popular_ranks_weighted_train_interactions(tmp_path: Path) -> None:
    path = tmp_path / "dataset.parquet"
    pq.write_table(
        pa.Table.from_pylist(
            [
                _row("u1", 1, 1.0),
                _row("u1", 2, 1.0),
                _row("u2", 2, 1.0),
                _row("u2", 3, 0.5),
                _row("u2", 4, 100.0, split="test"),
            ]
        ),
        path,
    )

    artifact = PopularTrainer(PopularConfig(top_k=2)).train(
        TaskContext("run", tmp_path / "workspace", "sha256:test", 7),
        [artifact_for(path, "training_dataset", {})],
    )[0]

    rows = pq.read_table(Path(artifact.uri[7:])).to_pylist()
    repo_one = [row for row in rows if row["source_repo_id"] == 1]
    assert [row["target_repo_id"] for row in repo_one] == [2, 3]
    assert {row["source_repo_id"] for row in rows} == {1, 2, 3}
    assert all(row["target_repo_id"] != row["source_repo_id"] for row in rows)
    assert {row["model"] for row in rows} == {"popular"}


def _row(subject: str, repo_id: int, weight: float, *, split: str = "train") -> dict[str, object]:
    return {
        "source_subject_id": subject,
        "repo_id": repo_id,
        "sample_weight": weight,
        "training_eligible": split == "train",
        "split": split,
        "occurred_at": datetime(2025, 1, 1, tzinfo=UTC),
    }
