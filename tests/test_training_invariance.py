"""训练器对 validation/test 数据变化的隔离回归测试。"""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from starcat_recsys.artifacts import artifact_for
from starcat_recsys.interfaces import Artifact, TaskContext
from starcat_recsys.trainers.costar import CoStarConfig, CoStarTrainer
from starcat_recsys.trainers.popular import PopularConfig, PopularTrainer
from starcat_recsys.trainers.svd import SVDConfig, SVDTrainer


def test_all_trainers_are_invariant_to_future_rows(tmp_path: Path) -> None:
    base_path = tmp_path / "base.parquet"
    future_path = tmp_path / "future.parquet"
    train_rows = [
        _row("u1", 1, "train", "2025-01-01"),
        _row("u1", 2, "train", "2025-02-01"),
        _row("u2", 1, "train", "2025-01-01"),
        _row("u2", 3, "train", "2025-03-01"),
        _row("u3", 4, "train", "2025-01-01"),
        _row("u3", 5, "train", "2025-04-01"),
    ]
    pq.write_table(pa.Table.from_pylist(train_rows), base_path)
    pq.write_table(
        pa.Table.from_pylist(
            [
                *train_rows,
                _row("u1", 6, "validation", "2025-07-01"),
                _row("u2", 7, "test", "2026-01-01"),
            ]
        ),
        future_path,
    )

    base_checksums = _train_checksums(tmp_path / "base", base_path)
    future_checksums = _train_checksums(tmp_path / "future", future_path)

    assert base_checksums == future_checksums


def _train_checksums(workspace: Path, dataset_path: Path) -> dict[str, str]:
    """以相同 seed 训练三个模型，checksum 不同即表示未来行进入了训练。"""

    context = TaskContext("run", workspace, "sha256:test", 42)
    dataset = [artifact_for(dataset_path, "training_dataset", {})]
    artifacts: list[Artifact] = []
    artifacts.extend(PopularTrainer(PopularConfig(top_k=3)).train(context, dataset))
    artifacts.extend(SVDTrainer(SVDConfig(components=2, top_k=3)).train(context, dataset))
    artifacts.extend(
        CoStarTrainer(
            CoStarConfig(
                reference_time=datetime(2025, 6, 1, tzinfo=UTC),
                half_life_days=365,
                shrinkage=1,
                top_k=3,
            )
        ).train(context, dataset)
    )
    return {str(artifact.metadata["model"]): artifact.checksum for artifact in artifacts}


def _row(subject: str, repo_id: int, split: str, occurred_date: str) -> dict[str, object]:
    occurred_at = datetime.fromisoformat(f"{occurred_date}T00:00:00+00:00")
    return {
        "source_subject_id": subject,
        "repo_id": repo_id,
        "sample_weight": 0.5,
        "training_eligible": True,
        "split": split,
        "occurred_at": occurred_at,
        "observed_at": occurred_at,
    }
