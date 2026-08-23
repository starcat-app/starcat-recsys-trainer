from __future__ import annotations

import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from starcat_recsys.artifacts import artifact_for
from starcat_recsys.evaluation import EvaluationConfig, OfflineEvaluator, _load_subject_sets
from starcat_recsys.interfaces import TaskContext
from starcat_recsys.recommendations import write_recommendations


def test_evaluates_holdout_metrics_for_multiple_models(tmp_path: Path) -> None:
    dataset_path = tmp_path / "dataset.parquet"
    pq.write_table(
        pa.Table.from_pylist(
            [
                _dataset_row("u1", 1, "train"),
                _dataset_row("u1", 2, "validation"),
                _dataset_row("u2", 1, "train"),
                _dataset_row("u2", 3, "validation"),
            ]
        ),
        dataset_path,
    )
    good = write_recommendations(
        tmp_path / "good.parquet",
        [
            _edge(1, 2, 1, "costar"),
            _edge(1, 3, 2, "costar"),
        ],
        "costar",
    )
    weak = write_recommendations(tmp_path / "weak.parquet", [_edge(1, 4, 1, "popular")], "popular")

    artifact = OfflineEvaluator(EvaluationConfig(k=2)).evaluate(
        TaskContext("run", tmp_path / "workspace", "sha256:test", 7),
        [artifact_for(dataset_path, "training_dataset", {}), good, weak],
    )[0]

    result = json.loads(Path(artifact.uri[7:]).read_text())
    assert result["models"]["costar"]["recall@2"] == 1.0
    assert result["models"]["costar"]["mrr@2"] == 0.75
    assert result["models"]["popular"]["recall@2"] == 0.0


def test_validation_catalog_ignores_test_only_repositories(tmp_path: Path) -> None:
    dataset_path = tmp_path / "dataset.parquet"
    pq.write_table(
        pa.Table.from_pylist(
            [
                _dataset_row("u1", 1, "train"),
                _dataset_row("u1", 3, "validation"),
                _dataset_row("u1", 99, "test"),
                _dataset_row("u2", 2, "train"),
            ]
        ),
        dataset_path,
    )

    histories, truths, catalog = _load_subject_sets(dataset_path, "validation")
    metrics = OfflineEvaluator(EvaluationConfig(k=1))._evaluate_model(
        histories,
        truths,
        catalog,
        {1: [(2, 1.0)]},
    )

    assert catalog == {1, 2}
    assert metrics["coverage"] == 0.5


def _edge(source: int, target: int, rank: int, model: str) -> dict[str, object]:
    return {
        "source_repo_id": source,
        "target_repo_id": target,
        "score": float(3 - rank),
        "rank": rank,
        "model": model,
        "signals": "{}",
    }


def _dataset_row(subject: str, repo_id: int, split: str) -> dict[str, object]:
    return {
        "source_subject_id": subject,
        "repo_id": repo_id,
        "split": split,
        "training_eligible": True,
    }
