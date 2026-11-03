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
        "source": "gh_archive",
        "source_subject_id": subject,
        "repo_id": repo_id,
        "split": split,
        "training_eligible": True,
    }


def test_independent_evaluation_retains_outside_catalog_and_scopes_sources(tmp_path: Path) -> None:
    """独立标签不能被训练目录静默删掉；相同主体字符串不得跨来源合并。"""
    training = tmp_path / "train.parquet"
    pq.write_table(pa.Table.from_pylist([_dataset_row("trainer", 1, "train")]), training)
    rows = [
        _dataset_row("same", 1, "train"),
        _dataset_row("same", 2, "validation"),
        {**_dataset_row("same", 99, "validation"), "source": "starcat_snapshot"},
    ]
    evaluation = tmp_path / "evaluation.parquet"
    pq.write_table(pa.Table.from_pylist(rows), evaluation)
    model = write_recommendations(tmp_path / "edges.parquet", [_edge(1, 2, 1, "costar")], "costar")
    result_artifact = OfflineEvaluator(EvaluationConfig(k=1)).evaluate(
        TaskContext("independent", tmp_path / "workspace", "sha256:test", 7),
        [
            artifact_for(training, "training_dataset", {}),
            artifact_for(evaluation, "evaluation_dataset", {}),
            model,
        ],
    )[0]
    result = json.loads(Path(result_artifact.uri[7:]).read_text())
    assert result["evaluation_dataset_checksum"] != result["dataset_checksum"]
    assert result["models"]["costar"]["evaluated_subjects"] == 1
    assert result["models"]["costar"]["skipped_subjects"] == 1
    assert result["models"]["costar"]["recall@1"] == 1
    assert result["models"]["costar"]["truth_catalog_coverage"] == 0


def test_seed_limit_keeps_full_history_exclusion(tmp_path: Path) -> None:
    """查询只发最近代表种子，但所有历史仍参与排除。"""
    path = tmp_path / "dataset.parquet"
    rows = [
        {**_dataset_row("u", repo, split), "occurred_at": date}
        for repo, split, date in [
            (1, "train", "2026-01-01"),
            (2, "train", "2026-02-01"),
            (3, "validation", "2026-03-01"),
        ]
    ]
    pq.write_table(pa.Table.from_pylist(rows), path)
    seeds: dict[str, set[int]] = {}
    histories, truths, catalog = _load_subject_sets(
        path, "validation", maximum_seeds=1, seed_histories=seeds
    )
    assert list(seeds.values()) == [{2}]
    result = OfflineEvaluator(EvaluationConfig(k=1))._evaluate_model(
        histories, truths, catalog, {2: [(1, 10), (3, 1)]}, seed_histories=seeds
    )
    assert result["recall@1"] == 1


def test_catalog_recall_counts_only_hits_inside_its_denominator() -> None:
    """主召回保留所有真值，辅助目录内召回不能因为目录外命中变成大于 1。"""
    metrics = OfflineEvaluator(EvaluationConfig(k=2))._evaluate_model(
        {"u": {1}}, {"u": {2, 3}}, {1, 2}, {1: [(3, 2.0), (2, 1.0)]}
    )
    assert metrics["recall@2"] == 1
    assert metrics["catalog_recall"] == 1
    assert metrics["truth_catalog_coverage"] == 0.5
