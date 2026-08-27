"""item-to-item 模型的时间留出离线评估。

以主体 train 仓库为 seeds，将各 seed 边分数相加后预测 validation 或 test 仓库。
评估器不读取原始 participant 标识，也不将训练主体写入模型产物。
"""

from __future__ import annotations

import math
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import duckdb
import pyarrow as pa

from starcat_recsys.artifacts import artifact_for, write_json_atomic
from starcat_recsys.interfaces import Artifact, TaskContext
from starcat_recsys.recommendations import artifact_path, training_dataset


@dataclass(frozen=True, slots=True)
class EvaluationConfig:
    """离线评估切分和截断位。"""

    split: str = "validation"
    k: int = 20
    maximum_subjects: int = 10_000

    def __post_init__(self) -> None:
        if self.split not in {"validation", "test"}:
            raise ValueError("split 只能是 validation 或 test")
        if self.k <= 0:
            raise ValueError("k 必须大于 0")
        if self.maximum_subjects <= 0:
            raise ValueError("maximum_subjects 必须大于 0")


class OfflineEvaluator:
    """对多个模型使用同一批时间留出主体进行比较。"""

    def __init__(self, config: EvaluationConfig) -> None:
        self._config = config

    def evaluate(self, context: TaskContext, inputs: list[Artifact]) -> list[Artifact]:
        dataset = training_dataset(inputs)
        models = [artifact for artifact in inputs if artifact.kind == "model_recommendations"]
        if not models:
            raise ValueError("至少需要一个 model_recommendations artifact")

        histories, truths, catalog = _load_subject_sets(
            artifact_path(dataset),
            self._config.split,
            self._config.maximum_subjects,
        )
        seed_repo_ids = {repo_id for history in histories.values() for repo_id in history}
        results = {}
        for model in models:
            model_name = str(model.metadata.get("model", "unknown"))
            edges = _load_edges(artifact_path(model), seed_repo_ids)
            results[model_name] = self._evaluate_model(histories, truths, catalog, edges)

        output_directory = context.workspace / "evaluation"
        output_directory.mkdir(parents=True, exist_ok=True)
        output_path = output_directory / f"{self._config.split}-metrics.json"
        write_json_atomic(
            output_path,
            {
                "schema_version": 1,
                "run_id": context.run_id,
                "config_hash": context.config_hash,
                "split": self._config.split,
                "k": self._config.k,
                "models": results,
            },
        )
        return [
            artifact_for(
                output_path,
                "evaluation_metrics",
                {"split": self._config.split, "models": sorted(results)},
            )
        ]

    def _evaluate_model(
        self,
        histories: dict[str, set[int]],
        truths: dict[str, set[int]],
        catalog: set[int],
        edges: dict[int, list[tuple[int, float]]],
    ) -> dict[str, Any]:
        recalls: list[float] = []
        ndcgs: list[float] = []
        reciprocal_ranks: list[float] = []
        recommended_catalog: set[int] = set()
        skipped = 0

        for subject, truth in sorted(truths.items()):
            history = histories.get(subject)
            if not history or not truth:
                skipped += 1
                continue
            scores: dict[int, float] = defaultdict(float)
            for seed in history:
                for target, score in edges.get(seed, []):
                    if target not in history:
                        scores[target] += score
            ranked = sorted(scores, key=lambda repo_id: (-scores[repo_id], repo_id))[
                : self._config.k
            ]
            recommended_catalog.update(ranked)
            hits = [index for index, repo_id in enumerate(ranked, start=1) if repo_id in truth]
            recalls.append(len(hits) / len(truth))
            dcg = sum(1 / math.log2(rank + 1) for rank in hits)
            ideal_count = min(len(truth), self._config.k)
            ideal_dcg = sum(1 / math.log2(rank + 1) for rank in range(1, ideal_count + 1))
            ndcgs.append(dcg / ideal_dcg if ideal_dcg else 0.0)
            reciprocal_ranks.append(1 / hits[0] if hits else 0.0)

        evaluated = len(recalls)
        return {
            f"recall@{self._config.k}": _mean(recalls),
            f"ndcg@{self._config.k}": _mean(ndcgs),
            f"mrr@{self._config.k}": _mean(reciprocal_ranks),
            "coverage": len(recommended_catalog) / len(catalog) if catalog else 0.0,
            "evaluated_subjects": evaluated,
            "skipped_subjects": skipped,
        }


def _load_subject_sets(
    path: Path, target_split: str, maximum_subjects: int | None = None
) -> tuple[dict[str, set[int]], dict[str, set[int]], set[int]]:
    connection = duckdb.connect()
    try:
        connection.read_parquet(path.as_posix()).create_view("dataset")
        limit_clause = f"LIMIT {maximum_subjects}" if maximum_subjects is not None else ""
        connection.execute(
            f"""
            CREATE TEMP TABLE evaluation_subjects AS
            SELECT source_subject_id
            FROM dataset
            WHERE split = ?
            GROUP BY source_subject_id
            ORDER BY hash(source_subject_id), source_subject_id
            {limit_clause}
            """,
            [target_split],
        )
        rows = connection.execute(
            """
            SELECT d.source_subject_id, d.repo_id, d.split, d.training_eligible
            FROM dataset d
            INNER JOIN evaluation_subjects USING (source_subject_id)
            WHERE d.split IN ('train', ?)
            ORDER BY d.source_subject_id, d.split, d.repo_id
            """,
            [target_split],
        ).fetchall()
        catalog_rows = connection.execute(
            """
            SELECT DISTINCT repo_id
            FROM dataset
            WHERE split = 'train' AND training_eligible
            """
        ).fetchall()
    finally:
        connection.close()
    histories: dict[str, set[int]] = defaultdict(set)
    truths: dict[str, set[int]] = defaultdict(set)
    catalog = {int(row[0]) for row in catalog_rows}
    for subject_value, repo_value, split, _training_eligible in rows:
        subject = str(subject_value)
        repo_id = int(repo_value)
        if split == "train":
            histories[subject].add(repo_id)
        elif split == target_split:
            truths[subject].add(repo_id)
    return dict(histories), dict(truths), catalog


def _load_edges(
    path: Path, source_repo_ids: set[int] | None = None
) -> dict[int, list[tuple[int, float]]]:
    connection = duckdb.connect()
    try:
        connection.read_parquet(path.as_posix()).create_view("model_edges")
        if source_repo_ids is not None:
            if not source_repo_ids:
                return {}
            connection.register(
                "evaluation_seeds",
                pa.table({"source_repo_id": sorted(source_repo_ids)}),
            )
            rows = connection.execute(
                """
                SELECT e.source_repo_id, e.target_repo_id, e.score
                FROM model_edges e
                INNER JOIN evaluation_seeds USING (source_repo_id)
                ORDER BY e.source_repo_id, e.rank, e.target_repo_id
                """
            ).fetchall()
        else:
            rows = connection.execute(
                """
                SELECT source_repo_id, target_repo_id, score
                FROM model_edges
                ORDER BY source_repo_id, rank, target_repo_id
                """
            ).fetchall()
    finally:
        connection.close()
    edges: dict[int, list[tuple[int, float]]] = defaultdict(list)
    for source, target, score in rows:
        edges[int(source)].append((int(target), float(score)))
    return dict(edges)


def _mean(values: list[float]) -> float:
    return sum(values) / len(values) if values else 0.0
