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

import pyarrow.parquet as pq

from starcat_recsys.artifacts import artifact_for, write_json_atomic
from starcat_recsys.interfaces import Artifact, TaskContext
from starcat_recsys.recommendations import artifact_path, training_dataset


@dataclass(frozen=True, slots=True)
class EvaluationConfig:
    """离线评估切分和截断位。"""

    split: str = "validation"
    k: int = 20

    def __post_init__(self) -> None:
        if self.split not in {"validation", "test"}:
            raise ValueError("split 只能是 validation 或 test")
        if self.k <= 0:
            raise ValueError("k 必须大于 0")


class OfflineEvaluator:
    """对多个模型使用同一批时间留出主体进行比较。"""

    def __init__(self, config: EvaluationConfig) -> None:
        self._config = config

    def evaluate(self, context: TaskContext, inputs: list[Artifact]) -> list[Artifact]:
        dataset = training_dataset(inputs)
        models = [artifact for artifact in inputs if artifact.kind == "model_recommendations"]
        if not models:
            raise ValueError("至少需要一个 model_recommendations artifact")

        histories, truths, catalog = _load_subject_sets(artifact_path(dataset), self._config.split)
        results = {}
        for model in models:
            model_name = str(model.metadata.get("model", "unknown"))
            edges = _load_edges(artifact_path(model))
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
    path: Path, target_split: str
) -> tuple[dict[str, set[int]], dict[str, set[int]], set[int]]:
    table = pq.read_table(
        path,
        columns=["source_subject_id", "repo_id", "split", "training_eligible"],
    )
    histories: dict[str, set[int]] = defaultdict(set)
    truths: dict[str, set[int]] = defaultdict(set)
    catalog: set[int] = set()
    for row in table.to_pylist():
        subject = str(row["source_subject_id"])
        repo_id = int(row["repo_id"])
        if row["split"] == "train":
            histories[subject].add(repo_id)
            # Coverage 的分母必须是训练时模型可见的 catalog，不能被 validation/test
            # 中首次出现的 repo 改变，否则模型不变时验证指标仍会漂移。
            if row["training_eligible"]:
                catalog.add(repo_id)
        elif row["split"] == target_split:
            truths[subject].add(repo_id)
    return dict(histories), dict(truths), catalog


def _load_edges(path: Path) -> dict[int, list[tuple[int, float]]]:
    edges: dict[int, list[tuple[int, float]]] = defaultdict(list)
    for row in pq.read_table(
        path, columns=["source_repo_id", "target_repo_id", "score"]
    ).to_pylist():
        edges[int(row["source_repo_id"])].append((int(row["target_repo_id"]), float(row["score"])))
    return dict(edges)


def _mean(values: list[float]) -> float:
    return sum(values) / len(values) if values else 0.0
