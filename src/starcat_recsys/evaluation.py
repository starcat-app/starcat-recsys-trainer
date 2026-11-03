"""item-to-item 模型的时间留出离线评估。

以主体 train 仓库为 seeds，将各 seed 边分数相加后预测 validation 或 test 仓库。
评估器不读取原始 participant 标识，也不将训练主体写入模型产物。
"""

from __future__ import annotations

import hashlib
import math
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from starcat_recsys.artifacts import artifact_for, write_json_atomic
from starcat_recsys.duckdb_runtime import connect_for_workspace
from starcat_recsys.interfaces import Artifact, TaskContext
from starcat_recsys.recommendations import artifact_path, training_dataset


@dataclass(frozen=True, slots=True)
class EvaluationConfig:
    """离线评估切分和截断位。"""

    split: str = "validation"
    k: int = 20
    maximum_subjects: int = 10_000
    maximum_seeds: int | None = None

    def __post_init__(self) -> None:
        if self.split not in {"validation", "test"}:
            raise ValueError("split 只能是 validation 或 test")
        if self.k <= 0:
            raise ValueError("k 必须大于 0")
        if self.maximum_subjects <= 0:
            raise ValueError("maximum_subjects 必须大于 0")
        if self.maximum_seeds is not None and not 0 < self.maximum_seeds <= 20:
            raise ValueError("maximum_seeds 必须在 1～20，与在线多种子上限一致")


class OfflineEvaluator:
    """对多个模型使用同一批时间留出主体进行比较。"""

    def __init__(self, config: EvaluationConfig) -> None:
        self._config = config

    def evaluate(self, context: TaskContext, inputs: list[Artifact]) -> list[Artifact]:
        dataset = training_dataset(inputs)
        models = [artifact for artifact in inputs if artifact.kind == "model_recommendations"]
        if not models:
            raise ValueError("至少需要一个 model_recommendations artifact")

        evaluation_datasets = [item for item in inputs if item.kind == "evaluation_dataset"]
        if len(evaluation_datasets) > 1:
            raise ValueError("evaluation_dataset 不能重复")
        evaluation_dataset = evaluation_datasets[0] if evaluation_datasets else dataset
        seed_histories: dict[str, set[int]] = {}
        histories, truths, catalog = _load_subject_sets(
            artifact_path(evaluation_dataset),
            self._config.split,
            self._config.maximum_subjects,
            context.workspace,
            catalog_path=artifact_path(dataset),
            maximum_seeds=self._config.maximum_seeds,
            seed_histories=seed_histories,
        )
        seed_repo_ids = {repo_id for history in seed_histories.values() for repo_id in history}
        output_directory = context.workspace / "evaluation"
        output_directory.mkdir(parents=True, exist_ok=True)
        results = {}
        for model in models:
            model_name = str(model.metadata.get("model", "unknown"))
            edges = _load_edges(artifact_path(model), seed_repo_ids, context.workspace)
            observations: list[dict[str, Any]] = []
            results[model_name] = self._evaluate_model(
                histories,
                truths,
                catalog,
                edges,
                seed_histories=seed_histories,
                observations=observations,
            )
            if observations:
                # 逐主体结果仅留在私有 workspace，用于同主体配对区间。输出散列而非来源
                # 主体原值；不能把该 Parquet 放入公开 ServingBundle。
                pq.write_table(
                    pa.Table.from_pylist(observations),
                    output_directory / f"{self._config.split}-{model_name}-subjects.parquet",
                    compression="zstd",
                )

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
                "dataset_checksum": dataset.checksum,
                "evaluation_dataset_checksum": evaluation_dataset.checksum,
                "maximum_seeds": self._config.maximum_seeds,
                "model_checksums": {
                    str(model.metadata.get("model", "unknown")): model.checksum for model in models
                },
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
        *,
        seed_histories: dict[str, set[int]] | None = None,
        observations: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        recalls: list[float] = []
        ndcgs: list[float] = []
        reciprocal_ranks: list[float] = []
        recommended_catalog: set[int] = set()
        skipped = 0
        empty = 0
        catalog_recalls: list[float] = []
        total_truths = 0
        catalog_truths = 0

        for subject, truth in sorted(truths.items()):
            history = histories.get(subject)
            if not history or not truth:
                skipped += 1
                continue
            scores: dict[int, float] = defaultdict(float)
            for seed in sorted(
                seed_histories.get(subject, set()) if seed_histories is not None else history
            ):
                for target, score in edges.get(seed, []):
                    if target not in history:
                        scores[target] += score
            ranked = sorted(scores, key=lambda repo_id: (-scores[repo_id], repo_id))[
                : self._config.k
            ]
            recommended_catalog.update(ranked)
            hits = [index for index, repo_id in enumerate(ranked, start=1) if repo_id in truth]
            recalls.append(len(hits) / len(truth))
            in_catalog = truth & catalog
            total_truths += len(truth)
            catalog_truths += len(in_catalog)
            if in_catalog:
                # 内容模型可以覆盖训练样本未观测、但公开候选目录已有的仓库。
                # 辅助的训练观测目录内召回只能计其交集，不能用全部命中除以子集真值。
                catalog_recalls.append(
                    sum(repo_id in in_catalog for repo_id in ranked) / len(in_catalog)
                )
            empty += int(not ranked)
            dcg = sum(1 / math.log2(rank + 1) for rank in hits)
            ideal_count = min(len(truth), self._config.k)
            ideal_dcg = sum(1 / math.log2(rank + 1) for rank in range(1, ideal_count + 1))
            ndcgs.append(dcg / ideal_dcg if ideal_dcg else 0.0)
            reciprocal_ranks.append(1 / hits[0] if hits else 0.0)
            if observations is not None:
                observations.append(
                    {
                        "subject_hash": hashlib.sha256(subject.encode()).hexdigest(),
                        "history_count": len(history),
                        "truth_count": len(truth),
                        "catalog_truth_count": len(in_catalog),
                        "empty": not ranked,
                        "recall": recalls[-1],
                        "ndcg": ndcgs[-1],
                        "mrr": reciprocal_ranks[-1],
                    }
                )

        evaluated = len(recalls)
        return {
            f"recall@{self._config.k}": _mean(recalls),
            f"ndcg@{self._config.k}": _mean(ndcgs),
            f"mrr@{self._config.k}": _mean(reciprocal_ranks),
            "coverage": len(recommended_catalog) / len(catalog) if catalog else 0.0,
            "evaluated_subjects": evaluated,
            "skipped_subjects": skipped,
            "empty_result_rate": empty / evaluated if evaluated else 0.0,
            "catalog_recall": _mean(catalog_recalls),
            "truth_catalog_coverage": catalog_truths / total_truths if total_truths else 0.0,
            "truth_count": total_truths,
            "catalog_truth_count": catalog_truths,
        }


def _load_subject_sets(
    path: Path,
    target_split: str,
    maximum_subjects: int | None = None,
    workspace: Path | None = None,
    *,
    catalog_path: Path | None = None,
    maximum_seeds: int | None = None,
    seed_histories: dict[str, set[int]] | None = None,
) -> tuple[dict[str, set[int]], dict[str, set[int]], set[int]]:
    connection = connect_for_workspace(workspace or path.parent, "evaluation-subjects")
    try:
        connection.read_parquet(path.as_posix()).create_view("dataset")
        connection.read_parquet((catalog_path or path).as_posix()).create_view("catalog_dataset")
        # 来源始终参与主体身份；不同数据源相同的随机字符串绝不能被错误拼接成一人。
        connection.execute("""
            CREATE TEMP VIEW scoped_dataset AS SELECT *,
                to_json([source, source_subject_id]) AS subject_key FROM dataset
        """)
        limit_clause = f"LIMIT {maximum_subjects}" if maximum_subjects is not None else ""
        connection.execute(
            f"""
            CREATE TEMP TABLE evaluation_subjects AS
            SELECT subject_key
            FROM scoped_dataset
            WHERE split = ?
            GROUP BY subject_key
            ORDER BY md5(subject_key), subject_key
            {limit_clause}
            """,
            [target_split],
        )
        time_order = "d.occurred_at DESC NULLS LAST," if maximum_seeds is not None else ""
        rows = connection.execute(
            f"""
            SELECT d.subject_key, d.repo_id, d.split, d.training_eligible
            FROM scoped_dataset d
            INNER JOIN evaluation_subjects USING (subject_key)
            WHERE d.split IN ('train', ?)
            ORDER BY d.subject_key, d.split, {time_order} d.repo_id
            """,
            [target_split],
        ).fetchall()
        catalog_rows = connection.execute(
            """
            SELECT DISTINCT repo_id
            FROM catalog_dataset
            WHERE split = 'train' AND training_eligible
            """
        ).fetchall()
    finally:
        connection.close()
    histories: dict[str, set[int]] = defaultdict(set)
    truths: dict[str, set[int]] = defaultdict(set)
    catalog = {int(row[0]) for row in catalog_rows}
    seeds: dict[str, set[int]] = defaultdict(set)
    for subject_value, repo_value, split, _training_eligible in rows:
        subject = str(subject_value)
        repo_id = int(repo_value)
        if split == "train":
            histories[subject].add(repo_id)
            if _training_eligible and (
                maximum_seeds is None or len(seeds[subject]) < maximum_seeds
            ):
                seeds[subject].add(repo_id)
        elif split == target_split:
            truths[subject].add(repo_id)
    if seed_histories is not None:
        seed_histories.update(seeds)
    return dict(histories), dict(truths), catalog


def _load_edges(
    path: Path,
    source_repo_ids: set[int] | None = None,
    workspace: Path | None = None,
) -> dict[int, list[tuple[int, float]]]:
    connection = connect_for_workspace(workspace or path.parent, "evaluation-edges")
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
