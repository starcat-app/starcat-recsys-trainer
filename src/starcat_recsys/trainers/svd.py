"""TruncatedSVD 隐式反馈基线。

使用稀疏 subject-repo 矩阵获取 item embedding，再通过 cosine 近邻输出统一 Top-K 边。
"""

from __future__ import annotations

import json
from dataclasses import dataclass

import duckdb
import numpy as np
from scipy.sparse import csr_matrix
from sklearn.decomposition import TruncatedSVD
from sklearn.neighbors import NearestNeighbors

from starcat_recsys.interfaces import Artifact, TaskContext
from starcat_recsys.recommendations import (
    artifact_path,
    training_dataset,
    write_recommendations,
)


@dataclass(frozen=True, slots=True)
class SVDConfig:
    """SVD 维数和候选数配置。"""

    components: int = 64
    top_k: int = 100

    def __post_init__(self) -> None:
        if self.components <= 0:
            raise ValueError("components 必须大于 0")
        if self.top_k <= 0:
            raise ValueError("top_k 必须大于 0")


class SVDTrainer:
    """以固定 seed 训练并生成 item-to-item 近邻。"""

    name = "svd"

    def __init__(self, config: SVDConfig) -> None:
        self._config = config

    def train(self, context: TaskContext, inputs: list[Artifact]) -> list[Artifact]:
        dataset = training_dataset(inputs)
        connection = duckdb.connect()
        try:
            connection.read_parquet(artifact_path(dataset).as_posix()).create_view("dataset")
            values = connection.execute(
                """
                SELECT source_subject_id, repo_id, sum(sample_weight) AS weight
                FROM dataset
                WHERE split = 'train'
                GROUP BY source_subject_id, repo_id
                ORDER BY source_subject_id, repo_id
                """
            ).fetchall()
        finally:
            connection.close()

        subjects = sorted({str(row[0]) for row in values})
        repo_ids = sorted({int(row[1]) for row in values})
        if len(subjects) < 2 or len(repo_ids) < 2:
            raise ValueError("SVD 至少需要 2 个主体和 2 个仓库")
        subject_index = {value: index for index, value in enumerate(subjects)}
        repo_index = {value: index for index, value in enumerate(repo_ids)}
        matrix = csr_matrix(
            (
                [float(row[2]) for row in values],
                (
                    [subject_index[str(row[0])] for row in values],
                    [repo_index[int(row[1])] for row in values],
                ),
            ),
            shape=(len(subjects), len(repo_ids)),
            dtype=np.float32,
        )
        component_count = min(self._config.components, min(matrix.shape) - 1)
        svd = TruncatedSVD(n_components=component_count, random_state=context.random_seed)
        svd.fit(matrix)
        item_embeddings = svd.components_.T

        neighbor_count = min(self._config.top_k + 1, len(repo_ids))
        neighbors = NearestNeighbors(metric="cosine", algorithm="brute", n_jobs=-1)
        neighbors.fit(item_embeddings)
        distances, indices = neighbors.kneighbors(
            item_embeddings, n_neighbors=neighbor_count, return_distance=True
        )

        recommendations = []
        signal = json.dumps(
            {"kind": "svd_cosine", "components": component_count},
            separators=(",", ":"),
            sort_keys=True,
        )
        for source_index, source_repo_id in enumerate(repo_ids):
            candidates = [
                (repo_ids[int(target_index)], max(0.0, 1.0 - float(distance)))
                for target_index, distance in zip(
                    indices[source_index], distances[source_index], strict=True
                )
                if int(target_index) != source_index
            ]
            candidates.sort(key=lambda value: (-value[1], value[0]))
            for rank, (target_repo_id, score) in enumerate(
                candidates[: self._config.top_k], start=1
            ):
                recommendations.append(
                    {
                        "source_repo_id": source_repo_id,
                        "target_repo_id": target_repo_id,
                        "score": score,
                        "rank": rank,
                        "model": self.name,
                        "signals": signal,
                    }
                )

        artifact = write_recommendations(
            context.workspace / "models" / self.name / "recommendations.parquet",
            recommendations,
            self.name,
        )
        return [artifact]
