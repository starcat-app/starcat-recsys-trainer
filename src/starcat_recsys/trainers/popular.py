"""全局热度基线。

该模型不使用仓库间关联，作为冷启动 fallback 和离线评估下限。
"""

from __future__ import annotations

from dataclasses import dataclass

import duckdb

from starcat_recsys.interfaces import Artifact, TaskContext
from starcat_recsys.recommendations import (
    artifact_path,
    training_dataset,
    write_recommendations,
)


@dataclass(frozen=True, slots=True)
class PopularConfig:
    """全局热度模型参数。"""

    top_k: int = 100

    def __post_init__(self) -> None:
        if self.top_k <= 0:
            raise ValueError("top_k 必须大于 0")


class PopularTrainer:
    """将加权互动量转换为每个 seed repo 可查询的 Top-K。"""

    name = "popular"

    def __init__(self, config: PopularConfig) -> None:
        self._config = config

    def train(self, context: TaskContext, inputs: list[Artifact]) -> list[Artifact]:
        dataset = training_dataset(inputs)
        connection = duckdb.connect()
        try:
            connection.read_parquet(artifact_path(dataset).as_posix()).create_view("dataset")
            rows = connection.execute(
                """
                WITH popularity AS (
                    SELECT repo_id, sum(sample_weight) AS score
                    FROM dataset
                    WHERE split = 'train' AND training_eligible
                    GROUP BY repo_id
                ),
                seeds AS (
                    SELECT DISTINCT repo_id
                    FROM dataset
                    WHERE split = 'train' AND training_eligible
                ),
                candidates AS (
                    SELECT
                        seeds.repo_id AS source_repo_id,
                        popularity.repo_id AS target_repo_id,
                        popularity.score,
                        row_number() OVER (
                            PARTITION BY seeds.repo_id
                            ORDER BY popularity.score DESC, popularity.repo_id
                        ) AS rank
                    FROM seeds CROSS JOIN popularity
                    WHERE seeds.repo_id <> popularity.repo_id
                )
                SELECT source_repo_id, target_repo_id, score, rank
                FROM candidates
                WHERE rank <= ?
                ORDER BY source_repo_id, rank
                """,
                [self._config.top_k],
            ).fetchall()
        finally:
            connection.close()

        recommendations = [
            {
                "source_repo_id": int(source),
                "target_repo_id": int(target),
                "score": float(score),
                "rank": int(rank),
                "model": self.name,
                "signals": '{"kind":"global_popularity"}',
            }
            for source, target, score, rank in rows
        ]
        artifact = write_recommendations(
            context.workspace / "models" / self.name / "recommendations.parquet",
            recommendations,
            self.name,
        )
        return [artifact]
