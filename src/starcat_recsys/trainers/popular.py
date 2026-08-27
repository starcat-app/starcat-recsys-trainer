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
    write_recommendations_query,
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
            artifact = write_recommendations_query(
                connection,
                context.workspace / "models" / self.name / "recommendations.parquet",
                f"""
                WITH popularity AS (
                    SELECT
                        repo_id,
                        sum(sample_weight) AS score,
                        row_number() OVER (
                            ORDER BY sum(sample_weight) DESC, repo_id
                        ) AS global_rank
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
                            ORDER BY popularity.global_rank
                        ) AS rank
                    FROM seeds
                    CROSS JOIN popularity
                    WHERE seeds.repo_id <> popularity.repo_id
                      AND popularity.global_rank <= {self._config.top_k + 1}
                )
                SELECT
                    CAST(source_repo_id AS BIGINT) AS source_repo_id,
                    CAST(target_repo_id AS BIGINT) AS target_repo_id,
                    CAST(score AS FLOAT) AS score,
                    CAST(rank AS INTEGER) AS rank,
                    'popular'::VARCHAR AS model,
                    '{{"kind":"global_popularity"}}'::VARCHAR AS signals
                FROM candidates
                WHERE rank <= {self._config.top_k}
                ORDER BY source_repo_id, rank
                """,
                self.name,
            )
        finally:
            connection.close()
        return [artifact]
