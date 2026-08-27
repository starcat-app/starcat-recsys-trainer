"""时间衰减 co-star 训练器。

按主体共现统计 repo pair，同时使用时间半衰期、超大集合主体权重、cosine
归一化和 shrinkage。所有 pair 计算由 DuckDB 执行，避免 Python 内存累积组合。
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

import duckdb

from starcat_recsys.domain import parse_datetime
from starcat_recsys.interfaces import Artifact, TaskContext
from starcat_recsys.recommendations import (
    artifact_path,
    training_dataset,
    write_recommendations_query,
)


@dataclass(frozen=True, slots=True)
class CoStarConfig:
    """co-star 时间、平滑和 pair 预算参数。"""

    reference_time: datetime
    half_life_days: float = 365.0
    legacy_time_weight: float = 0.35
    shrinkage: float = 10.0
    minimum_support: float = 0.0
    maximum_repositories_per_subject: int = 2_000
    maximum_pair_observations: int = 100_000_000
    top_k: int = 100
    model_name: str = "costar"

    def __post_init__(self) -> None:
        if parse_datetime(self.reference_time) is None:
            raise ValueError("reference_time 不能为空")
        if self.half_life_days <= 0:
            raise ValueError("half_life_days 必须大于 0")
        if not 0 < self.legacy_time_weight <= 1:
            raise ValueError("legacy_time_weight 必须在 (0, 1] 范围")
        if self.shrinkage < 0 or self.minimum_support < 0:
            raise ValueError("shrinkage 和 minimum_support 不能为负数")
        if self.maximum_repositories_per_subject < 2:
            raise ValueError("maximum_repositories_per_subject 不能小于 2")
        maximum_subject_pairs = (
            self.maximum_repositories_per_subject * (self.maximum_repositories_per_subject - 1) // 2
        )
        if self.maximum_pair_observations < maximum_subject_pairs:
            raise ValueError("maximum_pair_observations 不能小于单个主体的最大 pair 数")
        if self.top_k <= 0:
            raise ValueError("top_k 必须大于 0")
        if not self.model_name or not self.model_name.replace("_", "").isalnum():
            raise ValueError("model_name 只能包含字母、数字和下划线")


class CoStarTrainer:
    """生成对称的 item-to-item co-star Top-K 边。"""

    def __init__(self, config: CoStarConfig) -> None:
        self._config = config

    @property
    def name(self) -> str:
        return self._config.model_name

    def train(self, context: TaskContext, inputs: list[Artifact]) -> list[Artifact]:
        dataset = training_dataset(inputs)
        connection = duckdb.connect()
        try:
            connection.read_parquet(artifact_path(dataset).as_posix()).create_view("dataset")
            artifact = write_recommendations_query(
                connection,
                context.workspace / "models" / self.name / "recommendations.parquet",
                self._query(),
                self.name,
            )
        finally:
            connection.close()
        return [artifact]

    def _query(self) -> str:
        config = self._config
        reference_time = config.reference_time.isoformat()
        return f"""
        WITH weighted AS (
            SELECT
                source_subject_id,
                repo_id,
                sample_weight AS subject_weight,
                CASE
                    WHEN occurred_at IS NULL THEN {config.legacy_time_weight}
                    ELSE exp(
                        -ln(2) * greatest(
                            0,
                            date_diff('second', occurred_at, TIMESTAMPTZ '{reference_time}')
                        ) / 86400.0 / {config.half_life_days}
                    )
                END AS time_weight,
                row_number() OVER (
                    PARTITION BY source_subject_id, repo_id
                    ORDER BY COALESCE(occurred_at, observed_at) DESC
                ) AS duplicate_rank
            FROM dataset
            WHERE split = 'train' AND training_eligible
        ),
        unique_items AS (
            SELECT * EXCLUDE (duplicate_rank)
            FROM weighted
            WHERE duplicate_rank = 1
        ),
        capped AS (
            SELECT * EXCLUDE (subject_rank)
            FROM (
                SELECT *, row_number() OVER (
                    PARTITION BY source_subject_id
                    ORDER BY time_weight DESC, repo_id
                ) AS subject_rank
                FROM unique_items
            )
            WHERE subject_rank <= {config.maximum_repositories_per_subject}
        ),
        subject_pair_counts AS (
            SELECT
                source_subject_id,
                count(*) * (count(*) - 1) / 2 AS pair_count
            FROM capped
            GROUP BY source_subject_id
            HAVING count(*) >= 2
        ),
        sampled_subjects AS (
            SELECT source_subject_id
            FROM (
                SELECT
                    source_subject_id,
                    sum(pair_count) OVER (
                        ORDER BY hash(source_subject_id), source_subject_id
                    ) AS cumulative_pair_count
                FROM subject_pair_counts
            )
            WHERE cumulative_pair_count <= {config.maximum_pair_observations}
        ),
        sampled AS (
            SELECT capped.*
            FROM capped
            INNER JOIN sampled_subjects USING (source_subject_id)
        ),
        diagonals AS (
            SELECT
                repo_id,
                sum(subject_weight * time_weight) AS diagonal_support
            FROM sampled
            GROUP BY repo_id
        ),
        pair_support AS (
            SELECT
                a.repo_id AS left_repo_id,
                b.repo_id AS right_repo_id,
                sum(
                    least(a.subject_weight, b.subject_weight)
                    * least(a.time_weight, b.time_weight)
                ) AS support
            FROM sampled a
            INNER JOIN sampled b
              ON a.source_subject_id = b.source_subject_id
             AND a.repo_id < b.repo_id
            GROUP BY a.repo_id, b.repo_id
        ),
        scored AS (
            SELECT
                left_repo_id,
                right_repo_id,
                support,
                support / sqrt(dl.diagonal_support * dr.diagonal_support) AS cosine,
                support / sqrt(dl.diagonal_support * dr.diagonal_support)
                    * support / (support + {config.shrinkage}) AS score
            FROM pair_support
            INNER JOIN diagonals dl ON left_repo_id = dl.repo_id
            INNER JOIN diagonals dr ON right_repo_id = dr.repo_id
            WHERE support >= {config.minimum_support}
        ),
        symmetric_edges AS (
            SELECT left_repo_id AS source_repo_id, right_repo_id AS target_repo_id,
                   score, support, cosine
            FROM scored
            UNION ALL
            SELECT right_repo_id AS source_repo_id, left_repo_id AS target_repo_id,
                   score, support, cosine
            FROM scored
        ),
        ranked AS (
            SELECT *, row_number() OVER (
                PARTITION BY source_repo_id
                ORDER BY score DESC, target_repo_id
            ) AS rank
            FROM symmetric_edges
        )
        SELECT
            CAST(source_repo_id AS BIGINT) AS source_repo_id,
            CAST(target_repo_id AS BIGINT) AS target_repo_id,
            CAST(score AS FLOAT) AS score,
            CAST(rank AS INTEGER) AS rank,
            '{self.name}'::VARCHAR AS model,
            CAST(json_object(
                'cosine', round(cosine, 8),
                'half_life_days', {config.half_life_days},
                'kind', 'time_decayed_costar',
                'support', round(support, 8)
            ) AS VARCHAR) AS signals
        FROM ranked
        WHERE rank <= {config.top_k}
        ORDER BY source_repo_id, rank
        """
