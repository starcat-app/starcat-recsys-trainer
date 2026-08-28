"""Canonical Parquet 到训练数据集的质量门禁与时间切分。

主要转换由 DuckDB 执行，避免 BigQuery 历史数据全部加载到 Python 内存。
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import duckdb

from starcat_recsys.artifacts import artifact_for, write_json_atomic
from starcat_recsys.domain import parse_datetime
from starcat_recsys.duckdb_runtime import connect_for_workspace
from starcat_recsys.interfaces import Artifact, TaskContext


@dataclass(frozen=True, slots=True)
class DatasetConfig:
    """训练数据的可复现质量和切分参数。"""

    train_end: datetime
    validation_end: datetime
    minimum_repositories_per_subject: int = 2
    maximum_repositories_per_subject: int | None = None
    minimum_repository_interactions: int = 1
    maximum_repository_count: int | None = None
    subject_sample_rate: float = 1.0

    def __post_init__(self) -> None:
        train_end = parse_datetime(self.train_end)
        validation_end = parse_datetime(self.validation_end)
        if train_end is None or validation_end is None:
            raise ValueError("切分时间不能为空")
        if validation_end <= train_end:
            raise ValueError("validation_end 必须晚于 train_end")
        if self.minimum_repositories_per_subject < 2:
            raise ValueError("minimum_repositories_per_subject 不能小于 2")
        if (
            self.maximum_repositories_per_subject is not None
            and self.maximum_repositories_per_subject < self.minimum_repositories_per_subject
        ):
            raise ValueError("maximum_repositories_per_subject 不能小于最小值")
        if self.minimum_repository_interactions <= 0:
            raise ValueError("minimum_repository_interactions 必须大于 0")
        if self.maximum_repository_count is not None and self.maximum_repository_count <= 1:
            raise ValueError("maximum_repository_count 必须大于 1")
        if not 0 < self.subject_sample_rate <= 1:
            raise ValueError("subject_sample_rate 必须在 (0, 1] 范围")


class DuckDBDatasetBuilder:
    """用最新快照和公开仓库元数据生成时间切分。"""

    def __init__(self, config: DatasetConfig) -> None:
        self._config = config

    def build(self, context: TaskContext, inputs: list[Artifact]) -> list[Artifact]:
        interaction = _single_artifact(inputs, "canonical_interactions")
        repository = _single_artifact(inputs, "canonical_repositories")
        output_directory = context.workspace / "dataset"
        output_directory.mkdir(parents=True, exist_ok=True)
        dataset_path = output_directory / "interactions.parquet"
        repository_path = output_directory / "repositories.parquet"

        connection = connect_for_workspace(context.workspace, "dataset")
        try:
            connection.read_parquet(_artifact_path(interaction).as_posix()).create_view(
                "source_interactions"
            )
            connection.read_parquet(_artifact_path(repository).as_posix()).create_view(
                "source_repositories"
            )
            self._build_tables(connection)
            connection.execute(
                "COPY (SELECT * FROM final_dataset ORDER BY source_subject_id, "
                f"COALESCE(occurred_at, observed_at), repo_id) TO {_sql_literal(dataset_path)} "
                "(FORMAT PARQUET, COMPRESSION ZSTD)"
            )
            connection.execute(
                "COPY (SELECT * FROM valid_repositories ORDER BY repo_id) "
                f"TO {_sql_literal(repository_path)} (FORMAT PARQUET, COMPRESSION ZSTD)"
            )
            quality = self._quality_report(connection)
        finally:
            connection.close()

        dataset_artifact = artifact_for(
            dataset_path,
            "training_dataset",
            {"row_count": quality["final_rows"], "split_counts": quality["split_counts"]},
        )
        repository_artifact = artifact_for(
            repository_path,
            "dataset_repositories",
            {"row_count": quality["public_repository_count"]},
        )
        write_json_atomic(
            output_directory / "quality-report.json",
            {
                "schema_version": 1,
                "run_id": context.run_id,
                "config_hash": context.config_hash,
                "config": {
                    "train_end": self._config.train_end.isoformat(),
                    "validation_end": self._config.validation_end.isoformat(),
                    "minimum_repositories_per_subject": (
                        self._config.minimum_repositories_per_subject
                    ),
                    "maximum_repositories_per_subject": (
                        self._config.maximum_repositories_per_subject
                    ),
                    "minimum_repository_interactions": (
                        self._config.minimum_repository_interactions
                    ),
                    "maximum_repository_count": self._config.maximum_repository_count,
                    "subject_sample_rate": self._config.subject_sample_rate,
                },
                **quality,
            },
        )
        return [dataset_artifact, repository_artifact]

    def _build_tables(self, connection: duckdb.DuckDBPyConnection) -> None:
        train_end = self._config.train_end.isoformat()
        validation_end = self._config.validation_end.isoformat()
        minimum = self._config.minimum_repositories_per_subject
        maximum = self._config.maximum_repositories_per_subject
        minimum_repository_interactions = self._config.minimum_repository_interactions
        maximum_repository_count = self._config.maximum_repository_count
        # DuckDB hash() 的实现不承诺跨版本稳定；固定长度 MD5 前缀按字典序比较，才能让
        # 相同配置在依赖小版本升级后仍选择同一批 source-local subject。
        subject_sample_filter = ""
        if self._config.subject_sample_rate < 1:
            threshold = int(self._config.subject_sample_rate * (1 << 64))
            subject_sample_filter = (
                "WHERE substr(md5(i.source || ':' || i.source_subject_id), 1, 16) "
                f"< '{threshold:016x}'"
            )
        connection.execute(
            """
            CREATE TEMP TABLE latest_repositories AS
            SELECT * EXCLUDE (row_number)
            FROM (
                SELECT *, row_number() OVER (
                    PARTITION BY repo_id ORDER BY metadata_version DESC, fetched_at DESC
                ) AS row_number
                FROM source_repositories
            )
            WHERE row_number = 1
            """
        )
        # GitHub metadata目录已经定义本轮可训练的公开仓库集合。先用它裁剪十年互动流，
        # 避免为最终必然丢弃的仓库执行全局快照聚合和窗口去重，显著降低单机 spill。
        connection.execute(
            """
            CREATE TEMP TABLE candidate_source_interactions AS
            SELECT i.*
            FROM source_interactions i
            INNER JOIN latest_repositories r USING (repo_id)
            WHERE r.visibility = 'public'
              AND NOT r.archived
              AND NOT r.disabled
            """
        )
        connection.execute(
            """
            CREATE TEMP TABLE latest_snapshot_times AS
            SELECT source_subject_id, max(observed_at) AS observed_at
            FROM candidate_source_interactions
            WHERE relation_type = 'current_star_membership'
            GROUP BY source_subject_id
            """
        )
        connection.execute(
            """
            CREATE TEMP TABLE current_interactions AS
            SELECT i.*
            FROM candidate_source_interactions i
            LEFT JOIN latest_snapshot_times s
              ON i.source_subject_id = s.source_subject_id
            WHERE i.relation_type <> 'current_star_membership'
               OR i.observed_at = s.observed_at
            """
        )
        connection.execute(
            """
            CREATE TEMP TABLE deduplicated_interactions AS
            SELECT * EXCLUDE (row_number)
            FROM (
                SELECT *, row_number() OVER (
                    PARTITION BY source, source_record_id
                    ORDER BY observed_at DESC
                ) AS row_number
                FROM current_interactions
            )
            WHERE row_number = 1
            """
        )
        connection.execute(
            f"""
            CREATE TEMP TABLE repository_interaction_statistics AS
            SELECT
                repo_id,
                count(DISTINCT (source, source_subject_id)) AS training_stargazer_count
            FROM deduplicated_interactions
            WHERE occurred_at IS NULL
               OR occurred_at < TIMESTAMPTZ '{train_end}'
            GROUP BY repo_id
            """
        )
        connection.execute(
            f"""
            CREATE TEMP TABLE candidate_repositories AS
            SELECT r.*, s.training_stargazer_count
            FROM latest_repositories r
            INNER JOIN repository_interaction_statistics s USING (repo_id)
            WHERE r.visibility = 'public'
              AND NOT r.archived
              AND NOT r.disabled
              AND s.training_stargazer_count >= {minimum_repository_interactions}
            """
        )
        repository_limit_predicate = (
            f"WHERE repository_rank <= {maximum_repository_count}"
            if maximum_repository_count is not None
            else ""
        )
        connection.execute(
            f"""
            CREATE TEMP TABLE valid_repositories AS
            SELECT * EXCLUDE (repository_rank)
            FROM (
                SELECT *, row_number() OVER (
                    ORDER BY training_stargazer_count DESC, repo_id
                ) AS repository_rank
                FROM candidate_repositories
            )
            {repository_limit_predicate}
            """
        )
        # 仓库支持度和 Top N 已使用全量主体计算完成；此后提前应用与最终数据集相同的
        # 稳定抽样，避免为最终会被丢弃的主体构建数亿个主体-仓库聚合状态。
        connection.execute(
            f"""
            CREATE TEMP TABLE valid_interactions AS
            SELECT i.*
            FROM deduplicated_interactions i
            INNER JOIN valid_repositories r USING (repo_id)
            {subject_sample_filter}
            """
        )
        connection.execute(
            """
            CREATE TEMP TABLE unique_subject_repositories AS
            SELECT chosen.*
            FROM (
                -- arg_min 直接在分组聚合中选择目标行，避免 row_number 为十年互动流
                -- 生成第二份全局窗口排序；排序键保持“最早 Star、最新快照”的原语义。
                SELECT arg_min(
                    i,
                    row(
                        CASE
                            WHEN relation_type = 'star_event' AND occurred_at IS NOT NULL THEN 0
                            ELSE 1
                        END,
                        COALESCE(epoch_us(occurred_at), 9223372036854775807),
                        -epoch_us(observed_at),
                        source_record_id
                    )
                ) AS chosen
                FROM valid_interactions i
                GROUP BY source, source_subject_id, repo_id
            )
            """
        )
        connection.execute(
            f"""
            CREATE TEMP TABLE subject_statistics AS
            SELECT
                source,
                source_subject_id,
                count(DISTINCT repo_id) AS total_repo_count,
                count(DISTINCT repo_id) FILTER (
                    WHERE occurred_at IS NULL
                       OR occurred_at < TIMESTAMPTZ '{train_end}'
                ) AS train_repo_count
            FROM unique_subject_repositories
            GROUP BY source, source_subject_id
            """
        )
        maximum_predicate = f"AND total_repo_count <= {maximum}" if maximum is not None else ""
        connection.execute(
            f"""
            CREATE TEMP TABLE eligible_subjects AS
            SELECT *
            FROM subject_statistics
            WHERE total_repo_count >= {minimum}
              {maximum_predicate}
            """
        )
        connection.execute(
            f"""
            CREATE TEMP TABLE final_dataset AS
            SELECT
                i.*,
                CAST(i.source_weight / log2(2 + s.train_repo_count) AS FLOAT) AS sample_weight,
                s.train_repo_count >= {minimum} AS training_eligible,
                CASE
                    WHEN i.occurred_at IS NULL THEN 'train'
                    WHEN i.occurred_at < TIMESTAMPTZ '{train_end}' THEN 'train'
                    WHEN i.occurred_at < TIMESTAMPTZ '{validation_end}' THEN 'validation'
                    ELSE 'test'
                END AS split
            FROM unique_subject_repositories i
            INNER JOIN eligible_subjects s USING (source, source_subject_id)
            """
        )

    def _quality_report(self, connection: duckdb.DuckDBPyConnection) -> dict[str, Any]:
        tables = {
            "input_interaction_rows": "source_interactions",
            "latest_snapshot_rows": "current_interactions",
            "deduplicated_rows": "deduplicated_interactions",
            "public_repository_rows": "valid_interactions",
            "unique_subject_repository_rows": "unique_subject_repositories",
            "final_rows": "final_dataset",
        }
        report: dict[str, Any] = {key: _count(connection, table) for key, table in tables.items()}
        report["public_repository_count"] = _count(connection, "valid_repositories")
        report["candidate_repository_count"] = _count(connection, "candidate_repositories")
        report["eligible_subject_count"] = _count(connection, "eligible_subjects")
        report["training_eligible_subject_count"] = _count(
            connection,
            "eligible_subjects",
            f"train_repo_count >= {self._config.minimum_repositories_per_subject}",
        )
        report["missing_occurred_at_rows"] = _count(
            connection, "final_dataset", "occurred_at IS NULL"
        )
        report["split_counts"] = {
            str(split): int(count)
            for split, count in connection.execute(
                "SELECT split, count(*) FROM final_dataset GROUP BY split ORDER BY split"
            ).fetchall()
        }
        report["minimum_subject_weight"] = 1 / math.log2(
            2 + self._config.minimum_repositories_per_subject
        )
        return report


def _single_artifact(inputs: list[Artifact], kind: str) -> Artifact:
    matches = [artifact for artifact in inputs if artifact.kind == kind]
    if len(matches) != 1:
        raise ValueError(f"需要且只能有一个 {kind} artifact，实际为 {len(matches)}")
    return matches[0]


def _artifact_path(artifact: Artifact) -> Path:
    if not artifact.uri.startswith("file://"):
        raise ValueError("当前 DatasetBuilder 只支持 file:// artifact")
    return Path(artifact.uri.removeprefix("file://"))


def _count(
    connection: duckdb.DuckDBPyConnection,
    table: str,
    where: str | None = None,
) -> int:
    query = f"SELECT count(*) FROM {table}"
    if where:
        query += f" WHERE {where}"
    row = connection.execute(query).fetchone()
    if row is None:
        raise RuntimeError(f"无法读取 {table} 行数")
    return int(row[0])


def _sql_literal(path: Path) -> str:
    """DuckDB COPY 不接受 prepared parameter，因此对本地路径做 SQL 字面量转义。"""

    return "'" + path.as_posix().replace("'", "''") + "'"
