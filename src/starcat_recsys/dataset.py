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
from starcat_recsys.interfaces import Artifact, TaskContext


@dataclass(frozen=True, slots=True)
class DatasetConfig:
    """训练数据的可复现质量和切分参数。"""

    train_end: datetime
    validation_end: datetime
    minimum_repositories_per_subject: int = 2

    def __post_init__(self) -> None:
        train_end = parse_datetime(self.train_end)
        validation_end = parse_datetime(self.validation_end)
        if train_end is None or validation_end is None:
            raise ValueError("切分时间不能为空")
        if validation_end <= train_end:
            raise ValueError("validation_end 必须晚于 train_end")
        if self.minimum_repositories_per_subject < 2:
            raise ValueError("minimum_repositories_per_subject 不能小于 2")


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

        connection = duckdb.connect()
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
                },
                **quality,
            },
        )
        return [dataset_artifact, repository_artifact]

    def _build_tables(self, connection: duckdb.DuckDBPyConnection) -> None:
        train_end = self._config.train_end.isoformat()
        validation_end = self._config.validation_end.isoformat()
        minimum = self._config.minimum_repositories_per_subject
        connection.execute(
            """
            CREATE TEMP TABLE latest_repositories AS
            SELECT * EXCLUDE (row_number)
            FROM (
                SELECT *, row_number() OVER (
                    PARTITION BY repo_id ORDER BY fetched_at DESC
                ) AS row_number
                FROM source_repositories
            )
            WHERE row_number = 1
            """
        )
        connection.execute(
            """
            CREATE TEMP TABLE valid_repositories AS
            SELECT * FROM latest_repositories
            WHERE visibility = 'public' AND NOT archived AND NOT disabled
            """
        )
        connection.execute(
            """
            CREATE TEMP TABLE latest_snapshot_times AS
            SELECT source_subject_id, max(observed_at) AS observed_at
            FROM source_interactions
            WHERE relation_type = 'current_star_membership'
            GROUP BY source_subject_id
            """
        )
        connection.execute(
            """
            CREATE TEMP TABLE current_interactions AS
            SELECT i.*
            FROM source_interactions i
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
            """
            CREATE TEMP TABLE valid_interactions AS
            SELECT i.*
            FROM deduplicated_interactions i
            INNER JOIN valid_repositories r USING (repo_id)
            """
        )
        connection.execute(
            f"""
            CREATE TEMP TABLE eligible_subjects AS
            SELECT source_subject_id, count(DISTINCT repo_id) AS repo_count
            FROM valid_interactions
            GROUP BY source_subject_id
            HAVING count(DISTINCT repo_id) >= {minimum}
            """
        )
        connection.execute(
            f"""
            CREATE TEMP TABLE final_dataset AS
            SELECT
                i.*,
                CAST(i.source_weight / log2(2 + s.repo_count) AS FLOAT) AS sample_weight,
                CASE
                    WHEN i.occurred_at IS NULL THEN 'train'
                    WHEN i.occurred_at < TIMESTAMPTZ '{train_end}' THEN 'train'
                    WHEN i.occurred_at < TIMESTAMPTZ '{validation_end}' THEN 'validation'
                    ELSE 'test'
                END AS split
            FROM valid_interactions i
            INNER JOIN eligible_subjects s USING (source_subject_id)
            """
        )

    def _quality_report(self, connection: duckdb.DuckDBPyConnection) -> dict[str, Any]:
        tables = {
            "input_interaction_rows": "source_interactions",
            "latest_snapshot_rows": "current_interactions",
            "deduplicated_rows": "deduplicated_interactions",
            "public_repository_rows": "valid_interactions",
            "final_rows": "final_dataset",
        }
        report: dict[str, Any] = {key: _count(connection, table) for key, table in tables.items()}
        report["public_repository_count"] = _count(connection, "valid_repositories")
        report["eligible_subject_count"] = _count(connection, "eligible_subjects")
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
