"""TruncatedSVD 隐式反馈基线。

使用稀疏 subject-repo 矩阵获取 item embedding，再通过 cosine 近邻输出统一 Top-K 边。
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import duckdb
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
from scipy.sparse import csr_matrix
from sklearn.decomposition import TruncatedSVD
from sklearn.neighbors import NearestNeighbors

from starcat_recsys.artifacts import artifact_for
from starcat_recsys.duckdb_runtime import connect_for_workspace
from starcat_recsys.interfaces import Artifact, InsufficientTrainingDataError, TaskContext
from starcat_recsys.recommendations import (
    RECOMMENDATION_SCHEMA,
    artifact_path,
    training_dataset,
)


@dataclass(frozen=True, slots=True)
class SVDConfig:
    """SVD 维数和候选数配置。"""

    components: int = 64
    top_k: int = 100
    maximum_interactions: int = 5_000_000
    maximum_subjects: int = 200_000
    maximum_repositories: int = 50_000
    neighbor_batch_size: int = 1_000

    def __post_init__(self) -> None:
        if self.components <= 0:
            raise ValueError("components 必须大于 0")
        if self.top_k <= 0:
            raise ValueError("top_k 必须大于 0")
        if self.maximum_interactions < 4:
            raise ValueError("maximum_interactions 不能小于 4")
        if self.maximum_subjects < 2:
            raise ValueError("maximum_subjects 不能小于 2")
        if self.maximum_repositories < 2:
            raise ValueError("maximum_repositories 不能小于 2")
        if self.neighbor_batch_size <= 0:
            raise ValueError("neighbor_batch_size 必须大于 0")


class SVDTrainer:
    """以固定 seed 训练并生成 item-to-item 近邻。"""

    name = "svd"

    def __init__(self, config: SVDConfig) -> None:
        self._config = config

    def train(self, context: TaskContext, inputs: list[Artifact]) -> list[Artifact]:
        dataset = training_dataset(inputs)
        connection = connect_for_workspace(context.workspace, self.name)
        try:
            connection.read_parquet(artifact_path(dataset).as_posix()).create_view("dataset")
            self._prepare_values(connection)
            subjects, repo_ids, matrix = self._sparse_matrix(connection)
        finally:
            connection.close()

        if len(subjects) < 2 or len(repo_ids) < 2:
            raise InsufficientTrainingDataError("SVD 至少需要 2 个主体和 2 个仓库")
        column_mean = np.asarray(matrix.mean(axis=0)).ravel()
        column_square_mean = np.asarray(matrix.power(2).mean(axis=0)).ravel()
        if float(np.sum(column_square_mean - np.square(column_mean))) <= 1e-12:
            raise InsufficientTrainingDataError("SVD 训练矩阵没有可分解的列方差")
        component_count = min(self._config.components, min(matrix.shape) - 1)
        svd = TruncatedSVD(n_components=component_count, random_state=context.random_seed)
        svd.fit(matrix)
        item_embeddings = svd.components_.T

        neighbor_count = min(self._config.top_k + 1, len(repo_ids))
        neighbors = NearestNeighbors(metric="cosine", algorithm="brute", n_jobs=-1)
        neighbors.fit(item_embeddings)
        signal = json.dumps(
            {"kind": "svd_cosine", "components": component_count},
            separators=(",", ":"),
            sort_keys=True,
        )
        output_path = context.workspace / "models" / self.name / "recommendations.parquet"
        row_count = self._write_neighbors(
            output_path,
            repo_ids,
            item_embeddings,
            neighbors,
            neighbor_count,
            signal,
        )
        return [
            artifact_for(
                output_path,
                "model_recommendations",
                {"model": self.name, "row_count": row_count},
            )
        ]

    def _prepare_values(self, connection: duckdb.DuckDBPyConnection) -> None:
        """在 DuckDB 内先限制仓库、主体和非零项，再进入 SciPy。"""

        config = self._config
        connection.execute(
            f"""
            CREATE TEMP TABLE svd_repositories AS
            SELECT repo_id
            FROM dataset
            WHERE split = 'train' AND training_eligible
            GROUP BY repo_id
            ORDER BY sum(sample_weight) DESC, repo_id
            LIMIT {config.maximum_repositories}
            """
        )
        connection.execute(
            """
            CREATE TEMP TABLE svd_candidate_values AS
            SELECT d.source_subject_id, d.repo_id, sum(d.sample_weight) AS weight
            FROM dataset d
            INNER JOIN svd_repositories USING (repo_id)
            WHERE d.split = 'train' AND d.training_eligible
            GROUP BY d.source_subject_id, d.repo_id
            """
        )
        connection.execute(
            f"""
            CREATE TEMP TABLE svd_subject_candidates AS
            SELECT source_subject_id, count(*) AS interaction_count
            FROM svd_candidate_values
            GROUP BY source_subject_id
            ORDER BY md5(source_subject_id), source_subject_id
            LIMIT {config.maximum_subjects}
            """
        )
        connection.execute(
            f"""
            CREATE TEMP TABLE svd_subjects AS
            SELECT source_subject_id
            FROM (
                SELECT
                    source_subject_id,
                    sum(interaction_count) OVER (
                        ORDER BY md5(source_subject_id), source_subject_id
                    ) AS cumulative_interactions
                FROM svd_subject_candidates
            )
            WHERE cumulative_interactions <= {config.maximum_interactions}
            """
        )
        connection.execute(
            """
            CREATE TEMP TABLE svd_values AS
            SELECT values.*
            FROM svd_candidate_values values
            INNER JOIN svd_subjects USING (source_subject_id)
            """
        )

    def _sparse_matrix(
        self, connection: duckdb.DuckDBPyConnection
    ) -> tuple[list[str], list[int], csr_matrix]:
        """批量读取有界非零项，避免保留数百万个 Python tuple。"""

        subjects = [
            str(row[0])
            for row in connection.execute(
                "SELECT DISTINCT source_subject_id FROM svd_values ORDER BY source_subject_id"
            ).fetchall()
        ]
        repo_ids = [
            int(row[0])
            for row in connection.execute(
                "SELECT DISTINCT repo_id FROM svd_values ORDER BY repo_id"
            ).fetchall()
        ]
        if len(subjects) < 2 or len(repo_ids) < 2:
            return subjects, repo_ids, csr_matrix((len(subjects), len(repo_ids)), dtype=np.float32)

        count_row = connection.execute("SELECT count(*) FROM svd_values").fetchone()
        value_count = int(count_row[0] if count_row is not None else 0)
        row_indices = np.empty(value_count, dtype=np.int32)
        column_indices = np.empty(value_count, dtype=np.int32)
        weights = np.empty(value_count, dtype=np.float32)
        subject_index = {value: index for index, value in enumerate(subjects)}
        repo_index = {value: index for index, value in enumerate(repo_ids)}

        cursor = connection.execute(
            "SELECT source_subject_id, repo_id, weight "
            "FROM svd_values ORDER BY source_subject_id, repo_id"
        )
        offset = 0
        while rows := cursor.fetchmany(100_000):
            end = offset + len(rows)
            row_indices[offset:end] = [subject_index[str(row[0])] for row in rows]
            column_indices[offset:end] = [repo_index[int(row[1])] for row in rows]
            weights[offset:end] = [float(row[2]) for row in rows]
            offset = end
        if offset != value_count:
            raise RuntimeError("SVD 非零项读取数量与 DuckDB 统计不一致")
        return (
            subjects,
            repo_ids,
            csr_matrix(
                (weights, (row_indices, column_indices)),
                shape=(len(subjects), len(repo_ids)),
                dtype=np.float32,
            ),
        )

    def _write_neighbors(
        self,
        path: Path,
        repo_ids: list[int],
        item_embeddings: np.ndarray,
        neighbors: NearestNeighbors,
        neighbor_count: int,
        signal: str,
    ) -> int:
        """分批近邻检索并追加 Parquet，限制距离矩阵和结果行的峰值内存。"""

        path.parent.mkdir(parents=True, exist_ok=True)
        row_count = 0
        with pq.ParquetWriter(path, RECOMMENDATION_SCHEMA, compression="zstd") as writer:
            for start in range(0, len(repo_ids), self._config.neighbor_batch_size):
                end = min(start + self._config.neighbor_batch_size, len(repo_ids))
                distances, indices = neighbors.kneighbors(
                    item_embeddings[start:end],
                    n_neighbors=neighbor_count,
                    return_distance=True,
                )
                rows: list[dict[str, Any]] = []
                for batch_index, source_index in enumerate(range(start, end)):
                    candidates = [
                        (repo_ids[int(target_index)], max(0.0, 1.0 - float(distance)))
                        for target_index, distance in zip(
                            indices[batch_index], distances[batch_index], strict=True
                        )
                        if int(target_index) != source_index
                    ]
                    candidates.sort(key=lambda value: (-value[1], value[0]))
                    rows.extend(
                        {
                            "source_repo_id": repo_ids[source_index],
                            "target_repo_id": target_repo_id,
                            "score": score,
                            "rank": rank,
                            "model": self.name,
                            "signals": signal,
                        }
                        for rank, (target_repo_id, score) in enumerate(
                            candidates[: self._config.top_k], start=1
                        )
                    )
                writer.write_table(pa.Table.from_pylist(rows, schema=RECOMMENDATION_SCHEMA))
                row_count += len(rows)
        return row_count
