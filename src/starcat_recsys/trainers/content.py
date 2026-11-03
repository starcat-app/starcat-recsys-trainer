"""公开仓库元数据 TF-IDF 冷启动及 co-star 倒数排名融合。

不读取主体、留出标签或 GitHub 私有信息；与共现模型共享同一训练期候选目录。当前
metadata 是事后快照，离线结果必须注明存活/信息时点偏差，不能称为严格时间回测。
"""

from __future__ import annotations

import json
from dataclasses import dataclass

import pyarrow as pa
import pyarrow.parquet as pq
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.neighbors import NearestNeighbors

from starcat_recsys.artifacts import artifact_for
from starcat_recsys.duckdb_runtime import connect_for_workspace
from starcat_recsys.interfaces import Artifact, TaskContext
from starcat_recsys.recommendations import (
    RECOMMENDATION_SCHEMA,
    artifact_path,
    write_recommendations_query,
)


@dataclass(frozen=True, slots=True)
class ContentConfig:
    """限制特征和近邻批次，避免内容模型争抢桌面内存。"""

    top_k: int = 100
    maximum_features: int = 50_000
    batch_size: int = 128

    def __post_init__(self) -> None:
        if min(self.top_k, self.maximum_features, self.batch_size) <= 0:
            raise ValueError("内容模型预算必须大于 0")


class ContentTrainer:
    """以名称、描述、topics 和语言生成可解释的轻量内容相似度。"""

    name = "content"

    def __init__(self, config: ContentConfig) -> None:
        self._config = config

    def train(self, context: TaskContext, inputs: list[Artifact]) -> list[Artifact]:
        """仅在已冻结目录内拟合；零向量不以随机或热门近邻伪装内容推荐。"""
        repositories = [item for item in inputs if item.kind == "dataset_repositories"]
        if len(repositories) != 1:
            raise ValueError("需要一个 dataset_repositories")
        records = pq.read_table(artifact_path(repositories[0])).to_pylist()
        records.sort(key=lambda row: int(row["repo_id"]))
        documents = []
        for row in records:
            # 名称、topics 和语言加权为三个词袋副本；不把 stars 或留出热度作为内容。
            identity = " ".join(str(row.get("full_name") or "").replace("/", " ").split())
            topics = " ".join(row.get("topics") or [])
            language = str(row.get("primary_language") or "")
            documents.append(
                " ".join(
                    [
                        identity,
                        str(row.get("description") or ""),
                        topics,
                        topics,
                        topics,
                        language,
                        language,
                    ]
                )
            )
        vectorizer = TfidfVectorizer(
            max_features=self._config.maximum_features,
            strip_accents="unicode",
            sublinear_tf=True,
            ngram_range=(1, 2),
            stop_words="english",
        )
        matrix = vectorizer.fit_transform(documents)
        neighbors = NearestNeighbors(metric="cosine", algorithm="brute", n_jobs=1)
        neighbors.fit(matrix)
        output = context.workspace / "models/content/recommendations.parquet"
        output.parent.mkdir(parents=True, exist_ok=True)
        row_count = 0
        # 只保留一批候选，百万条边不能累积成 Python 字典列表挤占桌面内存。
        with pq.ParquetWriter(output, RECOMMENDATION_SCHEMA, compression="zstd") as writer:
            for start in range(0, len(records), self._config.batch_size):
                distances, indices = neighbors.kneighbors(
                    matrix[start : start + self._config.batch_size],
                    n_neighbors=min(self._config.top_k + 1, len(records)),
                )
                rows = []
                for offset, (distance_row, index_row) in enumerate(
                    zip(distances, indices, strict=True)
                ):
                    source = int(records[start + offset]["repo_id"])
                    targets = sorted(
                        [
                            (int(records[index]["repo_id"]), max(0.0, 1.0 - float(distance)))
                            for distance, index in zip(distance_row, index_row, strict=True)
                            if int(records[index]["repo_id"]) != source
                            and 1.0 - float(distance) > 1e-6
                        ],
                        key=lambda pair: (-pair[1], pair[0]),
                    )[: self._config.top_k]
                    for rank, (target, score) in enumerate(targets, 1):
                        rows.append(
                            {
                                "source_repo_id": source,
                                "target_repo_id": target,
                                "score": score,
                                "rank": rank,
                                "model": self.name,
                                "signals": json.dumps({"kind": "metadata_tfidf", "cosine": score}),
                            }
                        )
                writer.write_table(pa.Table.from_pylist(rows, schema=RECOMMENDATION_SCHEMA))
                row_count += len(rows)
        return [
            artifact_for(
                output,
                "model_recommendations",
                {
                    "model": self.name,
                    "row_count": row_count,
                },
            )
        ]


def fuse_models(
    context: TaskContext,
    collaborative: Artifact,
    content: Artifact,
    *,
    content_weight: float,
    top_k: int = 100,
) -> Artifact:
    """倒数排名融合避免两种分数尺度不同；无共现的 seed 仍有真实内容近邻。"""
    if not 0 < content_weight < 1 or top_k <= 0:
        raise ValueError("融合权重必须在 (0,1)，top_k 必须为正")
    name = f"hybrid_{round(content_weight * 100)}"
    connection = connect_for_workspace(context.workspace, name)
    try:
        connection.read_parquet(artifact_path(collaborative).as_posix()).create_view(
            "collaborative"
        )
        connection.read_parquet(artifact_path(content).as_posix()).create_view("content")
        return write_recommendations_query(
            connection,
            context.workspace / "models" / name / "recommendations.parquet",
            f"""
                WITH combined AS (
                    SELECT source_repo_id, target_repo_id,
                        {1 - content_weight} / (60.0 + rank) AS score,
                        TRUE AS is_collaborative FROM collaborative
                    UNION ALL
                    SELECT source_repo_id, target_repo_id,
                        {content_weight} / (60.0 + rank) AS score,
                        FALSE AS is_collaborative FROM content
                ), scores AS (
                    SELECT source_repo_id, target_repo_id, sum(score) AS score,
                        bool_or(is_collaborative) AS has_costar,
                        bool_or(NOT is_collaborative) AS has_content
                    FROM combined WHERE source_repo_id <> target_repo_id GROUP BY ALL
                ), ranked AS (
                    SELECT *, row_number() OVER (
                        PARTITION BY source_repo_id ORDER BY score DESC, target_repo_id
                    ) AS rank FROM scores
                )
                SELECT source_repo_id, target_repo_id, CAST(score AS FLOAT) AS score,
                    CAST(rank AS INTEGER) AS rank, '{name}' AS model,
                    CAST(json_object('kind','costar_content_rrf','content_weight',{content_weight},
                        'costar',has_costar,'content',has_content) AS VARCHAR) AS signals
                FROM ranked WHERE rank <= {top_k}
                ORDER BY source_repo_id, rank
            """,
            name,
        )
    finally:
        connection.close()
