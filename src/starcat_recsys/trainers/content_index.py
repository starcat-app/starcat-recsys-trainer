"""为目录外冷启动训练可移植的稀疏 TF-IDF 索引。

只输出公开候选的词表、IDF 和 L2 单位向量，不包含主体或行为明细。Go 服务按同一
ASCII 词元规则构造查询向量并计算余弦相似度，不需要在线装载 Python/sklearn。
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pyarrow.parquet as pq
from sklearn.feature_extraction.text import TfidfVectorizer

from starcat_recsys.artifacts import artifact_for
from starcat_recsys.interfaces import Artifact, TaskContext
from starcat_recsys.recommendations import artifact_path

# 名称/描述/topics 的拉丁词元跨 Python 与 Go 保持一致；非拉丁正文仍可通过名称、
# topics 和语言命中。不能把这称为多语种语义 Embedding。
TOKEN_PATTERN = r"[a-z][a-z0-9_]{1,}"


class ContentIndexTrainer:
    """训练有界词表并逐仓写出倒排索引，避免预计算所有仓库两两距离。"""

    name = "content_index"

    def train(self, context: TaskContext, inputs: list[Artifact]) -> list[Artifact]:
        """拟合公开目录；重复 ID 或空词表直接拒绝，不能静默发布坏索引。"""
        artifacts = [item for item in inputs if item.kind == "dataset_repositories"]
        if len(artifacts) != 1:
            raise ValueError("需要一个 dataset_repositories")
        rows = pq.read_table(artifact_path(artifacts[0])).to_pylist()
        rows.sort(key=lambda row: int(row["repo_id"]))
        if len({row["repo_id"] for row in rows}) != len(rows):
            raise ValueError("内容目录有重复仓库 ID")
        for row in rows:
            if row.get("visibility", "public") != "public" or row.get("private", False):
                raise ValueError("内容索引禁止包含私有仓库")
        documents = [
            " ".join(
                [
                    str(row.get("full_name") or ""),
                    str(row.get("description") or ""),
                    *[" ".join(row.get("topics") or [])] * 3,
                    *[str(row.get("primary_language") or "")] * 2,
                ]
            )
            for row in rows
        ]
        vectorizer = TfidfVectorizer(
            token_pattern=TOKEN_PATTERN,
            stop_words="english",
            sublinear_tf=True,
            max_features=50_000,
            # 普通语言词不能独占结果；小测试目录仍保留共同词以便精确核对。
            max_df=0.4 if len(rows) >= 20 else 1.0,
        )
        matrix = vectorizer.fit_transform(documents).tocsr()
        output = context.workspace / "models" / self.name / "index.sqlite"
        output.parent.mkdir(parents=True, exist_ok=True)
        if output.exists():
            raise FileExistsError("内容索引不可覆盖")
        _write_index(output, vectorizer, matrix, [int(row["repo_id"]) for row in rows])
        return [
            artifact_for(
                output,
                "content_index",
                {
                    "model": self.name,
                    "repositories": len(rows),
                    "features": matrix.shape[1],
                    "postings": matrix.nnz,
                    "token_pattern": TOKEN_PATTERN,
                },
            )
        ]


def _write_index(
    output: Path,
    vectorizer: TfidfVectorizer,
    matrix: object,
    repo_ids: list[int],
) -> None:
    """SQLite 主键直接作为倒排索引；只把一仓的非零项转换为 Python 对象。"""
    # SciPy 的稀疏矩阵缺少本项目 strict 所需完整 stubs，局部边界显式标注即可。
    from typing import Any, cast

    vectors = cast(Any, matrix)
    terms = vectorizer.get_feature_names_out()
    with sqlite3.connect(output) as database:
        database.executescript(
            """
            CREATE TABLE content_vocabulary (
                term TEXT PRIMARY KEY, idf REAL NOT NULL
            ) WITHOUT ROWID;
            CREATE TABLE content_postings (
                term TEXT NOT NULL, repo_id INTEGER NOT NULL, weight REAL NOT NULL,
                PRIMARY KEY(term, repo_id)
            ) WITHOUT ROWID;
            """
        )
        database.executemany(
            "INSERT INTO content_vocabulary VALUES (?, ?)",
            ((str(term), float(idf)) for term, idf in zip(terms, vectorizer.idf_, strict=True)),
        )
        for index, repo_id in enumerate(repo_ids):
            row = vectors.getrow(index)
            database.executemany(
                "INSERT INTO content_postings VALUES (?, ?, ?)",
                (
                    (str(terms[column]), repo_id, float(weight))
                    for column, weight in zip(row.indices, row.data, strict=True)
                ),
            )
