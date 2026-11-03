"""冷启动索引的公开性、余弦计算与不可变产物回归。"""

import sqlite3
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from starcat_recsys.artifacts import artifact_for
from starcat_recsys.bundle import _attach_content_index
from starcat_recsys.interfaces import TaskContext
from starcat_recsys.trainers.content_index import ContentIndexTrainer


def test_index_cosine_and_bundle_attachment(tmp_path: Path) -> None:
    """倒排点积应等于单位向量余弦；附加索引不能改已有推荐表。"""
    path = tmp_path / "repositories.parquet"
    pq.write_table(
        pa.Table.from_pylist(
            [
                {
                    "repo_id": i,
                    "full_name": name,
                    "description": text,
                    "topics": [],
                    "primary_language": None,
                }
                for i, name, text in [
                    (1, "alpha/parser", "database sql parser"),
                    (2, "beta/parser", "database sql parser"),
                    (3, "gamma/music", "guitar piano sound"),
                ]
            ]
        ),
        path,
    )
    context = TaskContext("test", tmp_path / "run", "sha256:test", 7)
    inputs = [artifact_for(path, "dataset_repositories", {})]
    artifact = ContentIndexTrainer().train(context, inputs)[0]
    with sqlite3.connect(Path(artifact.uri[7:])) as database:
        norms = database.execute(
            "SELECT SUM(weight*weight) FROM content_postings GROUP BY repo_id"
        ).fetchall()
        assert [row[0] for row in norms] == pytest.approx([1, 1, 1])
        near = database.execute(
            "SELECT SUM(a.weight*b.weight) FROM content_postings a "
            "JOIN content_postings b USING(term) WHERE a.repo_id=1 AND b.repo_id=2"
        ).fetchone()[0]
        assert near > 0.5
        assert (
            database.execute(
                "SELECT COUNT(*) FROM content_postings a JOIN content_postings b USING(term) "
                "WHERE a.repo_id=1 AND b.repo_id=3"
            ).fetchone()[0]
            == 0
        )
    target = tmp_path / "serving.sqlite"
    with sqlite3.connect(target) as database:
        database.executescript(
            "CREATE TABLE repositories(repo_id INTEGER PRIMARY KEY);"
            "INSERT INTO repositories VALUES(1),(2),(3);"
            "CREATE TABLE recommendations(score REAL); INSERT INTO recommendations VALUES(0.42);"
        )
    _attach_content_index(target, Path(artifact.uri[7:]))
    with sqlite3.connect(target) as database:
        assert database.execute("SELECT score FROM recommendations").fetchone()[0] == 0.42
        assert database.execute("SELECT COUNT(*) FROM content_vocabulary").fetchone()[0] > 0
    with pytest.raises(FileExistsError):
        ContentIndexTrainer().train(context, inputs)


@pytest.mark.parametrize("private", [True, False])
def test_private_or_duplicate_catalog_rejected(tmp_path: Path, private: bool) -> None:
    """混入私有资料或重复 ID 都必须在拟合前拒绝。"""
    path = tmp_path / "repos.parquet"
    rows = [{"repo_id": 1, "full_name": "one/parser", "private": private}]
    if not private:
        rows *= 2
    pq.write_table(pa.Table.from_pylist(rows), path)
    with pytest.raises(ValueError):
        ContentIndexTrainer().train(
            TaskContext("test", tmp_path, "sha256:test", 7),
            [artifact_for(path, "dataset_repositories", {})],
        )
