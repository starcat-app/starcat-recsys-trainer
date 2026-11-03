"""内容冷启动与融合契约回归；不依赖网络或真实主体。"""

from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from starcat_recsys.artifacts import artifact_for
from starcat_recsys.interfaces import TaskContext
from starcat_recsys.recommendations import write_recommendations
from starcat_recsys.trainers.content import ContentConfig, ContentTrainer, fuse_models


def test_content_and_fusion_keep_cold_start_and_unique_targets(tmp_path: Path) -> None:
    """相近描述靠前，完全没有共现出边的仓库仍能被内容模型覆盖。"""
    path = tmp_path / "repos.parquet"
    pq.write_table(
        pa.Table.from_pylist(
            [
                {
                    "repo_id": i,
                    "full_name": name,
                    "description": text,
                    "topics": [topic],
                    "primary_language": "Python",
                }
                for i, name, text, topic in [
                    (1, "one/parser", "SQL database query parser", "database"),
                    (2, "two/parser", "SQL database query analyzer", "database"),
                    (3, "three/art", "music image painting studio", "art"),
                ]
            ]
        ),
        path,
    )
    context = TaskContext("test", tmp_path / "run", "sha256:test", 7)
    content = ContentTrainer(ContentConfig(top_k=2, batch_size=1)).train(
        context, [artifact_for(path, "dataset_repositories", {})]
    )[0]
    rows = pq.read_table(Path(content.uri[7:])).to_pylist()
    assert next(row for row in rows if row["source_repo_id"] == 1)["target_repo_id"] == 2
    costar = write_recommendations(
        tmp_path / "costar.parquet",
        [
            {
                "source_repo_id": 1,
                "target_repo_id": 2,
                "rank": 1,
                "score": 1.0,
                "model": "costar",
                "signals": "{}",
            }
        ],
        "costar",
    )
    hybrid = fuse_models(context, costar, content, content_weight=0.5)
    merged = pq.read_table(Path(hybrid.uri[7:])).to_pylist()
    assert {row["source_repo_id"] for row in merged} == {1, 2, 3}
    assert all(row["source_repo_id"] != row["target_repo_id"] for row in merged)
    assert len({(row["source_repo_id"], row["target_repo_id"]) for row in merged}) == len(merged)
