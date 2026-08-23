from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from starcat_recsys.artifacts import artifact_for
from starcat_recsys.interfaces import TaskContext
from starcat_recsys.trainers.svd import SVDConfig, SVDTrainer


def test_svd_finds_repositories_with_shared_subjects(tmp_path: Path) -> None:
    path = tmp_path / "dataset.parquet"
    rows = []
    for subject, repo_ids in {
        "u1": [1, 2],
        "u2": [1, 2],
        "u3": [3, 4],
        "u4": [3, 4],
    }.items():
        rows.extend(
            {
                "source_subject_id": subject,
                "repo_id": repo_id,
                "sample_weight": 1.0,
                "split": "train",
            }
            for repo_id in repo_ids
        )
    pq.write_table(pa.Table.from_pylist(rows), path)

    artifact = SVDTrainer(SVDConfig(components=2, top_k=1)).train(
        TaskContext("run", tmp_path / "workspace", "sha256:test", 17),
        [artifact_for(path, "training_dataset", {})],
    )[0]

    recommendations = pq.read_table(Path(artifact.uri[7:])).to_pylist()
    nearest = {row["source_repo_id"]: row["target_repo_id"] for row in recommendations}
    assert nearest[1] == 2
    assert nearest[2] == 1
    assert nearest[3] == 4
    assert nearest[4] == 3
