"""独立抽样的合成回归，不访问真实主体或外置数据。"""

import json
from datetime import UTC, datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from starcat_recsys.artifacts import artifact_for
from starcat_recsys.evaluation_sampling import EvaluationSampleConfig, IndependentEvaluationBuilder
from starcat_recsys.interfaces import TaskContext


def test_independent_sample_excludes_training_subjects_and_keeps_unknown_targets(
    tmp_path: Path,
) -> None:
    """首次 Star 去重、独立人群和目录外标签必须同时满足。"""
    rows = []
    for subject in ["train", *[f"u{i}" for i in range(50)]]:
        for repo in range(1, 6):
            rows.append(
                {
                    "source": "gh_archive",
                    "source_subject_id": subject,
                    "repo_id": repo,
                    "relation_type": "star_event",
                    "occurred_at": datetime(2026, 8, 1, tzinfo=UTC),
                }
            )
        for month in (9, 10):
            rows.append(
                {
                    "source": "gh_archive",
                    "source_subject_id": subject,
                    "repo_id": month,
                    "relation_type": "star_event",
                    "occurred_at": datetime(2026, month, 5, tzinfo=UTC),
                }
            )
    rows.append(rows[0].copy())
    tables = {
        "canonical_interactions": rows,
        "canonical_repositories": [
            {
                "repo_id": repo,
                "metadata_version": 1,
                "fetched_at": datetime(2026, 11, 1, tzinfo=UTC),
                "visibility": "public",
                "archived": False,
                "disabled": False,
            }
            for repo in range(1, 11)
        ],
        "cohort_repositories": [{"repo_id": i} for i in range(1, 6)],
        "dataset_repositories": [{"repo_id": i} for i in range(1, 6)],
        "training_dataset": [
            {"source": "gh_archive", "source_subject_id": "train", "repo_id": 1, "split": "train"}
        ],
    }
    artifacts = []
    for kind, values in tables.items():
        path = tmp_path / f"{kind}.parquet"
        pq.write_table(pa.Table.from_pylist(values), path)
        artifacts.append(artifact_for(path, kind, {}))
    context = TaskContext("sample", tmp_path / "workspace", "sha256:test", 7)
    config = EvaluationSampleConfig(
        datetime(2026, 9, 1, tzinfo=UTC),
        datetime(2026, 10, 1, tzinfo=UTC),
        datetime(2026, 11, 1, tzinfo=UTC),
        maximum_subjects_per_split=10,
    )
    artifact = IndependentEvaluationBuilder(config).build(context, artifacts)
    result = pq.read_table(Path(artifact.uri[7:])).to_pylist()
    assert all(row["source_subject_id"] != "train" for row in result)
    assert len({(row["source_subject_id"], row["repo_id"]) for row in result}) == len(result)
    assert {row["repo_id"] for row in result if row["split"] != "train"} == {9, 10}
    assert all(not row["training_eligible"] for row in result if row["split"] != "train")
    manifest = json.loads((context.workspace / "evaluation-sample/manifest.json").read_text())
    assert manifest["aggregates"]["candidate_subjects"] == [{"count_star()": 50}]
    assert len({row["source_subject_id"] for row in result}) == 20
