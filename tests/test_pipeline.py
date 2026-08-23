from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import yaml

from starcat_recsys.artifacts import artifact_for
from starcat_recsys.cli import main
from starcat_recsys.pipeline import (
    _repository_ids_from_canonical,
    _training_reference_time,
    run_pipeline,
)


def test_runs_end_to_end_pipeline_and_cli_query(tmp_path: Path, capsys: object) -> None:
    interactions = tmp_path / "interactions.jsonl"
    rows: list[dict[str, object]] = []
    subject_rows = {
        "u1": [
            (1, "2025-01-01T00:00:00Z"),
            (2, "2025-02-01T00:00:00Z"),
            (3, "2025-09-01T00:00:00Z"),
        ],
        "u2": [
            (1, "2025-01-01T00:00:00Z"),
            (2, "2025-02-01T00:00:00Z"),
            (3, "2025-09-01T00:00:00Z"),
        ],
        "u3": [
            (1, "2025-01-01T00:00:00Z"),
            (4, "2025-03-01T00:00:00Z"),
            (3, "2025-09-01T00:00:00Z"),
        ],
    }
    for subject, subject_interactions in subject_rows.items():
        rows.extend(
            _interaction(subject, repo_id, occurred_at)
            for repo_id, occurred_at in subject_interactions
        )
    interactions.write_text("".join(json.dumps(row) + "\n" for row in rows))
    repositories = tmp_path / "repositories.jsonl"
    repositories.write_text(
        "".join(
            json.dumps(
                {
                    "fetched_at": "2026-01-01T00:00:00Z",
                    "repository": {
                        "id": repo_id,
                        "full_name": f"owner/repo-{repo_id}",
                        "topics": ["demo"],
                        "visibility": "public",
                    },
                }
            )
            + "\n"
            for repo_id in [1, 2, 3, 4]
        )
    )
    config = {
        "run_id": "demo-run",
        "workspace": "workspace",
        "registry": "registry",
        "random_seed": 7,
        "sources": [
            {
                "type": "local_file",
                "path": "interactions.jsonl",
                "source": "starcat_snapshot",
                "kind": "raw_interactions",
            },
            {
                "type": "local_file",
                "path": "repositories.jsonl",
                "source": "github_api",
                "kind": "raw_repositories",
            },
        ],
        "dataset": {
            "train_end": "2025-06-01T00:00:00Z",
            "validation_end": "2025-12-01T00:00:00Z",
        },
        "training": {
            "reference_time": "2025-06-01T00:00:00Z",
            "top_k": 2,
            "svd_components": 2,
            "run_ablations": True,
        },
        "evaluation": {"split": "validation", "k": 2},
        "publish": {"model_version": "demo-v1", "selected_model": "costar"},
    }
    config_path = tmp_path / "pipeline.yaml"
    config_path.write_text(yaml.safe_dump(config, sort_keys=False))

    artifact = run_pipeline(config_path)

    bundle = Path(artifact.metadata["bundle_uri"].removeprefix("file://"))
    assert bundle.is_dir()
    manifest = json.loads((tmp_path / "workspace/runs/demo-run/run-manifest.json").read_text())
    assert all(task["status"] == "completed" for task in manifest["tasks"])
    assert main(["bundle", "verify", bundle.as_posix()]) == 0
    assert main(["bundle", "query", bundle.as_posix(), "--repo-id", "1"]) == 0
    # capsys 的类型由 pytest 提供，此处只需证明 CLI 输出了 JSON。
    output = capsys.readouterr().out  # type: ignore[attr-defined]
    assert "model_version" in output
    assert "repo_id" in output

    with pytest.raises(FileExistsError, match="run_id 已存在"):
        run_pipeline(config_path)


def test_single_subject_skips_svd_and_publishes_costar_bundle(tmp_path: Path) -> None:
    interactions = tmp_path / "interactions.jsonl"
    interactions.write_text(
        "".join(
            json.dumps(_interaction("u1", repo_id, "2025-01-01T00:00:00Z")) + "\n"
            for repo_id in [1, 2, 3]
        )
    )
    repositories = tmp_path / "repositories.jsonl"
    repositories.write_text(
        "".join(
            json.dumps(
                {
                    "fetched_at": "2026-01-01T00:00:00Z",
                    "repository": {
                        "id": repo_id,
                        "full_name": f"owner/repo-{repo_id}",
                        "topics": ["demo"],
                        "visibility": "public",
                    },
                }
            )
            + "\n"
            for repo_id in [1, 2, 3]
        )
    )
    config = {
        "run_id": "single-subject-run",
        "workspace": "workspace",
        "registry": "registry",
        "sources": [
            {
                "type": "local_file",
                "path": "interactions.jsonl",
                "source": "starcat_snapshot",
                "kind": "raw_interactions",
            },
            {
                "type": "local_file",
                "path": "repositories.jsonl",
                "source": "github_api",
                "kind": "raw_repositories",
            },
        ],
        "dataset": {
            "train_end": "2025-06-01T00:00:00Z",
            "validation_end": "2025-12-01T00:00:00Z",
        },
        "training": {
            "reference_time": "2025-06-01T00:00:00Z",
            "top_k": 2,
            "svd_components": 2,
            "run_ablations": False,
        },
        "evaluation": {"split": "validation", "k": 2},
        "publish": {"model_version": "single-subject-v1", "selected_model": "costar"},
    }
    config_path = tmp_path / "pipeline.yaml"
    config_path.write_text(yaml.safe_dump(config, sort_keys=False))

    artifact = run_pipeline(config_path)

    assert Path(artifact.metadata["bundle_uri"].removeprefix("file://")).is_dir()
    manifest = json.loads(
        (tmp_path / "workspace/runs/single-subject-run/run-manifest.json").read_text()
    )
    svd_task = next(task for task in manifest["tasks"] if task["task_name"] == "train:svd")
    assert svd_task["status"] == "skipped"
    assert "至少需要 2 个主体" in svd_task["detail"]
    assert all(task["status"] != "failed" for task in manifest["tasks"])


def _interaction(subject: str, repo_id: int, occurred_at: str) -> dict[str, object]:
    return {
        "source_record_id": f"{subject}:{repo_id}",
        "source_subject_id": subject,
        "repo_id": repo_id,
        "starred_at": occurred_at,
        "captured_at": "2026-01-01T00:00:00Z",
        "snapshot_id": f"snapshot-{subject}",
    }


def test_selects_bounded_popular_repositories_for_deferred_metadata(tmp_path: Path) -> None:
    path = tmp_path / "interactions.parquet"
    pq.write_table(pa.table({"repo_id": [3, 2, 2, 1, 1, 1]}), path)

    repo_ids = _repository_ids_from_canonical([artifact_for(path, "canonical_interactions", {})], 2)

    assert repo_ids == [1, 2]


def test_rejects_training_reference_time_different_from_train_end() -> None:
    with pytest.raises(ValueError, match="reference_time"):
        _training_reference_time(
            {"reference_time": "2025-06-02T00:00:00Z"},
            datetime.fromisoformat("2025-06-01T00:00:00+00:00"),
        )


def test_accepts_equivalent_training_reference_time_with_different_offset() -> None:
    reference_time = _training_reference_time(
        {"reference_time": "2025-06-01T08:00:00+08:00"},
        datetime.fromisoformat("2025-06-01T00:00:00+00:00"),
    )

    assert reference_time.isoformat() == "2025-06-01T08:00:00+08:00"
