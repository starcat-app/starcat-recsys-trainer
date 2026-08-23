from __future__ import annotations

import json
from pathlib import Path

import yaml

from starcat_recsys.cli import main
from starcat_recsys.pipeline import run_pipeline


def test_runs_end_to_end_pipeline_and_cli_query(tmp_path: Path, capsys: object) -> None:
    interactions = tmp_path / "interactions.jsonl"
    rows = []
    for subject in ["u1", "u2", "u3"]:
        rows.extend(
            _interaction(subject, repo_id, occurred_at)
            for repo_id, occurred_at in [
                (1, "2025-01-01T00:00:00Z"),
                (2, "2025-02-01T00:00:00Z"),
                (3, "2025-09-01T00:00:00Z"),
            ]
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
            for repo_id in [1, 2, 3]
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


def _interaction(subject: str, repo_id: int, occurred_at: str) -> dict[str, object]:
    return {
        "source_record_id": f"{subject}:{repo_id}",
        "source_subject_id": subject,
        "repo_id": repo_id,
        "starred_at": occurred_at,
        "captured_at": "2026-01-01T00:00:00Z",
        "snapshot_id": f"snapshot-{subject}",
    }
