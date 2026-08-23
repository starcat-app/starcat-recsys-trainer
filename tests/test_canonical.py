from __future__ import annotations

import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from starcat_recsys.artifacts import artifact_for
from starcat_recsys.canonical import CanonicalNormalizer
from starcat_recsys.interfaces import TaskContext


def test_normalizes_all_sources_without_cross_source_identity_join(tmp_path: Path) -> None:
    raw = tmp_path / "raw"
    raw.mkdir()
    gh_path = raw / "gh.parquet"
    pq.write_table(
        pa.table(
            {
                "source_record_id": ["event-1"],
                "actor_id": ["participant-like-id"],
                "repo_id": [1],
                "created_at": ["2026-01-01T00:00:00Z"],
            }
        ),
        gh_path,
    )
    starcat_path = raw / "starcat.jsonl"
    starcat_path.write_text(
        json.dumps(
            {
                "source_record_id": "snapshot:1",
                "source_subject_id": "participant-like-id",
                "repo_id": 2,
                "starred_at": None,
                "captured_at": "2026-01-02T00:00:00+00:00",
                "snapshot_id": "snapshot",
            }
        )
        + "\n"
    )
    collection_path = raw / "collection.jsonl"
    collection_path.write_text(
        json.dumps(
            {
                "source_record_id": "collection-snapshot:3",
                "source_subject_id": "collection-subject",
                "repo_id": 3,
                "starred_at": "2025-01-01T00:00:00+00:00",
                "captured_at": "2026-01-02T00:00:00+00:00",
                "snapshot_id": "collection-snapshot",
            }
        )
        + "\n"
    )
    github_path = raw / "github.jsonl"
    github_path.write_text(
        json.dumps(
            {
                "fetched_at": "2026-01-03T00:00:00Z",
                "repository": {
                    "id": 1,
                    "full_name": "starcat/repo",
                    "description": "demo",
                    "topics": ["swift", "macos", "swift"],
                    "language": "Swift",
                    "license": {"spdx_id": "MIT"},
                    "stargazers_count": 10,
                    "forks_count": 2,
                    "archived": False,
                    "disabled": False,
                    "visibility": "public",
                    "pushed_at": "2026-01-02T00:00:00Z",
                },
            }
        )
        + "\n"
    )
    inputs = [
        artifact_for(gh_path, "raw_interactions", {"source": "gh_archive"}),
        artifact_for(starcat_path, "raw_interactions", {"source": "starcat_snapshot"}),
        artifact_for(collection_path, "raw_interactions", {"source": "starcat_collection_api"}),
        artifact_for(github_path, "raw_repositories", {"source": "github_api"}),
    ]

    artifacts = CanonicalNormalizer().normalize(
        TaskContext("run", tmp_path / "workspace", "sha256:config", 7), inputs
    )

    interaction_artifact = next(item for item in artifacts if item.kind == "canonical_interactions")
    interactions = pq.read_table(Path(interaction_artifact.uri.removeprefix("file://"))).to_pylist()
    assert len(interactions) == 3
    assert interactions[0]["source_subject_id"] != interactions[1]["source_subject_id"]
    assert {row["relation_type"] for row in interactions} == {
        "star_event",
        "current_star_membership",
    }

    repository_artifact = next(item for item in artifacts if item.kind == "canonical_repositories")
    repositories = pq.read_table(Path(repository_artifact.uri.removeprefix("file://"))).to_pylist()
    assert repositories[0]["topics"] == ["macos", "swift"]
    assert (tmp_path / "workspace/canonical/manifest.json").exists()
