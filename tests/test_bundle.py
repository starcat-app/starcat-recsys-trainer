from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from starcat_recsys.artifacts import artifact_for, write_json_atomic
from starcat_recsys.bundle import (
    BundleConfig,
    LocalModelRegistry,
    ServingBundlePublisher,
    query_bundle,
    verify_bundle,
)
from starcat_recsys.interfaces import TaskContext
from starcat_recsys.recommendations import write_recommendations


def test_publishes_verifies_and_queries_immutable_bundle(tmp_path: Path) -> None:
    recommendations = write_recommendations(
        tmp_path / "recommendations.parquet",
        [
            _edge(1, 2, 0.8, 1),
            _edge(1, 3, 0.4, 2),
            _edge(2, 3, 0.7, 1),
        ],
        "costar",
    )
    repositories_path = tmp_path / "repositories.parquet"
    pq.write_table(pa.Table.from_pylist([_repo(1), _repo(2), _repo(3)]), repositories_path)
    metrics_path = tmp_path / "metrics.json"
    write_json_atomic(metrics_path, {"models": {"costar": {"recall@2": 0.5}}})
    inputs = [
        recommendations,
        artifact_for(repositories_path, "dataset_repositories", {}),
        artifact_for(metrics_path, "evaluation_metrics", {}),
    ]
    registry = LocalModelRegistry(tmp_path / "registry")
    publisher = ServingBundlePublisher(
        BundleConfig("v1", metric_gates={"recall@2": 0.4}, git_commit="abc"), registry
    )

    artifact = publisher.publish(
        TaskContext("run", tmp_path / "workspace", "sha256:config", 7), inputs
    )[0]

    bundle = Path(artifact.metadata["bundle_uri"].removeprefix("file://"))
    manifest = verify_bundle(bundle)
    assert manifest["git_commit"] == "abc"
    results = query_bundle(bundle, [1, 2], limit=2)
    assert [row["repo_id"] for row in results] == [3]
    assert json.loads((tmp_path / "registry/active.json").read_text())["model_version"] == "v1"
    with pytest.raises(FileExistsError):
        publisher.publish(
            TaskContext("run-2", tmp_path / "workspace-2", "sha256:config", 7), inputs
        )
    with pytest.raises(ValueError, match="model_version"):
        registry.activate("../v1")
    checksums_path = bundle / "checksums.json"
    checksums = json.loads(checksums_path.read_text())
    checksums["../outside"] = "sha256:bad"
    write_json_atomic(checksums_path, checksums)
    with pytest.raises(ValueError, match="文件名不合法"):
        verify_bundle(bundle)


def test_rejects_metric_gate_failure(tmp_path: Path) -> None:
    recommendations = write_recommendations(
        tmp_path / "recommendations.parquet", [_edge(1, 2, 1, 1)], "costar"
    )
    repositories_path = tmp_path / "repositories.parquet"
    pq.write_table(pa.Table.from_pylist([_repo(1), _repo(2)]), repositories_path)
    metrics_path = tmp_path / "metrics.json"
    write_json_atomic(metrics_path, {"models": {"costar": {"recall@2": 0.1}}})

    with pytest.raises(RuntimeError, match="指标门禁"):
        ServingBundlePublisher(
            BundleConfig("v1", metric_gates={"recall@2": 0.5}),
            LocalModelRegistry(tmp_path / "registry"),
        ).publish(
            TaskContext("run", tmp_path / "workspace", "sha256:config", 7),
            [
                recommendations,
                artifact_for(repositories_path, "dataset_repositories", {}),
                artifact_for(metrics_path, "evaluation_metrics", {}),
            ],
        )


def _edge(source: int, target: int, score: float, rank: int) -> dict[str, object]:
    return {
        "source_repo_id": source,
        "target_repo_id": target,
        "score": score,
        "rank": rank,
        "model": "costar",
        "signals": '{"kind":"costar"}',
    }


def _repo(repo_id: int) -> dict[str, object]:
    return {
        "repo_id": repo_id,
        "full_name": f"owner/repo-{repo_id}",
        "description": None,
        "topics": ["demo"],
        "primary_language": "Python",
        "license_key": "MIT",
        "stars": 10,
        "forks": 1,
        "pushed_at": datetime(2026, 1, 1, tzinfo=UTC),
    }
