from __future__ import annotations

import json
import sqlite3
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
    _build_database,
    query_bundle,
    verify_bundle,
)
from starcat_recsys.interfaces import TaskContext
from starcat_recsys.recommendations import write_recommendations


def test_display_calibration_uses_indexed_lookup(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """百万边模型的校准不能退化为每条推荐扫描整个临时表。"""
    model = write_recommendations(
        tmp_path / "model.parquet", [_edge(1, 2, 0.8, 1), _edge(2, 1, 0.4, 1)], "costar"
    )
    repositories = tmp_path / "repos.parquet"
    pq.write_table(pa.Table.from_pylist([_repo(1), _repo(2)]), repositories)
    original_connect = sqlite3.connect
    plans: list[str] = []

    def traced_connect(path: Path) -> sqlite3.Connection:
        connection = original_connect(path)

        def capture_plan(statement: str) -> None:
            if statement.lstrip().startswith("UPDATE recommendations"):
                plans.extend(
                    str(row[-1])
                    for row in connection.execute("EXPLAIN QUERY PLAN " + statement).fetchall()
                )

        connection.set_trace_callback(capture_plan)
        return connection

    monkeypatch.setattr("starcat_recsys.bundle.sqlite3.connect", traced_connect)
    _build_database(tmp_path / "bundle.sqlite", Path(model.uri[7:]), repositories)
    assert any("SEARCH calibrated USING INDEX" in plan for plan in plans)
    assert not any("SCAN calibrated" in plan for plan in plans)


@pytest.mark.parametrize("install", [True, False])
def test_publishes_verifies_and_queries_immutable_bundle(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, install: bool
) -> None:
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
        BundleConfig("v1", metric_gates={"recall@2": 0.4}, git_commit="abc", install=install),
        registry,
    )
    monkeypatch.setattr(
        "starcat_recsys.bundle.pq.read_table",
        lambda *args, **kwargs: pytest.fail("Bundle 构建不应整表加载 Parquet"),
    )

    artifact = publisher.publish(
        TaskContext("run", tmp_path / "workspace", "sha256:config", 7), inputs
    )[0]

    bundle = Path(artifact.metadata["bundle_uri"].removeprefix("file://"))
    manifest = verify_bundle(bundle)
    assert manifest["git_commit"] == "abc"
    with sqlite3.connect(bundle / "recommendations.sqlite") as database:
        calibrated = database.execute(
            """
            SELECT score, display_score
            FROM recommendations
            ORDER BY score
            """
        ).fetchall()
    assert [row[0] for row in calibrated] == pytest.approx([0.4, 0.7, 0.8])
    assert [row[1] for row in calibrated] == pytest.approx([1 / 3, 2 / 3, 0.99])
    single_seed_results = query_bundle(bundle, [1], limit=2)
    assert single_seed_results[0]["score"] == pytest.approx(0.8)
    assert single_seed_results[0]["display_score"] == pytest.approx(0.99)
    results = query_bundle(bundle, [1, 2], limit=2)
    assert [row["repo_id"] for row in results] == [3]
    assert results[0]["display_score"] is None
    if install:
        assert json.loads((tmp_path / "registry/active.json").read_text())["model_version"] == "v1"
    else:
        assert not registry.root.exists()
    with pytest.raises(FileExistsError):
        publisher.publish(
            TaskContext(
                "run-2", tmp_path / ("workspace-2" if install else "workspace"), "sha256:config", 7
            ),
            inputs,
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


def test_refit_requires_explicit_non_install_and_provenance(tmp_path: Path) -> None:
    """引用成绩不能自动安装，也不能缺少被评估模型的身份。"""
    with pytest.raises(ValueError, match="禁止自动安装"):
        BundleConfig("refit", metrics_scope="pre_refit")
    recommendations = write_recommendations(
        tmp_path / "model.parquet", [_edge(1, 2, 1, 1)], "costar"
    )
    repositories = tmp_path / "repos.parquet"
    pq.write_table(pa.Table.from_pylist([_repo(1), _repo(2)]), repositories)
    metrics = tmp_path / "metrics.json"
    write_json_atomic(metrics, {"models": {"costar": {"recall@20": 0.1}}})
    inputs = [
        recommendations,
        artifact_for(repositories, "dataset_repositories", {}),
        artifact_for(metrics, "evaluation_metrics", {}),
    ]
    context = TaskContext("refit", tmp_path / "run", "test", 42)
    with pytest.raises(ValueError, match="缺少原模型校验和"):
        ServingBundlePublisher(
            BundleConfig("refit", install=False, metrics_scope="pre_refit"),
            LocalModelRegistry(tmp_path / "registry"),
        ).publish(context, inputs)
    write_json_atomic(
        metrics,
        {
            "models": {"costar": {"recall@20": 0.1}},
            "model_checksums": {"costar": "sha256:different"},
        },
    )
    inputs[-1] = artifact_for(metrics, "evaluation_metrics", {})
    with pytest.raises(ValueError, match="校验和不一致"):
        ServingBundlePublisher(
            BundleConfig("same", install=False), LocalModelRegistry(tmp_path / "registry")
        ).publish(context, inputs)


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
