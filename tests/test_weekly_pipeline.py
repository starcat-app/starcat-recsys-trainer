"""每周编排集成验收：真实训练、评估、Bundle、隔离 Go API，生产 HTTP 使用替身。"""

import hashlib
import json
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import httpx
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from starcat_recsys.artifacts import artifact_for, sha256_file, write_json_atomic
from starcat_recsys.bundle import BundleConfig, LocalModelRegistry, ServingBundlePublisher
from starcat_recsys.canonical import REPOSITORY_SCHEMA
from starcat_recsys.interfaces import TaskContext
from starcat_recsys.recommendations import write_recommendations
from starcat_recsys.trainers.content_index import ContentIndexTrainer
from starcat_recsys.weekly import WeeklyRun


@pytest.mark.parametrize("evaluation_subjects", [4999, 5000])
def test_complete_weekly_workflow_obeys_population_gate(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    evaluation_subjects: int,
) -> None:
    """4999 人必须零上传；5000 人通过完整路径，并可复验激活后的断点状态。"""
    catalog = tmp_path / "catalog.parquet"
    pq.write_table(
        pa.Table.from_pylist(
            [
                {
                    "repo_id": i,
                    "full_name": f"owner/project{i}",
                    "description": "database parser tools",
                    "topics": ["parser"],
                    "primary_language": "Python",
                    "license_key": "mit",
                    "stars": 100,
                    "forks": 1,
                    "archived": False,
                    "disabled": False,
                    "visibility": "public",
                    "pushed_at": datetime(2026, 11, 1, tzinfo=UTC),
                    "fetched_at": datetime(2026, 11, 1, tzinfo=UTC),
                    "metadata_version": 1,
                }
                for i in range(1, 21)
            ],
            schema=REPOSITORY_SCHEMA,
        ),
        catalog,
    )
    repositories = artifact_for(catalog, "dataset_repositories", {})
    baseline_context = TaskContext("baseline", tmp_path / "baseline", "sha256:test", 7)
    baseline_edges = write_recommendations(
        tmp_path / "old.parquet",
        [
            {
                "source_repo_id": source,
                "target_repo_id": target,
                "score": 0.01,
                "rank": rank,
                "model": "hybrid_25",
                "signals": "{}",
            }
            for source in range(1, 21)
            for rank, target in enumerate(
                [t for t in range(1, 6 if source <= 5 else 21) if t != source], 1
            )
        ],
        "hybrid_25",
    )
    metric_file = tmp_path / "old-metrics.json"
    write_json_atomic(
        metric_file,
        {"models": {"hybrid_25": {}}, "model_checksums": {"hybrid_25": baseline_edges.checksum}},
    )
    index = ContentIndexTrainer().train(baseline_context, [repositories])[0]
    baseline = ServingBundlePublisher(
        BundleConfig("old", selected_model="hybrid_25", install=False),
        LocalModelRegistry(tmp_path / "unused-registry"),
    ).publish(
        baseline_context,
        [baseline_edges, repositories, index, artifact_for(metric_file, "evaluation_metrics", {})],
    )[0]
    frozen = tmp_path / "frozen.parquet"
    pq.write_table(
        pa.Table.from_pylist(
            [
                {
                    "source": "gh_archive",
                    "source_subject_id": hashlib.sha256(f"gh_archive:{i}".encode()).hexdigest(),
                    "sample_weight": 1.0,
                    "split": "train",
                }
                for i in range(1, 11)
            ]
        ),
        frozen,
    )
    raw = tmp_path / "raw"
    raw.mkdir()
    partitions = {}
    for day in [1, 2, 3]:
        rows = []
        actors = [*range(1, 11), *range(100, 100 + evaluation_subjects)]
        for actor in actors:
            repos = range(1, 7 if actor < 100 else 6) if day == 1 else ([] if day == 2 else [6])
            for repo in repos:
                rows.append(
                    {
                        "source_record_id": str(repo),
                        "actor_id": actor,
                        "repo_id": repo,
                        "created_at": datetime(2026, 11, day, tzinfo=UTC),
                    }
                )
        path = raw / f"watch-events-2026110{day}.parquet"
        table = (
            pa.Table.from_pylist(rows)
            if rows
            else pa.table(
                {
                    "source_record_id": pa.array([], type=pa.string()),
                    "actor_id": pa.array([], type=pa.int64()),
                    "repo_id": pa.array([], type=pa.int64()),
                    "created_at": pa.array([], type=pa.timestamp("us", tz="UTC")),
                }
            )
        )
        pq.write_table(table, path)
        partitions[f"2026110{day}"] = {
            "checksum": sha256_file(path),
            "metadata": {"row_count": len(rows)},
        }
    write_json_atomic(
        raw / "download-state.json",
        {"scope": {"start_date": "2026-11-01"}, "completed_partitions": partitions},
    )
    config: dict[str, Any] = {
        "workspace": str(tmp_path / "weekly"),
        "initial_bundle": str(Path(baseline.uri[7:]).parent),
        "initial_watermark": "2026-11-01",
        "frozen_training_dataset": str(frozen),
        "core_catalog": str(catalog),
        "expanded_catalog": str(catalog),
        "raw_root": str(raw),
        "base_url": "https://recommend.test",
        "api_root": str(Path(__file__).parents[2] / "starcat-recommend-api"),
    }
    for name in ["publish", "query"]:
        path = tmp_path / (name + ".key")
        path.write_text(name + "-secret")
        path.chmod(0o600)
        config[name + "_key_file"] = str(path)
    remote: dict[str, Any] = {"active": "old", "uploads": []}

    def respond(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if path.endswith("/active"):
            return httpx.Response(200, json={"data": {"model_version": remote["active"]}})
        if "/model-bundles/" in path:
            remote["active"] = path.rsplit("/", 1)[-1]
            remote["uploads"].append(remote["active"])
            return httpx.Response(
                200,
                json={
                    "schema_version": 1,
                    "data": {
                        "model_version": remote["active"],
                        "active": True,
                    },
                },
            )
        if "Authorization" not in request.headers:
            return httpx.Response(401)
        if request.method == "POST" and path != "/api/v2/recommendations/query":
            return httpx.Response(404)
        if request.headers.get("If-None-Match"):
            return httpx.Response(304)
        return httpx.Response(
            200,
            headers={"ETag": '"test"'},
            json={
                "data": {
                    "model_version": remote["active"],
                    "items": [{"repo_id": 99}],
                    "has_more": False,
                }
            },
        )

    real_client = httpx.Client
    monkeypatch.setattr(
        httpx, "Client", lambda **kw: real_client(transport=httpx.MockTransport(respond), **kw)
    )
    state: dict[str, Any] = {}
    task = WeeklyRun(config, state, tmp_path / "state.json", date(2026, 11, 3), "2026-11-01")
    result = task.execute()
    if evaluation_subjects < 5000:
        assert result["status"] == "blocked"
        assert not result["checks"]["minimum_5000_subjects"]
        assert remote["uploads"] == []
        assert remote["active"] == "old"
        assert "publish" not in task.record["steps"]
        assert state["evaluation_train_end"] == "2026-11-02T00:00:00+00:00"
    else:
        assert result["status"] == "published"
        assert remote["uploads"] == [result["model_version"]]
        assert state["published_watermark"] == "2026-11-03"
        assert "evaluation_train_end" not in state
        assert json.loads((task.root / "publish/receipt.json").read_text())["passed"]
