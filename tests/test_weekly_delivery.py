"""模拟生产协议验证每周发布、上传超时后的回读与回退，不接触真实服务。"""

import json
import sqlite3
from pathlib import Path
from typing import Any

import httpx
import pytest

from starcat_recsys.artifacts import artifact_for, sha256_file, write_json_atomic
from starcat_recsys.interfaces import Artifact, TaskContext
from starcat_recsys.weekly_delivery import publish_with_rollback


def bundle(root: Path, version: str) -> Artifact:
    """构造可实际校验、打 ZIP 和再次上传的微型不可变 Bundle。"""
    root.mkdir()
    with sqlite3.connect(root / "recommendations.sqlite") as db:
        db.executescript(
            "CREATE TABLE recommendations(source_repo_id, target_repo_id);"
            "CREATE TABLE content_postings(repo_id);"
        )
        db.executemany(
            "INSERT INTO recommendations VALUES (?,?)",
            [(source, target) for source in [1, 2] for target in range(10, 22)],
        )
    write_json_atomic(root / "manifest.json", {"schema_version": 1, "model_version": version})
    write_json_atomic(
        root / "checksums.json",
        {name: sha256_file(root / name) for name in ["manifest.json", "recommendations.sqlite"]},
    )
    return artifact_for(root / "manifest.json", "serving_bundle", {"bundle_uri": root.as_uri()})


@pytest.mark.parametrize(
    "timeout_after_activation,already_activated",
    [
        (False, False),
        (True, False),
        (False, True),
    ],
)
def test_publish_or_rollback_after_unknown_upload_result(
    tmp_path: Path,
    timeout_after_activation: bool,
    already_activated: bool,
) -> None:
    """服务端已激活后客户端超时，必须识别真实 active 并回退，而不是报告未上线。"""
    old = bundle(tmp_path / "old", "old")
    new = bundle(tmp_path / "new", "new")
    context = TaskContext("weekly", tmp_path / "delivery", "sha256:test", 7)
    context.workspace.mkdir()
    config: dict[str, Any] = {"base_url": "https://recommend.test"}
    for name in ["publish", "query"]:
        path = tmp_path / (name + ".key")
        path.write_text(name + "-secret")
        path.chmod(0o600)
        config[name + "_key_file"] = str(path)
    state: dict[str, Any] = {"active": "new" if already_activated else "old", "uploads": []}
    if already_activated:
        write_json_atomic(
            context.workspace / "production-before.json",
            {
                "passed": True,
                "model_version": "old",
                "nonempty_seeds": [1, 2],
            },
        )

    def respond(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if path.endswith("/active"):
            assert request.headers["Authorization"] == "Bearer publish-secret"
            return httpx.Response(200, json={"data": {"model_version": state["active"]}})
        if "/model-bundles/" in path:
            version = path.rsplit("/", 1)[-1]
            assert request.headers["Authorization"] == "Bearer publish-secret"
            state["active"] = version
            state["uploads"].append(version)
            if timeout_after_activation and version == "new":
                raise httpx.ReadTimeout("simulated lost response", request=request)
            return httpx.Response(
                200,
                json={
                    "schema_version": 1,
                    "data": {
                        "model_version": version,
                        "active": True,
                    },
                },
            )
        if "Authorization" not in request.headers:
            return httpx.Response(401)
        if request.method == "POST" and path != "/api/v2/recommendations/query":
            return httpx.Response(404)
        assert request.headers["Authorization"] == "Bearer query-secret"
        if request.headers.get("If-None-Match"):
            return httpx.Response(304)
        return httpx.Response(
            200,
            headers={"ETag": '"test-etag"'},
            json={
                "data": {
                    "model_version": (state["active"] + "-c0123456789abcdef")
                    if "/repos/" in path
                    else state["active"],
                    "fallback": "/repos/" in path,
                    "items": [{"repo_id": 99}],
                    "has_more": False,
                }
            },
        )

    with httpx.Client(transport=httpx.MockTransport(respond)) as client:
        if timeout_after_activation:
            with pytest.raises(httpx.ReadTimeout):
                publish_with_rollback(context, config, new, Path(old.uri[7:]).parent, client=client)
            assert state["active"] == "old"
            assert state["uploads"] == ["new", "old"]
            assert json.loads((context.workspace / "rollback.json").read_text())["passed"]
        else:
            result = publish_with_rollback(
                context, config, new, Path(old.uri[7:]).parent, client=client
            )
            assert result["passed"]
            assert state["uploads"] == ([] if already_activated else ["new"])
            assert state["active"] == "new"
