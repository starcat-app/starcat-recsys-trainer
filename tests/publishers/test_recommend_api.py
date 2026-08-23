"""Recommend API Bundle Publisher 契约测试。"""

from __future__ import annotations

import io
import json
import sqlite3
import zipfile
from pathlib import Path

import httpx
import pytest

from starcat_recsys.artifacts import artifact_for, sha256_file, write_json_atomic
from starcat_recsys.interfaces import Artifact, TaskContext
from starcat_recsys.publishers import RecommendAPIBundlePublisher, RecommendAPIPublishConfig


def test_uploads_verified_bundle_and_writes_receipt(tmp_path: Path) -> None:
    artifact = _bundle_artifact(tmp_path, "costar-live-v1")

    def respond(request: httpx.Request) -> httpx.Response:
        assert request.headers["Authorization"] == "Bearer publish-test-key"
        assert request.headers["Content-Type"] == "application/zip"
        assert request.url.path == "/internal/v1/model-bundles/costar-live-v1"
        assert request.url.params["activate"] == "true"
        with zipfile.ZipFile(io.BytesIO(request.read())) as archive:
            assert sorted(archive.namelist()) == [
                "checksums.json",
                "manifest.json",
                "recommendations.sqlite",
            ]
        return httpx.Response(
            200,
            headers={"Content-Type": "application/json"},
            json={
                "schema_version": 1,
                "data": {
                    "model_version": "costar-live-v1",
                    "selected_model": "costar",
                    "active": True,
                },
            },
        )

    with httpx.Client(transport=httpx.MockTransport(respond)) as client:
        publisher = RecommendAPIBundlePublisher(
            RecommendAPIPublishConfig("http://recommend.test"),
            client=client,
            publish_key="publish-test-key",
        )
        delivered = publisher.publish(
            TaskContext("run", tmp_path / "workspace", "sha256:config", 42),
            [artifact],
        )

    assert delivered[0].kind == "serving_delivery"
    receipt = Path(delivered[0].uri.removeprefix("file://"))
    payload = json.loads(receipt.read_text())
    assert payload["model_version"] == "costar-live-v1"
    assert payload["active"] is True


def test_rejects_remote_failure_without_exposing_key(tmp_path: Path) -> None:
    artifact = _bundle_artifact(tmp_path, "costar-live-v2")

    def respond(_: httpx.Request) -> httpx.Response:
        return httpx.Response(422, text="server detail should not be forwarded")

    with httpx.Client(transport=httpx.MockTransport(respond)) as client:
        publisher = RecommendAPIBundlePublisher(
            RecommendAPIPublishConfig("http://recommend.test"),
            client=client,
            publish_key="publish-secret-value",
        )
        with pytest.raises(RuntimeError, match="HTTP 422") as captured:
            publisher.publish(
                TaskContext("run", tmp_path / "workspace", "sha256:config", 42),
                [artifact],
            )
    assert "publish-secret-value" not in str(captured.value)
    assert "server detail" not in str(captured.value)


def test_enforces_compressed_bundle_limit_before_network(tmp_path: Path) -> None:
    artifact = _bundle_artifact(tmp_path, "costar-live-v3")
    publisher = RecommendAPIBundlePublisher(
        RecommendAPIPublishConfig("http://recommend.test", maximum_bundle_bytes=1),
        publish_key="publish-test-key",
    )

    with pytest.raises(ValueError, match="maximum_bundle_bytes"):
        publisher.publish(
            TaskContext("run", tmp_path / "workspace", "sha256:config", 42),
            [artifact],
        )


def test_rejects_base_url_with_credentials() -> None:
    with pytest.raises(ValueError, match="base_url"):
        RecommendAPIPublishConfig("https://user:password@recommend.test")


def _bundle_artifact(tmp_path: Path, version: str) -> Artifact:
    bundle = tmp_path / version
    bundle.mkdir()
    database_path = bundle / "recommendations.sqlite"
    with sqlite3.connect(database_path) as database:
        database.executescript(
            """
            CREATE TABLE repositories (
                repo_id INTEGER PRIMARY KEY, full_name TEXT NOT NULL, description TEXT,
                topics_json TEXT NOT NULL, primary_language TEXT, license_key TEXT,
                stars INTEGER NOT NULL, forks INTEGER NOT NULL, pushed_at TEXT
            );
            CREATE TABLE recommendations (
                source_repo_id INTEGER NOT NULL, target_repo_id INTEGER NOT NULL,
                score REAL NOT NULL, rank INTEGER NOT NULL, model TEXT NOT NULL,
                signals TEXT NOT NULL, PRIMARY KEY(source_repo_id, target_repo_id)
            );
            """
        )
    write_json_atomic(
        bundle / "manifest.json",
        {
            "schema_version": 1,
            "model_version": version,
            "selected_model": "costar",
        },
    )
    write_json_atomic(
        bundle / "checksums.json",
        {
            "manifest.json": sha256_file(bundle / "manifest.json"),
            "recommendations.sqlite": sha256_file(database_path),
        },
    )
    return artifact_for(
        bundle / "manifest.json",
        "serving_bundle",
        {"model_version": version, "bundle_uri": bundle.resolve().as_uri()},
    )
