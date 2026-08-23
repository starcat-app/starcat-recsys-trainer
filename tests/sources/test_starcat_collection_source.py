from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

import httpx
import pytest

from starcat_recsys.interfaces import TaskContext
from starcat_recsys.sources.starcat_collection import (
    StarcatCollectionAPISource,
    StarcatCollectionConfig,
)


def _snapshot(**overrides: Any) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "schema_version": 1,
        "snapshot_id": "0191f9e3-95d4-7f52-b4d3-28d628e3a02b",
        "subject_id": "a" * 64,
        "captured_at": "2026-08-23T08:30:00Z",
        "content_hash": "sha256:" + "b" * 64,
        "repositories": [
            {"repo_id": 1342004, "starred_at": None},
            {"repo_id": 41881900, "starred_at": "2023-06-11T13:20:10Z"},
        ],
    }
    payload.update(overrides)
    return payload


def _body(*snapshots: dict[str, Any]) -> bytes:
    return b"".join(
        json.dumps(snapshot, sort_keys=True, separators=(",", ":")).encode() + b"\n"
        for snapshot in snapshots
    )


def _client(
    body: bytes,
    *,
    checksum: str | None = None,
    etag: str | None = None,
    content_type: str = "application/x-ndjson",
) -> httpx.Client:
    actual = "sha256:" + hashlib.sha256(body).hexdigest()

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/internal/v1/training/recommendation-snapshots/export"
        assert request.headers["Authorization"] == "Bearer test-admin-key"
        assert request.headers["Accept"] == "application/x-ndjson"
        return httpx.Response(
            200,
            content=body,
            headers={
                "X-Starcat-Export-Checksum": checksum or actual,
                "ETag": etag or f'"{actual}"',
                "Content-Type": content_type,
            },
        )

    return httpx.Client(transport=httpx.MockTransport(handler))


def _collect(tmp_path: Path, body: bytes, **config: Any) -> tuple[str, dict[str, Any]]:
    client = _client(body)
    try:
        artifact = StarcatCollectionAPISource(
            StarcatCollectionConfig(base_url="http://collection.test", **config),
            client=client,
            admin_key="test-admin-key",
        ).collect(TaskContext("run", tmp_path / "workspace", "sha256:test", 7))[0]
    finally:
        client.close()
    output = Path(artifact.uri.removeprefix("file://")).read_text(encoding="utf-8")
    return output, artifact.metadata


def test_collect_verifies_checksum_and_lands_anonymized_rows(tmp_path: Path) -> None:
    output, metadata = _collect(tmp_path, _body(_snapshot()))

    rows = [json.loads(line) for line in output.splitlines()]
    assert len(rows) == 2
    assert rows[0]["source_subject_id"] == "a" * 64
    assert rows[0]["snapshot_id"] == "0191f9e3-95d4-7f52-b4d3-28d628e3a02b"
    assert "participant_id" not in output
    assert metadata["source"] == "starcat_collection_api"
    assert metadata["snapshot_count"] == 1
    assert metadata["row_count"] == 2
    assert metadata["export_checksum"].startswith("sha256:")


def test_collect_rejects_checksum_mismatch(tmp_path: Path) -> None:
    body = _body(_snapshot())
    client = _client(body, checksum="sha256:" + "0" * 64)
    source = StarcatCollectionAPISource(
        StarcatCollectionConfig(base_url="http://collection.test"),
        client=client,
        admin_key="test-admin-key",
    )
    try:
        with pytest.raises(ValueError, match="checksum"):
            source.collect(TaskContext("run", tmp_path / "workspace", "sha256:test", 7))
    finally:
        client.close()


@pytest.mark.parametrize(
    ("client_options", "message"),
    [
        ({"content_type": "application/json"}, "Content-Type"),
        ({"etag": '"sha256:bad"'}, "ETag"),
    ],
)
def test_collect_rejects_invalid_export_headers(
    tmp_path: Path, client_options: dict[str, str], message: str
) -> None:
    client = _client(_body(_snapshot()), **client_options)
    source = StarcatCollectionAPISource(
        StarcatCollectionConfig(base_url="http://collection.test"),
        client=client,
        admin_key="test-admin-key",
    )
    try:
        with pytest.raises(ValueError, match=message):
            source.collect(TaskContext("run", tmp_path / "workspace", "sha256:test", 7))
    finally:
        client.close()


@pytest.mark.parametrize(
    ("payload", "message"),
    [
        (
            _snapshot(
                repositories=[
                    {"repo_id": 2, "starred_at": None},
                    {"repo_id": 1, "starred_at": None},
                ]
            ),
            "升序",
        ),
        (_snapshot(subject_id="raw-participant"), "subject_id"),
        (_snapshot(participant_id="must-not-pass"), "schema v1"),
    ],
)
def test_collect_rejects_invalid_or_identity_bearing_export(
    tmp_path: Path, payload: dict[str, Any], message: str
) -> None:
    with pytest.raises(ValueError, match=message):
        _collect(tmp_path, _body(payload))


def test_collect_requires_one_active_snapshot_per_subject(tmp_path: Path) -> None:
    second = _snapshot(snapshot_id="0191f9e3-95d4-7f52-b4d3-28d628e3a02c")
    with pytest.raises(ValueError, match="subject_id"):
        _collect(tmp_path, _body(_snapshot(), second))


def test_collect_enforces_response_and_repository_limits(tmp_path: Path) -> None:
    body = _body(_snapshot())
    with pytest.raises(ValueError, match="maximum_response_bytes"):
        _collect(tmp_path, body, maximum_response_bytes=len(body) - 1)
    with pytest.raises(ValueError, match="仓库数"):
        _collect(tmp_path, body, maximum_repositories_per_snapshot=1)


def test_config_rejects_credentials_in_url() -> None:
    with pytest.raises(ValueError, match="base_url"):
        StarcatCollectionConfig(base_url="https://admin:secret@collection.test")
