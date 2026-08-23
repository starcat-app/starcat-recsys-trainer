from __future__ import annotations

import json
from pathlib import Path

import pytest

from starcat_recsys.interfaces import TaskContext
from starcat_recsys.sources.starcat import (
    StarcatSnapshotConfig,
    StarcatSnapshotSource,
    snapshot_content_hash,
)


def _payload() -> dict[str, object]:
    payload: dict[str, object] = {
        "schema_version": 1,
        "snapshot_id": "0191f9e3-95d4-7f52-b4d3-28d628e3a02b",
        "participant_id": "63b7d101-88c0-4cee-859f-c61f64d8db96",
        "captured_at": "2026-08-22T08:30:00Z",
        "mode": "full",
        "repositories": [
            {"repo_id": 1342004, "starred_at": None},
            {"repo_id": 41881900, "starred_at": "2023-06-11T13:20:10Z"},
        ],
    }
    payload["content_hash"] = snapshot_content_hash(payload)
    return payload


def test_collect_validates_and_removes_participant_id(tmp_path: Path) -> None:
    input_path = tmp_path / "input.json"
    input_path.write_text(json.dumps(_payload()), encoding="utf-8")
    context = TaskContext("run", tmp_path / "workspace", "sha256:test", 7)

    artifacts = StarcatSnapshotSource(
        [input_path], StarcatSnapshotConfig(), hmac_key=b"test-key"
    ).collect(context)

    output = Path(artifacts[0].uri.removeprefix("file://")).read_text()
    rows = [json.loads(line) for line in output.splitlines()]
    assert len(rows) == 2
    assert "participant_id" not in output
    assert rows[0]["source_subject_id"] == rows[1]["source_subject_id"]
    assert len(rows[0]["source_subject_id"]) == 64


@pytest.mark.parametrize(
    ("mutation", "message"),
    [
        (lambda value: value.update(mode="partial"), "mode=full"),
        (
            lambda value: value.update(
                repositories=[
                    {"repo_id": 41881900, "starred_at": None},
                    {"repo_id": 1342004, "starred_at": None},
                ]
            ),
            "升序",
        ),
        (lambda value: value.update(content_hash="sha256:bad"), "content_hash"),
    ],
)
def test_collect_rejects_invalid_snapshot(tmp_path: Path, mutation: object, message: str) -> None:
    payload = _payload()
    assert callable(mutation)
    mutation(payload)
    # 排序错误需要重算 hash，才能证明是 schema 约束在拒绝数据。
    if message == "升序":
        payload["content_hash"] = snapshot_content_hash(payload)
    input_path = tmp_path / "input.json"
    input_path.write_text(json.dumps(payload), encoding="utf-8")
    source = StarcatSnapshotSource([input_path], StarcatSnapshotConfig(), hmac_key=b"test-key")

    with pytest.raises(ValueError, match=message):
        source.collect(TaskContext("run", tmp_path / "workspace", "sha256:test", 7))


def test_same_participant_uses_stable_source_local_identifier(tmp_path: Path) -> None:
    first = _payload()
    second = _payload()
    second["snapshot_id"] = "0191f9e3-95d4-7f52-b4d3-28d628e3a02c"
    second["content_hash"] = snapshot_content_hash(second)
    paths = []
    for index, payload in enumerate([first, second]):
        path = tmp_path / f"{index}.json"
        path.write_text(json.dumps(payload), encoding="utf-8")
        paths.append(path)

    artifact = StarcatSnapshotSource(paths, StarcatSnapshotConfig(), hmac_key=b"test-key").collect(
        TaskContext("run", tmp_path / "workspace", "sha256:test", 7)
    )[0]

    rows = [
        json.loads(line)
        for line in Path(artifact.uri.removeprefix("file://")).read_text().splitlines()
    ]
    assert len({row["source_subject_id"] for row in rows}) == 1
