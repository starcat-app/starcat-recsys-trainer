"""Starcat 匿名完整 Star 快照导入器。

输入文件遵循 Starcat 数据贡献 schema v1。落地 artifact 只保留 HMAC 后的训练
主体标识，避免将 participant_id 传播到训练与 Serving 产物。
"""

from __future__ import annotations

import hashlib
import hmac
import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from uuid import UUID

from starcat_recsys.artifacts import artifact_for, write_json_atomic
from starcat_recsys.domain import parse_datetime
from starcat_recsys.interfaces import Artifact, TaskContext


@dataclass(frozen=True, slots=True)
class StarcatSnapshotConfig:
    """快照导入配置，HMAC key 只从环境变量读取。"""

    subject_hmac_key_env: str = "STARCAT_SUBJECT_HMAC_KEY"
    maximum_repositories_per_snapshot: int = 100_000

    def __post_init__(self) -> None:
        if not self.subject_hmac_key_env:
            raise ValueError("subject_hmac_key_env 不能为空")
        if self.maximum_repositories_per_snapshot <= 0:
            raise ValueError("maximum_repositories_per_snapshot 必须大于 0")


class StarcatSnapshotSource:
    """验证并脱敏一组已 commit 的完整快照。"""

    name = "starcat_snapshot"

    def __init__(
        self,
        input_paths: list[Path],
        config: StarcatSnapshotConfig,
        *,
        hmac_key: bytes | None = None,
    ) -> None:
        self._input_paths = sorted(set(input_paths))
        self._config = config
        self._hmac_key = hmac_key

    def collect(self, context: TaskContext) -> list[Artifact]:
        key = self._hmac_key or _load_hmac_key(self._config.subject_hmac_key_env)
        rows: list[dict[str, Any]] = []
        snapshot_ids: list[str] = []
        seen_snapshot_ids: set[str] = set()

        for input_path in self._input_paths:
            payload = json.loads(input_path.read_text(encoding="utf-8"))
            snapshot_rows, snapshot_id = self._validate_and_transform(payload, key)
            if snapshot_id in seen_snapshot_ids:
                raise ValueError(f"重复的 snapshot_id：{snapshot_id}")
            seen_snapshot_ids.add(snapshot_id)
            snapshot_ids.append(snapshot_id)
            rows.extend(snapshot_rows)

        output_directory = context.workspace / "raw" / "starcat_snapshot"
        output_directory.mkdir(parents=True, exist_ok=True)
        output_path = output_directory / "snapshots.jsonl"
        _write_jsonl_atomic(output_path, rows)
        artifact = artifact_for(
            output_path,
            "raw_interactions",
            {
                "source": "starcat_snapshot",
                "snapshot_count": len(snapshot_ids),
                "row_count": len(rows),
            },
        )
        write_json_atomic(
            output_directory / "manifest.json",
            {
                "schema_version": 1,
                "source": self.name,
                "run_id": context.run_id,
                "snapshot_ids": snapshot_ids,
                "artifact": artifact.metadata | {"checksum": artifact.checksum},
            },
        )
        return [artifact]

    def _validate_and_transform(
        self,
        payload: dict[str, Any],
        key: bytes,
    ) -> tuple[list[dict[str, Any]], str]:
        if payload.get("schema_version") != 1:
            raise ValueError("仅支持 Starcat snapshot schema_version=1")
        if payload.get("mode") != "full":
            raise ValueError("Starcat snapshot v1 必须是 mode=full")

        snapshot_id = str(payload.get("snapshot_id", ""))
        participant_id = str(payload.get("participant_id", ""))
        try:
            UUID(snapshot_id)
            UUID(participant_id)
        except ValueError as error:
            raise ValueError("snapshot_id 和 participant_id 必须是 UUID") from error

        captured_at = parse_datetime(payload.get("captured_at"))
        if captured_at is None:
            raise ValueError("captured_at 不能为空")
        repositories = payload.get("repositories")
        if not isinstance(repositories, list):
            raise ValueError("repositories 必须是数组")
        if len(repositories) > self._config.maximum_repositories_per_snapshot:
            raise ValueError("快照仓库数超过配置上限")

        repo_ids: list[int] = []
        for item in repositories:
            if not isinstance(item, dict):
                raise ValueError("repositories[] 必须是对象")
            repo_id = item.get("repo_id")
            if not isinstance(repo_id, int) or isinstance(repo_id, bool) or repo_id <= 0:
                raise ValueError("repositories[].repo_id 必须全部为正整数")
            repo_ids.append(repo_id)
        if repo_ids != sorted(set(repo_ids)):
            raise ValueError("repositories 必须按 repo_id 升序且不重复")

        expected_hash = snapshot_content_hash(payload)
        if payload.get("content_hash") != expected_hash:
            raise ValueError("snapshot content_hash 校验失败")

        subject_id = hmac.new(key, participant_id.encode(), hashlib.sha256).hexdigest()
        rows = []
        for item in repositories:
            starred_at = parse_datetime(item.get("starred_at"))
            rows.append(
                {
                    "source_record_id": f"{snapshot_id}:{item['repo_id']}",
                    "source_subject_id": subject_id,
                    "repo_id": item["repo_id"],
                    "starred_at": starred_at.isoformat() if starred_at else None,
                    "captured_at": captured_at.isoformat(),
                    "snapshot_id": snapshot_id,
                }
            )
        return rows, snapshot_id


def snapshot_content_hash(payload: dict[str, Any]) -> str:
    """计算 schema v1 canonical payload hash，不包含 content_hash 自身。"""

    canonical = {
        "schema_version": payload.get("schema_version"),
        "snapshot_id": payload.get("snapshot_id"),
        "participant_id": payload.get("participant_id"),
        "captured_at": payload.get("captured_at"),
        "mode": payload.get("mode"),
        "repositories": payload.get("repositories"),
    }
    encoded = json.dumps(canonical, separators=(",", ":"), sort_keys=True).encode()
    return "sha256:" + hashlib.sha256(encoded).hexdigest()


def _load_hmac_key(environment_name: str) -> bytes:
    value = os.environ.get(environment_name)
    if not value:
        raise RuntimeError(f"环境变量 {environment_name} 未配置")
    return value.encode()


def _write_jsonl_atomic(path: Path, rows: list[dict[str, Any]]) -> None:
    temporary = path.with_suffix(path.suffix + ".tmp")
    with temporary.open("w", encoding="utf-8") as output:
        for row in rows:
            output.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")
    temporary.replace(path)
