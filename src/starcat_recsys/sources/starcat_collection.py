"""starcat-collection-api 内部快照导出 Connector。

Connector 只负责网络拉取、输出 checksum 校验、schema 验证和 raw landing。
它不重新实现 canonical、dataset 或训练逻辑。
"""

from __future__ import annotations

import hashlib
import json
import os
import re
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlsplit
from uuid import UUID

import httpx

from starcat_recsys.artifacts import artifact_for, write_json_atomic, write_jsonl_atomic
from starcat_recsys.domain import parse_datetime
from starcat_recsys.interfaces import Artifact, TaskContext

_SHA256_PATTERN = re.compile(r"^sha256:[0-9a-f]{64}$")
_SUBJECT_PATTERN = re.compile(r"^[0-9a-f]{64}$")
_EXPORT_PATH = "/internal/v1/training/recommendation-snapshots/export"


@dataclass(frozen=True, slots=True)
class StarcatCollectionConfig:
    """Collection Pull 限制，Admin key 只允许从环境变量读取。"""

    base_url: str
    admin_key_env: str = "STARCAT_COLLECTION_ADMIN_KEY"
    timeout_seconds: float = 60
    maximum_snapshots: int = 100_000
    maximum_repositories_per_snapshot: int = 100_000
    maximum_response_bytes: int = 512 * 1024 * 1024

    def __post_init__(self) -> None:
        parsed = urlsplit(self.base_url)
        if (
            parsed.scheme not in {"http", "https"}
            or not parsed.netloc
            or parsed.username is not None
            or parsed.password is not None
            or parsed.query
            or parsed.fragment
        ):
            raise ValueError("base_url 必须是不含凭据、query 或 fragment 的 HTTP(S) URL")
        if not self.admin_key_env:
            raise ValueError("admin_key_env 不能为空")
        if self.timeout_seconds <= 0:
            raise ValueError("timeout_seconds 必须大于 0")
        if self.maximum_snapshots <= 0:
            raise ValueError("maximum_snapshots 必须大于 0")
        if self.maximum_repositories_per_snapshot <= 0:
            raise ValueError("maximum_repositories_per_snapshot 必须大于 0")
        if self.maximum_response_bytes <= 0:
            raise ValueError("maximum_response_bytes 必须大于 0")


class StarcatCollectionAPISource:
    """从 Collection 内部导出拉取已匿名、已 commit 的 active 快照。"""

    name = "starcat_collection_api"

    def __init__(
        self,
        config: StarcatCollectionConfig,
        *,
        client: httpx.Client | None = None,
        admin_key: str | None = None,
    ) -> None:
        self._config = config
        self._client = client
        self._admin_key = admin_key

    def collect(self, context: TaskContext) -> list[Artifact]:
        key = self._admin_key or os.environ.get(self._config.admin_key_env)
        if not key:
            raise RuntimeError(f"环境变量 {self._config.admin_key_env} 未配置")

        if self._client is None:
            with httpx.Client(timeout=self._config.timeout_seconds) as client:
                body, response_checksum, etag = self._download(client, key)
        else:
            body, response_checksum, etag = self._download(self._client, key)
        rows, snapshot_ids = self._validate_and_transform(body)

        output_directory = context.workspace / "raw" / self.name
        output_path = output_directory / "snapshots.jsonl"
        write_jsonl_atomic(output_path, rows)
        artifact = artifact_for(
            output_path,
            "raw_interactions",
            {
                "source": self.name,
                "snapshot_count": len(snapshot_ids),
                "row_count": len(rows),
                "export_checksum": response_checksum,
                "export_etag": etag,
            },
        )
        write_json_atomic(
            output_directory / "manifest.json",
            {
                "schema_version": 1,
                "source": self.name,
                "run_id": context.run_id,
                "snapshot_ids": snapshot_ids,
                "export_checksum": response_checksum,
                "export_etag": etag,
                "artifact": artifact.metadata | {"checksum": artifact.checksum},
            },
        )
        return [artifact]

    def _download(self, client: httpx.Client, key: str) -> tuple[bytes, str, str | None]:
        url = self._config.base_url.rstrip("/") + _EXPORT_PATH
        with client.stream(
            "GET",
            url,
            headers={
                "Authorization": f"Bearer {key}",
                "Accept": "application/x-ndjson",
            },
        ) as response:
            response.raise_for_status()
            content_type = response.headers.get("Content-Type", "").split(";", 1)[0].strip()
            if content_type != "application/x-ndjson":
                raise ValueError("Collection 导出 Content-Type 必须是 application/x-ndjson")
            digest = hashlib.sha256()
            chunks: list[bytes] = []
            total = 0
            for chunk in response.iter_bytes():
                total += len(chunk)
                if total > self._config.maximum_response_bytes:
                    raise ValueError("Collection 导出超过 maximum_response_bytes")
                digest.update(chunk)
                chunks.append(chunk)
            body = b"".join(chunks)
            expected = response.headers.get("X-Starcat-Export-Checksum", "")
            actual = f"sha256:{digest.hexdigest()}"
            if expected != actual:
                raise ValueError("Collection 导出 checksum 校验失败")
            etag = response.headers.get("ETag")
            if etag != f'"{actual}"':
                raise ValueError("Collection 导出 ETag 与 checksum 不一致")
            return body, actual, etag

    def _validate_and_transform(self, body: bytes) -> tuple[list[dict[str, Any]], list[str]]:
        rows: list[dict[str, Any]] = []
        snapshot_ids: list[str] = []
        seen_snapshots: set[str] = set()
        seen_subjects: set[str] = set()
        for line_number, line in enumerate(body.splitlines(), start=1):
            if not line.strip():
                continue
            try:
                payload = json.loads(line)
            except (UnicodeDecodeError, json.JSONDecodeError) as error:
                raise ValueError(f"Collection 导出第 {line_number} 行不是有效 JSON") from error
            snapshot_rows, snapshot_id, subject_id = self._snapshot_rows(payload)
            if snapshot_id in seen_snapshots:
                raise ValueError(f"Collection 导出包含重复 snapshot_id：{snapshot_id}")
            if subject_id in seen_subjects:
                raise ValueError("Collection 导出每个 subject_id 只能有一个 active 快照")
            seen_snapshots.add(snapshot_id)
            seen_subjects.add(subject_id)
            snapshot_ids.append(snapshot_id)
            rows.extend(snapshot_rows)
            if len(snapshot_ids) > self._config.maximum_snapshots:
                raise ValueError("Collection 导出快照数超过配置上限")
        return rows, snapshot_ids

    def _snapshot_rows(self, payload: Any) -> tuple[list[dict[str, Any]], str, str]:
        if not isinstance(payload, dict) or payload.get("schema_version") != 1:
            raise ValueError("Collection 导出仅支持 schema_version=1")
        allowed_snapshot_keys = {
            "schema_version",
            "snapshot_id",
            "subject_id",
            "captured_at",
            "content_hash",
            "repositories",
        }
        if set(payload) != allowed_snapshot_keys:
            raise ValueError("Collection 导出快照字段与 schema v1 不一致")
        snapshot_id = str(payload.get("snapshot_id", ""))
        try:
            UUID(snapshot_id)
        except ValueError as error:
            raise ValueError("Collection snapshot_id 必须是 UUID") from error
        subject_id = str(payload.get("subject_id", ""))
        if not _SUBJECT_PATTERN.fullmatch(subject_id):
            raise ValueError("Collection subject_id 必须是 64 位小写 hex")
        captured_at = parse_datetime(payload.get("captured_at"))
        if captured_at is None:
            raise ValueError("Collection captured_at 不能为空")
        content_hash = str(payload.get("content_hash", ""))
        if not _SHA256_PATTERN.fullmatch(content_hash):
            raise ValueError("Collection content_hash 格式无效")
        repositories = payload.get("repositories")
        if not isinstance(repositories, list):
            raise ValueError("Collection repositories 必须是数组")
        if len(repositories) > self._config.maximum_repositories_per_snapshot:
            raise ValueError("Collection 快照仓库数超过配置上限")

        repo_ids: list[int] = []
        normalized: list[tuple[int, str | None]] = []
        for item in repositories:
            if not isinstance(item, dict):
                raise ValueError("Collection repositories[] 必须是对象")
            if set(item) != {"repo_id", "starred_at"}:
                raise ValueError("Collection repositories[] 字段与 schema v1 不一致")
            repo_id = item.get("repo_id")
            if not isinstance(repo_id, int) or isinstance(repo_id, bool) or repo_id <= 0:
                raise ValueError("Collection repo_id 必须是正整数")
            starred_at = parse_datetime(item.get("starred_at"))
            repo_ids.append(repo_id)
            normalized.append((repo_id, starred_at.isoformat() if starred_at else None))
        if repo_ids != sorted(set(repo_ids)):
            raise ValueError("Collection repositories 必须按 repo_id 升序且不重复")

        rows = [
            {
                "source_record_id": f"{snapshot_id}:{repo_id}",
                "source_subject_id": subject_id,
                "repo_id": repo_id,
                "starred_at": starred_at,
                "captured_at": captured_at.isoformat(),
                "snapshot_id": snapshot_id,
            }
            for repo_id, starred_at in normalized
        ]
        return rows, snapshot_id, subject_id
