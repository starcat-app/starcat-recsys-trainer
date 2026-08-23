"""GitHub Repository REST API metadata 数据源。

该 Connector 只补齐公开仓库元数据，不读取 stargazer 列表。ETag 缓存既降低
API 消耗，也使同一批 repo ID 可以安全重跑。
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol

import httpx

from starcat_recsys.artifacts import artifact_for, write_json_atomic
from starcat_recsys.interfaces import Artifact, TaskContext


@dataclass(frozen=True, slots=True)
class GitHubMetadataConfig:
    """可控的 GitHub metadata 采集配置。"""

    token_env: str = "GITHUB_TOKEN"
    api_version: str = "2026-03-10"
    timeout_seconds: float = 30.0
    maximum_requests: int = 5_000
    cache_directory: Path | None = None

    def __post_init__(self) -> None:
        if not self.token_env:
            raise ValueError("token_env 不能为空")
        if self.timeout_seconds <= 0:
            raise ValueError("timeout_seconds 必须大于 0")
        if self.maximum_requests <= 0:
            raise ValueError("maximum_requests 必须大于 0")


@dataclass(frozen=True, slots=True)
class GitHubResponse:
    """隔离 httpx 的最小响应模型。"""

    status_code: int
    headers: dict[str, str]
    payload: dict[str, Any] | None


class GitHubGateway(Protocol):
    """单测可替换的 GitHub 请求边界。"""

    def fetch_repository(self, repo_id: int, etag: str | None) -> GitHubResponse: ...

    def close(self) -> None: ...


class HttpxGitHubGateway:
    """使用 GitHub token 的生产 Gateway，token 不会写入 artifact。"""

    def __init__(
        self,
        config: GitHubMetadataConfig,
        *,
        transport: httpx.BaseTransport | None = None,
    ) -> None:
        token = os.environ.get(config.token_env)
        if not token:
            raise RuntimeError(f"环境变量 {config.token_env} 未配置")
        self._client = httpx.Client(
            base_url="https://api.github.com",
            timeout=config.timeout_seconds,
            headers={
                "Accept": "application/vnd.github+json",
                "Authorization": f"Bearer {token}",
                "X-GitHub-Api-Version": config.api_version,
                "User-Agent": "starcat-recsys-trainer",
            },
            transport=transport,
        )

    def fetch_repository(self, repo_id: int, etag: str | None) -> GitHubResponse:
        headers = {"If-None-Match": etag} if etag else None
        response = self._client.get(f"/repositories/{repo_id}", headers=headers)
        payload = response.json() if response.status_code == 200 else None
        return GitHubResponse(
            response.status_code,
            {key.lower(): value for key, value in response.headers.items()},
            payload,
        )

    def close(self) -> None:
        self._client.close()


class GitHubRepositoryMetadataSource:
    """根据 repo ID 生成可重放的原始 JSONL metadata。"""

    name = "github_repository_metadata"

    def __init__(
        self,
        repo_ids: list[int],
        config: GitHubMetadataConfig,
        gateway: GitHubGateway | None = None,
    ) -> None:
        invalid = [repo_id for repo_id in repo_ids if repo_id <= 0]
        if invalid:
            raise ValueError("repo_ids 必须全部为正整数")
        self._repo_ids = sorted(set(repo_ids))
        self._config = config
        self._gateway = gateway

    def collect(self, context: TaskContext) -> list[Artifact]:
        if len(self._repo_ids) > self._config.maximum_requests:
            raise RuntimeError(
                f"计划请求 {len(self._repo_ids)} 个仓库，超过上限 {self._config.maximum_requests}"
            )

        output_directory = context.workspace / "raw" / "github_metadata"
        cache_directory = self._config.cache_directory or output_directory / "cache"
        output_directory.mkdir(parents=True, exist_ok=True)
        cache_directory.mkdir(parents=True, exist_ok=True)
        gateway = self._gateway or HttpxGitHubGateway(self._config)
        rows: list[dict[str, Any]] = []
        not_found: list[int] = []
        cache_hits = 0

        try:
            for repo_id in self._repo_ids:
                cached_payload, etag = _read_cache(cache_directory, repo_id)
                response = gateway.fetch_repository(repo_id, etag)
                if response.status_code == 304:
                    if cached_payload is None:
                        raise RuntimeError(f"仓库 {repo_id} 返回 304 但本地无缓存")
                    rows.append(cached_payload)
                    cache_hits += 1
                    continue
                if response.status_code == 404:
                    not_found.append(repo_id)
                    continue
                if response.status_code == 429 or (
                    response.status_code == 403
                    and response.headers.get("x-ratelimit-remaining") == "0"
                ):
                    remaining = response.headers.get("x-ratelimit-remaining", "unknown")
                    reset = response.headers.get(
                        "x-ratelimit-reset", response.headers.get("retry-after", "unknown")
                    )
                    raise RuntimeError(
                        f"GitHub API 限流：repo_id={repo_id}, remaining={remaining}, reset={reset}"
                    )
                if response.status_code == 403:
                    raise RuntimeError(f"GitHub API 权限不足：repo_id={repo_id}, status=403")
                if response.status_code != 200 or response.payload is None:
                    raise RuntimeError(
                        f"GitHub API 请求失败：repo_id={repo_id}, status={response.status_code}"
                    )
                if int(response.payload.get("id", 0)) != repo_id:
                    raise RuntimeError(f"GitHub API 返回的 repo ID 与请求不一致：{repo_id}")
                envelope = {
                    "fetched_at": datetime.now(UTC).isoformat(),
                    "repository": response.payload,
                }
                rows.append(envelope)
                _write_cache(
                    cache_directory,
                    repo_id,
                    envelope,
                    response.headers.get("etag"),
                )
        finally:
            gateway.close()

        output_path = output_directory / "repositories.jsonl"
        _write_jsonl_atomic(output_path, rows)
        artifact = artifact_for(
            output_path,
            "raw_repositories",
            {
                "source": "github_api",
                "row_count": len(rows),
                "requested_count": len(self._repo_ids),
                "cache_hits": cache_hits,
            },
        )
        write_json_atomic(
            output_directory / "manifest.json",
            {
                "schema_version": 1,
                "source": self.name,
                "run_id": context.run_id,
                "artifact": artifact.metadata | {"checksum": artifact.checksum},
                "not_found_repo_ids": not_found,
            },
        )
        return [artifact]


def _read_cache(cache_directory: Path, repo_id: int) -> tuple[dict[str, Any] | None, str | None]:
    path = cache_directory / f"{repo_id}.json"
    if not path.exists():
        return None, None
    value = json.loads(path.read_text(encoding="utf-8"))
    return value["payload"], value.get("etag")


def _write_cache(
    cache_directory: Path,
    repo_id: int,
    payload: dict[str, Any],
    etag: str | None,
) -> None:
    write_json_atomic(cache_directory / f"{repo_id}.json", {"etag": etag, "payload": payload})


def _write_jsonl_atomic(path: Path, rows: list[dict[str, Any]]) -> None:
    temporary = path.with_suffix(path.suffix + ".tmp")
    with temporary.open("w", encoding="utf-8") as output:
        for row in rows:
            output.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")
    temporary.replace(path)
