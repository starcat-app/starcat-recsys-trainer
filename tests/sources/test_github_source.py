from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path

import httpx
import pytest

from starcat_recsys.interfaces import Artifact, TaskContext
from starcat_recsys.sources.github import (
    GitHubMetadataConfig,
    GitHubRepositoryMetadataSource,
    GitHubResponse,
    HttpxGitHubGateway,
)


@dataclass
class FakeGitHubGateway:
    responses: dict[int, GitHubResponse]
    calls: list[tuple[int, str | None]] = field(default_factory=list)
    closed: bool = False

    def fetch_repository(self, repo_id: int, etag: str | None) -> GitHubResponse:
        self.calls.append((repo_id, etag))
        return self.responses[repo_id]

    def close(self) -> None:
        self.closed = True


def _response(repo_id: int, *, etag: str = '"v1"') -> GitHubResponse:
    return GitHubResponse(
        status_code=200,
        headers={"etag": etag},
        payload={"id": repo_id, "full_name": f"owner/repo-{repo_id}"},
    )


def test_collect_writes_sorted_unique_jsonl_and_skips_404(tmp_path: Path) -> None:
    context = _context(tmp_path)
    gateway = FakeGitHubGateway(
        {
            1: _response(1),
            2: GitHubResponse(404, {}, None),
        }
    )
    source = GitHubRepositoryMetadataSource(
        [2, 1, 1], GitHubMetadataConfig(maximum_requests=2), gateway
    )

    artifacts = source.collect(context)

    rows = [json.loads(line) for line in _artifact_path(artifacts[0]).read_text().splitlines()]
    assert rows[0]["repository"] == {"full_name": "owner/repo-1", "id": 1}
    assert rows[0]["fetched_at"]
    assert gateway.calls == [(1, None), (2, None)]
    assert gateway.closed
    manifest = json.loads((context.workspace / "raw/github_metadata/manifest.json").read_text())
    assert manifest["not_found_repo_ids"] == [2]


def test_collect_reuses_cached_payload_on_304(tmp_path: Path) -> None:
    context = _context(tmp_path)
    first = FakeGitHubGateway({1: _response(1)})
    config = GitHubMetadataConfig(maximum_requests=1)
    GitHubRepositoryMetadataSource([1], config, first).collect(context)
    second = FakeGitHubGateway({1: GitHubResponse(304, {}, None)})

    artifacts = GitHubRepositoryMetadataSource([1], config, second).collect(context)

    assert second.calls == [(1, '"v1"')]
    assert artifacts[0].metadata["cache_hits"] == 1


def test_collect_reuses_configured_cache_across_runs(tmp_path: Path) -> None:
    cache = tmp_path / "shared-cache"
    config = GitHubMetadataConfig(maximum_requests=1, cache_directory=cache)
    GitHubRepositoryMetadataSource([1], config, FakeGitHubGateway({1: _response(1)})).collect(
        _context(tmp_path / "run-1")
    )
    second = FakeGitHubGateway({1: GitHubResponse(304, {}, None)})

    artifact = GitHubRepositoryMetadataSource([1], config, second).collect(
        _context(tmp_path / "run-2")
    )[0]

    assert second.calls == [(1, '"v1"')]
    assert artifact.metadata["cache_hits"] == 1


def test_collect_can_reuse_shared_cache_without_network_refresh(tmp_path: Path) -> None:
    """失败 run 可显式复用刚写入的 cache，避免重复刷新数千个仓库。"""

    cache = tmp_path / "shared-cache"
    initial = GitHubMetadataConfig(maximum_requests=1, cache_directory=cache)
    GitHubRepositoryMetadataSource([1], initial, FakeGitHubGateway({1: _response(1)})).collect(
        _context(tmp_path / "run-1")
    )
    gateway = FakeGitHubGateway({})
    retry = GitHubMetadataConfig(
        maximum_requests=1,
        cache_directory=cache,
        reuse_cached_payloads=True,
    )

    artifact = GitHubRepositoryMetadataSource([1], retry, gateway).collect(
        _context(tmp_path / "run-2")
    )[0]

    assert gateway.calls == []
    assert artifact.metadata["cache_hits"] == 1
    assert artifact.metadata["network_request_count"] == 0


def test_collect_skips_repository_unavailable_for_legal_reasons(tmp_path: Path) -> None:
    """单个 451 仓库不能中断其余公开仓库的 metadata 采集。"""

    gateway = FakeGitHubGateway(
        {
            1: _response(1),
            2: GitHubResponse(451, {}, None),
        }
    )

    GitHubRepositoryMetadataSource([1, 2], GitHubMetadataConfig(), gateway).collect(
        _context(tmp_path)
    )

    manifest = json.loads((tmp_path / "raw/github_metadata/manifest.json").read_text())
    assert manifest["unavailable_repo_ids"] == [2]


def test_collect_refuses_request_plan_over_cap(tmp_path: Path) -> None:
    context = _context(tmp_path)
    source = GitHubRepositoryMetadataSource(
        [1, 2], GitHubMetadataConfig(maximum_requests=1), FakeGitHubGateway({})
    )

    with pytest.raises(RuntimeError, match="超过上限"):
        source.collect(context)


def test_collect_reports_rate_limit_without_secret(tmp_path: Path) -> None:
    context = _context(tmp_path)
    gateway = FakeGitHubGateway(
        {
            1: GitHubResponse(
                403,
                {"x-ratelimit-remaining": "0", "x-ratelimit-reset": "123"},
                None,
            )
        }
    )

    with pytest.raises(RuntimeError, match="remaining=0, reset=123"):
        GitHubRepositoryMetadataSource([1], GitHubMetadataConfig(), gateway).collect(context)
    assert gateway.closed


def test_collect_distinguishes_permission_denied_from_rate_limit(tmp_path: Path) -> None:
    gateway = FakeGitHubGateway({1: GitHubResponse(403, {}, None)})

    with pytest.raises(RuntimeError, match="权限不足"):
        GitHubRepositoryMetadataSource([1], GitHubMetadataConfig(), gateway).collect(
            _context(tmp_path)
        )


def test_collect_skips_repository_blocked_with_remaining_quota(tmp_path: Path) -> None:
    """额度尚有剩余的单仓库 403 是仓库级阻断，不是 token 整体失效。"""

    gateway = FakeGitHubGateway({1: GitHubResponse(403, {"x-ratelimit-remaining": "4126"}, None)})

    GitHubRepositoryMetadataSource([1], GitHubMetadataConfig(), gateway).collect(_context(tmp_path))

    manifest = json.loads((tmp_path / "raw/github_metadata/manifest.json").read_text())
    assert manifest["unavailable_repo_ids"] == [1]


def test_httpx_gateway_sends_auth_version_and_etag_headers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("TEST_GITHUB_TOKEN", "secret-test-token")

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url == "https://api.github.com/repositories/42"
        assert request.headers["authorization"] == "Bearer secret-test-token"
        assert request.headers["x-github-api-version"] == "2026-03-10"
        assert request.headers["if-none-match"] == '"etag-v1"'
        return httpx.Response(200, headers={"ETag": '"etag-v2"'}, json={"id": 42})

    gateway = HttpxGitHubGateway(
        GitHubMetadataConfig(token_env="TEST_GITHUB_TOKEN"),
        transport=httpx.MockTransport(handler),
    )

    response = gateway.fetch_repository(42, '"etag-v1"')
    gateway.close()

    assert response.status_code == 200
    assert response.headers["etag"] == '"etag-v2"'
    assert response.payload == {"id": 42}


def _artifact_path(artifact: Artifact) -> Path:
    # Artifact URI 在当前实现中是 file:// 绝对路径；此辅助函数仅供测试解析。
    return Path(artifact.uri.removeprefix("file://"))


def _context(workspace: Path) -> TaskContext:
    return TaskContext("run-1", workspace, "sha256:test", 7)
