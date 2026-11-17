"""每周模型的隔离接口验收、生产交付与失败回退。

查询凭据和发布凭据分离；响应正文与凭据不进入日志。激活前保存旧 Bundle，网络
中断后先回读 active，再决定续验或回退，不能把上传超时直接当成服务端未激活。
"""

from __future__ import annotations

import json
import os
import re
import sqlite3
import subprocess
from pathlib import Path
from typing import Any

import httpx

from starcat_recsys.artifacts import artifact_for, write_json_atomic
from starcat_recsys.bundle import verify_bundle
from starcat_recsys.interfaces import Artifact, TaskContext
from starcat_recsys.publishers.recommend_api import (
    RecommendAPIBundlePublisher,
    RecommendAPIPublishConfig,
)
from starcat_recsys.recommendations import artifact_path


def active_version(client: httpx.Client, base_url: str, publish_key: str) -> str:
    """只回传活动版本，不输出认证头或服务端错误正文。"""
    response = client.get(
        base_url + "/internal/v1/model-bundles/active",
        headers={"Authorization": "Bearer " + publish_key},
    )
    if response.status_code != 200:
        raise RuntimeError(f"活动模型查询失败 HTTP {response.status_code}")
    return str(response.json()["data"]["model_version"])


def read_key(path: Path) -> str:
    """仅从用户私有文件读取凭据，拒绝空值与宽松权限。"""
    if path.stat().st_mode & 0o077:
        raise ValueError("凭据文件权限必须为 600")
    value = path.read_text().strip()
    if not value:
        raise ValueError("凭据不能为空")
    return value


def local_acceptance(context: TaskContext, api_root: Path, bundle: Path) -> dict[str, Any]:
    """运行真实 Go 服务的隔离 Registry 测试；原始产物只读、没有生产凭据。"""
    environment = {
        key: value
        for key, value in os.environ.items()
        if not any(word in key.upper() for word in ["TOKEN", "SECRET", "KEY"])
    }
    environment["STARCAT_TEST_CANDIDATE_BUNDLE"] = str(bundle)
    # 固定模块依赖已包含所需 Kit 能力；独立 checkout 必须可验收，不能借兄弟工作区过关。
    environment["GOWORK"] = "off"
    result = subprocess.run(
        [
            "go",
            "test",
            "./server",
            "-run",
            "^TestCandidateBundleLocalAcceptance$",
            "-count=1",
            "-v",
        ],
        cwd=api_root,
        env=environment,
        capture_output=True,
        text=True,
        timeout=900,
        check=False,
    )
    (context.workspace / "local-api-acceptance.log").write_text(result.stdout + result.stderr)
    passed = (
        result.returncode == 0 and "--- PASS: TestCandidateBundleLocalAcceptance" in result.stdout
    )
    if not passed:
        raise RuntimeError("隔离 API 验收失败，详见本机运行日志")
    return {"passed": True, "test": "TestCandidateBundleLocalAcceptance"}


def smoke_seeds(bundle: Path) -> list[int]:
    """固定选取有多页共现结果的源与只有内容补充的源，不从用户查询日志取样。"""
    with sqlite3.connect(f"file:{bundle / 'recommendations.sqlite'}?mode=ro", uri=True) as db:
        core = db.execute(
            "SELECT source_repo_id FROM recommendations GROUP BY source_repo_id "
            "HAVING count(*)>=10 ORDER BY source_repo_id LIMIT 5"
        ).fetchall()
        cold = db.execute(
            "SELECT DISTINCT c.repo_id FROM content_postings c "
            "WHERE NOT EXISTS (SELECT 1 FROM recommendations r "
            "WHERE r.source_repo_id=c.repo_id) ORDER BY c.repo_id LIMIT 5"
        ).fetchall()
    if not core:
        raise ValueError("Bundle 缺少可验收的共现源")
    return [int(row[0]) for row in core + cold]


def public_acceptance(
    client: httpx.Client,
    base_url: str,
    query_key: str,
    version: str,
    seeds: list[int],
    required_nonempty: set[int] | None = None,
) -> dict[str, Any]:
    """检查真实线上版本、固定源覆盖、分页、缓存与多种子排除；401 也必须正确。"""
    headers = {"Authorization": "Bearer " + query_key}
    nonempty = []
    for seed in seeds:
        url = f"{base_url}/api/v2/repos/{seed}/recommendations"
        response = client.get(url, headers=headers, params={"limit": 5})
        if response.status_code != 200:
            raise RuntimeError(f"公开推荐验收失败 HTTP {response.status_code}")
        data = response.json()["data"]
        effective_version = data["model_version"]
        # 内容补充的版本还绑定源 metadata 摘要；只接受当前 Bundle 的精确合法后缀，
        # 同一源的后续页仍必须保持完全相同的有效版本，不能只做宽松字符串前缀匹配。
        content_version = data.get("fallback") and re.fullmatch(
            re.escape(version) + r"-c[0-9a-f]{16}", effective_version
        )
        if effective_version != version and not content_version:
            raise ValueError("公开接口模型版本不符")
        ids = [int(item["repo_id"]) for item in data["items"]]
        if len(ids) != len(set(ids)) or seed in ids:
            raise ValueError("公开接口出现重复或自身推荐")
        if ids:
            nonempty.append(seed)
        if required_nonempty and seed in required_nonempty and not ids:
            raise ValueError("固定源的推荐覆盖退化")
        etag = response.headers.get("etag")
        if not etag:
            raise ValueError("公开推荐缺少 ETag")
        cached = client.get(url, headers={**headers, "If-None-Match": etag}, params={"limit": 5})
        if cached.status_code != 304:
            raise ValueError("公开推荐 304 验收失败")
        if data.get("has_more"):
            page = client.get(url, headers=headers, params={"limit": 5, "offset": 5})
            if page.status_code != 200:
                raise RuntimeError("公开推荐分页失败")
            second = page.json()["data"]
            if second["model_version"] != effective_version or set(ids) & {
                int(item["repo_id"]) for item in second["items"]
            }:
                raise ValueError("公开推荐分页版本或去重失败")
    unauthorized = client.get(f"{base_url}/api/v2/repos/{seeds[0]}/recommendations")
    if unauthorized.status_code != 401:
        raise ValueError("公开推荐鉴权验收失败")
    multi = client.post(
        base_url + "/api/v2/recommendations/query",
        headers=headers,
        json={"positive_repo_ids": seeds[:2], "exclude_repo_ids": seeds[2:3], "limit": 20},
    )
    if multi.status_code != 200:
        raise RuntimeError("多种子推荐验收失败")
    multi_data = multi.json()["data"]
    multi_ids = [int(item["repo_id"]) for item in multi_data["items"]]
    if (
        multi_data["model_version"] != version
        or len(multi_ids) != len(set(multi_ids))
        or set(multi_ids) & set(seeds[:3])
    ):
        raise ValueError("多种子推荐版本、去重或排除失败")
    return {"passed": True, "model_version": version, "nonempty_seeds": nonempty}


def publish_with_rollback(
    context: TaskContext,
    config: dict[str, Any],
    candidate: Artifact,
    incumbent: Path,
    *,
    client: httpx.Client | None = None,
) -> dict[str, Any]:
    """仅接收已过离线门禁的候选；重复进入同一 run 时可完成已激活候选的续验。"""
    if client is None:
        with httpx.Client(
            timeout=600, follow_redirects=False, headers={"X-SC-Svc": "recommend"}
        ) as owned:
            return publish_with_rollback(context, config, candidate, incumbent, client=owned)
    base = str(config["base_url"]).rstrip("/")
    publish_key = read_key(Path(config["publish_key_file"]))
    query_key = read_key(Path(config["query_key_file"]))
    previous = verify_bundle(incumbent)["model_version"]
    version = verify_bundle(artifact_path(candidate).parent)["model_version"]
    current = active_version(client, base, publish_key)
    if current not in {previous, version}:
        raise ValueError("线上活动模型已改变，停止交付")
    seeds = smoke_seeds(incumbent)
    before_path = context.workspace / "production-before.json"
    if current == previous:
        before = public_acceptance(client, base, query_key, previous, seeds)
        write_json_atomic(before_path, before)
    else:
        # 关机可能发生在服务端激活后、客户端写回执前；缺基线时不能伪造覆盖验收。
        before = json.loads(before_path.read_text())
    publisher = RecommendAPIBundlePublisher(
        RecommendAPIPublishConfig(base, timeout_seconds=600, activate=True),
        client=client,
        publish_key=publish_key,
    )
    try:
        if current == previous:
            publisher.publish(context, [candidate])
        if active_version(client, base, publish_key) != version:
            raise ValueError("发布后活动模型不符")
        result = public_acceptance(
            client, base, query_key, version, seeds, set(before["nonempty_seeds"])
        )
        write_json_atomic(context.workspace / "production-acceptance.json", result)
        return result
    except Exception:
        # 上传超时不能证明服务端没有切换；先查询，再回退，并真实核验回退后的公开接口。
        rollback: dict[str, Any] = {"attempted": False, "passed": False}
        try:
            current = active_version(client, base, publish_key)
            if current == version:
                rollback["attempted"] = True
                old = artifact_for(
                    incumbent / "manifest.json",
                    "serving_bundle",
                    {"bundle_uri": incumbent.as_uri()},
                )
                publisher.publish(context, [old])
            rollback["passed"] = active_version(client, base, publish_key) == previous
            if rollback["passed"]:
                public_acceptance(
                    client, base, query_key, previous, seeds, set(before["nonempty_seeds"])
                )
        except Exception as error:
            rollback["passed"] = False
            rollback["error_type"] = type(error).__name__
        write_json_atomic(context.workspace / "rollback.json", rollback)
        raise
