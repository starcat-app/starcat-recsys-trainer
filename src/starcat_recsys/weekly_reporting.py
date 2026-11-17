"""每周结果的公开归档和可独立重试的分发。

训练状态与 GitHub 分发状态分开：后者失败不能重训、回退线上模型或伪造发布成功。
默认只在训练 workspace 下生成公开报告，显式启用后才允许 GitHub 写入。
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

import httpx

from starcat_recsys.artifacts import sha256_file, write_json_atomic
from starcat_recsys.interfaces import Artifact
from starcat_recsys.model_history import (
    bundle_record,
    load_catalog,
    public_metrics,
    record_result,
    render_history,
)
from starcat_recsys.model_release import GitHubDistribution, prepare_release
from starcat_recsys.recommendations import artifact_path
from starcat_recsys.weekly_delivery import read_key


def github_client(token_env: str, token_file: str | None = None) -> httpx.Client:
    """环境优先，调度器可读权限为 600 的私有文件；配置和日志只记录文件路径。"""
    token = os.environ.get(token_env)
    if not token and token_file:
        token = read_key(Path(token_file).expanduser())
    if not token:
        raise ValueError("GitHub 分发凭据未配置")
    return httpx.Client(
        timeout=120,
        headers={
            "Authorization": "Bearer " + token,
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2026-03-10",
        },
    )


def _stage_artifact(run: dict[str, Any], stage: str) -> Path | None:
    """历史阶段必须通过记录摘要才能作为公开证据，缺失阶段不猜测结果。"""
    artifacts = run.get("steps", {}).get(stage, [])
    if not artifacts:
        return None
    artifact = Artifact(**artifacts[0])
    path = artifact_path(artifact)
    if sha256_file(path) != artifact.checksum:
        raise ValueError("每周成果证据摘要不符")
    return path


def weekly_record(run: dict[str, Any]) -> tuple[dict[str, Any], Path | None]:
    """优先记录最终模型；受阻候选只展示候选指标，失败不读取未知临时输出。"""
    manifest = _stage_artifact(run, "bundle") or _stage_artifact(run, "candidate")
    bundle = manifest.parent if manifest else None
    status = run["status"]
    if status not in {"published", "blocked", "failed"}:
        raise ValueError("只能归档已结束的每周运行")
    result: dict[str, Any] = bundle_record(bundle, status=status) if bundle else {}
    result.update(
        {
            "run_id": run["run_id"],
            "status": status,
            "raw_watermark": run["target"],
            "completed_at": run.get("updated_at"),
            "started_at": run.get("started_at"),
            "created_at": result.get("created_at") or run.get("updated_at"),
            "summary": run.get("reason") or run.get("error_type"),
        }
    )
    decision_path = _stage_artifact(run, "decision")
    if decision_path:
        decision = json.loads(decision_path.read_text())
        result["decision"] = {
            "passed": decision["passed"],
            "checks": decision.get("checks", {}),
            "candidate": public_metrics(decision.get("candidate", {})),
            "incumbent": public_metrics(decision.get("incumbent", {})),
        }
    return result, bundle


def sync_publication(
    config: dict[str, Any],
    state: dict[str, Any],
    *,
    apply: bool | None = None,
) -> dict[str, Any]:
    """归档当前运行并按配置分发；调用者持有周任务锁，不执行训练或生产写入。"""
    run = json.loads((Path(state["current_run"]) / "run.json").read_text())
    public, bundle = weekly_record(run)
    settings = config.get("public_distribution", {})
    root = Path(settings.get("output_directory", str(Path(config["workspace"]) / "public")))
    catalog = root / "docs/models/history.json"
    # 初次从仓库的公开历史播种，后续只维护运行时副本，不修改源码 checkout。
    if not catalog.exists():
        seed = Path(__file__).resolve().parents[2] / "docs/models/history.json"
        write_json_atomic(catalog, load_catalog(seed))
    record_result(catalog, public)
    render_history(catalog, root / "docs")
    enabled = settings.get("enabled", False) if apply is None else apply
    if not enabled:
        return {"status": "prepared", "run_id": run["run_id"]}
    repository = settings.get("repository", "starcat-app/starcat-recsys-trainer")
    branch = settings.get("branch", "main")
    with github_client(
        settings.get("token_env", "STARCAT_MODELS_GITHUB_TOKEN"), settings.get("token_file")
    ) as client:
        github = GitHubDistribution(repository, client)
        # 只有真实生产交付已通过的周模型进入正式 Release，失败/受阻只同步报告。
        if run["status"] == "published":
            if bundle is None:
                raise ValueError("已发布运行缺少最终 Bundle")
            package = prepare_release(bundle, root / "packages" / public["model_version"], public)
            release_url = github.publish(package, target=branch)
            public["release_url"] = release_url
            record_result(catalog, public)
            render_history(catalog, root / "docs")
        commit = github.sync_history(catalog, branch=branch)
    return {"status": "published", "run_id": run["run_id"], "history_commit": commit}


def finish_publication(
    config: dict[str, Any],
    state: dict[str, Any],
    state_file: Path,
    *,
    apply: bool | None = None,
) -> dict[str, Any]:
    """公开分发单独记账；失败只保留类型，不把私有路径或服务端正文写入公开记录。"""
    try:
        result = sync_publication(config, state, apply=apply)
    except Exception as error:
        result = {"status": "pending", "error_type": type(error).__name__}
    state["distribution"] = result
    write_json_atomic(state_file, state, mode=0o600)
    return result
