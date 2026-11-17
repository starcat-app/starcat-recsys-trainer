"""模型分发回归：只用合成 Bundle 和 HTTP 替身，不访问真实 GitHub 或线上服务。"""

from __future__ import annotations

import base64
import hashlib
import json
import sqlite3
import zipfile
from dataclasses import asdict
from pathlib import Path
from typing import Any

import httpx
import pytest

from starcat_recsys.artifacts import artifact_for, sha256_file, write_json_atomic
from starcat_recsys.cli import main
from starcat_recsys.history_backfill import backfill_history
from starcat_recsys.model_history import bundle_record, load_catalog, record_result, render_history
from starcat_recsys.model_release import GitHubDistribution, prepare_release
from starcat_recsys.weekly_reporting import finish_publication, github_client


def example_bundle(root: Path) -> Path:
    """构造可校验的微型模型，并故意放入不允许发布的旁路文件。"""
    root.mkdir()
    with sqlite3.connect(root / "recommendations.sqlite") as database:
        database.executescript(
            "CREATE TABLE repositories(repo_id INTEGER);"
            "CREATE TABLE recommendations(source_repo_id INTEGER, target_repo_id INTEGER);"
            "INSERT INTO repositories VALUES(1),(2);"
            "INSERT INTO recommendations VALUES(1,2);"
        )
    write_json_atomic(
        root / "manifest.json",
        {
            "schema_version": 1,
            "model_version": "test-v1",
            "selected_model": "hybrid_25",
            "created_at": "2026-11-17T03:00:00Z",
            "metrics_scope": "pre_refit",
            "metrics": {"recall@20": 0.04, "evaluated_subjects": 5000, "subject_id": "private"},
            "training": {
                "started_at": "2026-11-17T01:00:00Z",
                "completed_at": "2026-11-17T02:00:00Z",
                "private_path": "/private/input",
                "parameters": {"content_weight": 0.25},
            },
        },
    )
    write_json_atomic(
        root / "checksums.json",
        {name: sha256_file(root / name) for name in ("manifest.json", "recommendations.sqlite")},
    )
    (root / "private-subjects.json").write_text('{"secret": true}')
    return root


def test_package_is_reproducible_and_history_preserves_unknowns(tmp_path: Path) -> None:
    """重试不能改变 ZIP；公开报告只含契约字段，缺失历史时间不伪造。"""
    bundle = example_bundle(tmp_path / "bundle")
    record = bundle_record(bundle)
    assert record["training"]["started_at"] != record["created_at"]
    assert "private_path" not in record["training"]
    assert "subject_id" not in record["metrics"]
    catalog = tmp_path / "docs/models/history.json"
    record_result(catalog, record)
    record_result(
        catalog, {**record, "status": "published", "release_url": "https://example.test/release"}
    )
    assert len(load_catalog(catalog)["runs"]) == 1
    files = render_history(catalog, tmp_path / "docs")
    assert all("private-subjects" not in path.read_text() for path in files)
    first = prepare_release(bundle, tmp_path / "first")
    second = prepare_release(bundle, tmp_path / "second")
    assert sha256_file(first / "test-v1.zip") == sha256_file(second / "test-v1.zip")
    with zipfile.ZipFile(first / "test-v1.zip") as archive:
        assert set(archive.namelist()) == {
            "manifest.json",
            "checksums.json",
            "recommendations.sqlite",
        }
    with pytest.raises(ValueError, match="不能准备"):
        prepare_release(bundle, tmp_path / "blocked", {**record, "status": "blocked"})
    with pytest.raises(ValueError, match="摘要"):
        record_result(catalog, {**record, "manifest_checksum": "sha256:changed"})
    write_json_atomic(bundle / "checksums.json", {})
    with pytest.raises(ValueError, match="恰好校验"):
        prepare_release(bundle, tmp_path / "missing-checksums")


def test_release_resumes_partial_upload_and_never_replaces_assets(tmp_path: Path) -> None:
    """模拟中断后补传；只有四个资产完整才解除草稿，冲突资产不能 clobber。"""
    package = prepare_release(example_bundle(tmp_path / "bundle"), tmp_path / "release")
    state: dict[str, Any] = {"release": None, "assets": [], "fail_once": True, "uploads": 0}

    def respond(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if path.endswith("/releases/tags/model-test-v1"):
            return httpx.Response(
                404 if state["release"] is None else 200,
                json=state["release"] or {},
            )
        if path.endswith("/releases") and request.method == "POST":
            body = json.loads(request.content)
            assert body["draft"] is True
            state["release"] = {"id": 1, "draft": True, "html_url": "https://example.test/model"}
            return httpx.Response(201, json=state["release"])
        if path.endswith("/assets") and request.method == "GET":
            return httpx.Response(200, json=state["assets"])
        if request.url.host == "uploads.github.com":
            if state["assets"] and state["fail_once"]:
                state["fail_once"] = False
                return httpx.Response(503, text="private server body")
            asset = {
                "id": len(state["assets"]) + 1,
                "name": request.url.params["name"],
                "digest": "sha256:" + hashlib.sha256(request.content).hexdigest(),
            }
            state["assets"].append(asset)
            state["uploads"] += 1
            return httpx.Response(201, json=asset)
        if request.method == "PATCH":
            assert len(state["assets"]) == 4
            state["release"]["draft"] = False
            return httpx.Response(200, json=state["release"])
        pytest.fail(f"unexpected request: {request.method} {path}")

    with httpx.Client(transport=httpx.MockTransport(respond)) as client:
        publisher = GitHubDistribution("owner/models", client)
        with pytest.raises(RuntimeError, match="503") as failure:
            publisher.publish(package)
        assert "private server body" not in str(failure.value)
        assert state["release"]["draft"]
        assert publisher.publish(package) == "https://example.test/model"
        assert state["uploads"] == 4
        publisher.publish(package)
        assert state["uploads"] == 4
        state["assets"][0]["digest"] = "sha256:different"
        with pytest.raises(ValueError, match="拒绝覆盖"):
            publisher.publish(package)


def test_history_sync_merges_remote_and_updates_only_generated_files(tmp_path: Path) -> None:
    """本地副本落后时不能丢掉远端周次；一个非强制 commit 更新全部生成文档。"""
    catalog = tmp_path / "catalog.json"
    record_result(catalog, {"run_id": "new", "status": "blocked", "metrics": {}})
    remote = {"schema_version": 1, "runs": [{"run_id": "old", "status": "data_only"}]}
    mutations: list[dict[str, Any]] = []

    def respond(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if path.endswith("/git/ref/heads/main"):
            return httpx.Response(200, json={"object": {"sha": "head"}})
        if path.endswith("/git/commits/head"):
            return httpx.Response(200, json={"tree": {"sha": "tree"}})
        if "/contents/" in path:
            return httpx.Response(
                200,
                json={
                    "content": base64.b64encode(json.dumps(remote).encode()).decode(),
                },
            )
        body = json.loads(request.content)
        mutations.append(body)
        if path.endswith("/git/trees"):
            entries = body["tree"]
            assert all(entry["path"].startswith("docs/") for entry in entries)
            history = next(
                entry["content"] for entry in entries if entry["path"].endswith("history.json")
            )
            assert {record["run_id"] for record in json.loads(history)["runs"]} == {"old", "new"}
            return httpx.Response(201, json={"sha": "new-tree"})
        if path.endswith("/git/commits"):
            assert body["parents"] == ["head"]
            return httpx.Response(201, json={"sha": "new-commit"})
        assert body == {"sha": "new-commit", "force": False}
        return httpx.Response(200, json={})

    with httpx.Client(transport=httpx.MockTransport(respond)) as client:
        commit = GitHubDistribution("owner/models", client).sync_history(catalog, branch="main")
    assert commit == "new-commit"
    assert len(mutations) == 3


def test_distribution_failure_preserves_successful_training(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """GitHub 凭据缺失只留下待补传状态，不能把已上线模型改成训练失败。"""
    bundle = example_bundle(tmp_path / "bundle")
    run_root = tmp_path / "run"
    artifact = artifact_for(bundle / "manifest.json", "serving_bundle", {})
    write_json_atomic(
        run_root / "run.json",
        {
            "run_id": "weekly-test",
            "status": "published",
            "target": "2026-11-16",
            "updated_at": "2026-11-17T03:00:00Z",
            "steps": {"bundle": [asdict(artifact)]},
        },
    )
    state = {"status": "published", "current_run": str(run_root), "published_version": "test-v1"}
    config = {
        "workspace": str(tmp_path),
        "public_distribution": {
            "enabled": True,
            "token_env": "STARCAT_TEST_MISSING_TOKEN",
        },
    }
    monkeypatch.delenv("STARCAT_TEST_MISSING_TOKEN", raising=False)
    result = finish_publication(config, state, tmp_path / "state.json")
    assert result["status"] == "pending"
    assert state["status"] == "published"
    assert state["published_version"] == "test-v1"
    assert (tmp_path / "public/docs/MODEL_HISTORY.md").is_file()
    assert (
        finish_publication(config, state, tmp_path / "state.json", apply=False)["status"]
        == "prepared"
    )


def test_scheduler_credentials_use_private_file_and_environment_override(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """launchd 不继承 shell export；私有文件可供后台读取，显式环境值仍优先。"""
    token_file = tmp_path / "github.key"
    token_file.write_text("test-file-token\n")
    token_file.chmod(0o600)
    monkeypatch.delenv("STARCAT_TEST_DISTRIBUTION_TOKEN", raising=False)
    with github_client("STARCAT_TEST_DISTRIBUTION_TOKEN", str(token_file)) as client:
        assert client.headers["Authorization"] == "Bearer test-file-token"
    token_file.chmod(0o644)
    with pytest.raises(ValueError, match="600"):
        github_client("STARCAT_TEST_DISTRIBUTION_TOKEN", str(token_file))
    monkeypatch.setenv("STARCAT_TEST_DISTRIBUTION_TOKEN", "test-environment-token")
    with github_client("STARCAT_TEST_DISTRIBUTION_TOKEN", str(token_file)) as client:
        assert client.headers["Authorization"] == "Bearer test-environment-token"


def test_backfill_preserves_failed_evidence_and_data_only_iterations(tmp_path: Path) -> None:
    """缺失完成证据保留未知；失败的 test 指标与仅数据修复不能变成可发布模型。"""
    root = tmp_path / "training"
    (root / "gharchive-local-2016-2026-v1").mkdir(parents=True)
    write_json_atomic(
        root / "gharchive-20261031-v18-event-identity/runs/"
        "gharchive-20261031-v18-event-identity/dataset/quality-report.json",
        {"final_rows": 42, "private_path": "/not-public"},
    )
    (root / "gharchive-20261031-v17-recent").mkdir()
    write_json_atomic(
        root / "gharchive-v17-recent-controls-20261102/decision.json",
        {
            "passed": False,
            "test": {
                "split": "test",
                "k": 20,
                "dataset_checksum": "sha256:test",
                "models": {
                    "costar": {"recall@20": 0.01, "evaluated_subjects": 274},
                    "baseline": {"recall@20": 0.02},
                },
            },
        },
    )
    catalog = tmp_path / "docs/models/history.json"
    assert backfill_history(root, catalog, tmp_path / "docs") == 3
    before = catalog.read_bytes()
    backfill_history(root, catalog, tmp_path / "docs")
    assert catalog.read_bytes() == before
    records = load_catalog(catalog)["runs"]
    assert [record["iteration"] for record in records] == [1, 17, 18]
    assert records[0]["status"] == "unverified"
    assert records[1]["status"] == "blocked"
    assert records[1]["metrics"]["recall@20"] == 0.01
    assert records[1]["decision"]["passed"] is False
    assert records[2]["status"] == "data_only"
    assert "private_path" not in records[2]["data_processing"]


def test_release_preview_validates_assets_without_credentials(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """预览读回摘要并列出具体资产；损坏包在任何 GitHub 请求之前拒绝。"""
    package = prepare_release(example_bundle(tmp_path / "bundle"), tmp_path / "release")
    arguments = [
        "release",
        "publish",
        "--package",
        str(package),
        "--repository",
        "owner/models",
    ]
    assert main(arguments) == 0
    preview = json.loads(capsys.readouterr().out)
    assert preview["status"] == "preview"
    assert preview["tag"] == "model-test-v1"
    assert len(preview["assets"]) == 4
    (package / "test-v1.zip").write_bytes(b"changed")
    with pytest.raises(ValueError, match="摘要"):
        main(arguments)
