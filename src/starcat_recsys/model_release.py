"""ServingBundle 的可复现打包与 GitHub 分发。

只分发白名单文件；先上传完整草稿再公开。相同资产必须同摘要，网络失败后可续传，
绝不删除或覆盖不同内容的资产。文档同步只提交生成目录，不操作本地 Git 工作区。
"""

from __future__ import annotations

import base64
import hashlib
import json
import re
import shutil
import tempfile
import zipfile
from pathlib import Path
from typing import Any
from urllib.parse import quote

import httpx

from starcat_recsys.artifacts import sha256_file, write_json_atomic
from starcat_recsys.bundle import verify_bundle
from starcat_recsys.model_history import (
    bundle_record,
    identifier,
    load_catalog,
    model_card,
    record_result,
    render_history,
)

BUNDLE_FILES = ("manifest.json", "recommendations.sqlite", "checksums.json")
MAX_ARCHIVE_BYTES = 512 * 1024 * 1024


def prepare_release(
    bundle: Path,
    output: Path,
    record: dict[str, Any] | None = None,
) -> Path:
    """仅本地生成固定时间戳 ZIP 和公开说明；同一 Bundle 重试时 ZIP 字节不变。"""
    manifest = verify_bundle(bundle)
    version = identifier(str(manifest["model_version"]))
    checksums = json.loads((bundle / "checksums.json").read_text())
    required = {"manifest.json", "recommendations.sqlite"}
    extras = set(checksums) - required
    if not required.issubset(checksums) or extras - {"._" + name for name in required}:
        raise ValueError("分发包必须恰好校验 manifest.json 与 recommendations.sqlite")
    public = record.copy() if record else bundle_record(bundle)
    if public.get("model_version") != version or public.get("manifest_checksum") != sha256_file(
        bundle / "manifest.json"
    ):
        raise ValueError("成果记录与 Bundle 不一致")
    if public["status"] not in {"evaluated", "published"}:
        raise ValueError("未通过评估的成果不能准备正式模型 Release")
    output.mkdir(parents=True, exist_ok=True)
    archive = output / f"{version}.zip"
    # 不引用文件 mtime，避免补传时压缩包 checksum 改变；原始 Bundle 始终只读。
    with zipfile.ZipFile(archive, "w", compression=zipfile.ZIP_DEFLATED) as zipped:
        for name in BUNDLE_FILES:
            entry = zipfile.ZipInfo(name, date_time=(1980, 1, 1, 0, 0, 0))
            entry.compress_type = zipfile.ZIP_DEFLATED
            entry.external_attr = 0o100644 << 16
            if name == "checksums.json" and extras:
                # 旧包先完整校验，再仅为传输清单移除已验证的 AppleDouble 项。
                # 原 manifest、SQLite、源校验文件及模型版本均不改写。
                content = {key: checksums[key] for key in sorted(required)}
                zipped.writestr(entry, json.dumps(content, sort_keys=True, indent=2) + "\n")
                continue
            with (bundle / name).open("rb") as source, zipped.open(entry, "w") as destination:
                shutil.copyfileobj(source, destination)
    if archive.stat().st_size > MAX_ARCHIVE_BYTES:
        raise ValueError("模型压缩包超过推荐服务默认 512 MiB 上限")
    public["release_url"] = None
    (output / "model-card.md").write_text(model_card(public))
    write_json_atomic(
        output / "evaluation.json",
        {
            key: public.get(key)
            for key in (
                "model_version",
                "metrics",
                "metrics_scope",
                "evaluation",
                "evaluation_checksum",
                "evaluated_model_checksum",
            )
        },
    )
    assets = [archive, output / "model-card.md", output / "evaluation.json"]
    (output / "SHA256SUMS").write_text(
        "".join(f"{sha256_file(path).removeprefix('sha256:')}  {path.name}\n" for path in assets)
    )
    assets.append(output / "SHA256SUMS")
    write_json_atomic(
        output / "release.json",
        {
            "schema_version": 1,
            "model_version": version,
            "tag": f"model-{version}",
            "record": public,
            "assets": {path.name: sha256_file(path) for path in assets},
        },
    )
    return output


def read_release_package(package: Path) -> dict[str, Any]:
    """预览和上传共用本地校验，避免预览一个缺失或已改变的待发布包。"""
    document: dict[str, Any] = json.loads((package / "release.json").read_text())
    version = identifier(document["model_version"])
    expected = {f"{version}.zip", "model-card.md", "evaluation.json", "SHA256SUMS"}
    if set(document["assets"]) != expected:
        raise ValueError("Release 资产白名单不符")
    for name, checksum in document["assets"].items():
        if sha256_file(package / name) != checksum:
            raise ValueError("Release 本地资产摘要不符")
    if document["record"]["status"] not in {"evaluated", "published"}:
        raise ValueError("受阻或失败模型不得发布")
    return document


class GitHubDistribution:
    """仅使用指定仓库的 Contents 凭据；失败异常不包含响应正文或认证头。"""

    def __init__(self, repository: str, client: httpx.Client) -> None:
        if not re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", repository):
            raise ValueError("GitHub repository 必须为 owner/repo")
        self.repository = repository
        self.client = client
        self.root = f"https://api.github.com/repos/{repository}"

    def _json(self, method: str, path: str, **kwargs: Any) -> Any:
        response = self.client.request(method, self.root + path, **kwargs)
        if response.status_code not in {200, 201}:
            raise RuntimeError(f"GitHub {method} 操作失败 HTTP {response.status_code}")
        return response.json()

    def publish(self, package: Path, *, target: str = "main") -> str:
        """续传草稿中缺少的资产，全部摘要一致后才公开；已发布的不同内容拒绝覆盖。"""
        document = read_release_package(package)
        version = document["model_version"]
        tag = f"model-{version}"
        assets = document["assets"]
        response = self.client.get(self.root + f"/releases/tags/{tag}")
        if response.status_code == 404:
            # 首次草稿尚未创建 Git tag，按 tag 查询会返回 404；必须先找已有草稿。
            release = self._find_draft(tag)
            if release is None:
                release = self._json(
                    "POST",
                    "/releases",
                    json={
                        "tag_name": tag,
                        "target_commitish": target,
                        "name": version,
                        "body": (package / "model-card.md").read_text(),
                        "draft": True,
                        "prerelease": False,
                    },
                )
        elif response.status_code == 200:
            release = response.json()
        else:
            raise RuntimeError(f"GitHub Release 查询失败 HTTP {response.status_code}")
        release_id = release["id"]
        existing = {
            asset["name"]: asset
            for asset in self._json(
                "GET",
                f"/releases/{release_id}/assets",
                params={"per_page": 100},
            )
        }
        for name, checksum in assets.items():
            if name in existing:
                asset = existing[name]
                # 旧资产没有 digest 时实际读回校验，不凭文件名或大小判相同。
                if asset.get("digest") != checksum and (
                    asset.get("digest") or self._asset_checksum(asset["id"]) != checksum
                ):
                    raise ValueError("GitHub 同名资产内容不同，拒绝覆盖")
                continue
            if not release["draft"]:
                raise ValueError("已公开 Release 缺少资产，不能把不完整版本当作成功")
            path = package / name
            with path.open("rb") as source:
                response = self.client.post(
                    f"https://uploads.github.com/repos/{self.repository}/releases/{release_id}/assets",
                    params={"name": name},
                    # BinaryIO 默认按行迭代，ZIP 会形成大量小写入；固定块流式上传。
                    content=iter(lambda: source.read(1024 * 1024), b""),
                    headers={
                        "Content-Type": "application/octet-stream",
                        "Content-Length": str(path.stat().st_size),
                    },
                )
            if response.status_code != 201:
                raise RuntimeError(f"GitHub 资产上传失败 HTTP {response.status_code}")
            uploaded = response.json()
            if (
                uploaded.get("digest") != checksum
                and self._asset_checksum(uploaded["id"]) != checksum
            ):
                raise ValueError("GitHub 上传资产回读摘要不符")
        if release["draft"]:
            release = self._json(
                "PATCH",
                f"/releases/{release_id}",
                json={
                    "draft": False,
                    "make_latest": "true",
                },
            )
        return str(release["html_url"])

    def _find_draft(self, tag: str) -> dict[str, Any] | None:
        """周模型累积后仍按页找未发布草稿，避免中断重试创建重复 Release。"""
        page = 1
        while True:
            releases = self._json("GET", "/releases", params={"per_page": 100, "page": page})
            for release in releases:
                if release["tag_name"] == tag and release["draft"]:
                    return dict(release)
            if len(releases) < 100:
                return None
            page += 1

    def _asset_checksum(self, asset_id: int) -> str:
        """流式回读只用于缺少服务端 digest 的资产，不把模型包整体放进内存。"""
        digest = hashlib.sha256()
        with self.client.stream(
            "GET",
            self.root + f"/releases/assets/{asset_id}",
            headers={"Accept": "application/octet-stream"},
            follow_redirects=True,
        ) as response:
            if response.status_code != 200:
                raise RuntimeError(f"GitHub 资产读取失败 HTTP {response.status_code}")
            for chunk in response.iter_bytes():
                digest.update(chunk)
        return "sha256:" + digest.hexdigest()

    def sync_history(self, catalog: Path, *, branch: str) -> str:
        """合并远端历史并用一个 Git tree/commit 更新生成文档，不强推分支。"""
        branch_path = quote(branch, safe="")
        head = self._json("GET", f"/git/ref/heads/{branch_path}")["object"]["sha"]
        commit = self._json("GET", f"/git/commits/{head}")
        response = self.client.get(
            self.root + "/contents/docs/models/history.json",
            params={"ref": head},
        )
        with tempfile.TemporaryDirectory(prefix="starcat-public-history-") as temporary:
            root = Path(temporary)
            merged = root / "models/history.json"
            if response.status_code == 200:
                payload = response.json()
                content = json.loads(base64.b64decode(payload["content"]))
                write_json_atomic(merged, content)
            elif response.status_code != 404:
                raise RuntimeError(f"GitHub 历史读取失败 HTTP {response.status_code}")
            for record in load_catalog(catalog)["runs"]:
                record_result(merged, record)
            files = render_history(merged, root)
            tree = self._json(
                "POST",
                "/git/trees",
                json={
                    "base_tree": commit["tree"]["sha"],
                    "tree": [
                        {
                            "path": "docs/" + str(path.relative_to(root)),
                            "mode": "100644",
                            "type": "blob",
                            "content": path.read_text(),
                        }
                        for path in files
                    ],
                },
            )
            if tree["sha"] == commit["tree"]["sha"]:
                return str(head)
            created = self._json(
                "POST",
                "/git/commits",
                json={
                    "message": "docs(models): 更新模型训练成果",
                    "tree": tree["sha"],
                    "parents": [head],
                },
            )
            self._json(
                "PATCH",
                f"/git/refs/heads/{branch_path}",
                json={
                    "sha": created["sha"],
                    "force": False,
                },
            )
            return str(created["sha"])
