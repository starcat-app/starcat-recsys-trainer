"""把已校验的 ServingBundle 发布到 starcat-recommend-api。

本模块只处理交付，不参与训练和本地 Registry 安装。发布密钥只从环境变量或测试注入读取，
不会写入 artifact、日志或异常正文。
"""

from __future__ import annotations

import json
import os
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import unquote, urlsplit

import httpx

from starcat_recsys.artifacts import artifact_for, write_json_atomic
from starcat_recsys.bundle import verify_bundle
from starcat_recsys.interfaces import Artifact, TaskContext

_BUNDLE_FILES = ("recommendations.sqlite", "manifest.json", "checksums.json")


@dataclass(frozen=True, slots=True)
class RecommendAPIPublishConfig:
    """Recommend API 地址、密钥来源与交付上限。"""

    base_url: str
    publish_key_env: str = "STARCAT_RECOMMEND_PUBLISH_KEY"
    timeout_seconds: float = 120
    maximum_bundle_bytes: int = 512 * 1024 * 1024
    activate: bool = True

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
        if not self.publish_key_env:
            raise ValueError("publish_key_env 不能为空")
        if self.timeout_seconds <= 0:
            raise ValueError("timeout_seconds 必须大于 0")
        if self.maximum_bundle_bytes <= 0:
            raise ValueError("maximum_bundle_bytes 必须大于 0")


class RecommendAPIBundlePublisher:
    """将本地 Registry 中的不可变 Bundle 压缩并上传到 Recommend API。"""

    def __init__(
        self,
        config: RecommendAPIPublishConfig,
        *,
        client: httpx.Client | None = None,
        publish_key: str | None = None,
    ) -> None:
        self._config = config
        self._client = client
        self._publish_key = publish_key

    def publish(self, context: TaskContext, inputs: list[Artifact]) -> list[Artifact]:
        bundle_artifact = _single_bundle(inputs)
        bundle = _bundle_directory(bundle_artifact)
        manifest = verify_bundle(bundle)
        model_version = str(manifest.get("model_version", ""))
        if not model_version:
            raise ValueError("ServingBundle manifest 缺少 model_version")
        key = self._publish_key or os.environ.get(self._config.publish_key_env)
        if not key:
            raise RuntimeError(f"环境变量 {self._config.publish_key_env} 未配置")

        delivery_directory = context.workspace / "published" / "recommend-api"
        delivery_directory.mkdir(parents=True, exist_ok=True)
        archive = delivery_directory / f"{model_version}.zip"
        _write_bundle_zip(bundle, archive)
        archive_size = archive.stat().st_size
        if archive_size > self._config.maximum_bundle_bytes:
            raise ValueError("ServingBundle 压缩包超过 maximum_bundle_bytes")

        if self._client is None:
            with httpx.Client(timeout=self._config.timeout_seconds) as client:
                response_data = self._upload(client, key, model_version, archive)
        else:
            response_data = self._upload(self._client, key, model_version, archive)
        if response_data.get("model_version") != model_version:
            raise ValueError("Recommend API 返回的 model_version 与发布版本不一致")
        if bool(response_data.get("active")) != self._config.activate:
            raise ValueError("Recommend API 返回的 active 状态与发布请求不一致")

        receipt = delivery_directory / f"{model_version}-receipt.json"
        write_json_atomic(
            receipt,
            {
                "schema_version": 1,
                "model_version": model_version,
                "selected_model": response_data.get("selected_model"),
                "active": self._config.activate,
                "bundle_checksum": bundle_artifact.checksum,
                "archive_size": archive_size,
            },
        )
        return [
            artifact_for(
                receipt,
                "serving_delivery",
                {
                    "model_version": model_version,
                    "destination": "starcat-recommend-api",
                    "active": self._config.activate,
                },
            )
        ]

    def _upload(
        self,
        client: httpx.Client,
        key: str,
        model_version: str,
        archive: Path,
    ) -> dict[str, Any]:
        endpoint = self._config.base_url.rstrip("/") + f"/internal/v1/model-bundles/{model_version}"
        with archive.open("rb") as handle:
            response = client.post(
                endpoint,
                params={"activate": str(self._config.activate).lower()},
                headers={
                    "Authorization": f"Bearer {key}",
                    "Content-Type": "application/zip",
                    "Accept": "application/json",
                },
                content=handle,
            )
        if response.status_code < 200 or response.status_code >= 300:
            raise RuntimeError(f"Recommend API Bundle 发布失败：HTTP {response.status_code}")
        content_type = response.headers.get("Content-Type", "").split(";", 1)[0].strip()
        if content_type != "application/json":
            raise ValueError("Recommend API 发布响应 Content-Type 必须是 application/json")
        try:
            payload = response.json()
        except json.JSONDecodeError as error:
            raise ValueError("Recommend API 发布响应不是有效 JSON") from error
        if not isinstance(payload, dict) or payload.get("schema_version") != 1:
            raise ValueError("Recommend API 发布响应 schema 无效")
        data = payload.get("data")
        if not isinstance(data, dict):
            raise ValueError("Recommend API 发布响应缺少 data")
        return data


def _single_bundle(inputs: list[Artifact]) -> Artifact:
    bundles = [artifact for artifact in inputs if artifact.kind == "serving_bundle"]
    if len(bundles) != 1:
        raise ValueError(f"需要一个 serving_bundle artifact，实际为 {len(bundles)}")
    return bundles[0]


def _bundle_directory(artifact: Artifact) -> Path:
    uri = artifact.metadata.get("bundle_uri")
    if not isinstance(uri, str):
        raise ValueError("serving_bundle artifact 缺少 bundle_uri")
    parsed = urlsplit(uri)
    if parsed.scheme != "file" or parsed.netloc not in {"", "localhost"}:
        raise ValueError("当前 Publisher 只接受本机 file:// Bundle")
    bundle = Path(unquote(parsed.path))
    if not bundle.is_dir():
        raise ValueError("serving_bundle bundle_uri 不是目录")
    return bundle


def _write_bundle_zip(bundle: Path, destination: Path) -> None:
    """只打包稳定白名单文件，防止工作目录中的临时文件被意外上传。"""

    with zipfile.ZipFile(destination, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for filename in _BUNDLE_FILES:
            path = bundle / filename
            if path.is_symlink() or not path.is_file():
                raise ValueError(f"ServingBundle 文件不合法：{filename}")
            archive.write(path, filename)
