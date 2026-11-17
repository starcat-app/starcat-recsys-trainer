"""从用户自己的首次训练结果生成周任务配置，不分发官方固定训练主体。"""

from __future__ import annotations

import json
import os
from datetime import date
from pathlib import Path

import pyarrow.parquet as pq
import yaml

from starcat_recsys.artifacts import sha256_file
from starcat_recsys.bundle import verify_bundle
from starcat_recsys.interfaces import Artifact
from starcat_recsys.recommendations import artifact_path


def initialize_weekly(
    run: Path,
    raw_root: Path,
    watermark: date,
    output: Path,
    api_root: Path,
) -> None:
    """仅生成权限为 600 的配置；不安装调度、不训练、不上传或激活模型。"""
    if output.exists():
        raise FileExistsError("周任务配置已存在")
    manifest = json.loads((run / "run-manifest.json").read_text())
    bundle_info = manifest["bundle"]
    bundle = artifact_path(
        Artifact(
            "serving_bundle",
            bundle_info["uri"],
            bundle_info["checksum"],
            {},
        )
    ).parent
    if sha256_file(bundle / "manifest.json") != bundle_info["checksum"]:
        raise ValueError("首次训练 Bundle 与运行记录摘要不符")
    verify_bundle(bundle)
    dataset = run / "dataset/interactions.parquet"
    catalog = run / "dataset/repositories.parquet"
    if not dataset.is_file() or not catalog.is_file():
        raise ValueError("首次训练缺少 Dataset 或公开目录")
    # 周训练的主体哈希语义属于 GH Archive，不允许混入其他来源的身份。
    sources = set(pq.read_table(dataset, columns=["source"]).column("source").unique().to_pylist())
    if sources != {"gh_archive"}:
        raise ValueError("周训练初始化只接受纯 GH Archive 训练集")
    checkpoint = json.loads((raw_root / "download-state.json").read_text())
    if watermark.strftime("%Y%m%d") not in checkpoint["completed_partitions"]:
        raise ValueError("初始水位不在已下载的完整 Raw 中")
    workspace = output.parent / "weekly"
    workspace.mkdir(parents=True, exist_ok=True)
    document = {
        "volume_root": str(Path(raw_root.resolve().anchor)),
        "workspace": str(workspace.resolve()),
        "state_file": str((output.parent / "weekly-state.json").resolve()),
        "raw_root": str(raw_root.resolve()),
        "frozen_training_dataset": str(dataset.resolve()),
        "core_catalog": str(catalog.resolve()),
        "expanded_catalog": str(catalog.resolve()),
        "initial_bundle": str(bundle.resolve()),
        "initial_watermark": watermark.isoformat(),
        "api_root": str(api_root.resolve()),
        "base_url": "http://127.0.0.1:5005",
        "publish_key_file": str((output.parent / "recommend-publish.key").resolve()),
        "query_key_file": str((output.parent / "recommend-query.key").resolve()),
        "public_distribution": {"enabled": False},
    }
    output.parent.mkdir(parents=True, exist_ok=True)
    descriptor = os.open(output, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(descriptor, "w") as handle:
        yaml.safe_dump(document, handle, allow_unicode=True, sort_keys=False)
