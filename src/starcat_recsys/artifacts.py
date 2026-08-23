"""不可变 artifact 的落盘、摘要和 manifest 工具。"""

from __future__ import annotations

import hashlib
import json
import os
import tempfile
from pathlib import Path
from typing import Any

from starcat_recsys.interfaces import Artifact


def sha256_file(path: Path) -> str:
    """流式计算文件摘要，避免大 Parquet 文件整体进入内存。"""

    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return f"sha256:{digest.hexdigest()}"


def write_json_atomic(path: Path, payload: Any, *, mode: int = 0o644) -> None:
    """使用同目录临时文件原子替换 JSON，防止任务中断留下半个 manifest。"""

    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(dir=path.parent, prefix=f".{path.name}.")
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, ensure_ascii=False, indent=2, sort_keys=True)
            handle.write("\n")
        temporary.chmod(mode)
        temporary.replace(path)
    except BaseException:
        temporary.unlink(missing_ok=True)
        raise


def artifact_for(path: Path, kind: str, metadata: dict[str, Any]) -> Artifact:
    """为已落盘文件生成带 checksum 的 artifact 描述。"""

    return Artifact(
        kind=kind, uri=path.resolve().as_uri(), checksum=sha256_file(path), metadata=metadata
    )
