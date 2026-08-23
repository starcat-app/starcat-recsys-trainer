"""训练器和 Serving 之间的统一 Top-K 边格式。"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from starcat_recsys.artifacts import artifact_for
from starcat_recsys.interfaces import Artifact

RECOMMENDATION_SCHEMA = pa.schema(
    [
        ("source_repo_id", pa.int64()),
        ("target_repo_id", pa.int64()),
        ("score", pa.float32()),
        ("rank", pa.int32()),
        ("model", pa.string()),
        ("signals", pa.string()),
    ]
)


def write_recommendations(
    path: Path,
    rows: list[dict[str, Any]],
    model: str,
) -> Artifact:
    """以稳定排序写入压缩 Parquet，便于 checksum 和回归比较。"""

    rows.sort(key=lambda row: (row["source_repo_id"], row["rank"], row["target_repo_id"]))
    table = pa.Table.from_pylist(rows, schema=RECOMMENDATION_SCHEMA)
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, path, compression="zstd")
    return artifact_for(path, "model_recommendations", {"model": model, "row_count": len(rows)})


def artifact_path(artifact: Artifact) -> Path:
    if not artifact.uri.startswith("file://"):
        raise ValueError("当前训练器只支持 file:// artifact")
    return Path(artifact.uri.removeprefix("file://"))


def training_dataset(inputs: list[Artifact]) -> Artifact:
    matches = [artifact for artifact in inputs if artifact.kind == "training_dataset"]
    if len(matches) != 1:
        raise ValueError(f"需要一个 training_dataset artifact，实际为 {len(matches)}")
    return matches[0]
