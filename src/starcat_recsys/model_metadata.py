"""公开模型元数据的生成边界。

只输出有界训练集的聚合计数、时间和数值参数；不复制运行配置、路径或主体。
历史模型没有记录的字段由调用方保留为空，不能用文件 mtime 猜测训练时间。
"""

from __future__ import annotations

import subprocess
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from starcat_recsys.duckdb_runtime import connect_for_workspace
from starcat_recsys.interfaces import Artifact, TaskContext
from starcat_recsys.recommendations import artifact_path

PARAMETERS = (
    "top_k",
    "half_life_days",
    "legacy_time_weight",
    "shrinkage",
    "minimum_support",
    "maximum_repositories_per_subject",
    "maximum_pair_observations",
    "content_weight",
)


def describe_training(
    context: TaskContext,
    dataset: Artifact,
    started_at: str,
    completed_at: str,
    parameters: dict[str, Any],
) -> dict[str, Any]:
    """在有界 Dataset 上统计实际参与训练的关系，不重新扫描 Raw 或暴露主体列表。"""
    connection = connect_for_workspace(context.workspace, "model-metadata")
    try:
        connection.read_parquet(str(artifact_path(dataset))).create_view("training")
        row = connection.execute("""
            SELECT count(*), count(DISTINCT (source, source_subject_id)),
                min(occurred_at)::VARCHAR, max(occurred_at)::VARCHAR
            FROM training WHERE split='train' AND training_eligible
        """).fetchone()
        assert row is not None
    finally:
        connection.close()
    return {
        "started_at": started_at,
        "completed_at": completed_at,
        # 先取 ISO 文本，避免 DuckDB 的 Python TIMESTAMPTZ 转换额外依赖 pytz。
        "data_start": datetime.fromisoformat(row[2]).astimezone(UTC).isoformat()
        if row[2]
        else None,
        "data_end": datetime.fromisoformat(row[3]).astimezone(UTC).isoformat() if row[3] else None,
        "interactions": row[0],
        "subjects": row[1],
        "parameters": {
            key: parameters[key]
            for key in PARAMETERS
            if isinstance(parameters.get(key), (int, float))
        },
    }


def timestamp() -> str:
    """所有新时间戳使用带时区的 UTC，展示层自行转换当地时间。"""
    return datetime.now(UTC).isoformat()


def git_revision() -> str:
    """记录源码提交；安装包不含 Git 元数据时明确返回 unknown。"""
    result = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=Path(__file__).resolve().parents[2],
        capture_output=True,
        text=True,
        check=False,
        timeout=5,
    )
    return result.stdout.strip() if result.returncode == 0 else "unknown"
