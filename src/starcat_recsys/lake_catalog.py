"""只读检查既有 BigQuery Raw 数据并生成可登记的 Catalog 快照。

该模块只读取下载器已经写入的 checkpoint 与 Parquet，不移动、复制或改写 Raw。
输出只包含逻辑 URI 和公开统计，避免把管理员用户名、磁盘名称或绝对路径带入 Web。
"""

from __future__ import annotations

import json
import shutil
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from starcat_recsys.artifacts import sha256_file
from starcat_recsys.sources.bigquery import BigQueryWatchEventSource
from starcat_recsys.sources.bigquery_push import BigQueryPushEventSource


@dataclass(frozen=True)
class DatasetInspectionSpec:
    """描述一个既有下载目录如何映射为稳定 Dataset 身份。"""

    event: str
    dataset_id: str
    display_name: str
    output_directory_name: str
    file_prefix: str
    logical_uri: str
    expected_columns: tuple[str, ...]


_SPECS = {
    "watch-events": DatasetInspectionSpec(
        event="WatchEvent",
        dataset_id="githubarchive_watch_event",
        display_name="GH Archive WatchEvent",
        output_directory_name=BigQueryWatchEventSource.output_directory_name,
        file_prefix=BigQueryWatchEventSource.file_prefix,
        logical_uri="lake://raw/bigquery/githubarchive_watch_event/schema=v1",
        expected_columns=BigQueryWatchEventSource.expected_columns,
    ),
    "push-events": DatasetInspectionSpec(
        event="PushEvent",
        dataset_id="githubarchive_push_event",
        display_name="GH Archive PushEvent",
        output_directory_name=BigQueryPushEventSource.output_directory_name,
        file_prefix=BigQueryPushEventSource.file_prefix,
        logical_uri="lake://raw/bigquery/githubarchive_push_event/schema=v1",
        expected_columns=BigQueryPushEventSource.expected_columns,
    ),
}


def inspect_bigquery_dataset(workspace: Path, event: str) -> dict[str, Any]:
    """校验 checkpoint 中的全部分区并返回不含真实路径的 Catalog 快照。

    checkpoint 是下载器提交分区的事实入口，但登记时仍重新检查 Parquet footer、
    行数和 SHA-256，避免把丢失或被修改的文件标记为 ready。
    """

    spec = _SPECS.get(event)
    if spec is None:
        raise ValueError(f"不支持的数据集事件：{event}")
    workspace = workspace.resolve()
    raw_directory = workspace / "raw" / spec.output_directory_name
    state = _read_checkpoint(raw_directory / "download-state.json")
    scope = _read_scope(state)
    completed = _read_completed_partitions(state)
    partitions: list[dict[str, Any]] = []

    for current_date in _date_range(scope["start_date"], scope["end_date"]):
        suffix = current_date.strftime("%Y%m%d")
        saved = completed.get(suffix)
        logical_uri = f"{spec.logical_uri}/event_date={current_date.isoformat()}"
        if saved is None:
            partitions.append(
                _partition_document(
                    suffix=suffix,
                    current_date=current_date,
                    logical_uri=logical_uri,
                    state="missing",
                    error_code="PARTITION_NOT_DOWNLOADED",
                )
            )
            continue
        path = raw_directory / f"{spec.file_prefix}-{suffix}.parquet"
        partitions.append(
            _inspect_partition(
                path,
                saved,
                spec.expected_columns,
                suffix,
                current_date,
                logical_uri,
            )
        )

    ready = sum(item["state"] == "ready" for item in partitions)
    failed = sum(item["state"] == "failed" for item in partitions)
    missing = sum(item["state"] == "missing" for item in partitions)
    total_rows = sum(int(item["row_count"] or 0) for item in partitions)
    total_bytes = sum(int(item["file_size_bytes"] or 0) for item in partitions)
    estimated_bytes = sum(int(item["estimated_bytes"] or 0) for item in partitions)
    tracked_files = {
        f"{spec.file_prefix}-{suffix}.parquet" for suffix in completed if suffix.isdigit()
    }
    existing_files = {path.name for path in raw_directory.glob(f"{spec.file_prefix}-*.parquet")}
    watermark = _contiguous_ready_watermark(partitions)
    observed_at = datetime.now(UTC).isoformat()
    disk = shutil.disk_usage(workspace)

    return {
        "schema_version": 1,
        "dataset": {
            "dataset_id": spec.dataset_id,
            "schema_version": 1,
            "display_name": spec.display_name,
            "source": "bigquery",
            "partition_key": "event_date",
            "logical_uri": spec.logical_uri,
            "state": "degraded" if failed else "ready" if missing == 0 else "partial",
            "start_date": scope["start_date"].isoformat(),
            "end_date": scope["end_date"].isoformat(),
            "ready_partitions": ready,
            "failed_partitions": failed,
            "missing_partitions": missing,
            "total_partitions": len(partitions),
            "total_rows": total_rows,
            "total_bytes": total_bytes,
            "estimated_total_bytes": estimated_bytes,
            "watermark": watermark,
            "untracked_file_count": len(existing_files - tracked_files),
            "observed_at": observed_at,
        },
        "partitions": partitions,
        "storage": {
            "storage_id": "primary-data-volume",
            "logical_uri": "storage://primary-data-volume",
            "capacity_bytes": disk.total,
            "used_bytes": disk.used,
            "available_bytes": disk.free,
            "dataset_bytes": total_bytes,
            "observed_at": observed_at,
        },
    }


def _read_checkpoint(path: Path) -> dict[str, Any]:
    try:
        document = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as error:
        raise RuntimeError("BigQuery 下载 checkpoint 不存在") from error
    except (OSError, json.JSONDecodeError) as error:
        raise RuntimeError("BigQuery 下载 checkpoint 无法读取") from error
    if not isinstance(document, dict) or document.get("schema_version") != 1:
        raise RuntimeError("BigQuery 下载 checkpoint schema 不受支持")
    return document


def _read_scope(document: dict[str, Any]) -> dict[str, date]:
    scope = document.get("scope")
    if not isinstance(scope, dict):
        raise RuntimeError("BigQuery 下载 checkpoint 缺少 scope")
    try:
        start = date.fromisoformat(str(scope["start_date"]))
        end = date.fromisoformat(str(scope["end_date"]))
    except (KeyError, ValueError) as error:
        raise RuntimeError("BigQuery 下载 checkpoint 日期范围无效") from error
    if end < start:
        raise RuntimeError("BigQuery 下载 checkpoint 日期范围倒置")
    return {"start_date": start, "end_date": end}


def _read_completed_partitions(document: dict[str, Any]) -> dict[str, dict[str, Any]]:
    completed = document.get("completed_partitions")
    if not isinstance(completed, dict):
        raise RuntimeError("BigQuery 下载 checkpoint 缺少 completed_partitions")
    return {str(key): value for key, value in completed.items() if isinstance(value, dict)}


def _inspect_partition(
    path: Path,
    saved: dict[str, Any],
    expected_columns: tuple[str, ...],
    suffix: str,
    current_date: date,
    logical_uri: str,
) -> dict[str, Any]:
    metadata = saved.get("metadata")
    checksum = saved.get("checksum")
    if not isinstance(metadata, dict) or not isinstance(checksum, str):
        return _partition_document(
            suffix=suffix,
            current_date=current_date,
            logical_uri=logical_uri,
            state="failed",
            error_code="CHECKPOINT_METADATA_INVALID",
        )
    if not path.is_file():
        return _partition_document(
            suffix=suffix,
            current_date=current_date,
            logical_uri=logical_uri,
            state="failed",
            checksum=checksum,
            error_code="PARTITION_FILE_MISSING",
            metadata=metadata,
        )
    try:
        parquet_metadata = pq.read_metadata(path)
        schema = pq.read_schema(path)
        file_size = path.stat().st_size
    except (OSError, pa.ArrowException):
        return _partition_document(
            suffix=suffix,
            current_date=current_date,
            logical_uri=logical_uri,
            state="failed",
            checksum=checksum,
            error_code="PARQUET_INVALID",
            metadata=metadata,
        )
    if tuple(schema.names) != expected_columns:
        error_code = "PARQUET_SCHEMA_MISMATCH"
    elif int(metadata.get("row_count", -1)) != parquet_metadata.num_rows:
        error_code = "PARQUET_ROW_COUNT_MISMATCH"
    elif sha256_file(path) != checksum:
        error_code = "PARTITION_CHECKSUM_MISMATCH"
    else:
        error_code = None
    return _partition_document(
        suffix=suffix,
        current_date=current_date,
        logical_uri=logical_uri,
        state="failed" if error_code else "ready",
        checksum=checksum,
        error_code=error_code,
        metadata=metadata,
        file_size=file_size,
    )


def _partition_document(
    *,
    suffix: str,
    current_date: date,
    logical_uri: str,
    state: str,
    checksum: str | None = None,
    error_code: str | None = None,
    metadata: dict[str, Any] | None = None,
    file_size: int | None = None,
) -> dict[str, Any]:
    metadata = metadata or {}
    return {
        "partition_key": "event_date",
        "partition_value": current_date.isoformat(),
        "source_partition": suffix,
        "state": state,
        "validation_state": "valid" if state == "ready" else state,
        "logical_uri": logical_uri,
        "checksum": checksum,
        "row_count": _optional_nonnegative_int(metadata.get("row_count")),
        "file_size_bytes": file_size,
        "estimated_bytes": _optional_nonnegative_int(metadata.get("estimated_bytes")),
        "error_code": error_code,
    }


def _optional_nonnegative_int(value: object) -> int | None:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        return None
    return value


def _contiguous_ready_watermark(partitions: list[dict[str, Any]]) -> str | None:
    watermark: str | None = None
    for partition in partitions:
        if partition["state"] != "ready":
            break
        watermark = str(partition["partition_value"])
    return watermark


def _date_range(start: date, end: date) -> list[date]:
    return [start + timedelta(days=offset) for offset in range((end - start).days + 1)]
