"""GH Archive BigQuery `WatchEvent` 数据源。

Connector 按 UTC 日逐分区执行查询。每个分区必须先 dry run，且实际任务继续携带
`maximum_bytes_billed`，避免一个配置错误扫描无法控制的历史范围。
"""

from __future__ import annotations

import json
import logging
import os
import tempfile
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from functools import partial
from pathlib import Path
from typing import Any, Protocol, TypeVar

import pyarrow as pa
import pyarrow.parquet as pq
from google.api_core import exceptions as google_exceptions
from google.cloud import bigquery

from starcat_recsys.artifacts import artifact_for, sha256_file, write_json_atomic
from starcat_recsys.interfaces import Artifact, TaskContext

LOGGER = logging.getLogger(__name__)

_EXPECTED_COLUMNS = ("source_record_id", "actor_id", "repo_id", "created_at")
_T = TypeVar("_T")
_RETRYABLE_GOOGLE_ERRORS = (
    google_exceptions.TooManyRequests,
    google_exceptions.InternalServerError,
    google_exceptions.BadGateway,
    google_exceptions.ServiceUnavailable,
    google_exceptions.GatewayTimeout,
    google_exceptions.DeadlineExceeded,
)

GH_ARCHIVE_QUERY_TEMPLATE = """
SELECT
  CAST(id AS STRING) AS source_record_id,
  CAST(actor.id AS STRING) AS actor_id,
  CAST(repo.id AS INT64) AS repo_id,
  created_at
FROM `githubarchive.day.{table_suffix}`
WHERE type = 'WatchEvent'
  AND actor.id IS NOT NULL
  AND repo.id IS NOT NULL
""".strip()


@dataclass(frozen=True, slots=True)
class BigQueryConfig:
    """一次有明确预算和日期范围的 GH Archive 采集配置。"""

    billing_project: str
    start_date: date
    end_date: date
    maximum_bytes_billed_per_day: int
    location: str = "US"
    maximum_total_bytes_billed: int | None = None
    retry_attempts: int = 5
    retry_initial_delay_seconds: float = 1.0

    def __post_init__(self) -> None:
        if not self.billing_project:
            raise ValueError("billing_project 不能为空")
        if self.end_date < self.start_date:
            raise ValueError("end_date 不能早于 start_date")
        if self.maximum_bytes_billed_per_day <= 0:
            raise ValueError("maximum_bytes_billed_per_day 必须大于 0")
        if self.maximum_total_bytes_billed is not None and self.maximum_total_bytes_billed <= 0:
            raise ValueError("maximum_total_bytes_billed 必须大于 0")
        if self.retry_attempts <= 0:
            raise ValueError("retry_attempts 必须大于 0")
        if self.retry_initial_delay_seconds < 0:
            raise ValueError("retry_initial_delay_seconds 不能小于 0")


class BigQueryGateway(Protocol):
    """隔离 Google SDK，单测可以证明预算门禁而不访问网络。"""

    def estimate_bytes(self, table_suffix: str) -> int: ...

    def fetch_watch_events(self, table_suffix: str, maximum_bytes_billed: int) -> pa.Table: ...


class GoogleBigQueryGateway:
    """基于 Application Default Credentials 的生产 Gateway。"""

    def __init__(self, config: BigQueryConfig) -> None:
        self._config = config
        self._client = bigquery.Client(project=config.billing_project, location=config.location)

    def estimate_bytes(self, table_suffix: str) -> int:
        query = _gh_archive_query(table_suffix)
        job_config = self._job_config(dry_run=True)
        job = self._client.query(query, job_config=job_config)
        return int(job.total_bytes_processed or 0)

    def fetch_watch_events(self, table_suffix: str, maximum_bytes_billed: int) -> pa.Table:
        query = _gh_archive_query(table_suffix)
        job_config = self._job_config(
            dry_run=False,
            maximum_bytes_billed=maximum_bytes_billed,
        )
        result = self._client.query(query, job_config=job_config).result()
        # 禁用 BigQuery Storage API，避免第一版额外要求该权限；查询已经按日和列裁剪。
        return result.to_arrow(create_bqstorage_client=False)

    def _job_config(
        self,
        *,
        dry_run: bool,
        maximum_bytes_billed: int | None = None,
    ) -> bigquery.QueryJobConfig:
        job_config = bigquery.QueryJobConfig(
            dry_run=dry_run,
            use_query_cache=False,
        )
        # SDK 3.43 会把构造参数 None 序列化成字符串 "None"，BigQuery API 会以
        # TYPE_INT64 错误拒绝任务；dry run 不需要该字段，因此只在有值时设置。
        if maximum_bytes_billed is not None:
            job_config.maximum_bytes_billed = maximum_bytes_billed
        return job_config


class BigQueryWatchEventSource:
    """把 GH Archive WatchEvent 分区写入可断点续传的 raw Parquet。"""

    name = "gharchive_bigquery"

    def __init__(
        self,
        config: BigQueryConfig,
        gateway: BigQueryGateway | None = None,
    ) -> None:
        self._config = config
        self._gateway = gateway or GoogleBigQueryGateway(config)

    def collect(self, context: TaskContext) -> list[Artifact]:
        output_directory = context.workspace / "raw" / "gh_archive"
        output_directory.mkdir(parents=True, exist_ok=True)
        state_path = output_directory / "download-state.json"
        state_scope = self._state_scope(context)
        completed = _load_download_state(state_path, state_scope)
        artifacts: list[Artifact] = []
        partitions: list[dict[str, Any]] = []
        estimated_total_bytes = 0
        dates = _date_range(self._config.start_date, self._config.end_date)

        for index, current_date in enumerate(dates, start=1):
            suffix = current_date.strftime("%Y%m%d")
            path = output_directory / f"watch-events-{suffix}.parquet"
            saved = completed.get(suffix)
            if saved is not None and _partition_matches_state(path, saved):
                artifact = _artifact_from_state(path, saved)
                estimated_total_bytes += int(artifact.metadata["estimated_bytes"])
                self._enforce_total_budget(estimated_total_bytes)
                artifacts.append(artifact)
                partitions.append(
                    {"partition": suffix, **artifact.metadata, "checksum": artifact.checksum}
                )
                if index == 1 or index == len(dates) or index % 100 == 0:
                    LOGGER.info("[%s/%s] 已校验并跳过分区 %s", index, len(dates), suffix)
                continue
            if saved is not None:
                LOGGER.warning("分区 %s 与 checkpoint 不一致，将重新下载", suffix)

            estimated_bytes = int(
                self._with_retry(
                    f"dry run {suffix}",
                    partial(self._gateway.estimate_bytes, suffix),
                )
            )
            if estimated_bytes > self._config.maximum_bytes_billed_per_day:
                raise RuntimeError(
                    f"BigQuery 分区 {suffix} 预计扫描 {estimated_bytes} 字节，"
                    f"超过上限 {self._config.maximum_bytes_billed_per_day}"
                )
            self._enforce_total_budget(estimated_total_bytes + estimated_bytes)

            # 文件成功替换、checkpoint 尚未来得及写入时，下次运行可以收养这个孤立分区；
            # 若已有 checkpoint 却校验失败，则必须重新查询，不能把被篡改文件重新登记为正确产物。
            if saved is None and _partition_is_readable(path):
                row_count = pq.read_metadata(path).num_rows
                artifact = artifact_for(
                    path,
                    "raw_interactions",
                    _partition_metadata(suffix, row_count, estimated_bytes),
                )
                LOGGER.info("[%s/%s] 已收养完整分区 %s", index, len(dates), suffix)
            else:
                table = self._with_retry(
                    f"download {suffix}",
                    partial(
                        self._gateway.fetch_watch_events,
                        suffix,
                        self._config.maximum_bytes_billed_per_day,
                    ),
                )
                if not isinstance(table, pa.Table):
                    raise TypeError("BigQuery Gateway 必须返回 pyarrow.Table")
                _validate_table_columns(table.column_names)
                _write_parquet_atomic(table, path)
                artifact = artifact_for(
                    path,
                    "raw_interactions",
                    _partition_metadata(suffix, table.num_rows, estimated_bytes),
                )
                LOGGER.info(
                    "[%s/%s] 已下载分区 %s：%s 行，扫描 %s 字节",
                    index,
                    len(dates),
                    suffix,
                    table.num_rows,
                    estimated_bytes,
                )

            estimated_total_bytes += estimated_bytes
            artifacts.append(artifact)
            partitions.append(
                {"partition": suffix, **artifact.metadata, "checksum": artifact.checksum}
            )
            completed[suffix] = {
                "checksum": artifact.checksum,
                "kind": artifact.kind,
                "metadata": artifact.metadata,
            }
            _write_download_state(state_path, state_scope, completed, estimated_total_bytes)

        write_json_atomic(
            output_directory / "manifest.json",
            {
                "schema_version": 1,
                "source": self.name,
                "run_id": context.run_id,
                "start_date": self._config.start_date.isoformat(),
                "end_date": self._config.end_date.isoformat(),
                "maximum_bytes_billed_per_day": self._config.maximum_bytes_billed_per_day,
                "maximum_total_bytes_billed": self._config.maximum_total_bytes_billed,
                "estimated_total_bytes": estimated_total_bytes,
                "partitions": partitions,
            },
        )
        return artifacts

    def _state_scope(self, context: TaskContext) -> dict[str, Any]:
        """固定 checkpoint 的数据语义，避免换日期或预算后误复用旧结果。"""

        return {
            "run_id": context.run_id,
            "start_date": self._config.start_date.isoformat(),
            "end_date": self._config.end_date.isoformat(),
            "maximum_bytes_billed_per_day": self._config.maximum_bytes_billed_per_day,
            "maximum_total_bytes_billed": self._config.maximum_total_bytes_billed,
            "location": self._config.location,
        }

    def _enforce_total_budget(self, estimated_total_bytes: int) -> None:
        maximum = self._config.maximum_total_bytes_billed
        if maximum is not None and estimated_total_bytes > maximum:
            raise RuntimeError(
                f"BigQuery WatchEvent 累计预计扫描 {estimated_total_bytes} 字节，"
                f"超过总上限 {maximum}"
            )

    def _with_retry(self, operation: str, callback: Callable[[], _T]) -> _T:
        """只重试 Google 明确标识的瞬时错误，权限和预算错误立即暴露。"""

        for attempt in range(1, self._config.retry_attempts + 1):
            try:
                return callback()
            except _RETRYABLE_GOOGLE_ERRORS as error:
                if attempt == self._config.retry_attempts:
                    raise
                delay = self._config.retry_initial_delay_seconds * (2 ** (attempt - 1))
                LOGGER.warning(
                    "%s 遇到瞬时错误 %s；%.1f 秒后进行第 %s/%s 次尝试",
                    operation,
                    type(error).__name__,
                    delay,
                    attempt + 1,
                    self._config.retry_attempts,
                )
                time.sleep(delay)
        raise AssertionError("retry loop 不应运行到这里")


def _partition_metadata(suffix: str, row_count: int, estimated_bytes: int) -> dict[str, Any]:
    return {
        "source": "gh_archive",
        "partition": suffix,
        "row_count": row_count,
        "estimated_bytes": estimated_bytes,
    }


def _load_download_state(path: Path, expected_scope: dict[str, Any]) -> dict[str, dict[str, Any]]:
    """读取严格绑定当前任务范围的 checkpoint；损坏状态不能静默忽略。"""

    if not path.exists():
        return {}
    try:
        document = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise RuntimeError(f"无法读取 BigQuery 下载状态：{path}") from error
    if document.get("schema_version") != 1 or document.get("scope") != expected_scope:
        raise RuntimeError("BigQuery 下载状态与当前任务范围不一致")
    completed = document.get("completed_partitions")
    if not isinstance(completed, dict):
        raise RuntimeError("BigQuery 下载状态缺少 completed_partitions")
    return {str(key): value for key, value in completed.items() if isinstance(value, dict)}


def _write_download_state(
    path: Path,
    scope: dict[str, Any],
    completed: dict[str, dict[str, Any]],
    estimated_total_bytes: int,
) -> None:
    write_json_atomic(
        path,
        {
            "schema_version": 1,
            "scope": scope,
            "estimated_total_bytes": estimated_total_bytes,
            "completed_partitions": completed,
        },
    )


def _artifact_from_state(path: Path, saved: dict[str, Any]) -> Artifact:
    metadata = saved.get("metadata")
    checksum = saved.get("checksum")
    kind = saved.get("kind")
    if not isinstance(metadata, dict) or not isinstance(checksum, str) or not isinstance(kind, str):
        raise RuntimeError(f"BigQuery 分区状态字段不完整：{path.name}")
    return Artifact(kind=kind, uri=path.resolve().as_uri(), checksum=checksum, metadata=metadata)


def _partition_matches_state(path: Path, saved: dict[str, Any]) -> bool:
    checksum = saved.get("checksum")
    if not isinstance(checksum, str) or not _partition_is_readable(path):
        return False
    return sha256_file(path) == checksum


def _partition_is_readable(path: Path) -> bool:
    if not path.is_file():
        return False
    try:
        schema = pq.read_schema(path)
        _validate_table_columns(schema.names)
        pq.read_metadata(path)
    except (OSError, pa.ArrowException, ValueError):
        return False
    return True


def _validate_table_columns(columns: list[str]) -> None:
    if tuple(columns) != _EXPECTED_COLUMNS:
        raise ValueError(
            f"WatchEvent Parquet 字段不匹配：期望 {_EXPECTED_COLUMNS}，实际 {tuple(columns)}"
        )


def _write_parquet_atomic(table: pa.Table, path: Path) -> None:
    """在同目录完成 Parquet 后再替换目标，进程中断不会留下半个正式分区。"""

    descriptor, temporary_name = tempfile.mkstemp(
        dir=path.parent,
        prefix=f".{path.name}.",
        suffix=".tmp",
    )
    os.close(descriptor)
    temporary = Path(temporary_name)
    try:
        pq.write_table(table, temporary, compression="zstd")
        temporary.replace(path)
    except BaseException:
        temporary.unlink(missing_ok=True)
        raise


def _date_range(start: date, end: date) -> list[date]:
    days = (end - start).days
    return [start + timedelta(days=offset) for offset in range(days + 1)]


def _gh_archive_query(table_suffix: str) -> str:
    """构造只指向一个 GH Archive 日表的查询。

    `githubarchive.day` 同时包含 `yesterday` View，不能使用 `day.*` 通配符。
    表名又不支持 BigQuery 参数绑定，因此必须先完成严格日期校验，再插入 SQL。
    """

    if len(table_suffix) != 8 or not table_suffix.isascii() or not table_suffix.isdigit():
        raise ValueError("GH Archive 表后缀必须是有效的 YYYYMMDD 日期")
    try:
        datetime.strptime(table_suffix, "%Y%m%d")
    except ValueError as error:
        raise ValueError("GH Archive 表后缀必须是有效的 YYYYMMDD 日期") from error
    return GH_ARCHIVE_QUERY_TEMPLATE.format(table_suffix=table_suffix)
