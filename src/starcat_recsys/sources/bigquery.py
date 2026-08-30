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
from requests import exceptions as requests_exceptions

from starcat_recsys.artifacts import artifact_for, sha256_file, write_json_atomic
from starcat_recsys.interfaces import Artifact, TaskContext

LOGGER = logging.getLogger(__name__)

_WATCH_EVENT_COLUMNS = ("source_record_id", "actor_id", "repo_id", "created_at")
_T = TypeVar("_T")
_RETRYABLE_ERRORS = (
    google_exceptions.TooManyRequests,
    google_exceptions.InternalServerError,
    google_exceptions.BadGateway,
    google_exceptions.ServiceUnavailable,
    google_exceptions.GatewayTimeout,
    google_exceptions.DeadlineExceeded,
    # BigQuery 查询任务已经完成后，分页拉取结果仍可能遇到短暂 TLS EOF、连接重置
    # 或读取超时。这些错误不代表查询参数或权限有问题，应沿用同一有界退避策略。
    requests_exceptions.ConnectionError,
    requests_exceptions.Timeout,
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
    """一次有明确预算和日期范围的 GH Archive 日事件采集配置。"""

    billing_project: str
    start_date: date
    end_date: date
    maximum_bytes_billed_per_day: int
    location: str = "US"
    maximum_total_bytes_billed: int | None = None
    retry_attempts: int = 5
    retry_initial_delay_seconds: float = 1.0
    append_existing: bool = False

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


class BigQueryDailyEventGateway(Protocol):
    """隔离 Google SDK，单测可以证明按日采集和预算门禁而不访问网络。"""

    def estimate_bytes(self, table_suffix: str) -> int: ...

    def fetch_events(self, table_suffix: str, maximum_bytes_billed: int) -> pa.Table: ...


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

    def fetch_events(self, table_suffix: str, maximum_bytes_billed: int) -> pa.Table:
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


class BigQueryDailyEventSource:
    """按模板方法把一种 GH Archive 日事件写入可断点续传的 raw Parquet。"""

    name = ""
    event_label = ""
    output_directory_name = ""
    file_prefix = ""
    artifact_kind = ""
    metadata_source = ""
    expected_columns: tuple[str, ...] = ()

    def __init__(
        self,
        config: BigQueryConfig,
        gateway: BigQueryDailyEventGateway,
    ) -> None:
        self._config = config
        self._gateway = gateway

    def collect(self, context: TaskContext) -> list[Artifact]:
        output_directory = context.workspace / "raw" / self.output_directory_name
        output_directory.mkdir(parents=True, exist_ok=True)
        state_path = output_directory / "download-state.json"
        requested_scope = self._state_scope(context)
        completed, stored_estimated_bytes, state_scope = _load_download_state(
            state_path,
            requested_scope,
            append_existing=self._config.append_existing,
        )
        artifacts: list[Artifact] = []
        partitions: list[dict[str, Any]] = []
        estimated_total_bytes = stored_estimated_bytes if self._config.append_existing else 0
        dates = _date_range(self._config.start_date, self._config.end_date)

        for index, current_date in enumerate(dates, start=1):
            suffix = current_date.strftime("%Y%m%d")
            path = output_directory / f"{self.file_prefix}-{suffix}.parquet"
            saved = completed.get(suffix)
            if saved is not None and _partition_matches_state(
                path,
                saved,
                self.expected_columns,
                self.event_label,
            ):
                artifact = _artifact_from_state(path, saved)
                if not self._config.append_existing:
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

            estimated_bytes, is_missing_partition = self._estimate_partition(suffix)
            if is_missing_partition:
                # GH Archive 历史数据存在少量缺失日表。空 Parquet 仍作为已完成分区进入
                # checkpoint，避免每次恢复时重复请求同一个确定不存在的历史日期。
                table = self._empty_table()
                _write_parquet_atomic(table, path)
                artifact = artifact_for(
                    path,
                    self.artifact_kind,
                    _partition_metadata(suffix, 0, 0, self.metadata_source),
                )
                LOGGER.warning(
                    "[%s/%s] GH Archive 日表 %s 不存在，已记录为空 %s 分区",
                    index,
                    len(dates),
                    suffix,
                    self.event_label,
                )
            else:
                if estimated_bytes > self._config.maximum_bytes_billed_per_day:
                    raise RuntimeError(
                        f"BigQuery 分区 {suffix} 预计扫描 {estimated_bytes} 字节，"
                        f"超过上限 {self._config.maximum_bytes_billed_per_day}"
                    )
                self._enforce_total_budget(estimated_total_bytes + estimated_bytes)

                # 文件成功替换、checkpoint 尚未来得及写入时，下次运行可以收养这个孤立分区；
                # 若已有 checkpoint 却校验失败，则必须重新查询，不能把被篡改文件重新登记为正确产物。
                if saved is None and _partition_is_readable(
                    path,
                    self.expected_columns,
                    self.event_label,
                ):
                    row_count = pq.read_metadata(path).num_rows
                    artifact = artifact_for(
                        path,
                        self.artifact_kind,
                        _partition_metadata(
                            suffix,
                            row_count,
                            estimated_bytes,
                            self.metadata_source,
                        ),
                    )
                    LOGGER.info("[%s/%s] 已收养完整分区 %s", index, len(dates), suffix)
                else:
                    table = self._with_retry(
                        f"download {suffix}",
                        partial(
                            self._gateway.fetch_events,
                            suffix,
                            self._config.maximum_bytes_billed_per_day,
                        ),
                    )
                    if not isinstance(table, pa.Table):
                        raise TypeError("BigQuery Gateway 必须返回 pyarrow.Table")
                    _validate_table_columns(
                        table.column_names,
                        self.expected_columns,
                        self.event_label,
                    )
                    _write_parquet_atomic(table, path)
                    artifact = artifact_for(
                        path,
                        self.artifact_kind,
                        _partition_metadata(
                            suffix,
                            table.num_rows,
                            estimated_bytes,
                            self.metadata_source,
                        ),
                    )
                    LOGGER.info(
                        "[%s/%s] 已下载 %s 分区 %s：%s 行，扫描 %s 字节",
                        index,
                        len(dates),
                        self.event_label,
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

        manifest_partitions = partitions
        if self._config.append_existing:
            # append 只校验请求日期，不应为了重写 manifest 再读取十年 Raw；历史分区直接
            # 使用已经过校验的 checkpoint 元数据重建目录。
            manifest_partitions = [
                {
                    "partition": suffix,
                    **saved["metadata"],
                    "checksum": saved["checksum"],
                }
                for suffix, saved in sorted(completed.items())
            ]
        write_json_atomic(
            output_directory / "manifest.json",
            {
                "schema_version": 1,
                "source": self.name,
                "run_id": context.run_id,
                "start_date": state_scope["start_date"],
                "end_date": state_scope["end_date"],
                "maximum_bytes_billed_per_day": self._config.maximum_bytes_billed_per_day,
                "maximum_total_bytes_billed": self._config.maximum_total_bytes_billed,
                "estimated_total_bytes": estimated_total_bytes,
                "partitions": manifest_partitions,
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
                f"BigQuery {self.event_label} 累计预计扫描 {estimated_total_bytes} 字节，"
                f"超过总上限 {maximum}"
            )

    def _estimate_partition(self, suffix: str) -> tuple[int, bool]:
        """返回分区预计扫描量，并只把明确缺失的 GH Archive 日表视为空分区。"""

        try:
            estimated_bytes = int(
                self._with_retry(
                    f"dry run {suffix}",
                    partial(self._gateway.estimate_bytes, suffix),
                )
            )
        except google_exceptions.NotFound as error:
            # NotFound 也可能表示 billing project 配置错误。只有错误正文明确指向当前
            # GH Archive 日表时才能跳过，其他 404 必须继续暴露，避免静默生成全空数据。
            if not _is_missing_day_table(error, suffix):
                raise
            return 0, True
        return estimated_bytes, False

    def _empty_table(self) -> pa.Table:
        raise NotImplementedError

    def _with_retry(self, operation: str, callback: Callable[[], _T]) -> _T:
        """只重试 Google 明确标识的瞬时错误，权限和预算错误立即暴露。"""

        for attempt in range(1, self._config.retry_attempts + 1):
            try:
                return callback()
            except _RETRYABLE_ERRORS as error:
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


class BigQueryWatchEventSource(BigQueryDailyEventSource):
    """把 GH Archive WatchEvent 分区写入可断点续传的 raw Parquet。"""

    name = "gharchive_bigquery"
    event_label = "WatchEvent"
    output_directory_name = "gh_archive"
    file_prefix = "watch-events"
    artifact_kind = "raw_interactions"
    metadata_source = "gh_archive"
    expected_columns = _WATCH_EVENT_COLUMNS

    def __init__(
        self,
        config: BigQueryConfig,
        gateway: BigQueryDailyEventGateway | None = None,
    ) -> None:
        super().__init__(config, gateway or GoogleBigQueryGateway(config))

    def _empty_table(self) -> pa.Table:
        return _empty_watch_event_table()


def _partition_metadata(
    suffix: str,
    row_count: int,
    estimated_bytes: int,
    source: str,
) -> dict[str, Any]:
    return {
        "source": source,
        "partition": suffix,
        "row_count": row_count,
        "estimated_bytes": estimated_bytes,
    }


def _empty_watch_event_table() -> pa.Table:
    """创建与 BigQuery WatchEvent 查询结果一致的零行 Arrow 表。"""

    return pa.table(
        {
            "source_record_id": pa.array([], type=pa.string()),
            "actor_id": pa.array([], type=pa.string()),
            "repo_id": pa.array([], type=pa.int64()),
            "created_at": pa.array([], type=pa.timestamp("us", tz="UTC")),
        }
    )


def _is_missing_day_table(error: google_exceptions.NotFound, suffix: str) -> bool:
    expected_table = f"Table githubarchive:day.{suffix} was not found"
    return expected_table in str(error)


def _load_download_state(
    path: Path,
    expected_scope: dict[str, Any],
    *,
    append_existing: bool = False,
) -> tuple[dict[str, dict[str, Any]], int, dict[str, Any]]:
    """读取 checkpoint；append 只允许连续向后扩展日期，其他任务语义必须一致。"""

    if not path.exists():
        return {}, 0, expected_scope
    try:
        document = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise RuntimeError(f"无法读取 BigQuery 下载状态：{path}") from error
    stored_scope = document.get("scope")
    if document.get("schema_version") != 1 or not isinstance(stored_scope, dict):
        raise RuntimeError("BigQuery 下载状态与当前任务范围不一致")
    effective_scope = expected_scope
    if append_existing:
        stable_keys = {
            "run_id",
            "maximum_bytes_billed_per_day",
            "maximum_total_bytes_billed",
            "location",
        }
        if any(stored_scope.get(key) != expected_scope.get(key) for key in stable_keys):
            raise RuntimeError("BigQuery 下载状态与当前任务范围不一致")
        try:
            stored_start = date.fromisoformat(str(stored_scope["start_date"]))
            stored_end = date.fromisoformat(str(stored_scope["end_date"]))
            requested_start = date.fromisoformat(str(expected_scope["start_date"]))
            requested_end = date.fromisoformat(str(expected_scope["end_date"]))
        except (KeyError, ValueError) as error:
            raise RuntimeError("BigQuery 下载状态日期范围无效") from error
        # 允许安全重放已覆盖日期或从当前水位的下一天继续；禁止跳日和向前扩展。
        if requested_start < stored_start or requested_start > stored_end + timedelta(days=1):
            raise RuntimeError("BigQuery append 日期必须已覆盖或紧邻当前水位")
        effective_scope = {
            **stored_scope,
            "end_date": max(stored_end, requested_end).isoformat(),
        }
    elif stored_scope != expected_scope:
        raise RuntimeError("BigQuery 下载状态与当前任务范围不一致")
    completed = document.get("completed_partitions")
    if not isinstance(completed, dict):
        raise RuntimeError("BigQuery 下载状态缺少 completed_partitions")
    try:
        estimated_total_bytes = int(document.get("estimated_total_bytes", 0))
    except (TypeError, ValueError) as error:
        raise RuntimeError("BigQuery 下载状态累计扫描量无效") from error
    return (
        {str(key): value for key, value in completed.items() if isinstance(value, dict)},
        estimated_total_bytes,
        effective_scope,
    )


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


def _partition_matches_state(
    path: Path,
    saved: dict[str, Any],
    expected_columns: tuple[str, ...],
    event_label: str,
) -> bool:
    checksum = saved.get("checksum")
    if not isinstance(checksum, str) or not _partition_is_readable(
        path,
        expected_columns,
        event_label,
    ):
        return False
    return sha256_file(path) == checksum


def _partition_is_readable(
    path: Path,
    expected_columns: tuple[str, ...],
    event_label: str,
) -> bool:
    if not path.is_file():
        return False
    try:
        schema = pq.read_schema(path)
        _validate_table_columns(schema.names, expected_columns, event_label)
        pq.read_metadata(path)
    except (OSError, pa.ArrowException, ValueError):
        return False
    return True


def _validate_table_columns(
    columns: list[str],
    expected_columns: tuple[str, ...],
    event_label: str,
) -> None:
    if tuple(columns) != expected_columns:
        raise ValueError(
            f"{event_label} Parquet 字段不匹配：期望 {expected_columns}，实际 {tuple(columns)}"
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
