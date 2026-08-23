"""GH Archive BigQuery `WatchEvent` 数据源。

Connector 按 UTC 日逐分区执行查询。每个分区必须先 dry run，且实际任务继续携带
`maximum_bytes_billed`，避免一个配置错误扫描无法控制的历史范围。
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from typing import Any, Protocol

import pyarrow as pa
import pyarrow.parquet as pq
from google.cloud import bigquery

from starcat_recsys.artifacts import artifact_for, write_json_atomic
from starcat_recsys.interfaces import Artifact, TaskContext

GH_ARCHIVE_QUERY = """
SELECT
  CAST(id AS STRING) AS source_record_id,
  CAST(actor.id AS STRING) AS actor_id,
  CAST(repo.id AS INT64) AS repo_id,
  created_at
FROM `githubarchive.day.*`
WHERE _TABLE_SUFFIX = @table_suffix
  AND type = 'WatchEvent'
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

    def __post_init__(self) -> None:
        if not self.billing_project:
            raise ValueError("billing_project 不能为空")
        if self.end_date < self.start_date:
            raise ValueError("end_date 不能早于 start_date")
        if self.maximum_bytes_billed_per_day <= 0:
            raise ValueError("maximum_bytes_billed_per_day 必须大于 0")


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
        job_config = self._job_config(table_suffix, dry_run=True)
        job = self._client.query(GH_ARCHIVE_QUERY, job_config=job_config)
        return int(job.total_bytes_processed or 0)

    def fetch_watch_events(self, table_suffix: str, maximum_bytes_billed: int) -> pa.Table:
        job_config = self._job_config(
            table_suffix,
            dry_run=False,
            maximum_bytes_billed=maximum_bytes_billed,
        )
        result = self._client.query(GH_ARCHIVE_QUERY, job_config=job_config).result()
        # 禁用 BigQuery Storage API，避免第一版额外要求该权限；查询已经按日和列裁剪。
        return result.to_arrow(create_bqstorage_client=False)

    def _job_config(
        self,
        table_suffix: str,
        *,
        dry_run: bool,
        maximum_bytes_billed: int | None = None,
    ) -> bigquery.QueryJobConfig:
        return bigquery.QueryJobConfig(
            dry_run=dry_run,
            use_query_cache=False,
            maximum_bytes_billed=maximum_bytes_billed,
            query_parameters=[
                bigquery.ScalarQueryParameter("table_suffix", "STRING", table_suffix)
            ],
        )


class BigQueryWatchEventSource:
    """把 GH Archive WatchEvent 分区写入 raw Parquet。"""

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
        artifacts: list[Artifact] = []
        partitions: list[dict[str, Any]] = []

        for current_date in _date_range(self._config.start_date, self._config.end_date):
            suffix = current_date.strftime("%Y%m%d")
            estimated_bytes = self._gateway.estimate_bytes(suffix)
            if estimated_bytes > self._config.maximum_bytes_billed_per_day:
                raise RuntimeError(
                    f"BigQuery 分区 {suffix} 预计扫描 {estimated_bytes} 字节，"
                    f"超过上限 {self._config.maximum_bytes_billed_per_day}"
                )

            table = self._gateway.fetch_watch_events(
                suffix,
                self._config.maximum_bytes_billed_per_day,
            )
            path = output_directory / f"watch-events-{suffix}.parquet"
            pq.write_table(table, path, compression="zstd")
            artifact = artifact_for(
                path,
                "raw_interactions",
                {
                    "source": "gh_archive",
                    "partition": suffix,
                    "row_count": table.num_rows,
                    "estimated_bytes": estimated_bytes,
                },
            )
            artifacts.append(artifact)
            partitions.append(
                {"partition": suffix, **artifact.metadata, "checksum": artifact.checksum}
            )

        write_json_atomic(
            output_directory / "manifest.json",
            {
                "schema_version": 1,
                "source": self.name,
                "run_id": context.run_id,
                "start_date": self._config.start_date.isoformat(),
                "end_date": self._config.end_date.isoformat(),
                "maximum_bytes_billed_per_day": self._config.maximum_bytes_billed_per_day,
                "partitions": partitions,
            },
        )
        return artifacts


def _date_range(start: date, end: date) -> list[date]:
    days = (end - start).days
    return [start + timedelta(days=offset) for offset in range(days + 1)]
