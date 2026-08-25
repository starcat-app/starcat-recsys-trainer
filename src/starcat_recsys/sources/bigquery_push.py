"""GH Archive BigQuery ``PushEvent`` 仓库基础元数据源。

该 Connector 只负责一次性批量建立仓库目录，不把 Push 行为当成推荐互动信号。
查询先 dry run，再携带同一个 ``maximum_bytes_billed`` 执行，避免大范围年份配置
意外消耗超出预算。更丰富、权威的当前字段仍由 GitHub Repository API 补齐。
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

import pyarrow as pa
import pyarrow.parquet as pq
from google.cloud import bigquery

from starcat_recsys.artifacts import artifact_for, write_json_atomic
from starcat_recsys.interfaces import Artifact, TaskContext

PUSH_METADATA_QUERY_TEMPLATE = """
WITH push_events AS (
  SELECT
    CAST(repo.id AS INT64) AS repo_id,
    repo.name AS full_name,
    created_at
  FROM `githubarchive.year.*`
  WHERE _TABLE_SUFFIX BETWEEN '{start_year}' AND '{end_year}'
    AND type = 'PushEvent'
    AND repo.id IS NOT NULL
    AND repo.name IS NOT NULL
),
repository_timeline AS (
  SELECT
    repo_id,
    ARRAY_AGG(full_name ORDER BY created_at DESC, full_name DESC LIMIT 1)[OFFSET(0)]
      AS full_name,
    MIN(created_at) AS first_push_at,
    MAX(created_at) AS last_push_at,
    COUNT(*) AS push_event_count
  FROM push_events
  GROUP BY repo_id
  HAVING COUNT(*) >= {minimum_push_events}
)
SELECT
  repo_id,
  full_name,
  first_push_at,
  last_push_at,
  push_event_count,
  CURRENT_TIMESTAMP() AS fetched_at
FROM repository_timeline
ORDER BY repo_id
""".strip()


@dataclass(frozen=True, slots=True)
class BigQueryPushMetadataConfig:
    """一次有明确年份范围和总预算的 PushEvent 元数据采集配置。"""

    billing_project: str
    start_year: int
    end_year: int
    maximum_bytes_billed: int
    minimum_push_events: int = 1
    location: str = "US"

    def __post_init__(self) -> None:
        if not self.billing_project:
            raise ValueError("billing_project 不能为空")
        if not 2011 <= self.start_year <= 9999 or not 2011 <= self.end_year <= 9999:
            raise ValueError("年份必须是 2011 之后的四位数字")
        if self.end_year < self.start_year:
            raise ValueError("end_year 不能早于 start_year")
        if self.maximum_bytes_billed <= 0:
            raise ValueError("maximum_bytes_billed 必须大于 0")
        if self.minimum_push_events <= 0:
            raise ValueError("minimum_push_events 必须大于 0")


class BigQueryPushMetadataGateway(Protocol):
    """隔离 Google SDK，使查询和预算门禁可以在无网络单测中验证。"""

    def estimate_bytes(self) -> int: ...

    def fetch_repository_metadata(self, maximum_bytes_billed: int) -> pa.Table: ...


class GoogleBigQueryPushMetadataGateway:
    """使用 Application Default Credentials 执行 PushEvent 聚合查询。"""

    def __init__(self, config: BigQueryPushMetadataConfig) -> None:
        self._config = config
        self._client = bigquery.Client(project=config.billing_project, location=config.location)

    def estimate_bytes(self) -> int:
        job = self._client.query(self._query(), job_config=self._job_config(dry_run=True))
        return int(job.total_bytes_processed or 0)

    def fetch_repository_metadata(self, maximum_bytes_billed: int) -> pa.Table:
        result = self._client.query(
            self._query(),
            job_config=self._job_config(
                dry_run=False,
                maximum_bytes_billed=maximum_bytes_billed,
            ),
        ).result()
        # 数百万条聚合后的仓库行仍适合单机 Arrow 落地；禁用 Storage API 以免扩大权限面。
        return result.to_arrow(create_bqstorage_client=False)

    def _query(self) -> str:
        return _push_metadata_query(
            self._config.start_year,
            self._config.end_year,
            self._config.minimum_push_events,
        )

    @staticmethod
    def _job_config(
        *,
        dry_run: bool,
        maximum_bytes_billed: int | None = None,
    ) -> bigquery.QueryJobConfig:
        job_config = bigquery.QueryJobConfig(dry_run=dry_run, use_query_cache=False)
        # 与 WatchEvent Gateway 保持相同约束：dry run 不序列化空的预算字段。
        if maximum_bytes_billed is not None:
            job_config.maximum_bytes_billed = maximum_bytes_billed
        return job_config


class BigQueryPushEventMetadataSource:
    """把跨年份聚合后的仓库基础目录写入单个 raw Parquet。"""

    name = "gharchive_push_metadata"

    def __init__(
        self,
        config: BigQueryPushMetadataConfig,
        gateway: BigQueryPushMetadataGateway | None = None,
    ) -> None:
        self._config = config
        self._gateway = gateway or GoogleBigQueryPushMetadataGateway(config)

    def collect(self, context: TaskContext) -> list[Artifact]:
        output_directory = context.workspace / "raw" / "gh_archive_push"
        output_directory.mkdir(parents=True, exist_ok=True)

        estimated_bytes = self._gateway.estimate_bytes()
        if estimated_bytes > self._config.maximum_bytes_billed:
            raise RuntimeError(
                f"BigQuery PushEvent 元数据预计扫描 {estimated_bytes} 字节，"
                f"超过上限 {self._config.maximum_bytes_billed}"
            )

        table = self._gateway.fetch_repository_metadata(self._config.maximum_bytes_billed)
        path = output_directory / "repositories.parquet"
        pq.write_table(table, path, compression="zstd")
        artifact = artifact_for(
            path,
            "raw_repositories",
            {
                "source": "gh_archive_push",
                "start_year": self._config.start_year,
                "end_year": self._config.end_year,
                "minimum_push_events": self._config.minimum_push_events,
                "row_count": table.num_rows,
                "estimated_bytes": estimated_bytes,
            },
        )
        write_json_atomic(
            output_directory / "manifest.json",
            {
                "schema_version": 1,
                "source": self.name,
                "run_id": context.run_id,
                "start_year": self._config.start_year,
                "end_year": self._config.end_year,
                "minimum_push_events": self._config.minimum_push_events,
                "maximum_bytes_billed": self._config.maximum_bytes_billed,
                "artifact": {
                    "uri": artifact.uri,
                    "checksum": artifact.checksum,
                    "metadata": artifact.metadata,
                },
            },
        )
        return [artifact]


def _push_metadata_query(start_year: int, end_year: int, minimum_push_events: int) -> str:
    """生成只扫描给定四位年份后缀的 GH Archive 聚合查询。"""

    config = BigQueryPushMetadataConfig(
        billing_project="query-validation",
        start_year=start_year,
        end_year=end_year,
        maximum_bytes_billed=1,
        minimum_push_events=minimum_push_events,
    )
    return PUSH_METADATA_QUERY_TEMPLATE.format(
        start_year=config.start_year,
        end_year=config.end_year,
        minimum_push_events=config.minimum_push_events,
    )
