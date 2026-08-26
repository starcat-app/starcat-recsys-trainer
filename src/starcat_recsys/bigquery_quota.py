"""读取当前 GCP Project 的 BigQuery 月度查询计费字节。

BigQuery 免费层按 billing account 共享，但 Jobs 元数据按 project 查询。本模块提供的
数值用于同一 Project 内 WatchEvent / PushEvent 并行任务的共同保护；若 billing account
还绑定其他 Project，仍应以 Cloud Billing 报表作为最终账单口径。
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Protocol

from google.cloud import bigquery

BIGQUERY_MONTHLY_FREE_TIER_BYTES = 1 << 40
QUOTA_STOP_EXIT_CODE = 3


@dataclass(frozen=True, slots=True)
class BigQueryQuotaUsage:
    """当前 Project 在本 UTC 月内的 BigQuery 查询用量。"""

    query_jobs: int
    billed_bytes: int
    processed_bytes: int


class BigQueryQuotaGateway(Protocol):
    """隔离 Jobs 元数据查询，便于无网络验证额度计算。"""

    def fetch_monthly_usage(self) -> BigQueryQuotaUsage: ...


class GoogleBigQueryQuotaGateway:
    """从区域级 ``INFORMATION_SCHEMA.JOBS_BY_PROJECT`` 读取月度用量。"""

    def __init__(self, billing_project: str, location: str = "US") -> None:
        if not billing_project:
            raise ValueError("billing_project 不能为空")
        if not re.fullmatch(r"[A-Za-z0-9-]+", location):
            raise ValueError("BigQuery location 格式不合法")
        self._client = bigquery.Client(project=billing_project, location=location)
        self._query = _monthly_usage_query(location)

    def fetch_monthly_usage(self) -> BigQueryQuotaUsage:
        rows = list(self._client.query(self._query).result())
        if len(rows) != 1:
            raise RuntimeError("BigQuery 月度额度查询没有返回唯一汇总行")
        row = rows[0]
        return BigQueryQuotaUsage(
            query_jobs=int(row["query_jobs"] or 0),
            billed_bytes=int(row["billed_bytes"] or 0),
            processed_bytes=int(row["processed_bytes"] or 0),
        )


def read_bigquery_quota(
    billing_project: str,
    location: str = "US",
    gateway: BigQueryQuotaGateway | None = None,
) -> BigQueryQuotaUsage:
    """读取实际 Job 用量；注入 Gateway 时不访问 GCP。"""

    return (gateway or GoogleBigQueryQuotaGateway(billing_project, location)).fetch_monthly_usage()


def format_quota_usage(
    usage: BigQueryQuotaUsage,
    *,
    warning_percent: float,
    stop_percent: float,
) -> tuple[str, bool]:
    """返回适合长期日志的额度摘要，以及是否达到停止阈值。"""

    if not 0 < warning_percent < stop_percent <= 100:
        raise ValueError("额度阈值必须满足 0 < warning < stop <= 100")
    used_percent = usage.billed_bytes / BIGQUERY_MONTHLY_FREE_TIER_BYTES * 100
    remaining_bytes = max(BIGQUERY_MONTHLY_FREE_TIER_BYTES - usage.billed_bytes, 0)
    status = "正常"
    if used_percent >= stop_percent:
        status = "已达到停止阈值"
    elif used_percent >= warning_percent:
        status = "警告"
    line = (
        "BigQuery 本月项目计费额度："
        f"{_gib(usage.billed_bytes):.2f} GiB / 1024.00 GiB "
        f"({used_percent:.2f}%)，剩余 {_gib(remaining_bytes):.2f} GiB，"
        f"查询 {usage.query_jobs} 次，状态：{status}"
    )
    return line, used_percent >= stop_percent


def _monthly_usage_query(location: str) -> str:
    region = f"region-{location.lower()}"
    return f"""
SELECT
  COUNTIF(job_type = 'QUERY') AS query_jobs,
  COALESCE(SUM(IF(job_type = 'QUERY', total_bytes_billed, 0)), 0) AS billed_bytes,
  COALESCE(SUM(IF(job_type = 'QUERY', total_bytes_processed, 0)), 0) AS processed_bytes
FROM `{region}`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
WHERE creation_time >= TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), MONTH)
""".strip()


def _gib(value: int) -> float:
    return value / (1 << 30)
