"""读取当前 GCP Project 的 BigQuery 月度查询计费字节。

BigQuery 免费层按 billing account 共享，但 Jobs 元数据按 project 查询。本模块提供的
数值用于同一 Project 内 WatchEvent / PushEvent 并行任务的共同保护；若 billing account
还绑定其他 Project，仍应以 Cloud Billing 报表作为最终账单口径。
"""

from __future__ import annotations

import re
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Protocol

from google.api_core import exceptions as google_exceptions
from google.api_core.retry import Retry
from google.auth import exceptions as google_auth_exceptions
from google.cloud import bigquery
from requests import exceptions as requests_exceptions

BIGQUERY_MONTHLY_FREE_TIER_BYTES = 1 << 40
QUOTA_STOP_EXIT_CODE = 3
QUOTA_QUERY_TIMEOUT_SECONDS = 20.0
QUOTA_QUERY_RETRY_ATTEMPTS = 3
QUOTA_QUERY_RETRY_INITIAL_DELAY_SECONDS = 1.0

_RETRYABLE_QUOTA_ERRORS = (
    google_exceptions.TooManyRequests,
    google_exceptions.InternalServerError,
    google_exceptions.BadGateway,
    google_exceptions.ServiceUnavailable,
    google_exceptions.GatewayTimeout,
    google_exceptions.DeadlineExceeded,
    google_auth_exceptions.TransportError,
    requests_exceptions.ConnectionError,
    requests_exceptions.Timeout,
    TimeoutError,
)


class BigQueryQuotaConnectionError(RuntimeError):
    """额度预检的瞬时网络错误在有限重试后仍未恢复。"""


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

    def __init__(
        self,
        billing_project: str,
        location: str = "US",
        *,
        timeout_seconds: float = QUOTA_QUERY_TIMEOUT_SECONDS,
        retry_attempts: int = QUOTA_QUERY_RETRY_ATTEMPTS,
        retry_initial_delay_seconds: float = QUOTA_QUERY_RETRY_INITIAL_DELAY_SECONDS,
        sleeper: Callable[[float], None] = time.sleep,
    ) -> None:
        if not billing_project:
            raise ValueError("billing_project 不能为空")
        if not re.fullmatch(r"[A-Za-z0-9-]+", location):
            raise ValueError("BigQuery location 格式不合法")
        if timeout_seconds <= 0:
            raise ValueError("额度查询 timeout_seconds 必须大于 0")
        if retry_attempts <= 0:
            raise ValueError("额度查询 retry_attempts 必须大于 0")
        if retry_initial_delay_seconds < 0:
            raise ValueError("额度查询 retry_initial_delay_seconds 不能小于 0")
        self._client = bigquery.Client(project=billing_project, location=location)
        self._query = _monthly_usage_query(location)
        self._timeout_seconds = timeout_seconds
        self._retry_attempts = retry_attempts
        self._retry_initial_delay_seconds = retry_initial_delay_seconds
        self._sleeper = sleeper
        # SDK 默认 retry deadline 较长。额度预检位于后台任务启动前，必须限制单次
        # HTTP 重试窗口，避免代理链路异常时终端长时间没有反馈。
        self._request_retry = Retry(deadline=timeout_seconds)

    def fetch_monthly_usage(self) -> BigQueryQuotaUsage:
        rows = self._fetch_rows_with_retry()
        if len(rows) != 1:
            raise RuntimeError("BigQuery 月度额度查询没有返回唯一汇总行")
        row = rows[0]
        return BigQueryQuotaUsage(
            query_jobs=int(row["query_jobs"] or 0),
            billed_bytes=int(row["billed_bytes"] or 0),
            processed_bytes=int(row["processed_bytes"] or 0),
        )

    def _fetch_rows_with_retry(self) -> list[bigquery.Row]:
        """仅重试 TLS、代理、超时及 Google 瞬时错误，配置和权限错误立即暴露。"""

        for attempt in range(1, self._retry_attempts + 1):
            try:
                job = self._client.query(
                    self._query,
                    retry=self._request_retry,
                    timeout=self._timeout_seconds,
                    # 月度额度查询很小；业务层已经有界重试，不再让 SDK 重新创建 Job。
                    job_retry=None,
                )
                return list(
                    job.result(
                        retry=self._request_retry,
                        timeout=self._timeout_seconds,
                        job_retry=None,
                    )
                )
            except _RETRYABLE_QUOTA_ERRORS as error:
                if attempt == self._retry_attempts:
                    raise BigQueryQuotaConnectionError(
                        f"BigQuery 额度预检连续 {self._retry_attempts} 次连接失败；"
                        "请检查 HTTP(S)_PROXY、代理节点和 Google API TLS 连通性后重试"
                    ) from error
                delay = self._retry_initial_delay_seconds * (2 ** (attempt - 1))
                self._sleeper(delay)
        raise AssertionError("quota retry loop 不应运行到这里")


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

    document = quota_usage_document(
        usage,
        warning_percent=warning_percent,
        stop_percent=stop_percent,
    )
    line = (
        "BigQuery 本月项目计费额度："
        f"{_gib(usage.billed_bytes):.2f} GiB / 1024.00 GiB "
        f"({document['used_percent']:.2f}%)，"
        f"剩余 {_gib(int(document['remaining_bytes'])):.2f} GiB，"
        f"查询 {usage.query_jobs} 次，状态：{document['status']}"
    )
    return line, bool(document["should_stop"])


def quota_usage_document(
    usage: BigQueryQuotaUsage,
    *,
    warning_percent: float,
    stop_percent: float,
) -> dict[str, int | float | str | bool]:
    """返回控制台可直接消费的稳定月度额度文档。"""

    if not 0 < warning_percent < stop_percent <= 100:
        raise ValueError("额度阈值必须满足 0 < warning < stop <= 100")
    used_percent = usage.billed_bytes / BIGQUERY_MONTHLY_FREE_TIER_BYTES * 100
    remaining_bytes = max(BIGQUERY_MONTHLY_FREE_TIER_BYTES - usage.billed_bytes, 0)
    status = "正常"
    if used_percent >= stop_percent:
        status = "已达到停止阈值"
    elif used_percent >= warning_percent:
        status = "警告"
    return {
        "schema_version": 1,
        "query_jobs": usage.query_jobs,
        "billed_bytes": usage.billed_bytes,
        "processed_bytes": usage.processed_bytes,
        "free_tier_bytes": BIGQUERY_MONTHLY_FREE_TIER_BYTES,
        "remaining_bytes": remaining_bytes,
        "used_percent": used_percent,
        "warning_percent": warning_percent,
        "stop_percent": stop_percent,
        "status": status,
        "should_stop": used_percent >= stop_percent,
    }


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
