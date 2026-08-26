"""BigQuery 月度额度计算测试。"""

import math

import pytest

from starcat_recsys.bigquery_quota import (
    BIGQUERY_MONTHLY_FREE_TIER_BYTES,
    BigQueryQuotaUsage,
    _monthly_usage_query,
    format_quota_usage,
    quota_usage_document,
    read_bigquery_quota,
)


class FakeQuotaGateway:
    def __init__(self, usage: BigQueryQuotaUsage) -> None:
        self.usage = usage

    def fetch_monthly_usage(self) -> BigQueryQuotaUsage:
        return self.usage


def test_formats_normal_monthly_project_usage() -> None:
    usage = read_bigquery_quota(
        "starcat-test",
        gateway=FakeQuotaGateway(
            BigQueryQuotaUsage(
                query_jobs=100,
                billed_bytes=BIGQUERY_MONTHLY_FREE_TIER_BYTES // 10,
                processed_bytes=1,
            )
        ),
    )

    line, should_stop = format_quota_usage(usage, warning_percent=80, stop_percent=90)

    assert "10.00%" in line
    assert "状态：正常" in line
    assert should_stop is False


@pytest.mark.parametrize(
    ("percent", "status", "should_stop"),
    [(80, "警告", False), (90, "已达到停止阈值", True)],
)
def test_applies_warning_and_stop_thresholds(percent: int, status: str, should_stop: bool) -> None:
    usage = BigQueryQuotaUsage(
        query_jobs=1,
        billed_bytes=math.ceil(BIGQUERY_MONTHLY_FREE_TIER_BYTES * percent / 100),
        processed_bytes=1,
    )

    line, actual_should_stop = format_quota_usage(usage, warning_percent=80, stop_percent=90)

    assert f"状态：{status}" in line
    assert actual_should_stop is should_stop


def test_builds_region_scoped_monthly_jobs_query() -> None:
    query = _monthly_usage_query("US")

    assert "`region-us`.INFORMATION_SCHEMA.JOBS_BY_PROJECT" in query
    assert "TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), MONTH)" in query


def test_builds_stable_console_quota_document() -> None:
    document = quota_usage_document(
        BigQueryQuotaUsage(query_jobs=2, billed_bytes=100, processed_bytes=200),
        warning_percent=80,
        stop_percent=90,
    )

    assert document["schema_version"] == 1
    assert document["billed_bytes"] == 100
    assert document["remaining_bytes"] > 0
    assert document["should_stop"] is False
