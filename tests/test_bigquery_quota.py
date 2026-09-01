"""BigQuery 月度额度计算测试。"""

import math
from typing import Any

import pytest
from requests import exceptions as requests_exceptions

from starcat_recsys import bigquery_quota, cli
from starcat_recsys.bigquery_quota import (
    BIGQUERY_MONTHLY_FREE_TIER_BYTES,
    BigQueryQuotaConnectionError,
    BigQueryQuotaUsage,
    GoogleBigQueryQuotaGateway,
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


class FakeQueryJob:
    def __init__(self) -> None:
        self.result_timeouts: list[float] = []

    def result(self, **kwargs: Any) -> list[dict[str, int]]:
        self.result_timeouts.append(float(kwargs["timeout"]))
        return [{"query_jobs": 2, "billed_bytes": 100, "processed_bytes": 200}]


class FlakyQuotaClient:
    def __init__(self, failures: int) -> None:
        self.failures = failures
        self.query_timeouts: list[float] = []
        self.job = FakeQueryJob()

    def query(self, _query: str, **kwargs: Any) -> FakeQueryJob:
        self.query_timeouts.append(float(kwargs["timeout"]))
        if len(self.query_timeouts) <= self.failures:
            raise requests_exceptions.SSLError("simulated TLS EOF")
        return self.job


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


def test_quota_gateway_retries_tls_error_with_explicit_timeout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = FlakyQuotaClient(failures=2)
    delays: list[float] = []
    monkeypatch.setattr(bigquery_quota.bigquery, "Client", lambda **_kwargs: client)
    gateway = GoogleBigQueryQuotaGateway(
        "starcat-test",
        timeout_seconds=7,
        retry_attempts=3,
        retry_initial_delay_seconds=0.5,
        sleeper=delays.append,
    )

    usage = gateway.fetch_monthly_usage()

    assert usage.query_jobs == 2
    assert client.query_timeouts == [7, 7, 7]
    assert client.job.result_timeouts == [7]
    assert delays == [0.5, 1.0]


def test_quota_gateway_reports_proxy_hint_after_retry_exhaustion(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = FlakyQuotaClient(failures=3)
    monkeypatch.setattr(bigquery_quota.bigquery, "Client", lambda **_kwargs: client)
    gateway = GoogleBigQueryQuotaGateway(
        "starcat-test",
        retry_attempts=3,
        retry_initial_delay_seconds=0,
        sleeper=lambda _delay: None,
    )

    with pytest.raises(BigQueryQuotaConnectionError, match=r"HTTP\(S\)_PROXY.*TLS"):
        gateway.fetch_monthly_usage()


def test_quota_cli_prints_concise_connection_error(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    def fail_quota(_billing_project: str, _location: str) -> BigQueryQuotaUsage:
        raise BigQueryQuotaConnectionError("请检查代理和 TLS 连通性")

    monkeypatch.setattr(cli, "read_bigquery_quota", fail_quota)

    exit_code = cli.main(
        [
            "bigquery",
            "quota-usage",
            "--billing-project",
            "starcat-test",
        ]
    )

    captured = capsys.readouterr()
    assert exit_code == 1
    assert captured.out == ""
    assert captured.err == "错误：请检查代理和 TLS 连通性\n"
    assert "Traceback" not in captured.err
