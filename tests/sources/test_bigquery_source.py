"""BigQuery 数据源测试。"""

import json
from datetime import date
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from google.api_core import exceptions as google_exceptions
from requests import exceptions as requests_exceptions

from starcat_recsys.interfaces import TaskContext
from starcat_recsys.sources.bigquery import (
    BigQueryConfig,
    BigQueryTransientConnectionError,
    BigQueryWatchEventSource,
    GoogleBigQueryGateway,
    _gh_archive_query,
)


class FakeGateway:
    def __init__(self, estimates: dict[str, int]) -> None:
        self.estimates = estimates
        self.estimate_calls: list[str] = []
        self.fetches: list[tuple[str, int]] = []

    def estimate_bytes(self, table_suffix: str) -> int:
        self.estimate_calls.append(table_suffix)
        return self.estimates[table_suffix]

    def fetch_events(self, table_suffix: str, maximum_bytes_billed: int) -> pa.Table:
        self.fetches.append((table_suffix, maximum_bytes_billed))
        return pa.table(
            {
                "source_record_id": [f"event-{table_suffix}"],
                "actor_id": ["123"],
                "repo_id": [41881900],
                "created_at": [
                    f"{table_suffix[:4]}-{table_suffix[4:6]}-{table_suffix[6:]}T00:00:00Z"
                ],
            }
        )


class FailingSecondGateway(FakeGateway):
    def fetch_events(self, table_suffix: str, maximum_bytes_billed: int) -> pa.Table:
        if table_suffix == "20260823":
            raise RuntimeError("simulated interruption")
        return super().fetch_events(table_suffix, maximum_bytes_billed)


class FlakyGateway(FakeGateway):
    def __init__(self, estimates: dict[str, int]) -> None:
        super().__init__(estimates)
        self.attempts = 0

    def fetch_events(self, table_suffix: str, maximum_bytes_billed: int) -> pa.Table:
        self.attempts += 1
        if self.attempts == 1:
            raise google_exceptions.ServiceUnavailable("temporary")
        return super().fetch_events(table_suffix, maximum_bytes_billed)


class FlakyTransportGateway(FakeGateway):
    """模拟 BigQuery 分页结果下载期间发生一次 TLS EOF。"""

    def __init__(self, estimates: dict[str, int]) -> None:
        super().__init__(estimates)
        self.attempts = 0

    def fetch_events(self, table_suffix: str, maximum_bytes_billed: int) -> pa.Table:
        self.attempts += 1
        if self.attempts == 1:
            raise requests_exceptions.SSLError("temporary TLS EOF")
        return super().fetch_events(table_suffix, maximum_bytes_billed)


class FailingTransportGateway(FakeGateway):
    """模拟代理链路持续中断直到业务层重试耗尽。"""

    def __init__(self, estimates: dict[str, int]) -> None:
        super().__init__(estimates)
        self.attempts = 0

    def fetch_events(self, table_suffix: str, maximum_bytes_billed: int) -> pa.Table:
        self.attempts += 1
        raise requests_exceptions.SSLError("persistent TLS EOF")


class MissingDayGateway(FakeGateway):
    """模拟 GH Archive 历史日期没有对应日表。"""

    def estimate_bytes(self, table_suffix: str) -> int:
        self.estimate_calls.append(table_suffix)
        if table_suffix == "20260822":
            raise google_exceptions.NotFound(
                "Not found: Table githubarchive:day.20260822 was not found in location US"
            )
        return self.estimates[table_suffix]


def make_context(tmp_path: Path) -> TaskContext:
    return TaskContext(run_id="run-1", workspace=tmp_path, config_hash="sha256:x", random_seed=7)


def test_collects_each_day_after_dry_run(tmp_path: Path) -> None:
    gateway = FakeGateway({"20260822": 100, "20260823": 200})
    source = BigQueryWatchEventSource(
        BigQueryConfig(
            billing_project="starcat-test",
            start_date=date(2026, 8, 22),
            end_date=date(2026, 8, 23),
            maximum_bytes_billed_per_day=1000,
        ),
        gateway,
    )

    artifacts = source.collect(make_context(tmp_path))

    assert len(artifacts) == 2
    assert gateway.fetches == [("20260822", 1000), ("20260823", 1000)]
    assert all(Path(artifact.uri.removeprefix("file://")).exists() for artifact in artifacts)
    assert (tmp_path / "raw" / "gh_archive" / "manifest.json").exists()


def test_refuses_partition_over_budget(tmp_path: Path) -> None:
    gateway = FakeGateway({"20260823": 1001})
    source = BigQueryWatchEventSource(
        BigQueryConfig(
            billing_project="starcat-test",
            start_date=date(2026, 8, 23),
            end_date=date(2026, 8, 23),
            maximum_bytes_billed_per_day=1000,
        ),
        gateway,
    )

    with pytest.raises(RuntimeError, match="超过上限"):
        source.collect(make_context(tmp_path))

    assert gateway.fetches == []


def test_resumes_verified_partitions_without_querying_again(tmp_path: Path) -> None:
    config = BigQueryConfig(
        billing_project="starcat-test",
        start_date=date(2026, 8, 22),
        end_date=date(2026, 8, 22),
        maximum_bytes_billed_per_day=1000,
        maximum_total_bytes_billed=2000,
    )
    first_gateway = FakeGateway({"20260822": 100})
    BigQueryWatchEventSource(config, first_gateway).collect(make_context(tmp_path))

    resumed_gateway = FakeGateway({})
    artifacts = BigQueryWatchEventSource(config, resumed_gateway).collect(make_context(tmp_path))

    assert len(artifacts) == 1
    assert resumed_gateway.estimate_calls == []
    assert resumed_gateway.fetches == []


def test_appends_next_day_without_rescanning_existing_partitions(tmp_path: Path) -> None:
    initial = BigQueryConfig(
        billing_project="starcat-test",
        start_date=date(2026, 8, 22),
        end_date=date(2026, 8, 22),
        maximum_bytes_billed_per_day=1000,
        maximum_total_bytes_billed=2000,
    )
    BigQueryWatchEventSource(initial, FakeGateway({"20260822": 100})).collect(
        make_context(tmp_path)
    )

    append = BigQueryConfig(
        billing_project="starcat-test",
        start_date=date(2026, 8, 23),
        end_date=date(2026, 8, 23),
        maximum_bytes_billed_per_day=1000,
        maximum_total_bytes_billed=2000,
        append_existing=True,
    )
    gateway = FakeGateway({"20260823": 200})
    artifacts = BigQueryWatchEventSource(append, gateway).collect(make_context(tmp_path))

    assert len(artifacts) == 1
    assert gateway.estimate_calls == ["20260823"]
    assert gateway.fetches == [("20260823", 1000)]
    state = json.loads((tmp_path / "raw" / "gh_archive" / "download-state.json").read_text())
    assert state["scope"]["start_date"] == "2026-08-22"
    assert state["scope"]["end_date"] == "2026-08-23"
    assert state["estimated_total_bytes"] == 300
    assert sorted(state["completed_partitions"]) == ["20260822", "20260823"]
    manifest = json.loads((tmp_path / "raw" / "gh_archive" / "manifest.json").read_text())
    assert len(manifest["partitions"]) == 2

    replay_gateway = FakeGateway({})
    BigQueryWatchEventSource(append, replay_gateway).collect(make_context(tmp_path))
    assert replay_gateway.estimate_calls == []
    assert replay_gateway.fetches == []


def test_append_rejects_date_gap(tmp_path: Path) -> None:
    initial = BigQueryConfig(
        billing_project="starcat-test",
        start_date=date(2026, 8, 22),
        end_date=date(2026, 8, 22),
        maximum_bytes_billed_per_day=1000,
    )
    BigQueryWatchEventSource(initial, FakeGateway({"20260822": 100})).collect(
        make_context(tmp_path)
    )
    append = BigQueryConfig(
        billing_project="starcat-test",
        start_date=date(2026, 8, 24),
        end_date=date(2026, 8, 24),
        maximum_bytes_billed_per_day=1000,
        append_existing=True,
    )

    with pytest.raises(RuntimeError, match="紧邻当前水位"):
        BigQueryWatchEventSource(append, FakeGateway({"20260824": 100})).collect(
            make_context(tmp_path)
        )


def test_redownloads_partition_when_checkpoint_checksum_mismatches(tmp_path: Path) -> None:
    config = BigQueryConfig(
        billing_project="starcat-test",
        start_date=date(2026, 8, 22),
        end_date=date(2026, 8, 22),
        maximum_bytes_billed_per_day=1000,
    )
    BigQueryWatchEventSource(config, FakeGateway({"20260822": 100})).collect(make_context(tmp_path))
    partition = tmp_path / "raw" / "gh_archive" / "watch-events-20260822.parquet"
    partition.write_bytes(b"corrupted")

    gateway = FakeGateway({"20260822": 100})
    BigQueryWatchEventSource(config, gateway).collect(make_context(tmp_path))

    assert gateway.fetches == [("20260822", 1000)]
    assert partition.read_bytes() != b"corrupted"


def test_checkpoint_survives_later_partition_failure(tmp_path: Path) -> None:
    gateway = FailingSecondGateway({"20260822": 100, "20260823": 200})
    source = BigQueryWatchEventSource(
        BigQueryConfig(
            billing_project="starcat-test",
            start_date=date(2026, 8, 22),
            end_date=date(2026, 8, 23),
            maximum_bytes_billed_per_day=1000,
        ),
        gateway,
    )

    with pytest.raises(RuntimeError, match="simulated interruption"):
        source.collect(make_context(tmp_path))

    state = json.loads((tmp_path / "raw" / "gh_archive" / "download-state.json").read_text())
    assert list(state["completed_partitions"]) == ["20260822"]


def test_retries_transient_google_error(tmp_path: Path) -> None:
    gateway = FlakyGateway({"20260822": 100})
    source = BigQueryWatchEventSource(
        BigQueryConfig(
            billing_project="starcat-test",
            start_date=date(2026, 8, 22),
            end_date=date(2026, 8, 22),
            maximum_bytes_billed_per_day=1000,
            retry_attempts=2,
            retry_initial_delay_seconds=0,
        ),
        gateway,
    )

    source.collect(make_context(tmp_path))

    assert gateway.attempts == 2
    assert gateway.fetches == [("20260822", 1000)]


def test_retries_transient_transport_error(tmp_path: Path) -> None:
    gateway = FlakyTransportGateway({"20260822": 100})
    source = BigQueryWatchEventSource(
        BigQueryConfig(
            billing_project="starcat-test",
            start_date=date(2026, 8, 22),
            end_date=date(2026, 8, 22),
            maximum_bytes_billed_per_day=1000,
            retry_attempts=2,
            retry_initial_delay_seconds=0,
        ),
        gateway,
    )

    source.collect(make_context(tmp_path))

    assert gateway.attempts == 2
    assert gateway.fetches == [("20260822", 1000)]


def test_reports_proxy_hint_when_transport_retries_are_exhausted(tmp_path: Path) -> None:
    gateway = FailingTransportGateway({"20260822": 100})
    source = BigQueryWatchEventSource(
        BigQueryConfig(
            billing_project="starcat-test",
            start_date=date(2026, 8, 22),
            end_date=date(2026, 8, 22),
            maximum_bytes_billed_per_day=1000,
            retry_attempts=2,
            retry_initial_delay_seconds=0,
        ),
        gateway,
    )

    with pytest.raises(
        BigQueryTransientConnectionError,
        match=r"TLS/代理连接.*HTTP\(S\)_PROXY.*同一 workspace 续传",
    ):
        source.collect(make_context(tmp_path))

    assert gateway.attempts == 2


def test_records_missing_historical_day_as_empty_partition(tmp_path: Path) -> None:
    gateway = MissingDayGateway({"20260823": 200})
    source = BigQueryWatchEventSource(
        BigQueryConfig(
            billing_project="starcat-test",
            start_date=date(2026, 8, 22),
            end_date=date(2026, 8, 23),
            maximum_bytes_billed_per_day=1000,
        ),
        gateway,
    )

    artifacts = source.collect(make_context(tmp_path))

    missing_partition = tmp_path / "raw" / "gh_archive" / "watch-events-20260822.parquet"
    assert len(artifacts) == 2
    assert pq.read_metadata(missing_partition).num_rows == 0
    assert gateway.estimate_calls == ["20260822", "20260823"]
    assert gateway.fetches == [("20260823", 1000)]
    state = json.loads((tmp_path / "raw" / "gh_archive" / "download-state.json").read_text())
    assert state["completed_partitions"]["20260822"]["metadata"]["estimated_bytes"] == 0


def test_refuses_when_cumulative_estimate_exceeds_total_budget(tmp_path: Path) -> None:
    gateway = FakeGateway({"20260822": 600, "20260823": 600})
    source = BigQueryWatchEventSource(
        BigQueryConfig(
            billing_project="starcat-test",
            start_date=date(2026, 8, 22),
            end_date=date(2026, 8, 23),
            maximum_bytes_billed_per_day=1000,
            maximum_total_bytes_billed=1000,
        ),
        gateway,
    )

    with pytest.raises(RuntimeError, match="总上限"):
        source.collect(make_context(tmp_path))

    assert gateway.fetches == [("20260822", 1000)]


def test_adopts_complete_partition_left_before_checkpoint(tmp_path: Path) -> None:
    config = BigQueryConfig(
        billing_project="starcat-test",
        start_date=date(2026, 8, 22),
        end_date=date(2026, 8, 22),
        maximum_bytes_billed_per_day=1000,
    )
    BigQueryWatchEventSource(config, FakeGateway({"20260822": 100})).collect(make_context(tmp_path))
    (tmp_path / "raw" / "gh_archive" / "download-state.json").unlink()

    gateway = FakeGateway({"20260822": 100})
    BigQueryWatchEventSource(config, gateway).collect(make_context(tmp_path))

    assert gateway.estimate_calls == ["20260822"]
    assert gateway.fetches == []


def test_rejects_reversed_date_range() -> None:
    with pytest.raises(ValueError, match="end_date"):
        BigQueryConfig(
            billing_project="starcat-test",
            start_date=date(2026, 8, 24),
            end_date=date(2026, 8, 23),
            maximum_bytes_billed_per_day=1000,
        )


def test_builds_query_for_one_explicit_day_table() -> None:
    query = _gh_archive_query("20240101")

    assert "`githubarchive.day.20240101`" in query
    assert "githubarchive.day.*" not in query
    assert "_TABLE_SUFFIX" not in query


@pytest.mark.parametrize(
    "table_suffix",
    ["2024011", "20240132", "２０２４０１０１", "20240101` WHERE TRUE --"],
)
def test_rejects_invalid_or_unsafe_table_suffix(table_suffix: str) -> None:
    with pytest.raises(ValueError, match="YYYYMMDD"):
        _gh_archive_query(table_suffix)


def test_dry_run_job_config_omits_empty_maximum_bytes() -> None:
    gateway = object.__new__(GoogleBigQueryGateway)

    job_config = gateway._job_config(dry_run=True)

    assert "maximumBytesBilled" not in job_config.to_api_repr()["query"]


def test_query_job_config_serializes_maximum_bytes() -> None:
    gateway = object.__new__(GoogleBigQueryGateway)

    job_config = gateway._job_config(dry_run=False, maximum_bytes_billed=200_000_000)

    assert job_config.to_api_repr()["query"]["maximumBytesBilled"] == "200000000"
