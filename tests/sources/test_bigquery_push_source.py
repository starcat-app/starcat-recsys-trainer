"""BigQuery PushEvent 仓库基础元数据源测试。"""

from datetime import UTC, date, datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from starcat_recsys.interfaces import TaskContext
from starcat_recsys.sources.bigquery import BigQueryConfig
from starcat_recsys.sources.bigquery_push import (
    BigQueryPushEventMetadataSource,
    BigQueryPushEventSource,
    BigQueryPushMetadataConfig,
    GoogleBigQueryPushEventGateway,
    GoogleBigQueryPushMetadataGateway,
    _push_event_query,
    _push_metadata_query,
)


class FakePushMetadataGateway:
    def __init__(self, estimated_bytes: int) -> None:
        self.estimated_bytes = estimated_bytes
        self.fetches: list[int] = []

    def estimate_bytes(self) -> int:
        return self.estimated_bytes

    def fetch_repository_metadata(self, maximum_bytes_billed: int) -> pa.Table:
        self.fetches.append(maximum_bytes_billed)
        return pa.table(
            {
                "repo_id": [1],
                "full_name": ["starcat/repo"],
                "first_push_at": [datetime(2020, 1, 1, tzinfo=UTC)],
                "last_push_at": [datetime(2025, 1, 1, tzinfo=UTC)],
                "push_event_count": [42],
                "fetched_at": [datetime(2026, 8, 26, tzinfo=UTC)],
            }
        )


class FakePushEventGateway:
    def __init__(self, estimates: dict[str, int]) -> None:
        self.estimates = estimates
        self.fetches: list[tuple[str, int]] = []

    def estimate_bytes(self, table_suffix: str) -> int:
        return self.estimates[table_suffix]

    def fetch_events(self, table_suffix: str, maximum_bytes_billed: int) -> pa.Table:
        self.fetches.append((table_suffix, maximum_bytes_billed))
        return pa.table(
            {
                "source_record_id": [f"push-{table_suffix}"],
                "actor_id": ["100"],
                "repo_id": [200],
                "full_name": ["starcat/repo"],
                "created_at": [datetime(2026, 8, 22, tzinfo=UTC)],
            }
        )


def _context(tmp_path: Path) -> TaskContext:
    return TaskContext("run", tmp_path, "sha256:config", 7)


def test_collects_aggregated_repository_metadata_after_dry_run(tmp_path: Path) -> None:
    gateway = FakePushMetadataGateway(estimated_bytes=700)
    source = BigQueryPushEventMetadataSource(
        BigQueryPushMetadataConfig(
            billing_project="starcat-test",
            start_year=2016,
            end_year=2025,
            maximum_bytes_billed=1_000,
            minimum_push_events=2,
        ),
        gateway,
    )

    artifacts = source.collect(_context(tmp_path))

    assert gateway.fetches == [1_000]
    assert len(artifacts) == 1
    assert artifacts[0].metadata["source"] == "gh_archive_push"
    path = Path(artifacts[0].uri.removeprefix("file://"))
    assert pq.read_table(path).to_pylist()[0]["push_event_count"] == 42
    assert (tmp_path / "raw/gh_archive_push/manifest.json").exists()


def test_refuses_push_metadata_query_over_budget(tmp_path: Path) -> None:
    gateway = FakePushMetadataGateway(estimated_bytes=1_001)
    source = BigQueryPushEventMetadataSource(
        BigQueryPushMetadataConfig(
            billing_project="starcat-test",
            start_year=2024,
            end_year=2025,
            maximum_bytes_billed=1_000,
        ),
        gateway,
    )

    with pytest.raises(RuntimeError, match="超过上限"):
        source.collect(_context(tmp_path))

    assert gateway.fetches == []


def test_collects_daily_push_events_with_independent_checkpoint(tmp_path: Path) -> None:
    gateway = FakePushEventGateway({"20260822": 100, "20260823": 200})
    source = BigQueryPushEventSource(
        BigQueryConfig(
            billing_project="starcat-test",
            start_date=date(2026, 8, 22),
            end_date=date(2026, 8, 23),
            maximum_bytes_billed_per_day=1_000,
            maximum_total_bytes_billed=2_000,
        ),
        gateway,
    )

    artifacts = source.collect(_context(tmp_path))

    assert len(artifacts) == 2
    assert gateway.fetches == [("20260822", 1_000), ("20260823", 1_000)]
    assert artifacts[0].kind == "raw_push_events"
    path = tmp_path / "raw/gh_archive_push_events/push-events-20260822.parquet"
    assert pq.read_table(path).column_names == [
        "source_record_id",
        "actor_id",
        "repo_id",
        "full_name",
        "created_at",
    ]
    assert (tmp_path / "raw/gh_archive_push_events/download-state.json").exists()


def test_builds_bounded_year_query_with_latest_repository_name() -> None:
    query = _push_metadata_query(2016, 2025, 3)

    assert "`githubarchive.year.*`" in query
    assert "_TABLE_SUFFIX BETWEEN '2016' AND '2025'" in query
    assert "type = 'PushEvent'" in query
    assert "ORDER BY created_at DESC" in query
    assert "HAVING COUNT(*) >= 3" in query


def test_builds_push_event_query_for_one_explicit_day() -> None:
    query = _push_event_query("20260822")

    assert "`githubarchive.day.20260822`" in query
    assert "type = 'PushEvent'" in query
    assert "repo.name AS full_name" in query
    assert "payload" not in query


@pytest.mark.parametrize(
    ("start_year", "end_year", "minimum_push_events"),
    [(2010, 2025, 1), (2026, 2025, 1), (2016, 2025, 0)],
)
def test_rejects_invalid_query_bounds(
    start_year: int, end_year: int, minimum_push_events: int
) -> None:
    with pytest.raises(ValueError):
        _push_metadata_query(start_year, end_year, minimum_push_events)


def test_push_metadata_dry_run_omits_empty_maximum_bytes() -> None:
    job_config = GoogleBigQueryPushMetadataGateway._job_config(dry_run=True)

    assert "maximumBytesBilled" not in job_config.to_api_repr()["query"]


def test_push_metadata_query_serializes_maximum_bytes() -> None:
    job_config = GoogleBigQueryPushMetadataGateway._job_config(
        dry_run=False,
        maximum_bytes_billed=800_000_000_000,
    )

    assert job_config.to_api_repr()["query"]["maximumBytesBilled"] == "800000000000"


def test_push_event_query_serializes_daily_maximum_bytes() -> None:
    job_config = GoogleBigQueryPushEventGateway._job_config(
        dry_run=False,
        maximum_bytes_billed=1_000_000_000,
    )

    assert job_config.to_api_repr()["query"]["maximumBytesBilled"] == "1000000000"
