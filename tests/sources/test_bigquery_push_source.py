"""BigQuery PushEvent 仓库基础元数据源测试。"""

from datetime import UTC, datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from starcat_recsys.interfaces import TaskContext
from starcat_recsys.sources.bigquery_push import (
    BigQueryPushEventMetadataSource,
    BigQueryPushMetadataConfig,
    GoogleBigQueryPushMetadataGateway,
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


def test_builds_bounded_year_query_with_latest_repository_name() -> None:
    query = _push_metadata_query(2016, 2025, 3)

    assert "`githubarchive.year.*`" in query
    assert "_TABLE_SUFFIX BETWEEN '2016' AND '2025'" in query
    assert "type = 'PushEvent'" in query
    assert "ORDER BY created_at DESC" in query
    assert "HAVING COUNT(*) >= 3" in query


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
