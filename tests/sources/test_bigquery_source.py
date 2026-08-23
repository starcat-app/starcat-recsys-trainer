"""BigQuery 数据源测试。"""

from datetime import date
from pathlib import Path

import pyarrow as pa
import pytest

from starcat_recsys.interfaces import TaskContext
from starcat_recsys.sources.bigquery import (
    BigQueryConfig,
    BigQueryWatchEventSource,
    GoogleBigQueryGateway,
    _gh_archive_query,
)


class FakeGateway:
    def __init__(self, estimates: dict[str, int]) -> None:
        self.estimates = estimates
        self.fetches: list[tuple[str, int]] = []

    def estimate_bytes(self, table_suffix: str) -> int:
        return self.estimates[table_suffix]

    def fetch_watch_events(self, table_suffix: str, maximum_bytes_billed: int) -> pa.Table:
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
