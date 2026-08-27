"""既有 BigQuery Raw 只读登记测试。"""

from __future__ import annotations

import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from starcat_recsys.artifacts import sha256_file
from starcat_recsys.cli import main
from starcat_recsys.lake_catalog import inspect_bigquery_dataset
from starcat_recsys.sources.bigquery import BigQueryWatchEventSource


def test_inspects_ready_failed_and_missing_partitions_without_real_paths(tmp_path: Path) -> None:
    raw = tmp_path / "raw" / "gh_archive"
    raw.mkdir(parents=True)
    ready_path = raw / "watch-events-20260101.parquet"
    failed_path = raw / "watch-events-20260102.parquet"
    table = _table(BigQueryWatchEventSource.expected_columns)
    pq.write_table(table, ready_path)
    pq.write_table(table, failed_path)
    state = {
        "schema_version": 1,
        "scope": {"start_date": "2026-01-01", "end_date": "2026-01-03"},
        "completed_partitions": {
            "20260101": _saved_partition(ready_path, row_count=1),
            "20260102": {
                **_saved_partition(failed_path, row_count=1),
                "checksum": "sha256:" + "0" * 64,
            },
        },
    }
    (raw / "download-state.json").write_text(json.dumps(state), encoding="utf-8")

    document = inspect_bigquery_dataset(tmp_path, "watch-events")

    assert document["dataset"]["state"] == "degraded"
    assert document["dataset"]["ready_partitions"] == 1
    assert document["dataset"]["failed_partitions"] == 1
    assert document["dataset"]["missing_partitions"] == 1
    assert document["dataset"]["watermark"] == "2026-01-01"
    assert [item["state"] for item in document["partitions"]] == [
        "ready",
        "failed",
        "missing",
    ]
    assert document["partitions"][1]["error_code"] == "PARTITION_CHECKSUM_MISMATCH"
    assert tmp_path.as_posix() not in json.dumps(document)


def test_cli_emits_compact_catalog_snapshot(tmp_path: Path, capsys: object) -> None:
    raw = tmp_path / "raw" / "gh_archive"
    raw.mkdir(parents=True)
    path = raw / "watch-events-20260101.parquet"
    pq.write_table(_table(BigQueryWatchEventSource.expected_columns), path)
    (raw / "download-state.json").write_text(
        json.dumps(
            {
                "schema_version": 1,
                "scope": {"start_date": "2026-01-01", "end_date": "2026-01-01"},
                "completed_partitions": {"20260101": _saved_partition(path, row_count=1)},
            }
        ),
        encoding="utf-8",
    )

    assert main(["lake", "inspect-watch-events", "--workspace", str(tmp_path)]) == 0
    captured = capsys.readouterr()  # type: ignore[attr-defined]
    document = json.loads(captured.out)
    assert document["dataset"]["dataset_id"] == "githubarchive_watch_event"
    assert document["dataset"]["ready_partitions"] == 1


def _table(columns: tuple[str, ...]) -> pa.Table:
    return pa.table({column: pa.array(["value"]) for column in columns})


def _saved_partition(path: Path, *, row_count: int) -> dict[str, object]:
    return {
        "checksum": sha256_file(path),
        "kind": "raw_interactions",
        "metadata": {
            "source": "gh_archive",
            "partition": path.stem.rsplit("-", 1)[-1],
            "row_count": row_count,
            "estimated_bytes": 123,
        },
    }
