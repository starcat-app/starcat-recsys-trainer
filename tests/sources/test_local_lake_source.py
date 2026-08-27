"""本地 BigQuery Raw 分区只读接入测试。"""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from starcat_recsys.artifacts import sha256_file
from starcat_recsys.interfaces import TaskContext
from starcat_recsys.sources.local_lake import LocalLakeConfig, LocalLakePartitionSource


def test_registers_existing_watch_partitions_without_copying(tmp_path: Path) -> None:
    lake = tmp_path / "lake"
    raw = lake / "raw" / "gh_archive"
    raw.mkdir(parents=True)
    partitions = {}
    for suffix in ["20260822", "20260823"]:
        path = raw / f"watch-events-{suffix}.parquet"
        pq.write_table(
            pa.table(
                {
                    "source_record_id": [f"event-{suffix}"],
                    "actor_id": ["123"],
                    "repo_id": [1],
                    "created_at": [f"{suffix[:4]}-{suffix[4:6]}-{suffix[6:]}T00:00:00Z"],
                }
            ),
            path,
        )
        partitions[suffix] = {
            "checksum": sha256_file(path),
            "kind": "raw_interactions",
            "metadata": {"partition": suffix, "row_count": 1, "source": "gh_archive"},
        }
    (raw / "download-state.json").write_text(
        json.dumps({"schema_version": 1, "completed_partitions": partitions})
    )
    context = TaskContext("run", tmp_path / "run", "sha256:config", 7)

    artifacts = LocalLakePartitionSource(
        LocalLakeConfig(
            workspace=lake,
            dataset="watch_events",
            start_date=date(2026, 8, 22),
            end_date=date(2026, 8, 23),
        )
    ).collect(context)

    assert len(artifacts) == 2
    assert all(item.metadata["external"] is True for item in artifacts)
    assert all(Path(item.uri.removeprefix("file://")).parent == raw for item in artifacts)
    assert not (context.workspace / "raw" / "gh_archive").exists()
    manifest = json.loads(
        (context.workspace / "raw/local_lake/watch_events-manifest.json").read_text()
    )
    assert manifest["partition_count"] == 2
    assert manifest["row_count"] == 2


def test_rejects_missing_or_changed_partition(tmp_path: Path) -> None:
    raw = tmp_path / "lake/raw/gh_archive"
    raw.mkdir(parents=True)
    (raw / "download-state.json").write_text(
        json.dumps(
            {
                "completed_partitions": {
                    "20260822": {
                        "checksum": "sha256:missing",
                        "metadata": {"partition": "20260822", "row_count": 1},
                    }
                }
            }
        )
    )
    source = LocalLakePartitionSource(
        LocalLakeConfig(
            workspace=tmp_path / "lake",
            dataset="watch_events",
            start_date=date(2026, 8, 22),
            end_date=date(2026, 8, 22),
        )
    )

    with pytest.raises(FileNotFoundError, match="分区文件不存在"):
        source.collect(TaskContext("run", tmp_path / "run", "sha256:config", 7))


def test_rejects_unknown_dataset(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="dataset"):
        LocalLakeConfig(
            workspace=tmp_path,
            dataset="unknown",
            start_date=date(2026, 8, 22),
            end_date=date(2026, 8, 22),
        )
