"""本地 BigQuery Raw 分区的只读数据源。

下载任务与训练任务生命周期不同：前者把不可变日分区长期保存在本地数据湖，后者只登记
这些分区为 Artifact。Connector 不复制 Parquet，也不修改下载 checkpoint，避免十年 Raw
随着每个训练 run 重复占用磁盘。
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from starcat_recsys.artifacts import sha256_file, write_json_atomic
from starcat_recsys.interfaces import Artifact, TaskContext
from starcat_recsys.sources.bigquery import BigQueryWatchEventSource
from starcat_recsys.sources.bigquery_push import BigQueryPushEventSource


@dataclass(frozen=True, slots=True)
class LocalLakeConfig:
    """一次有界的本地 Raw 分区读取配置。"""

    workspace: Path
    dataset: str
    start_date: date
    end_date: date
    verify_checksums: bool = True

    def __post_init__(self) -> None:
        if self.dataset not in {"watch_events", "push_events"}:
            raise ValueError("dataset 只能是 watch_events 或 push_events")
        if self.end_date < self.start_date:
            raise ValueError("end_date 不能早于 start_date")


@dataclass(frozen=True, slots=True)
class _DatasetSpec:
    output_directory_name: str
    file_prefix: str
    kind: str
    source: str
    expected_columns: tuple[str, ...]


_DATASETS = {
    "watch_events": _DatasetSpec(
        output_directory_name=BigQueryWatchEventSource.output_directory_name,
        file_prefix=BigQueryWatchEventSource.file_prefix,
        kind=BigQueryWatchEventSource.artifact_kind,
        source=BigQueryWatchEventSource.metadata_source,
        expected_columns=BigQueryWatchEventSource.expected_columns,
    ),
    "push_events": _DatasetSpec(
        output_directory_name=BigQueryPushEventSource.output_directory_name,
        file_prefix=BigQueryPushEventSource.file_prefix,
        kind=BigQueryPushEventSource.artifact_kind,
        source=BigQueryPushEventSource.metadata_source,
        expected_columns=BigQueryPushEventSource.expected_columns,
    ),
}


class LocalLakePartitionSource:
    """校验下载 checkpoint，并把已有日分区登记为外部只读 Artifact。"""

    name = "local_lake_partitions"

    def __init__(self, config: LocalLakeConfig) -> None:
        self._config = config

    def collect(self, context: TaskContext) -> list[Artifact]:
        spec = _DATASETS[self._config.dataset]
        raw_directory = self._config.workspace / "raw" / spec.output_directory_name
        state_path = raw_directory / "download-state.json"
        state = _read_document(state_path)
        completed = state.get("completed_partitions")
        if not isinstance(completed, dict):
            raise ValueError(f"下载 checkpoint 缺少 completed_partitions：{state_path}")

        artifacts = [
            self._partition_artifact(raw_directory, spec, completed, current_date)
            for current_date in _date_range(self._config.start_date, self._config.end_date)
        ]
        output_path = (
            context.workspace / "raw" / "local_lake" / f"{self._config.dataset}-manifest.json"
        )
        write_json_atomic(
            output_path,
            {
                "schema_version": 1,
                "run_id": context.run_id,
                "dataset": self._config.dataset,
                "start_date": self._config.start_date.isoformat(),
                "end_date": self._config.end_date.isoformat(),
                "partition_count": len(artifacts),
                "row_count": sum(int(item.metadata["row_count"]) for item in artifacts),
                "verify_checksums": self._config.verify_checksums,
                "artifacts": [
                    {
                        "kind": item.kind,
                        "uri": item.uri,
                        "checksum": item.checksum,
                        "metadata": item.metadata,
                    }
                    for item in artifacts
                ],
            },
        )
        return artifacts

    def _partition_artifact(
        self,
        raw_directory: Path,
        spec: _DatasetSpec,
        completed: dict[str, Any],
        current_date: date,
    ) -> Artifact:
        suffix = current_date.strftime("%Y%m%d")
        saved = completed.get(suffix)
        if not isinstance(saved, dict):
            raise FileNotFoundError(f"本地数据湖缺少已完成分区：{suffix}")
        checksum = saved.get("checksum")
        metadata = saved.get("metadata")
        if not isinstance(checksum, str) or not isinstance(metadata, dict):
            raise ValueError(f"本地数据湖分区 checkpoint 非法：{suffix}")

        path = raw_directory / f"{spec.file_prefix}-{suffix}.parquet"
        if not path.is_file():
            raise FileNotFoundError(f"本地数据湖分区文件不存在：{path}")
        try:
            parquet_metadata = pq.read_metadata(path)
            schema = pq.read_schema(path)
        except (OSError, pa.ArrowException) as error:
            raise ValueError(f"本地数据湖分区不是有效 Parquet：{path}") from error
        if tuple(schema.names) != spec.expected_columns:
            raise ValueError(f"本地数据湖分区字段不匹配：{suffix}，实际为 {tuple(schema.names)}")
        row_count = int(metadata.get("row_count", -1))
        if parquet_metadata.num_rows != row_count:
            raise ValueError(f"本地数据湖分区行数与 checkpoint 不一致：{suffix}")
        if self._config.verify_checksums and sha256_file(path) != checksum:
            raise ValueError(f"本地数据湖分区 checksum 不一致：{suffix}")

        return Artifact(
            kind=spec.kind,
            uri=path.resolve().as_uri(),
            checksum=checksum,
            metadata={
                **metadata,
                "source": spec.source,
                "external": True,
                "dataset": self._config.dataset,
            },
        )


def _read_document(path: Path) -> dict[str, Any]:
    if not path.is_file():
        raise FileNotFoundError(f"本地数据湖下载 checkpoint 不存在：{path}")
    document = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(document, dict):
        raise ValueError(f"本地数据湖下载 checkpoint 根节点必须是对象：{path}")
    return document


def _date_range(start_date: date, end_date: date) -> list[date]:
    days = (end_date - start_date).days
    return [start_date + timedelta(days=offset) for offset in range(days + 1)]
