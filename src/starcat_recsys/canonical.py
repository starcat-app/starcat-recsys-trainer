"""原始数据到稳定 Parquet schema 的统一转换层。

训练器只读取本模块生成的 canonical artifact，不依赖任何外部数据源 DTO。
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from starcat_recsys.artifacts import artifact_for, write_json_atomic
from starcat_recsys.domain import (
    InteractionRecord,
    RelationType,
    RepositoryRecord,
    SourceKind,
    parse_datetime,
)
from starcat_recsys.interfaces import Artifact, TaskContext

INTERACTION_SCHEMA = pa.schema(
    [
        ("source", pa.string()),
        ("source_record_id", pa.string()),
        ("source_subject_id", pa.string()),
        ("repo_id", pa.int64()),
        ("relation_type", pa.string()),
        ("occurred_at", pa.timestamp("us", tz="UTC")),
        ("observed_at", pa.timestamp("us", tz="UTC")),
        ("source_weight", pa.float32()),
        ("snapshot_id", pa.string()),
        ("schema_version", pa.int16()),
    ]
)

REPOSITORY_SCHEMA = pa.schema(
    [
        ("repo_id", pa.int64()),
        ("full_name", pa.string()),
        ("description", pa.string()),
        ("topics", pa.list_(pa.string())),
        ("primary_language", pa.string()),
        ("license_key", pa.string()),
        ("stars", pa.int64()),
        ("forks", pa.int64()),
        ("archived", pa.bool_()),
        ("disabled", pa.bool_()),
        ("visibility", pa.string()),
        ("pushed_at", pa.timestamp("us", tz="UTC")),
        ("fetched_at", pa.timestamp("us", tz="UTC")),
        ("metadata_version", pa.int16()),
    ]
)


class CanonicalNormalizer:
    """按 artifact source 路由，生成行为与仓库两类 canonical 数据。"""

    def normalize(self, context: TaskContext, inputs: list[Artifact]) -> list[Artifact]:
        interactions: list[InteractionRecord] = []
        repositories: list[RepositoryRecord] = []
        input_checksums: list[str] = []

        for artifact in inputs:
            input_checksums.append(artifact.checksum)
            source = artifact.metadata.get("source")
            if source == "gh_archive":
                interactions.extend(_normalize_gh_archive(_artifact_path(artifact)))
            elif source in {"starcat_snapshot", "starcat_collection_api"}:
                interactions.extend(_normalize_starcat_snapshot(_artifact_path(artifact)))
            elif source == "github_api":
                repositories.extend(_normalize_github_metadata(_artifact_path(artifact)))
            else:
                raise ValueError(f"不支持的 artifact source：{source}")

        output_directory = context.workspace / "canonical"
        output_directory.mkdir(parents=True, exist_ok=True)
        artifacts: list[Artifact] = []
        if interactions:
            path = output_directory / "interactions.parquet"
            _write_records(
                path, [_interaction_row(record) for record in interactions], INTERACTION_SCHEMA
            )
            artifacts.append(
                artifact_for(path, "canonical_interactions", {"row_count": len(interactions)})
            )
        if repositories:
            path = output_directory / "repositories.parquet"
            _write_records(
                path, [_repository_row(record) for record in repositories], REPOSITORY_SCHEMA
            )
            artifacts.append(
                artifact_for(path, "canonical_repositories", {"row_count": len(repositories)})
            )

        write_json_atomic(
            output_directory / "manifest.json",
            {
                "schema_version": 1,
                "run_id": context.run_id,
                "config_hash": context.config_hash,
                "input_checksums": sorted(input_checksums),
                "artifacts": [
                    {
                        "kind": artifact.kind,
                        "uri": artifact.uri,
                        "checksum": artifact.checksum,
                        "metadata": artifact.metadata,
                    }
                    for artifact in artifacts
                ],
            },
        )
        return artifacts


def _normalize_gh_archive(path: Path) -> list[InteractionRecord]:
    rows = pq.read_table(path).to_pylist()
    records = []
    for row in rows:
        occurred_at = parse_datetime(row["created_at"])
        if occurred_at is None:
            raise ValueError("GH Archive created_at 不能为空")
        subject_id = hashlib.sha256(f"gh_archive:{row['actor_id']}".encode()).hexdigest()
        records.append(
            InteractionRecord(
                source=SourceKind.GH_ARCHIVE,
                source_record_id=str(row["source_record_id"]),
                source_subject_id=subject_id,
                repo_id=int(row["repo_id"]),
                relation_type=RelationType.STAR_EVENT,
                occurred_at=occurred_at,
                observed_at=occurred_at,
            )
        )
    return records


def _normalize_starcat_snapshot(path: Path) -> list[InteractionRecord]:
    records = []
    for row in _read_jsonl(path):
        observed_at = parse_datetime(row["captured_at"])
        if observed_at is None:
            raise ValueError("Starcat snapshot captured_at 不能为空")
        records.append(
            InteractionRecord(
                source=SourceKind.STARCAT_SNAPSHOT,
                source_record_id=str(row["source_record_id"]),
                source_subject_id=str(row["source_subject_id"]),
                repo_id=int(row["repo_id"]),
                relation_type=RelationType.CURRENT_STAR_MEMBERSHIP,
                occurred_at=parse_datetime(row.get("starred_at")),
                observed_at=observed_at,
                snapshot_id=str(row["snapshot_id"]),
            )
        )
    return records


def _normalize_github_metadata(path: Path) -> list[RepositoryRecord]:
    records = []
    for envelope in _read_jsonl(path):
        row = envelope["repository"]
        fetched_at = parse_datetime(envelope["fetched_at"])
        if fetched_at is None:
            raise ValueError("GitHub metadata fetched_at 不能为空")
        license_value = row.get("license") or {}
        visibility = "private" if row.get("private") else row.get("visibility", "public")
        records.append(
            RepositoryRecord(
                repo_id=int(row["id"]),
                full_name=str(row["full_name"]),
                description=row.get("description"),
                topics=tuple(sorted(set(row.get("topics") or []))),
                primary_language=row.get("language"),
                license_key=license_value.get("spdx_id"),
                stars=int(row.get("stargazers_count", 0)),
                forks=int(row.get("forks_count", 0)),
                archived=bool(row.get("archived", False)),
                disabled=bool(row.get("disabled", False)),
                visibility=str(visibility),
                pushed_at=parse_datetime(row.get("pushed_at")),
                fetched_at=fetched_at,
            )
        )
    return records


def _write_records(path: Path, rows: list[dict[str, Any]], schema: pa.Schema) -> None:
    table = pa.Table.from_pylist(rows, schema=schema)
    pq.write_table(table, path, compression="zstd")


def _interaction_row(record: InteractionRecord) -> dict[str, Any]:
    """Parquet timestamp 保留为 datetime，不走 JSON 序列化。"""

    return {
        "source": record.source.value,
        "source_record_id": record.source_record_id,
        "source_subject_id": record.source_subject_id,
        "repo_id": record.repo_id,
        "relation_type": record.relation_type.value,
        "occurred_at": record.occurred_at,
        "observed_at": record.observed_at,
        "source_weight": record.source_weight,
        "snapshot_id": record.snapshot_id,
        "schema_version": record.schema_version,
    }


def _repository_row(record: RepositoryRecord) -> dict[str, Any]:
    return {
        "repo_id": record.repo_id,
        "full_name": record.full_name,
        "description": record.description,
        "topics": list(record.topics),
        "primary_language": record.primary_language,
        "license_key": record.license_key,
        "stars": record.stars,
        "forks": record.forks,
        "archived": record.archived,
        "disabled": record.disabled,
        "visibility": record.visibility,
        "pushed_at": record.pushed_at,
        "fetched_at": record.fetched_at,
        "metadata_version": record.metadata_version,
    }


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    with path.open(encoding="utf-8") as source:
        return [json.loads(line) for line in source if line.strip()]


def _artifact_path(artifact: Artifact) -> Path:
    if not artifact.uri.startswith("file://"):
        raise ValueError(f"当前只支持 file:// artifact：{artifact.uri}")
    return Path(artifact.uri.removeprefix("file://"))
