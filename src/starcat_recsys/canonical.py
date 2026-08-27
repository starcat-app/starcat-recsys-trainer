"""原始数据到稳定 Parquet schema 的统一转换层。

训练器只读取本模块生成的 canonical artifact，不依赖任何外部数据源 DTO。
"""

from __future__ import annotations

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
from starcat_recsys.duckdb_runtime import connect_for_workspace
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

    def normalize(
        self,
        context: TaskContext,
        inputs: list[Artifact],
        *,
        existing: list[Artifact] | None = None,
    ) -> list[Artifact]:
        """流式标准化本轮 Raw，并可在延迟 metadata 阶段复用已有 Canonical。

        WatchEvent 和 PushEvent 可能包含数亿行，必须由 DuckDB 直接从 Parquet 写入
        Canonical，不能先转换成 Python 对象。Starcat/GitHub API 的有界 JSONL 仍使用领域
        对象校验，再作为小型 Parquet 参与同一 SQL 合并。
        """

        interactions: list[InteractionRecord] = []
        repositories: list[RepositoryRecord] = []
        gh_archive_paths: list[Path] = []
        push_metadata_paths: list[Path] = []
        push_event_paths: list[Path] = []
        input_checksums = set(_previous_input_checksums(context))

        for artifact in inputs:
            input_checksums.add(artifact.checksum)
            source = artifact.metadata.get("source")
            if source == "gh_archive":
                gh_archive_paths.append(_artifact_path(artifact))
            elif source in {"starcat_snapshot", "starcat_collection_api"}:
                interactions.extend(_normalize_starcat_snapshot(_artifact_path(artifact)))
            elif source == "github_api":
                repositories.extend(_normalize_github_metadata(_artifact_path(artifact)))
            elif source == "gh_archive_push":
                push_metadata_paths.append(_artifact_path(artifact))
            elif source == "gh_archive_push_events":
                push_event_paths.append(_artifact_path(artifact))
            else:
                raise ValueError(f"不支持的 artifact source：{source}")

        output_directory = context.workspace / "canonical"
        output_directory.mkdir(parents=True, exist_ok=True)
        existing = existing or []
        existing_interaction = _optional_artifact(existing, "canonical_interactions")
        existing_repository = _optional_artifact(existing, "canonical_repositories")
        for artifact in existing:
            input_checksums.add(artifact.checksum)

        interaction_artifact = _write_canonical_interactions(
            output_directory,
            gh_archive_paths,
            interactions,
            existing_interaction,
        )
        repository_artifact = _write_canonical_repositories(
            output_directory,
            push_metadata_paths,
            push_event_paths,
            repositories,
            existing_repository,
        )
        artifacts = [
            artifact
            for artifact in (interaction_artifact, repository_artifact)
            if artifact is not None
        ]

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


def _write_canonical_interactions(
    output_directory: Path,
    gh_archive_paths: list[Path],
    interactions: list[InteractionRecord],
    existing: Artifact | None,
) -> Artifact | None:
    """把超大 WatchEvent 关系流式写入一个稳定 Canonical Parquet。"""

    if not gh_archive_paths and not interactions:
        return existing
    if existing is not None:
        raise ValueError("已有 canonical_interactions 时不能追加新的行为 Raw")

    target = output_directory / "interactions.parquet"
    temporary = output_directory / ".interactions.next.parquet"
    small_path = output_directory / ".interactions-small.parquet"
    temporary.unlink(missing_ok=True)
    small_path.unlink(missing_ok=True)
    connection = connect_for_workspace(output_directory.parent, "canonical-interactions")
    try:
        queries: list[str] = []
        if gh_archive_paths:
            connection.read_parquet(
                [path.as_posix() for path in sorted(gh_archive_paths)]
            ).create_view("gh_archive_raw")
            missing_row = connection.execute(
                "SELECT count(*) FROM gh_archive_raw WHERE created_at IS NULL"
            ).fetchone()
            missing_created_at = int(missing_row[0] if missing_row is not None else 0)
            if missing_created_at:
                raise ValueError("GH Archive created_at 不能为空")
            queries.append(
                """
                SELECT
                    'gh_archive'::VARCHAR AS source,
                    CAST(source_record_id AS VARCHAR) AS source_record_id,
                    sha256('gh_archive:' || CAST(actor_id AS VARCHAR)) AS source_subject_id,
                    CAST(repo_id AS BIGINT) AS repo_id,
                    'star_event'::VARCHAR AS relation_type,
                    CAST(created_at AS TIMESTAMPTZ) AS occurred_at,
                    CAST(created_at AS TIMESTAMPTZ) AS observed_at,
                    1.0::FLOAT AS source_weight,
                    NULL::VARCHAR AS snapshot_id,
                    1::SMALLINT AS schema_version
                FROM gh_archive_raw
                """
            )
        if interactions:
            _write_records(
                small_path,
                [_interaction_row(record) for record in interactions],
                INTERACTION_SCHEMA,
            )
            connection.read_parquet(small_path.as_posix()).create_view("small_interactions")
            queries.append("SELECT * FROM small_interactions")
        connection.execute(
            f"COPY ({' UNION ALL '.join(queries)}) TO {_sql_literal(temporary)} "
            "(FORMAT PARQUET, COMPRESSION ZSTD)"
        )
        temporary.replace(target)
    finally:
        connection.close()
        temporary.unlink(missing_ok=True)
        small_path.unlink(missing_ok=True)

    row_count = pq.read_metadata(target).num_rows
    return artifact_for(target, "canonical_interactions", {"row_count": row_count})


def _write_canonical_repositories(
    output_directory: Path,
    push_metadata_paths: list[Path],
    push_event_paths: list[Path],
    repositories: list[RepositoryRecord],
    existing: Artifact | None,
) -> Artifact | None:
    """流式合并基础 PushEvent 目录和有界 GitHub 权威 metadata。"""

    if not push_metadata_paths and not push_event_paths and not repositories:
        return existing

    target = output_directory / "repositories.parquet"
    temporary = output_directory / ".repositories.next.parquet"
    small_path = output_directory / ".repositories-small.parquet"
    temporary.unlink(missing_ok=True)
    small_path.unlink(missing_ok=True)
    connection = connect_for_workspace(output_directory.parent, "canonical-repositories")
    try:
        queries: list[str] = []
        if existing is not None:
            connection.read_parquet(_artifact_path(existing).as_posix()).create_view(
                "existing_repositories"
            )
            queries.append("SELECT * FROM existing_repositories")
        if push_metadata_paths:
            connection.read_parquet(
                [path.as_posix() for path in sorted(push_metadata_paths)]
            ).create_view("push_metadata_raw")
            queries.append(_push_metadata_query("push_metadata_raw"))
        if push_event_paths:
            connection.read_parquet(
                [path.as_posix() for path in sorted(push_event_paths)]
            ).create_view("push_events_raw")
            queries.append(_push_events_query("push_events_raw"))
        if repositories:
            # GitHub API 的数量受 maximum_requests 约束；先走领域模型校验，再以小型
            # Parquet 参与流式优先级合并，避免对历史仓库目录执行 Python 字典聚合。
            _write_records(
                small_path,
                [_repository_row(record) for record in repositories],
                REPOSITORY_SCHEMA,
            )
            connection.read_parquet(small_path.as_posix()).create_view("small_repositories")
            queries.append("SELECT * FROM small_repositories")
        union_query = " UNION ALL ".join(queries)
        connection.execute(
            f"""
            COPY (
                SELECT * EXCLUDE (metadata_rank)
                FROM (
                    SELECT *, row_number() OVER (
                        PARTITION BY repo_id
                        ORDER BY metadata_version DESC, fetched_at DESC, full_name DESC
                    ) AS metadata_rank
                    FROM ({union_query})
                )
                WHERE metadata_rank = 1
                ORDER BY repo_id
            ) TO {_sql_literal(temporary)} (FORMAT PARQUET, COMPRESSION ZSTD)
            """
        )
        temporary.replace(target)
    finally:
        connection.close()
        temporary.unlink(missing_ok=True)
        small_path.unlink(missing_ok=True)

    row_count = pq.read_metadata(target).num_rows
    return artifact_for(target, "canonical_repositories", {"row_count": row_count})


def _push_metadata_query(view_name: str) -> str:
    return f"""
        SELECT
            CAST(repo_id AS BIGINT) AS repo_id,
            CAST(full_name AS VARCHAR) AS full_name,
            NULL::VARCHAR AS description,
            []::VARCHAR[] AS topics,
            NULL::VARCHAR AS primary_language,
            NULL::VARCHAR AS license_key,
            0::BIGINT AS stars,
            0::BIGINT AS forks,
            false AS archived,
            false AS disabled,
            'public'::VARCHAR AS visibility,
            CAST(last_push_at AS TIMESTAMPTZ) AS pushed_at,
            CAST(fetched_at AS TIMESTAMPTZ) AS fetched_at,
            0::SMALLINT AS metadata_version
        FROM {view_name}
    """


def _push_events_query(view_name: str) -> str:
    return f"""
        WITH ranked AS (
            SELECT
                CAST(repo_id AS BIGINT) AS repo_id,
                CAST(full_name AS VARCHAR) AS full_name,
                CAST(created_at AS TIMESTAMPTZ) AS created_at,
                row_number() OVER (
                    PARTITION BY repo_id ORDER BY created_at DESC, full_name DESC
                ) AS name_rank
            FROM {view_name}
            WHERE repo_id IS NOT NULL AND full_name IS NOT NULL AND created_at IS NOT NULL
        ),
        statistics AS (
            SELECT repo_id, max(created_at) AS last_push_at
            FROM ranked
            GROUP BY repo_id
        )
        SELECT
            ranked.repo_id,
            ranked.full_name,
            NULL::VARCHAR AS description,
            []::VARCHAR[] AS topics,
            NULL::VARCHAR AS primary_language,
            NULL::VARCHAR AS license_key,
            0::BIGINT AS stars,
            0::BIGINT AS forks,
            false AS archived,
            false AS disabled,
            'public'::VARCHAR AS visibility,
            statistics.last_push_at AS pushed_at,
            statistics.last_push_at AS fetched_at,
            0::SMALLINT AS metadata_version
        FROM ranked
        INNER JOIN statistics USING (repo_id)
        WHERE name_rank = 1
    """


def _optional_artifact(inputs: list[Artifact], kind: str) -> Artifact | None:
    matches = [artifact for artifact in inputs if artifact.kind == kind]
    if len(matches) > 1:
        raise ValueError(f"最多允许一个 {kind} artifact，实际为 {len(matches)}")
    return matches[0] if matches else None


def _previous_input_checksums(context: TaskContext) -> list[str]:
    manifest_path = context.workspace / "canonical" / "manifest.json"
    if not manifest_path.is_file():
        return []
    document = json.loads(manifest_path.read_text(encoding="utf-8"))
    values = document.get("input_checksums", []) if isinstance(document, dict) else []
    return [str(value) for value in values] if isinstance(values, list) else []


def _sql_literal(path: Path) -> str:
    return "'" + path.as_posix().replace("'", "''") + "'"


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
