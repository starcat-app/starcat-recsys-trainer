"""每周训练的固定输入与有界 Dataset。

Raw 保持只读；沿用已批准的训练主体、权重和候选目录。只保留这些主体与本周留出
主体的首次 Star，避免把十年事件复制进每个 run，也避免重新抽样改变评估含义。
"""

from __future__ import annotations

import json
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from starcat_recsys.artifacts import artifact_for, sha256_file, write_json_atomic
from starcat_recsys.duckdb_runtime import connect_for_workspace
from starcat_recsys.interfaces import Artifact, TaskContext


class RawValidationError(ValueError):
    """可公开的分区错误，只携带日期与检查类别，不包含路径或底层错误正文。"""


def verify_raw(root: Path, target: date) -> list[Path]:
    """核对 checkpoint 的连续日期、实际文件、footer、行数和摘要；不访问 BigQuery。"""
    state = json.loads((root / "download-state.json").read_text())
    partitions = state["completed_partitions"]
    start = date.fromisoformat(state["scope"]["start_date"])
    if start > target:
        raise RawValidationError("Raw 起点晚于目标水位")
    paths = []
    day = start
    while day <= target:
        key = day.strftime("%Y%m%d")
        if key not in partitions:
            raise RawValidationError(f"Raw 缺失日期 {day.isoformat()}")
        record = partitions[key]
        path = root / f"watch-events-{key}.parquet"
        try:
            parquet = pq.ParquetFile(path)
        except (OSError, pa.ArrowException) as error:
            raise RawValidationError(f"Raw 文件缺失或损坏 {key}") from error
        required = {"source_record_id", "actor_id", "repo_id", "created_at"}
        if not required <= set(parquet.schema_arrow.names):
            raise RawValidationError(f"Raw schema 不符 {key}")
        if parquet.metadata.num_rows != record["metadata"]["row_count"]:
            raise RawValidationError(f"Raw 行数不符 {key}")
        if sha256_file(path) != record["checksum"]:
            raise RawValidationError(f"Raw checksum 不符 {key}")
        paths.append(path)
        if len(paths) % 500 == 0:
            print(json.dumps({"stage": "verify_raw", "partitions": len(paths)}), flush=True)
        day += timedelta(days=1)
    return paths


def prepare_datasets(
    context: TaskContext,
    config: dict[str, Any],
    paths: list[Path],
    train_end: datetime,
    data_end: datetime,
    challenger: Path | None = None,
) -> list[Artifact]:
    """冻结训练人群与新时间留出；训练、评估均使用首次关系，禁止未来行为进入 train。

    直接从 Raw 重新计算首次关系，避开旧 Canonical 曾有的跨年原始 ID 去重错误。
    评估主体完全排除固定训练人群，seed 最多取 20 个，真值不按行为训练目录裁剪。
    """
    output = context.workspace / "dataset"
    output.mkdir(parents=True, exist_ok=True)
    connection = connect_for_workspace(context.workspace, "weekly-dataset")
    try:
        connection.read_parquet([str(path) for path in paths]).create_view("raw")
        connection.read_parquet(config["frozen_training_dataset"]).create_view("frozen")
        connection.read_parquet(config["core_catalog"]).create_view("core")
        connection.read_parquet(config["expanded_catalog"]).create_view("catalog")
        connection.execute("""
            CREATE TEMP TABLE weights AS
            SELECT source, source_subject_id, max(sample_weight)::FLOAT AS sample_weight
            FROM frozen WHERE split='train' GROUP BY source, source_subject_id
        """)
        foreign = connection.execute(
            "SELECT count(*) FROM weights WHERE source <> 'gh_archive'"
        ).fetchone()
        if foreign and foreign[0]:
            raise ValueError("每周 WatchEvent 训练只允许 gh_archive 固定主体")
        # 先取两份名单的并集：同一份首次关系表供两组训练使用，评估排除整个并集。
        connection.execute("CREATE TEMP VIEW excluded AS SELECT source_subject_id FROM weights")
        if challenger is not None:
            connection.read_parquet(str(challenger)).create_view("new_weights")
            connection.execute("""CREATE OR REPLACE TEMP VIEW excluded AS
                SELECT source_subject_id FROM weights
                UNION SELECT source_subject_id FROM new_weights""")
        connection.execute("""
            CREATE TEMP VIEW events AS
            SELECT sha256('gh_archive:' || CAST(actor_id AS VARCHAR)) AS source_subject_id,
                repo_id::BIGINT AS repo_id, created_at::TIMESTAMPTZ AS occurred_at
            FROM raw
        """)
        connection.execute(
            """
            CREATE TEMP TABLE recent_subjects AS
            SELECT source_subject_id FROM events
            WHERE occurred_at >= ? AND occurred_at < ?
            GROUP BY source_subject_id
            ORDER BY md5(source_subject_id), source_subject_id LIMIT 1000000
            """,
            [train_end, data_end],
        )
        connection.execute(
            """
            CREATE TEMP TABLE first_pairs AS
            SELECT e.source_subject_id, e.repo_id, min(e.occurred_at) AS occurred_at
            FROM events e
            INNER JOIN (SELECT source_subject_id FROM excluded
                UNION SELECT source_subject_id FROM recent_subjects) s USING(source_subject_id)
            INNER JOIN catalog USING(repo_id)
            WHERE e.occurred_at < ?
            GROUP BY e.source_subject_id, e.repo_id
            """,
            [data_end],
        )
        sets = [("weights", "")]
        if challenger is not None:
            sets.append(("new_weights", "challenger_"))
        training_files = []
        for weights_table, prefix in sets:
            for name, kind, end in [
                ("interactions", "training_dataset", train_end),
                ("refit", "refit_dataset", data_end),
            ]:
                path = output / f"{prefix}{name}.parquet"
                connection.execute(
                    f"""
                    COPY (SELECT 'gh_archive' AS source, p.*, p.occurred_at AS observed_at,
                        1.0::FLOAT AS source_weight, w.sample_weight, TRUE AS training_eligible,
                        'train' AS split FROM first_pairs p
                    INNER JOIN {weights_table} w USING(source_subject_id)
                    INNER JOIN core USING(repo_id) WHERE p.occurred_at < ?)
                    TO {_literal(path)} (FORMAT PARQUET, COMPRESSION ZSTD)
                """,
                    [end],
                )
                training_files.append((path, prefix + kind))
        # 用训练前的完整参考目录历史量准入；既不按新目录重新选训练人，也不按成绩选评估人。
        connection.execute(
            """
            CREATE TEMP TABLE evaluation_subjects AS
            SELECT p.source_subject_id
            FROM first_pairs p ANTI JOIN excluded USING(source_subject_id)
            GROUP BY p.source_subject_id
            HAVING count(*) FILTER (WHERE p.occurred_at < ? AND p.repo_id IN (
                SELECT repo_id FROM core)) BETWEEN 5 AND 200
                AND count(*) FILTER (WHERE p.occurred_at >= ?) > 0
            ORDER BY md5(p.source_subject_id), p.source_subject_id LIMIT 10000
            """,
            [train_end, train_end],
        )
        evaluation = output / "evaluation.parquet"
        connection.execute(
            f"""
            COPY (SELECT 'gh_archive' AS source, p.*, p.occurred_at AS observed_at,
                p.repo_id IN (SELECT repo_id FROM core) AS training_eligible,
                CASE WHEN p.occurred_at < ? THEN 'train' ELSE 'test' END AS split
            FROM first_pairs p INNER JOIN evaluation_subjects USING(source_subject_id))
            TO {_literal(evaluation)} (FORMAT PARQUET, COMPRESSION ZSTD)
            """,
            [train_end],
        )
        training_count = connection.execute("SELECT count(*) FROM weights").fetchone()
        evaluation_count = connection.execute("SELECT count(*) FROM evaluation_subjects").fetchone()
        counts = {
            "training_subjects": training_count[0] if training_count else 0,
            "evaluation_subjects": evaluation_count[0] if evaluation_count else 0,
            "train_end": train_end.isoformat(),
            "data_end": data_end.isoformat(),
            "training_subject_overlap": 0,
            "policy": "fixed_training_subjects_weights_new_time_holdout",
            "limitations": ["frozen_metadata_snapshot", "warm_start_known_public_catalog"],
        }
        write_json_atomic(output / "population.json", counts)
    finally:
        connection.close()
    return [
        *(artifact_for(path, kind, counts) for path, kind in training_files),
        artifact_for(evaluation, "evaluation_dataset", counts),
        artifact_for(Path(config["core_catalog"]), "dataset_repositories", {}),
    ]


def _literal(path: Path) -> str:
    """COPY 目标为本地路径，只按 SQL 字符串规则转义。"""
    return "'" + str(path).replace("'", "''") + "'"


def utc_day_end(day: date) -> datetime:
    """水位是完整 UTC 日，训练/评估统一使用次日零点的半开边界。"""
    return datetime.combine(day + timedelta(days=1), datetime.min.time(), UTC)
