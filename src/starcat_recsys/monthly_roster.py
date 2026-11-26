"""月度名单实验：仅从留出窗口之前的 GH Archive 行为选择新主体。"""

from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

from starcat_recsys.artifacts import artifact_for
from starcat_recsys.duckdb_runtime import connect_for_workspace
from starcat_recsys.interfaces import Artifact, TaskContext


def propose_roster(
    context: TaskContext, current: Path, core: Path, paths: list[Path], cutoff: datetime
) -> list[Artifact]:
    """等量替换最多一成近期不活跃账号；保留者权重原样，新人沿用历史量权重公式。"""
    recent_start = cutoff - timedelta(days=30)
    history = [str(p) for p in paths if p.stem[-8:] < cutoff.strftime("%Y%m%d")]
    recent = [p for p in history if Path(p).stem[-8:] >= recent_start.strftime("%Y%m%d")]
    if not recent:
        return []
    db = connect_for_workspace(context.workspace, "monthly-roster")
    try:
        db.read_parquet(str(current)).create_view("frozen")
        db.read_parquet(str(core)).create_view("core")
        db.read_parquet(recent).create_view("recent")
        db.read_parquet(history).create_view("history")
        db.execute("""
            CREATE TEMP TABLE roster AS SELECT source_subject_id, max(sample_weight) sample_weight
            FROM frozen WHERE split='train' AND source='gh_archive' GROUP BY source_subject_id
        """)
        db.execute(
            """
            CREATE TEMP TABLE active AS SELECT DISTINCT actor_id,
                sha256('gh_archive:' || actor_id::VARCHAR) source_subject_id
            FROM recent SEMI JOIN core USING(repo_id) WHERE created_at >= ? AND created_at < ?
        """,
            [recent_start, cutoff],
        )
        db.execute("""
            CREATE TEMP TABLE outgoing AS SELECT source_subject_id FROM roster
            ANTI JOIN active USING(source_subject_id)
            ORDER BY md5(source_subject_id), source_subject_id
            LIMIT (SELECT count(*) // 10 FROM roster)
        """)
        db.execute(
            """
            CREATE TEMP TABLE incoming AS SELECT a.source_subject_id,
                (1.0 / log2(2 + count(DISTINCT h.repo_id)))::FLOAT AS sample_weight
            FROM (SELECT * FROM active ANTI JOIN roster USING(source_subject_id)) a
            JOIN history h USING(actor_id) SEMI JOIN core USING(repo_id)
            WHERE h.created_at < ? GROUP BY a.source_subject_id
            HAVING count(DISTINCT h.repo_id) BETWEEN 5 AND 200
            ORDER BY md5(a.source_subject_id), a.source_subject_id
            LIMIT (SELECT count(*) FROM outgoing)
        """,
            [cutoff],
        )
        count = db.sql("SELECT count(*) FROM incoming").fetchone()
        if not count or not count[0]:
            return []
        output = context.workspace / "roster.parquet"
        db.sql("""
            SELECT 'gh_archive' AS source, 'train' AS split, * FROM (
                SELECT * FROM roster ANTI JOIN (
                    SELECT * FROM outgoing ORDER BY md5(source_subject_id), source_subject_id
                    LIMIT (SELECT count(*) FROM incoming)
                ) USING(source_subject_id)
                UNION ALL SELECT * FROM incoming
            ) ORDER BY source_subject_id
        """).write_parquet(str(output))
        output.chmod(0o600)
        return [
            artifact_for(
                output, "monthly_roster", {"replaced": count[0], "cutoff": cutoff.isoformat()}
            )
        ]
    finally:
        db.close()
