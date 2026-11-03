"""从 Canonical 独立构建评估人群，不复用训练的 2% 抽样。

当前入口只处理 GH Archive Star 事件；首次主体—仓库关系取最早事件时间，因此同 ID
复用或重复下载都不会删除历史。主体身份始终包含来源，公开报告只记录聚合统计。
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from starcat_recsys.artifacts import artifact_for, write_json_atomic
from starcat_recsys.domain import parse_datetime
from starcat_recsys.duckdb_runtime import connect_for_workspace
from starcat_recsys.interfaces import Artifact, TaskContext
from starcat_recsys.recommendations import artifact_path


@dataclass(frozen=True, slots=True)
class EvaluationSampleConfig:
    """在查看模型成绩前冻结的窗口、样本预算和抽样盐。"""

    train_end: datetime
    validation_end: datetime
    data_end: datetime
    salt: str = "independent-evaluation-v1"
    maximum_subjects_per_split: int = 10_000
    maximum_candidate_subjects: int = 100_000

    def __post_init__(self) -> None:
        boundaries = [
            parse_datetime(t) for t in (self.train_end, self.validation_end, self.data_end)
        ]
        if any(t is None for t in boundaries):
            raise ValueError("评估时间边界不能为空")
        if not self.train_end < self.validation_end < self.data_end:
            raise ValueError("评估窗口必须严格递增")
        if not self.salt or "'" in self.salt:
            raise ValueError("抽样盐不能为空或包含引号")
        if not 0 < self.maximum_subjects_per_split <= self.maximum_candidate_subjects:
            raise ValueError("主体预算无效")


class IndependentEvaluationBuilder:
    """先固定有后续事件的候选池，再计算训练期准入；不依据模型表现选人。"""

    def __init__(self, config: EvaluationSampleConfig) -> None:
        self._config = config

    def build(self, context: TaskContext, inputs: list[Artifact]) -> Artifact:
        """物化独立 Artifact，保留冷启动统计与全部已知公开目录的真实标签。"""
        config = self._config
        by_kind = {item.kind: item for item in inputs}
        required = {
            "canonical_interactions",
            "canonical_repositories",
            "cohort_repositories",
            "training_dataset",
            "dataset_repositories",
        }
        if not required <= by_kind.keys() or len(by_kind) != len(inputs):
            raise ValueError("独立评估输入缺失或重复")
        output = context.workspace / "evaluation-sample"
        output.mkdir(parents=True, exist_ok=False)
        connection = connect_for_workspace(context.workspace, "evaluation-sample")
        try:
            for kind in sorted(required):
                connection.read_parquet(artifact_path(by_kind[kind]).as_posix()).create_view(kind)
            connection.execute("""
                CREATE TEMP TABLE public_catalog AS
                SELECT repo_id FROM canonical_repositories
                QUALIFY row_number() OVER (
                    PARTITION BY repo_id ORDER BY metadata_version DESC, fetched_at DESC
                ) = 1 AND visibility = 'public' AND NOT archived AND NOT disabled
            """)
            # 候选池不要求命中训练样本，反而显式排除所有训练主体。MD5 只用于稳定顺序；
            # 抽样分组含 source，避免跨来源相同字符串被视为同一个主体。
            connection.execute(
                f"""
                CREATE TEMP TABLE candidate_subjects AS
                SELECT i.source, i.source_subject_id,
                    md5('{config.salt}:' || i.source || ':' || i.source_subject_id) AS sample_hash
                FROM canonical_interactions i
                ANTI JOIN (
                    SELECT DISTINCT source, source_subject_id FROM training_dataset
                    WHERE split = 'train'
                ) t USING (source, source_subject_id)
                WHERE i.source = 'gh_archive' AND i.relation_type = 'star_event'
                    AND i.occurred_at >= ? AND i.occurred_at < ?
                GROUP BY i.source, i.source_subject_id
                ORDER BY sample_hash, i.source, i.source_subject_id
                LIMIT {config.maximum_candidate_subjects}
            """,
                [config.train_end, config.data_end],
            )
            # 只对入选候选池读完整历史，不对 5 亿条事件整体排序或把主体加载到 Python。
            # min(occurred_at) 正是首次 Star 的语义，对重复事件具有幂等性。
            connection.execute(
                """
                CREATE TEMP TABLE first_pairs AS
                SELECT i.source, i.source_subject_id, i.repo_id,
                    min(i.occurred_at) AS occurred_at
                FROM canonical_interactions i
                INNER JOIN candidate_subjects USING (source, source_subject_id)
                WHERE i.source = 'gh_archive' AND i.relation_type = 'star_event'
                    AND i.occurred_at IS NOT NULL AND i.occurred_at < ?
                GROUP BY i.source, i.source_subject_id, i.repo_id
            """,
                [config.data_end],
            )
            connection.execute(
                """
                CREATE TEMP TABLE subject_counts AS
                SELECT c.source, c.source_subject_id, c.sample_hash,
                    count(p.repo_id) FILTER (
                        WHERE p.occurred_at < ? AND p.repo_id IN (
                            SELECT repo_id FROM cohort_repositories
                        )
                    ) AS reference_history,
                    count(p.repo_id) FILTER (
                        WHERE p.occurred_at < ? AND p.repo_id IN (
                            SELECT repo_id FROM dataset_repositories
                        )
                    ) AS seed_history
                FROM candidate_subjects c
                LEFT JOIN first_pairs p USING (source, source_subject_id)
                GROUP BY c.source, c.source_subject_id, c.sample_hash
            """,
                [config.train_end, config.train_end],
            )
            # 用哈希的独立一位分隔 validation 与确认人群。十月已被观察过，确认人群
            # 只能称为新的主体留出，不能声称是未知的前瞻时间测试。
            connection.execute("""
                CREATE TEMP TABLE eligible_subjects AS
                SELECT s.*, CASE WHEN right(sample_hash, 1) IN ('0','1','2','3','4','5','6','7')
                    THEN 'validation' ELSE 'test' END AS evaluation_split
                FROM subject_counts s
                WHERE reference_history BETWEEN 5 AND 200 AND seed_history > 0
            """)
            connection.execute(
                """
                CREATE TEMP TABLE target_subjects AS
                SELECT DISTINCT s.*
                FROM eligible_subjects s
                INNER JOIN first_pairs p USING (source, source_subject_id)
                INNER JOIN public_catalog USING (repo_id)
                WHERE (s.evaluation_split = 'validation' AND p.occurred_at >= ?
                    AND p.occurred_at < ?)
                    OR (s.evaluation_split = 'test' AND p.occurred_at >= ?
                    AND p.occurred_at < ?)
            """,
                [config.train_end, config.validation_end, config.validation_end, config.data_end],
            )
            connection.execute(f"""
                CREATE TEMP TABLE selected_subjects AS
                SELECT * FROM target_subjects
                QUALIFY row_number() OVER (
                    PARTITION BY evaluation_split ORDER BY sample_hash, source, source_subject_id
                ) <= {config.maximum_subjects_per_split}
            """)
            destination = output / "interactions.parquet"
            connection.execute(
                f"""
                COPY (
                    SELECT p.*, p.occurred_at AS observed_at,
                        CASE WHEN p.occurred_at < ? THEN 'train'
                            WHEN p.occurred_at < ? THEN 'validation' ELSE 'test' END AS split,
                        p.repo_id IN (SELECT repo_id FROM dataset_repositories) AS training_eligible
                    FROM first_pairs p
                    INNER JOIN selected_subjects s USING (source, source_subject_id)
                    INNER JOIN public_catalog USING (repo_id)
                    WHERE p.occurred_at < ? OR (
                        s.evaluation_split = 'validation' AND p.occurred_at < ?
                    ) OR (s.evaluation_split = 'test' AND p.occurred_at >= ?)
                    ORDER BY p.source, p.source_subject_id, p.occurred_at, p.repo_id
                ) TO {_literal(destination)} (FORMAT PARQUET, COMPRESSION ZSTD)
            """,
                [
                    config.train_end,
                    config.validation_end,
                    config.train_end,
                    config.validation_end,
                    config.validation_end,
                ],
            )
            aggregate: dict[str, object] = {}
            for name, query in {
                "candidate_subjects": "SELECT count(*) FROM candidate_subjects",
                "history_strata": """SELECT
                    CASE WHEN reference_history=0 THEN '0' WHEN reference_history<5 THEN '1-4'
                        WHEN reference_history<=20 THEN '5-20'
                        WHEN reference_history<=50 THEN '21-50'
                        WHEN reference_history<=200 THEN '51-200' ELSE '>200' END AS stratum,
                    count(*) AS subjects FROM subject_counts GROUP BY stratum ORDER BY stratum""",
                "target_subjects": """SELECT evaluation_split, count(*) AS subjects
                    FROM target_subjects GROUP BY evaluation_split ORDER BY evaluation_split""",
                "selected_subjects": """SELECT evaluation_split, count(*) AS subjects
                    FROM selected_subjects GROUP BY evaluation_split ORDER BY evaluation_split""",
                "unknown_metadata_labels": """SELECT count(*) AS labels
                    FROM first_pairs p INNER JOIN selected_subjects USING(source,source_subject_id)
                    ANTI JOIN public_catalog USING(repo_id) WHERE p.occurred_at >= ?""",
            }.items():
                cursor = connection.execute(
                    query, [config.train_end] if name == "unknown_metadata_labels" else []
                )
                columns = [col[0] for col in cursor.description]
                aggregate[name] = [
                    dict(zip(columns, row, strict=True)) for row in cursor.fetchall()
                ]
        finally:
            connection.close()
        artifact = artifact_for(
            destination,
            "evaluation_dataset",
            {
                "sampling_policy": "independent-subject-holdout-v1",
                "training_subject_overlap": 0,
            },
        )
        write_json_atomic(
            output / "manifest.json",
            {
                "schema_version": 1,
                "run_id": context.run_id,
                "sampling_policy": artifact.metadata["sampling_policy"],
                "config_hash": context.config_hash,
                "salt": config.salt,
                "train_end": config.train_end.isoformat(),
                "validation_end": config.validation_end.isoformat(),
                "data_end": config.data_end.isoformat(),
                "maximum_candidate_subjects": config.maximum_candidate_subjects,
                "maximum_subjects_per_split": config.maximum_subjects_per_split,
                "input_checksums": {kind: by_kind[kind].checksum for kind in sorted(required)},
                "checksum": artifact.checksum,
                "aggregates": aggregate,
                "limitations": [
                    "time_window_previously_observed",
                    "current_metadata_survivorship_bias",
                    "unknown_metadata_labels_not_scored",
                    "warm_start_primary_population_only",
                ],
            },
        )
        return artifact


def _literal(path: Path) -> str:
    """路径只进入 COPY 目标，使用 SQL 字符串转义而非未经处理的拼接。"""
    return "'" + path.as_posix().replace("'", "''") + "'"
