"""每周固定配方重训、同输入评估与不可变 Bundle 构建。

保持 v19 的 75% co-star / 25% 内容排名融合，并保留 v20 独立内容索引。
固定配方不在 test 上选参数；最终 refit 明确引用重训前评估，不冒充同模型实测。
"""

from __future__ import annotations

import json
import math
import sqlite3
from dataclasses import replace
from datetime import datetime
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from starcat_recsys.artifacts import artifact_for, sha256_file, write_json_atomic
from starcat_recsys.bundle import (
    BundleConfig,
    LocalModelRegistry,
    ServingBundlePublisher,
    verify_bundle,
)
from starcat_recsys.interfaces import Artifact, TaskContext
from starcat_recsys.recommendations import RECOMMENDATION_SCHEMA, artifact_path
from starcat_recsys.trainers.content import ContentConfig, ContentTrainer, fuse_models
from starcat_recsys.trainers.costar import CoStarConfig, CoStarTrainer


def train_model(
    context: TaskContext,
    inputs: list[Artifact],
    reference_time: datetime,
    content: Artifact | None = None,
) -> list[Artifact]:
    """复用已验收参数和严格配对预算；预算不足直接失败，不静默换训练主体。"""
    costar = CoStarTrainer(
        CoStarConfig(
            reference_time=reference_time,
            minimum_support=0.5,
            maximum_repositories_per_subject=100,
            maximum_pair_observations=200_000_000,
            require_full_pair_budget=True,
        )
    ).train(context, inputs)[0]
    content = content or ContentTrainer(ContentConfig()).train(context, inputs)[0]
    return [content, fuse_models(context, costar, content, content_weight=0.25)]


def export_incumbent(context: TaskContext, bundle: Path) -> Artifact:
    """按原始边分数导出线上 Bundle；不使用展示分，也不把旧训练报告当本周基线。"""
    verify_bundle(bundle)
    output = context.workspace / "incumbent.parquet"
    with sqlite3.connect(f"file:{bundle / 'recommendations.sqlite'}?mode=ro", uri=True) as db:
        db.row_factory = sqlite3.Row
        cursor = db.execute(
            "SELECT source_repo_id,target_repo_id,score,rank,signals "
            "FROM recommendations ORDER BY source_repo_id,rank"
        )
        with pq.ParquetWriter(output, RECOMMENDATION_SCHEMA, compression="zstd") as writer:
            while batch := cursor.fetchmany(10000):
                rows = [{**dict(row), "model": "incumbent"} for row in batch]
                writer.write_table(pa.Table.from_pylist(rows, schema=RECOMMENDATION_SCHEMA))
    return artifact_for(output, "model_recommendations", {"model": "incumbent"})


def quality_decision(document: dict[str, Any]) -> dict[str, Any]:
    """执行已确认的硬门槛；缺指标、NaN 和无有效人群均不能获得发布许可。"""
    candidate = document["models"]["hybrid_25"]
    incumbent = document["models"]["incumbent"]
    fields = ["recall@20", "ndcg@20", "empty_result_rate", "evaluated_subjects"]
    finite = all(
        math.isfinite(float(metrics[field]))
        for metrics in [candidate, incumbent]
        for field in fields
    )
    checks = {
        "finite_metrics": finite,
        "same_population": candidate["evaluated_subjects"] == incumbent["evaluated_subjects"],
        "minimum_5000_subjects": candidate["evaluated_subjects"] >= 5000,
        "recall_not_lower": candidate["recall@20"] >= incumbent["recall@20"],
        "ndcg_not_lower": candidate["ndcg@20"] >= incumbent["ndcg@20"],
        "empty_rate_not_higher": candidate["empty_result_rate"] <= incumbent["empty_result_rate"],
        "empty_rate_at_most_10_percent": candidate["empty_result_rate"] <= 0.1,
    }
    return {
        "passed": all(checks.values()),
        "checks": checks,
        "candidate": candidate,
        "incumbent": incumbent,
    }


def build_bundle(
    context: TaskContext,
    inputs: list[Artifact],
    model: Artifact,
    metrics: Artifact,
    index: Artifact,
    expanded: Artifact,
    version: str,
    watermark: str,
    *,
    refit: bool,
) -> Artifact:
    """候选只在 run 内落盘；补齐水位后封存摘要，不修改共享 Registry。"""
    scope = "pre_refit" if refit else "same_model"
    artifact = ServingBundlePublisher(
        BundleConfig(version, selected_model="hybrid_25", install=False, metrics_scope=scope),
        LocalModelRegistry(context.workspace / "unused-registry"),
    ).publish(context, [model, metrics, index, expanded])[0]
    bundle = artifact_path(artifact).parent
    manifest = verify_bundle(bundle)
    manifest["raw_watermark"] = watermark
    manifest["weekly_protocol"] = context.config_hash
    manifest["evaluation_train_end"] = next(
        item.metadata["train_end"] for item in inputs if item.kind == "training_dataset"
    )
    write_json_atomic(bundle / "manifest.json", manifest)
    checksums = json.loads((bundle / "checksums.json").read_text())
    checksums["manifest.json"] = sha256_file(bundle / "manifest.json")
    write_json_atomic(bundle / "checksums.json", checksums)
    verify_bundle(bundle)
    return artifact_for(bundle / "manifest.json", "serving_bundle", artifact.metadata)


def refit_inputs(inputs: list[Artifact]) -> list[Artifact]:
    """只推进原固定训练人群的行为截止时间，不把留出主体纳入最终重训。"""
    return [
        replace(item, kind="training_dataset") if item.kind == "refit_dataset" else item
        for item in inputs
        if item.kind in {"refit_dataset", "dataset_repositories"}
    ]


def check_coverage(candidate: Path, incumbent: Path) -> dict[str, Any]:
    """旧版有出边的源不得变空，旧内容索引目标也不得丢失；不要求排序原样不变。"""
    with sqlite3.connect(f"file:{candidate / 'recommendations.sqlite'}?mode=ro", uri=True) as db:
        db.execute(
            "ATTACH DATABASE ? AS old", (f"file:{incumbent / 'recommendations.sqlite'}?mode=ro",)
        )
        missing = db.execute(
            "SELECT count(*) FROM (SELECT DISTINCT source_repo_id "
            "FROM old.recommendations EXCEPT SELECT DISTINCT source_repo_id "
            "FROM recommendations)"
        ).fetchone()[0]
        missing_index = db.execute(
            "SELECT count(*) FROM (SELECT DISTINCT repo_id "
            "FROM old.content_postings EXCEPT SELECT DISTINCT repo_id "
            "FROM content_postings)"
        ).fetchone()[0]
        invalid = db.execute(
            "SELECT count(*) FROM recommendations WHERE source_repo_id=target_repo_id OR score<=0"
        ).fetchone()[0]
    return {
        "passed": missing == missing_index == invalid == 0,
        "missing_sources": missing,
        "missing_index_repositories": missing_index,
        "invalid_edges": invalid,
    }
