"""模型成果的公开索引与 Markdown 生成。

运行记录只保留显式公开字段。训练成功、模型上线与 GitHub 分发状态独立记录；
历史字段缺失保持 null，生成器不扫描 Raw、不复制私有配置或逐主体评估文件。
"""

from __future__ import annotations

import json
import math
import re
import sqlite3
from pathlib import Path
from typing import Any

from starcat_recsys.artifacts import sha256_file, write_json_atomic
from starcat_recsys.bundle import verify_bundle

STATUSES = {"published", "evaluated", "blocked", "failed", "data_only", "unverified"}
METRIC_NAMES = {
    "coverage",
    "empty_result_rate",
    "evaluated_subjects",
    "skipped_subjects",
    "catalog_recall",
    "catalog_truth_count",
    "truth_count",
    "truth_catalog_coverage",
}
PUBLIC_FIELDS = (
    "model_version",
    "selected_model",
    "created_at",
    "git_commit",
    "config_hash",
    "metrics_scope",
    "evaluation_checksum",
    "evaluated_model_checksum",
    "raw_watermark",
)


def identifier(value: str) -> str:
    """版本也是生成文件名；复用 Bundle 的稳定字符范围，禁止路径片段。"""
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}", value) or ".." in value:
        raise ValueError("成果标识只能包含字母、数字、点、下划线和连字符")
    return value


def public_metrics(document: dict[str, Any]) -> dict[str, int | float]:
    """只公开聚合数值，不把评估文档中的主体数组或任意字符串透传出去。"""
    return {
        key: value
        for key, value in document.items()
        if (key in METRIC_NAMES or re.fullmatch(r"(recall|ndcg|mrr)@\d+", key))
        and isinstance(value, (int, float))
        and not isinstance(value, bool)
        and math.isfinite(value)
    }


def bundle_record(bundle: Path, *, status: str = "evaluated") -> dict[str, Any]:
    """从校验后的不可变 Bundle 构建公开记录；不会把本地 Registry 激活当线上发布。"""
    if status not in STATUSES:
        raise ValueError("未知成果状态")
    manifest = verify_bundle(bundle)
    version = identifier(str(manifest["model_version"]))
    with sqlite3.connect(
        (bundle / "recommendations.sqlite").resolve().as_uri() + "?mode=ro", uri=True
    ) as database:
        repositories = database.execute("SELECT count(*) FROM repositories").fetchone()[0]
        edges = database.execute("SELECT count(*) FROM recommendations").fetchone()[0]
    result = {key: manifest.get(key) for key in PUBLIC_FIELDS}
    result.update(
        {
            "run_id": version,
            "status": status,
            "metrics": public_metrics(manifest.get("metrics", {})),
            "manifest_checksum": sha256_file(bundle / "manifest.json"),
            "repositories": repositories,
            "recommendation_edges": edges,
            "database_bytes": (bundle / "recommendations.sqlite").stat().st_size,
            "training": _public_training(manifest.get("training")),
            "evaluation": _public_evaluation(manifest.get("evaluation")),
            "release_url": None,
        }
    )
    return result


def _public_training(value: Any) -> dict[str, Any] | None:
    """按元数据契约导出时间、计数和白名单参数。"""
    if not isinstance(value, dict):
        return None
    from starcat_recsys.model_metadata import PARAMETERS

    return {
        **{
            key: value.get(key)
            for key in (
                "started_at",
                "completed_at",
                "data_start",
                "data_end",
                "interactions",
                "subjects",
            )
        },
        "parameters": {
            key: parameter
            for key, parameter in value.get("parameters", {}).items()
            if key in PARAMETERS
            and isinstance(parameter, (int, float))
            and math.isfinite(parameter)
        },
    }


def _public_evaluation(value: Any) -> dict[str, Any] | None:
    """只导出评估协议，不导出 artifact URI 或逐主体观测。"""
    if not isinstance(value, dict):
        return None
    return {
        key: value.get(key)
        for key in (
            "split",
            "k",
            "maximum_seeds",
            "dataset_checksum",
            "train_end",
            "validation_end",
            "data_end",
        )
    }


def load_catalog(path: Path) -> dict[str, Any]:
    """首次创建空索引；损坏文件必须报错，不能静默清掉历史。"""
    if not path.exists():
        return {"schema_version": 1, "runs": []}
    document: dict[str, Any] = json.loads(path.read_text())
    if document.get("schema_version") != 1 or not isinstance(document.get("runs"), list):
        raise ValueError("成果索引格式不正确")
    return document


def record_result(catalog: Path, record: dict[str, Any]) -> None:
    """同一 run 可更新交付状态，已存在的不可变 Bundle 摘要不能变。"""
    key = identifier(str(record["run_id"]))
    if record["status"] not in STATUSES:
        raise ValueError("未知成果状态")
    document = load_catalog(catalog)
    previous: dict[str, Any] = next(
        (item for item in document["runs"] if item["run_id"] == key), {}
    )
    if (
        previous.get("manifest_checksum")
        and record.get("manifest_checksum")
        and previous["manifest_checksum"] != record["manifest_checksum"]
    ):
        raise ValueError("同一成果的 Bundle 摘要发生变化")
    merged = {**previous, **record}
    if previous.get("release_url") and record.get("release_url") is None:
        merged["release_url"] = previous["release_url"]
    document["runs"] = [item for item in document["runs"] if item["run_id"] != key]
    document["runs"].append(merged)
    document["runs"].sort(
        key=lambda item: (
            item.get("iteration") is None,
            item.get("iteration") or 0,
            item.get("created_at") or "",
            item["run_id"],
        )
    )
    write_json_atomic(catalog, document)


def model_card(record: dict[str, Any]) -> str:
    """同一模型说明供文档和 Release 使用，始终展示评估范围与缺证字段。"""
    identity = record.get("model_version") or record["run_id"]
    lines = [
        f"# {identity}",
        "",
        f"- Status / 状态: `{record['status']}`",
        f"- Algorithm / 算法: `{record.get('selected_model') or 'unknown'}`",
        f"- Bundle created / 打包时间: `{record.get('created_at') or 'unknown'}`",
        f"- Raw watermark / 数据水位: `{record.get('raw_watermark') or 'unknown'}`",
        f"- Metrics scope / 指标口径: `{record.get('metrics_scope') or 'unknown'}`",
        f"- Source commit / 源码提交: `{record.get('git_commit') or 'unknown'}`",
        "",
        "Missing historical values are unknown, not zero. / 历史缺失值未知，不代表零。",
        "Different evaluation datasets are not directly comparable. / 不同评估集不可直接比较。",
        "pre_refit means these metrics describe the evaluated candidate, not the final refit.",
        "`pre_refit` 表示重训前候选的成绩，不是最终重训模型再次实测。",
        "",
    ]
    if record.get("summary"):
        lines.extend([str(record["summary"]), ""])
    for title, value in (
        ("Training / 训练", record.get("training")),
        ("Evaluation protocol / 评估协议", record.get("evaluation")),
        ("Metrics / 指标", record.get("metrics")),
        ("Decision / 检查结论", record.get("decision")),
        ("Data processing / 数据处理", record.get("data_processing")),
    ):
        lines.extend(
            [
                f"## {title}",
                "",
                "```json",
                json.dumps(value, ensure_ascii=False, indent=2),
                "```",
                "",
            ]
        )
    lines.extend(
        [
            "## Artifact / 产物",
            "",
            f"- Repositories / 仓库: {record.get('repositories', 'unknown')}",
            f"- Edges / 推荐边: {record.get('recommendation_edges', 'unknown')}",
            f"- SQLite bytes: {record.get('database_bytes', 'unknown')}",
            f"- Manifest SHA-256: `{record.get('manifest_checksum') or 'unknown'}`",
            f"- Release: {record.get('release_url') or 'not published / 未公开发布'}",
            "- Evidence / 证据: "
            + (", ".join(record.get("evidence", [])) or "manifest and checksums"),
            "",
            "The bundle contains public repository metadata, aggregate edges and a content index.",
            "It excludes raw events, training subjects and private Starcat snapshots.",
            "模型只含公开仓库元数据、聚合推荐边和内容索引，不含原始事件、训练主体或私有快照。",
            "",
        ]
    )
    return "\n".join(lines)


def render_history(catalog: Path, output: Path) -> list[Path]:
    """生成固定目录下的历史页与模型卡；不改其他文档或仓库文件。"""
    document = load_catalog(catalog)
    cards = output / "models"
    cards.mkdir(parents=True, exist_ok=True)
    lines = [
        "# Model history / 模型训练成果",
        "",
        "Generated from `models/history.json`. / 由机器可读索引生成，请勿手工改表。",
        "Iterations include unsuccessful experiments and data fixes; they are not a release count.",
        "版本包含失败实验和数据修复，不等于训练次数或正式发布次数。",
        "Compare metrics only under the same evaluation protocol. Missing evidence stays unknown.",
        "仅在相同评估口径下比较指标，缺失证据保留未知。",
        "",
        "| Iteration | Run / model | Status | Recall@20 | NDCG@20 | Subjects | Scope |",
        "| --- | --- | --- | ---: | ---: | ---: | --- |",
    ]
    files: list[Path] = []
    for record in document["runs"]:
        run_id = identifier(str(record["run_id"]))
        metrics = record.get("metrics") or {}
        path = cards / f"{run_id}.md"
        path.write_text(model_card(record))
        files.append(path)
        values = [
            str(record.get("iteration") or "weekly"),
            f"[{run_id}](models/{run_id}.md)",
            record["status"],
            *[str(metrics.get(key, "—")) for key in ("recall@20", "ndcg@20", "evaluated_subjects")],
            record.get("metrics_scope") or "unknown",
        ]
        lines.append("| " + " | ".join(values) + " |")
    index = output / "MODEL_HISTORY.md"
    index.write_text("\n".join(lines) + "\n")
    destination = cards / "history.json"
    if destination.resolve() != catalog.resolve():
        write_json_atomic(destination, document)
    return [index, destination, *files]
