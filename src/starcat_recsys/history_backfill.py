"""从已有训练成果回补公开历史，不访问 Raw 或执行训练。

仅查看固定层级的 manifest、评估摘要与目录存在性。版本号可能属于不同实验系列，
因此保留完整 run_id；找不到完成证据时写 unverified，不把目录存在称作训练完成。
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

from starcat_recsys.model_history import (
    bundle_record,
    identifier,
    public_metrics,
    record_result,
    render_history,
)
from starcat_recsys.weekly_reporting import weekly_record


def backfill_history(root: Path, catalog: Path, output: Path) -> int:
    """只写指定公开目录；旧模型的 checksum、manifest 和 Registry 均保持不变。"""
    manifests = set(root.glob("registry/versions/*/manifest.json"))
    for pattern in (
        "gharchive*/published/*/manifest.json",
        "gharchive*/delivery*/published/*/manifest.json",
        "gharchive*/refit*/published/*/manifest.json",
    ):
        manifests.update(root.glob(pattern))
    covered: set[str] = set()
    count = 0
    for path in sorted(manifests):
        try:
            record: dict[str, Any] = bundle_record(path.parent)
        except (OSError, ValueError):
            # 部分早期卷带有已变化的 AppleDouble 校验项；不改旧包，也不把失败跳成通过。
            manifest = json.loads(path.read_text())
            version = identifier(str(manifest["model_version"]))
            record = {
                "run_id": version,
                "model_version": version,
                "status": "unverified",
                "selected_model": manifest.get("selected_model"),
                "created_at": manifest.get("created_at"),
                "metrics": public_metrics(manifest.get("metrics", {})),
                "metrics_scope": manifest.get("metrics_scope"),
                "evidence": ["manifest.json (Bundle validation failed)"],
                "summary": (
                    "Historical Bundle validation failed; metrics are unverified manifest records. "
                    "历史 Bundle 校验失败，指标仅为未核实的 manifest 记录，不可直接分发。"
                ),
            }
        version = str(record["model_version"])
        match = re.search(r"-v(\d+)(?:-|$)", version)
        record["iteration"] = int(match[1]) if match else None
        record.setdefault(
            "summary",
            (
                "Recovered from an immutable Bundle; "
                "local installation is not deployment evidence. "
                "从不可变 Bundle 恢复；本地安装不代表曾上线。"
            ),
        )
        record.setdefault("evidence", ["manifest.json", "checksums.json", "recommendations.sqlite"])
        if record["iteration"] == 20:
            record["summary"] = (
                "Coverage expansion and content fallback; metrics are inherited from v19 "
                "before refit, not a new quality measurement. "
                "扩容与内容补充；继承 v19 重训前指标，不代表新增效果实测。"
            )
        record_result(catalog, record)
        covered.add(version)
        count += 1
    for directory in sorted(root.glob("gharchive*")):
        if not directory.is_dir() or directory.name in covered:
            continue
        # 消融组、审计目录属于其版本报告，不伪造成额外的正式模型。
        match = re.search(r"-v(\d+)(?:-|$)", directory.name)
        if not match or not (
            directory.name.startswith("gharchive-local-")
            or directory.name.startswith("gharchive-20261031-")
        ):
            continue
        run_root = directory / "runs" / directory.name
        stage = run_root / "canonical/manifest.json"
        quality = run_root / "dataset/quality-report.json"
        record = {
            "run_id": directory.name,
            "model_version": None,
            "iteration": int(match[1]),
            "status": "data_only" if quality.exists() else "unverified",
            "metrics": {},
            "metrics_scope": None,
            "created_at": None,
            "summary": (
                "No completed serving Bundle was recovered. Missing metrics are unknown. "
                "未找到完整 ServingBundle，缺失指标保留未知。"
            ),
            "evidence": [],
        }
        if stage.exists():
            record["evidence"].append("canonical/manifest.json")
        if quality.exists():
            report = json.loads(quality.read_text())
            record["data_processing"] = {
                key: report.get(key)
                for key in (
                    "input_interaction_rows",
                    "final_rows",
                    "public_repository_count",
                    "candidate_repository_count",
                    "training_eligible_subject_count",
                    "split_counts",
                )
            }
            record["evidence"].append("dataset/quality-report.json")
        if directory.name == "gharchive-20261031-v17-recent":
            record.update(
                status="blocked",
                summary=(
                    "The v17 recent holdout gate failed; final refit was stopped. "
                    "v17 近期留出未通过，停止最终 refit；详见 v17 结果报告。"
                ),
            )
            decision_path = root / "gharchive-v17-recent-controls-20261102/decision.json"
            if decision_path.is_file():
                decision = json.loads(decision_path.read_text())
                test = decision["test"]
                record.update(
                    selected_model="costar",
                    metrics=public_metrics(test["models"]["costar"]),
                    metrics_scope="same_model",
                    evaluation={
                        "split": test["split"],
                        "k": test["k"],
                        "dataset_checksum": test["dataset_checksum"],
                    },
                    decision={
                        "passed": decision["passed"],
                        "candidate": public_metrics(test["models"]["costar"]),
                        "incumbent": public_metrics(test["models"]["baseline"]),
                    },
                )
                record["evidence"].append("v17 controls decision.json")
        if record["iteration"] == 18:
            record["summary"] = (
                "Event identity repair and dataset rebuild only; no new quality claim. "
                "事件身份修复与 Dataset 重建，不代表推荐质量提升。"
            )
        record_result(catalog, record)
        count += 1
    for path in sorted(root.glob("weekly*/runs/*/run.json")):
        run = json.loads(path.read_text())
        if run.get("status") not in {"published", "blocked", "failed"}:
            continue
        record, _ = weekly_record(run)
        record["evidence"] = ["run.json", "verified stage artifacts"]
        record_result(catalog, record)
        count += 1
    render_history(catalog, output)
    return count
