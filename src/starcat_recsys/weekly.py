"""每周推荐训练的可恢复入口。

周日北京时间 11:00 到期，登录后补跑漏掉的周次。内核文件锁随进程退出自动释放；
每阶段只重建自己的临时产物，完成阶段通过 checksum 恢复。任何质量门槛失败都不会
进入发布阶段，生产发布结果未知时也保留足够材料供下一次回读与续验。
"""

from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import os
import shutil
from collections.abc import Callable
from dataclasses import asdict
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import httpx
import yaml

from starcat_recsys.artifacts import artifact_for, sha256_file, write_json_atomic
from starcat_recsys.bundle import verify_bundle
from starcat_recsys.evaluation import EvaluationConfig, OfflineEvaluator
from starcat_recsys.interfaces import Artifact, TaskContext
from starcat_recsys.recommendations import artifact_path
from starcat_recsys.trainers.content_index import ContentIndexTrainer
from starcat_recsys.weekly_data import RawValidationError, prepare_datasets, utc_day_end, verify_raw
from starcat_recsys.weekly_delivery import (
    active_version,
    local_acceptance,
    publish_with_rollback,
    read_key,
)
from starcat_recsys.weekly_training import (
    build_bundle,
    check_coverage,
    export_incumbent,
    quality_decision,
    refit_inputs,
    train_model,
)


def due_slot(now: datetime) -> str:
    """返回最近已到期的周日，避免 RunAtLoad 让登录变成无条件训练。"""
    local = now.astimezone(ZoneInfo("Asia/Shanghai"))
    sunday = local.date() - timedelta(days=(local.weekday() + 1) % 7)
    if local.date() == sunday and local.hour < 11:
        sunday -= timedelta(days=7)
    return sunday.isoformat()


def source_checksums() -> dict[str, str]:
    """冻结实际 Python 源码和依赖锁摘要，训练期间变更配方时拒绝自动发布。"""
    package = Path(__file__).resolve().parent
    paths = [*sorted(package.rglob("*.py")), package.parents[1] / "uv.lock"]
    return {str(path.relative_to(package.parents[1])): sha256_file(path) for path in paths}


class WeeklyRun:
    """维护单次不可变输入和阶段检查点，不借用本地旧 Registry 的 active 状态。"""

    def __init__(
        self,
        config: dict[str, Any],
        state: dict[str, Any],
        state_file: Path,
        target: date,
        slot: str,
    ) -> None:
        self.config = config
        self.state = state
        self.state_file = state_file
        self.target = target
        self.slot = slot
        digest = hashlib.sha256(json.dumps(config, sort_keys=True).encode()).hexdigest()
        self.sources = source_checksums()
        old_run = state.get("current_run")
        old = _read_json(Path(old_run) / "run.json") if old_run else {}
        resumable = (
            old.get("target") == target.isoformat()
            and old.get("config_hash") == digest
            and old.get("sources") == self.sources
            and old.get("status") in {"running", "failed"}
        )
        if resumable:
            self.root = Path(str(old_run))
            self.record = old
        else:
            run_id = (
                "weekly-"
                + target.strftime("%Y%m%d")
                + "-"
                + datetime.now(UTC).strftime("%Y%m%dT%H%M%S%f")
            )
            self.root = Path(config["workspace"]) / "runs" / run_id
            self.root.mkdir(parents=True, exist_ok=False)
            incumbent = state.get("published_bundle", config["initial_bundle"])
            self.record = {
                "run_id": run_id,
                "target": target.isoformat(),
                "slot": slot,
                "config_hash": digest,
                "sources": self.sources,
                "steps": {},
                "incumbent_bundle": incumbent,
                "status": "running",
            }
            self.record["input_checksums"] = {
                key: sha256_file(Path(config[key]))
                for key in ["frozen_training_dataset", "core_catalog", "expanded_catalog"]
                if key in config
            }
            write_json_atomic(self.root / "config.json", config, mode=0o600)
            shutil.copytree(
                Path(__file__).parent,
                self.root / "source" / "starcat_recsys",
                ignore=shutil.ignore_patterns("__pycache__"),
            )
        self.state["current_run"] = str(self.root)
        self.save("running")

    def save(self, status: str, **values: Any) -> None:
        """先保存 run，再推进用户目录状态；即使外接卷掉线也保留最后阶段。"""
        self.record.update(status=status, updated_at=datetime.now(UTC).isoformat(), **values)
        write_json_atomic(self.root / "run.json", self.record, mode=0o600)
        self.state.update(
            status=status,
            stage=self.record.get("stage"),
            run_id=self.record["run_id"],
            target=self.target.isoformat(),
            pid=os.getpid(),
            updated_at=self.record["updated_at"],
        )
        if status in {"blocked", "published"}:
            self.state["last_checked_slot"] = self.slot
        write_json_atomic(self.state_file, self.state, mode=0o600)

    def step(
        self,
        name: str,
        action: Callable[[TaskContext], list[Artifact]],
        *,
        mutation: bool = False,
    ) -> list[Artifact]:
        """复用已完整落盘且摘要匹配的阶段；发布阶段保留关机前的生产基线回执。"""
        previous = self.record["steps"].get(name)
        if previous is not None:
            artifacts = [Artifact(**item) for item in previous]
            for item in artifacts:
                if sha256_file(artifact_path(item)) != item.checksum:
                    raise ValueError("恢复阶段的产物摘要不符")
                if item.kind == "serving_bundle":
                    verify_bundle(artifact_path(item).parent)
            return artifacts
        self.record["stage"] = name
        self.save("running")
        print(json.dumps({"stage": name, "status": "started"}), flush=True)
        directory = self.root / name
        if directory.exists() and not mutation:
            shutil.rmtree(directory)
        directory.mkdir(parents=True, exist_ok=True)
        context = TaskContext(self.record["run_id"], directory, self.record["config_hash"], 7)
        artifacts = action(context)
        self.record["steps"][name] = [asdict(item) for item in artifacts]
        self.save("running")
        print(json.dumps({"stage": name, "status": "completed"}), flush=True)
        return artifacts

    def execute(self) -> dict[str, Any]:
        """数据、评估、产物和接口检查顺序固定，只有所有证据通过才执行最终交付。"""
        config = self.config
        incumbent = Path(self.record["incumbent_bundle"])
        manifest = verify_bundle(incumbent)
        version = self.record["run_id"] + "-hybrid"
        with httpx.Client(
            timeout=60, follow_redirects=False, headers={"X-SC-Svc": "recommend"}
        ) as client:
            active = active_version(
                client, config["base_url"], read_key(Path(config["publish_key_file"]))
            )
        if active not in {manifest["model_version"], version}:
            raise ValueError("本机基线与线上版本不符")
        baseline_day = date.fromisoformat(
            manifest.get("raw_watermark", config["initial_watermark"])
        )
        if self.target <= baseline_day:
            self.save("blocked", reason="no_new_data")
            return {"status": "blocked", "reason": "no_new_data"}
        data_end = utc_day_end(self.target)
        train_end = max(data_end - timedelta(days=7), utc_day_end(baseline_day))
        # 样本不足时固定留出起点、只延长终点，后续周才能真正积累样本；滚动七天会永远
        # 丢掉已积累的稀疏事件。成功发布后才开启下一轮全新的时间留出。
        if self.state.get("evaluation_train_end"):
            train_end = datetime.fromisoformat(self.state["evaluation_train_end"])
        self.state["evaluation_train_end"] = train_end.isoformat()
        self.save("running")

        def raw_stage(context: TaskContext) -> list[Artifact]:
            paths = verify_raw(Path(config["raw_root"]), self.target)
            checkpoint = _read_json(Path(config["raw_root"]) / "download-state.json")
            records = {
                path.name: checkpoint["completed_partitions"][path.stem[-8:]] for path in paths
            }
            return _report(context, "raw", {"partitions": records})

        raw = self.step("raw", raw_stage)[0]
        raw_records = _read_json(artifact_path(raw))["partitions"]
        paths = [Path(config["raw_root"]) / name for name in sorted(raw_records)]
        datasets = self.step(
            "data", lambda c: prepare_datasets(c, config, paths, train_end, data_end)
        )
        models = self.step(
            "models",
            lambda c: [*train_model(c, datasets, train_end), export_incumbent(c, incumbent)],
        )
        selected = next(item for item in models if item.metadata["model"] == "hybrid_25")
        previous_model = next(item for item in models if item.metadata["model"] == "incumbent")
        metrics = self.step(
            "evaluation",
            lambda c: OfflineEvaluator(EvaluationConfig(split="test", maximum_seeds=20)).evaluate(
                c, [*datasets, selected, previous_model]
            ),
        )[0]
        expanded = artifact_for(Path(config["expanded_catalog"]), "dataset_repositories", {})
        index = self.step("index", lambda c: ContentIndexTrainer().train(c, [expanded]))[0]
        candidate = self.step(
            "candidate",
            lambda c: [
                build_bundle(
                    c,
                    datasets,
                    selected,
                    metrics,
                    index,
                    expanded,
                    self.record["run_id"] + "-evaluated",
                    self.target.isoformat(),
                    refit=False,
                )
            ],
        )[0]
        coverage = check_coverage(artifact_path(candidate).parent, incumbent)
        self.step(
            "candidate_api",
            lambda c: _report(
                c,
                "acceptance",
                local_acceptance(c, Path(config["api_root"]), artifact_path(candidate).parent),
            ),
        )
        decision = quality_decision(_read_json(artifact_path(metrics)))
        decision["coverage"] = coverage
        decision["passed"] = decision["passed"] and coverage["passed"]
        self.step("decision", lambda c: _report(c, "decision", decision))
        if not decision["passed"]:
            self.save("blocked", decision=decision)
            return {"status": "blocked", **decision}
        content = next(item for item in models if item.metadata["model"] == "content")
        final_models = self.step(
            "refit", lambda c: train_model(c, refit_inputs(datasets), data_end, content)
        )
        final_model = next(item for item in final_models if item.metadata["model"] == "hybrid_25")
        final = self.step(
            "bundle",
            lambda c: [
                build_bundle(
                    c,
                    datasets,
                    final_model,
                    metrics,
                    index,
                    expanded,
                    version,
                    self.target.isoformat(),
                    refit=True,
                )
            ],
        )[0]
        final_coverage = check_coverage(artifact_path(final).parent, incumbent)
        if not final_coverage["passed"]:
            self.save("blocked", reason="refit_coverage", decision=final_coverage)
            return {"status": "blocked", "reason": "refit_coverage", **final_coverage}
        self.step(
            "final_api",
            lambda c: _report(
                c,
                "acceptance",
                local_acceptance(c, Path(config["api_root"]), artifact_path(final).parent),
            ),
        )
        if source_checksums() != self.sources:
            raise ValueError("运行期间源码已变化，禁止发布")
        for key, checksum in self.record["input_checksums"].items():
            if sha256_file(Path(config[key])) != checksum:
                raise ValueError("运行期间固定主体或 metadata 快照已变化")
        # 下载任务可以追加新日期，但本轮已冻结的分区与目录必须保持原样。
        for path in paths:
            if sha256_file(path) != raw_records[path.name]["checksum"]:
                raise ValueError("训练期间 Raw 输入已变化")
        for item in datasets:
            if sha256_file(artifact_path(item)) != item.checksum:
                raise ValueError("训练期间 Dataset 输入已变化")
        delivered = self.step(
            "publish",
            lambda c: _report(c, "receipt", publish_with_rollback(c, config, final, incumbent)),
            mutation=True,
        )[0]
        self.state.update(
            published_bundle=str(artifact_path(final).parent),
            published_watermark=self.target.isoformat(),
            published_version=version,
        )
        self.state.pop("evaluation_train_end", None)
        self.save("published")
        return {
            "status": "published",
            "model_version": version,
            "receipt": _read_json(artifact_path(delivered)),
        }


def _report(context: TaskContext, name: str, payload: dict[str, Any]) -> list[Artifact]:
    """报告也使用阶段产物摘要，恢复时不能把缺失证据视为成功。"""
    path = context.workspace / (name + ".json")
    write_json_atomic(path, payload, mode=0o600)
    return [artifact_for(path, "weekly_report", {})]


def _read_json(path: Path) -> dict[str, Any]:
    """状态首次运行时不存在；坏 JSON 必须报错，不静默丢弃断点。"""
    return json.loads(path.read_text()) if path.exists() else {}


def run(config: dict[str, Any], *, scheduled: bool = False) -> dict[str, Any]:
    """内核锁防止人工补跑和 LaunchAgent 重叠；关机后锁自动释放。"""
    state_file = Path(config["state_file"])
    state_file.parent.mkdir(parents=True, exist_ok=True)
    with state_file.with_suffix(".lock").open("w") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return {"status": "already_running"}
        state = _read_json(state_file)
        now = datetime.now(UTC)
        slot = due_slot(now)
        if (
            scheduled
            and state.get("last_checked_slot", "") >= slot
            and state.get("status") in {"blocked", "published"}
        ):
            return {"status": "not_due", "slot": slot}
        if not os.path.ismount(config["volume_root"]):
            raise RuntimeError("训练数据卷未挂载")
        if shutil.disk_usage(config["workspace"]).free < 20 * 1024**3:
            raise RuntimeError("训练数据卷剩余空间不足 20 GiB")
        task = WeeklyRun(config, state, state_file, now.date() - timedelta(days=1), slot)
        try:
            return task.execute()
        except Exception as error:
            details = {"error_type": type(error).__name__}
            if isinstance(error, RawValidationError):
                details["reason"] = str(error)
            task.save("failed", **details)
            raise


def main() -> int:
    """命令行仅提供正常运行和状态读取，没有绕过门槛的发布开关。"""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=["run", "status"])
    parser.add_argument(
        "--config", type=Path, default=Path.home() / ".config/starcat/weekly-recommendation.yaml"
    )
    parser.add_argument("--scheduled", action="store_true")
    args = parser.parse_args()
    try:
        config = yaml.safe_load(args.config.read_text())
        if args.command == "status":
            state = _read_json(Path(config["state_file"]))
            if state.get("status") == "running":
                # PID 在重启后可能被其它进程复用，实际内核锁才是正在运行的事实来源。
                with Path(config["state_file"]).with_suffix(".lock").open("a") as lock:
                    try:
                        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    except BlockingIOError:
                        pass
                    else:
                        state["status"] = "interrupted"
            # 不输出 artifact URI、凭据路径或私有主体。
            result = {
                key: state[key]
                for key in [
                    "status",
                    "stage",
                    "run_id",
                    "target",
                    "updated_at",
                    "published_watermark",
                    "published_version",
                ]
                if key in state
            }
        else:
            result = run(config, scheduled=args.scheduled)
        print(json.dumps(result, ensure_ascii=False), flush=True)
        return 0
    except Exception as error:
        failure = {"status": "failed", "error_type": type(error).__name__}
        if isinstance(error, RawValidationError):
            failure["reason"] = str(error)
        print(json.dumps(failure, ensure_ascii=False), flush=True)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
