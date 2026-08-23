"""YAML 驱动的端到端离线训练管道。"""

from __future__ import annotations

import hashlib
import subprocess
from dataclasses import asdict
from datetime import date, datetime
from functools import partial
from pathlib import Path
from typing import Any

import yaml

from starcat_recsys.artifacts import write_json_atomic
from starcat_recsys.bundle import (
    BundleConfig,
    LocalModelRegistry,
    ServingBundlePublisher,
)
from starcat_recsys.canonical import CanonicalNormalizer
from starcat_recsys.dataset import DatasetConfig, DuckDBDatasetBuilder
from starcat_recsys.evaluation import EvaluationConfig, OfflineEvaluator
from starcat_recsys.executor import LocalExecutor
from starcat_recsys.interfaces import Artifact, SourceConnector, TaskContext
from starcat_recsys.sources.bigquery import BigQueryConfig, BigQueryWatchEventSource
from starcat_recsys.sources.github import (
    GitHubMetadataConfig,
    GitHubRepositoryMetadataSource,
)
from starcat_recsys.sources.local import LocalFileSource
from starcat_recsys.sources.starcat import StarcatSnapshotConfig, StarcatSnapshotSource
from starcat_recsys.trainers.costar import CoStarConfig, CoStarTrainer
from starcat_recsys.trainers.popular import PopularConfig, PopularTrainer
from starcat_recsys.trainers.svd import SVDConfig, SVDTrainer


def run_pipeline(config_path: Path) -> Artifact:
    """读取并执行完整管道，返回已注册 ServingBundle artifact。"""

    document, config_hash = _load_config(config_path)
    base_directory = config_path.resolve().parent
    workspace = _resolve_path(base_directory, _required_string(document, "workspace"))
    registry_root = _resolve_path(base_directory, _required_string(document, "registry"))
    run_id = _required_string(document, "run_id")
    random_seed = int(document.get("random_seed", 42))
    context = TaskContext(run_id, workspace / "runs" / run_id, config_hash, random_seed)
    context.workspace.mkdir(parents=True, exist_ok=True)
    executor = LocalExecutor()

    raw_artifacts: list[Artifact] = []
    for index, source_config in enumerate(_required_list(document, "sources")):
        if not isinstance(source_config, dict):
            raise ValueError("sources[] 必须是对象")
        connector = _source_connector(source_config, base_directory)
        raw_artifacts.extend(
            executor.execute(
                f"collect:{index}:{connector.name}", partial(connector.collect, context)
            )
        )

    canonical = executor.execute(
        "normalize", lambda: CanonicalNormalizer().normalize(context, raw_artifacts)
    )
    dataset_config = _required_dict(document, "dataset")
    dataset = executor.execute(
        "dataset",
        lambda: DuckDBDatasetBuilder(
            DatasetConfig(
                train_end=_datetime(dataset_config["train_end"]),
                validation_end=_datetime(dataset_config["validation_end"]),
                minimum_repositories_per_subject=int(
                    dataset_config.get("minimum_repositories_per_subject", 2)
                ),
            )
        ).build(context, canonical),
    )

    training_config = _required_dict(document, "training")
    top_k = int(training_config.get("top_k", 100))
    models: list[Artifact] = []
    models.extend(
        executor.execute(
            "train:popular", lambda: PopularTrainer(PopularConfig(top_k)).train(context, dataset)
        )
    )
    models.extend(
        executor.execute(
            "train:svd",
            lambda: SVDTrainer(
                SVDConfig(int(training_config.get("svd_components", 64)), top_k)
            ).train(context, dataset),
        )
    )
    costar_config = _costar_config(training_config, "costar", top_k)
    models.extend(
        executor.execute(
            "train:costar", lambda: CoStarTrainer(costar_config).train(context, dataset)
        )
    )
    if bool(training_config.get("run_ablations", True)):
        no_decay = _costar_config(training_config, "costar_no_decay", top_k, no_decay=True)
        no_shrinkage = _costar_config(
            training_config, "costar_no_shrinkage", top_k, no_shrinkage=True
        )
        models.extend(
            executor.execute(
                "train:costar_no_decay", lambda: CoStarTrainer(no_decay).train(context, dataset)
            )
        )
        models.extend(
            executor.execute(
                "train:costar_no_shrinkage",
                lambda: CoStarTrainer(no_shrinkage).train(context, dataset),
            )
        )

    evaluation_config = _required_dict(document, "evaluation")
    metrics = executor.execute(
        "evaluate",
        lambda: OfflineEvaluator(
            EvaluationConfig(
                split=str(evaluation_config.get("split", "validation")),
                k=int(evaluation_config.get("k", 20)),
            )
        ).evaluate(context, [*dataset, *models]),
    )
    publish_config = _required_dict(document, "publish")
    publisher = ServingBundlePublisher(
        BundleConfig(
            model_version=_required_string(publish_config, "model_version"),
            selected_model=str(publish_config.get("selected_model", "costar")),
            metric_gates={
                str(key): float(value)
                for key, value in _optional_dict(publish_config, "metric_gates").items()
            },
            git_commit=_git_commit(),
        ),
        LocalModelRegistry(registry_root),
    )
    bundle = executor.execute(
        "publish", lambda: publisher.publish(context, [*dataset, *models, *metrics])
    )[0]
    write_json_atomic(
        context.workspace / "run-manifest.json",
        {
            "schema_version": 1,
            "run_id": run_id,
            "config_hash": config_hash,
            "tasks": [asdict(item) for item in executor.executions],
            "bundle": {"uri": bundle.uri, "checksum": bundle.checksum, **bundle.metadata},
        },
    )
    return bundle


def _source_connector(config: dict[str, Any], base: Path) -> SourceConnector:
    source_type = _required_string(config, "type")
    if source_type == "local_file":
        return LocalFileSource(
            _resolve_path(base, _required_string(config, "path")),
            source=_required_string(config, "source"),
            kind=_required_string(config, "kind"),
        )
    if source_type == "starcat_snapshot":
        return StarcatSnapshotSource(
            [_resolve_path(base, str(path)) for path in _required_list(config, "paths")],
            StarcatSnapshotConfig(
                subject_hmac_key_env=str(
                    config.get("subject_hmac_key_env", "STARCAT_SUBJECT_HMAC_KEY")
                ),
                maximum_repositories_per_snapshot=int(
                    config.get("maximum_repositories_per_snapshot", 100_000)
                ),
            ),
        )
    if source_type == "github_api":
        return GitHubRepositoryMetadataSource(
            [int(value) for value in _required_list(config, "repo_ids")],
            GitHubMetadataConfig(
                token_env=str(config.get("token_env", "GITHUB_TOKEN")),
                api_version=str(config.get("api_version", "2026-03-10")),
                timeout_seconds=float(config.get("timeout_seconds", 30)),
                maximum_requests=int(config.get("maximum_requests", 5_000)),
            ),
        )
    if source_type == "bigquery":
        return BigQueryWatchEventSource(
            BigQueryConfig(
                billing_project=_required_string(config, "billing_project"),
                start_date=date.fromisoformat(_required_string(config, "start_date")),
                end_date=date.fromisoformat(_required_string(config, "end_date")),
                maximum_bytes_billed_per_day=int(config["maximum_bytes_billed_per_day"]),
                location=str(config.get("location", "US")),
            )
        )
    raise ValueError(f"不支持的 source type：{source_type}")


def _costar_config(
    config: dict[str, Any],
    model_name: str,
    top_k: int,
    *,
    no_decay: bool = False,
    no_shrinkage: bool = False,
) -> CoStarConfig:
    return CoStarConfig(
        reference_time=_datetime(config["reference_time"]),
        half_life_days=1_000_000_000.0 if no_decay else float(config.get("half_life_days", 365)),
        legacy_time_weight=1.0 if no_decay else float(config.get("legacy_time_weight", 0.35)),
        shrinkage=0.0 if no_shrinkage else float(config.get("shrinkage", 10)),
        minimum_support=float(config.get("minimum_support", 0)),
        maximum_repositories_per_subject=int(config.get("maximum_repositories_per_subject", 2_000)),
        top_k=top_k,
        model_name=model_name,
    )


def _load_config(path: Path) -> tuple[dict[str, Any], str]:
    content = path.read_bytes()
    document = yaml.safe_load(content)
    if not isinstance(document, dict):
        raise ValueError("管道配置根节点必须是对象")
    return document, "sha256:" + hashlib.sha256(content).hexdigest()


def _resolve_path(base: Path, value: str) -> Path:
    path = Path(value)
    return path if path.is_absolute() else (base / path).resolve()


def _datetime(value: Any) -> datetime:
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError(f"时间必须包含时区：{value}")
    return parsed


def _required_string(document: dict[str, Any], key: str) -> str:
    value = document.get(key)
    if not isinstance(value, str) or not value:
        raise ValueError(f"配置 {key} 必须是非空字符串")
    return value


def _required_dict(document: dict[str, Any], key: str) -> dict[str, Any]:
    value = document.get(key)
    if not isinstance(value, dict):
        raise ValueError(f"配置 {key} 必须是对象")
    return value


def _optional_dict(document: dict[str, Any], key: str) -> dict[str, Any]:
    value = document.get(key, {})
    if not isinstance(value, dict):
        raise ValueError(f"配置 {key} 必须是对象")
    return value


def _required_list(document: dict[str, Any], key: str) -> list[Any]:
    value = document.get(key)
    if not isinstance(value, list):
        raise ValueError(f"配置 {key} 必须是数组")
    return value


def _git_commit() -> str:
    result = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        capture_output=True,
        check=False,
        text=True,
    )
    return result.stdout.strip() if result.returncode == 0 else "unknown"
