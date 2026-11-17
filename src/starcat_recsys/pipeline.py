"""YAML 驱动的端到端离线训练管道。"""

from __future__ import annotations

import hashlib
import json
import subprocess
from dataclasses import asdict
from datetime import date, datetime
from functools import partial
from pathlib import Path
from typing import Any

import yaml

from starcat_recsys.artifacts import artifact_for, sha256_file, write_json_atomic
from starcat_recsys.bundle import (
    BundleConfig,
    LocalModelRegistry,
    ServingBundlePublisher,
)
from starcat_recsys.canonical import CanonicalNormalizer
from starcat_recsys.dataset import DatasetConfig, DuckDBDatasetBuilder
from starcat_recsys.duckdb_runtime import connect_for_workspace
from starcat_recsys.evaluation import EvaluationConfig, OfflineEvaluator
from starcat_recsys.executor import LocalExecutor
from starcat_recsys.interfaces import (
    Artifact,
    InsufficientTrainingDataError,
    SourceConnector,
    TaskContext,
)
from starcat_recsys.model_metadata import describe_training, timestamp
from starcat_recsys.publishers import RecommendAPIBundlePublisher, RecommendAPIPublishConfig
from starcat_recsys.sources.bigquery import BigQueryConfig, BigQueryWatchEventSource
from starcat_recsys.sources.bigquery_push import (
    BigQueryPushEventMetadataSource,
    BigQueryPushMetadataConfig,
)
from starcat_recsys.sources.github import (
    GitHubMetadataConfig,
    GitHubRepositoryMetadataSource,
)
from starcat_recsys.sources.local import LocalFileSource
from starcat_recsys.sources.local_lake import LocalLakeConfig, LocalLakePartitionSource
from starcat_recsys.sources.starcat import StarcatSnapshotConfig, StarcatSnapshotSource
from starcat_recsys.sources.starcat_collection import (
    StarcatCollectionAPISource,
    StarcatCollectionConfig,
)
from starcat_recsys.trainers.content import ContentConfig, ContentTrainer, fuse_models
from starcat_recsys.trainers.content_index import ContentIndexTrainer
from starcat_recsys.trainers.costar import CoStarConfig, CoStarTrainer
from starcat_recsys.trainers.popular import PopularConfig, PopularTrainer
from starcat_recsys.trainers.svd import SVDConfig, SVDTrainer


def run_pipeline(config_path: Path) -> Artifact:
    """读取并执行完整管道，返回已注册 ServingBundle artifact。"""

    document, config_hash = _load_config(config_path)
    publish_config = _required_dict(document, "publish")
    install = publish_config.get("install", True)
    if not isinstance(install, bool):
        raise ValueError("publish.install 必须是布尔值")
    if not install and _optional_dict(publish_config, "recommend_api"):
        raise ValueError("候选模式禁止向 Recommend API 交付")
    base_directory = config_path.resolve().parent
    workspace = _resolve_path(base_directory, _required_string(document, "workspace"))
    registry_root = _resolve_path(base_directory, _required_string(document, "registry"))
    run_id = _required_string(document, "run_id")
    random_seed = int(document.get("random_seed", 42))
    context = TaskContext(run_id, workspace / "runs" / run_id, config_hash, random_seed)
    if context.workspace.exists():
        raise FileExistsError(f"run_id 已存在，不允许覆盖：{run_id}")
    context.workspace.mkdir(parents=True)
    # 运行开始就冻结实际源码摘要，避免长任务期间其它本地编辑污染运行溯源。
    source_checksums = {
        str(path.relative_to(Path(__file__).resolve().parents[2])): sha256_file(path)
        for path in [
            *sorted(Path(__file__).resolve().parent.rglob("*.py")),
            Path(__file__).resolve().parents[2] / "uv.lock",
        ]
    }
    executor = LocalExecutor()

    resume_manifest_value = document.get("resume_canonical_manifest")
    if resume_manifest_value is not None:
        if not isinstance(resume_manifest_value, str) or not resume_manifest_value.strip():
            raise ValueError("resume_canonical_manifest 必须是非空路径")
        resume_manifest = _resolve_path(base_directory, resume_manifest_value)
        canonical = executor.execute(
            "resume:canonical",
            lambda: _load_canonical_artifacts(context, resume_manifest),
        )
    else:
        raw_artifacts: list[Artifact] = []
        source_configs = _required_list(document, "sources")
        deferred_github_configs: list[dict[str, Any]] = []
        for index, source_config in enumerate(source_configs):
            if not isinstance(source_config, dict):
                raise ValueError("sources[] 必须是对象")
            if source_config.get("type") == "github_api" and bool(
                source_config.get("repo_ids_from_canonical")
            ):
                deferred_github_configs.append(source_config)
                continue
            connector = _source_connector(source_config, base_directory)
            raw_artifacts.extend(
                executor.execute(
                    f"collect:{index}:{connector.name}", partial(connector.collect, context)
                )
            )

        canonical = executor.execute(
            "normalize", lambda: CanonicalNormalizer().normalize(context, raw_artifacts)
        )
        for index, github_config in enumerate(deferred_github_configs):
            repo_ids = _repository_ids_from_canonical(
                canonical,
                int(github_config.get("repository_limit", 5_000)),
                context.workspace,
            )
            connector = _source_connector(github_config, base_directory, repo_ids=repo_ids)
            deferred_artifacts = executor.execute(
                f"collect:deferred-github:{index}", partial(connector.collect, context)
            )
            canonical = executor.execute(
                "normalize:with-github-metadata",
                partial(
                    CanonicalNormalizer().normalize,
                    context,
                    deferred_artifacts,
                    existing=canonical,
                ),
            )
    dataset_config = _required_dict(document, "dataset")
    cohort_catalog = dataset_config.get("frozen_cohort_catalog")
    if cohort_catalog is not None:
        if not isinstance(cohort_catalog, str) or not cohort_catalog.strip():
            raise ValueError("dataset.frozen_cohort_catalog 必须是非空路径")
        canonical.append(
            artifact_for(_resolve_path(base_directory, cohort_catalog), "cohort_repositories", {})
        )
    train_end = _datetime(dataset_config["train_end"])
    validation_end = _datetime(dataset_config["validation_end"])
    dataset = executor.execute(
        "dataset",
        lambda: DuckDBDatasetBuilder(
            DatasetConfig(
                train_end=train_end,
                validation_end=validation_end,
                minimum_repositories_per_subject=int(
                    dataset_config.get("minimum_repositories_per_subject", 2)
                ),
                maximum_repositories_per_subject=(
                    int(dataset_config["maximum_repositories_per_subject"])
                    if dataset_config.get("maximum_repositories_per_subject") is not None
                    else None
                ),
                minimum_repository_interactions=int(
                    dataset_config.get("minimum_repository_interactions", 1)
                ),
                maximum_repository_count=(
                    int(dataset_config["maximum_repository_count"])
                    if dataset_config.get("maximum_repository_count") is not None
                    else None
                ),
                subject_sample_rate=float(dataset_config.get("subject_sample_rate", 1.0)),
                freeze_cohort=cohort_catalog is not None,
            )
        ).build(context, canonical),
    )

    training_config = _required_dict(document, "training")
    reference_time = _training_reference_time(training_config, train_end)
    top_k = int(training_config.get("top_k", 100))
    models: list[Artifact] = []
    training_started_at = timestamp()
    models.extend(
        executor.execute(
            "train:popular", lambda: PopularTrainer(PopularConfig(top_k)).train(context, dataset)
        )
    )
    svd_models = (
        executor.execute_optional(
            "train:svd",
            lambda: SVDTrainer(
                SVDConfig(
                    components=int(training_config.get("svd_components", 64)),
                    top_k=top_k,
                    maximum_interactions=int(
                        training_config.get("svd_maximum_interactions", 5_000_000)
                    ),
                    maximum_subjects=int(training_config.get("svd_maximum_subjects", 200_000)),
                    maximum_repositories=int(
                        training_config.get("svd_maximum_repositories", 50_000)
                    ),
                    neighbor_batch_size=int(training_config.get("svd_neighbor_batch_size", 1_000)),
                )
            ).train(context, dataset),
            skip_on=InsufficientTrainingDataError,
        )
        if bool(training_config.get("train_svd", True))
        else None
    )
    if svd_models is not None:
        models.extend(svd_models)
    costar_config = _costar_config(training_config, "costar", top_k, reference_time)
    models.extend(
        executor.execute(
            "train:costar", lambda: CoStarTrainer(costar_config).train(context, dataset)
        )
    )
    if bool(training_config.get("run_ablations", True)):
        no_decay = _costar_config(
            training_config,
            "costar_no_decay",
            top_k,
            reference_time,
            no_decay=True,
        )
        no_shrinkage = _costar_config(
            training_config,
            "costar_no_shrinkage",
            top_k,
            reference_time,
            no_shrinkage=True,
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

    content_indexes: list[Artifact] = []
    if training_config.get("content_weight") is not None:
        content = executor.execute(
            "train:content",
            lambda: ContentTrainer(ContentConfig(top_k=top_k)).train(context, dataset),
        )[0]
        costar = next(item for item in models if item.metadata["model"] == "costar")
        hybrid = executor.execute(
            "train:hybrid",
            lambda: fuse_models(
                context,
                costar,
                content,
                content_weight=float(training_config["content_weight"]),
                top_k=top_k,
            ),
        )
        models.extend([content, hybrid])
        content_indexes = executor.execute(
            "train:content-index", lambda: ContentIndexTrainer().train(context, dataset)
        )
    training = describe_training(
        context,
        next(item for item in dataset if item.kind == "training_dataset"),
        training_started_at,
        timestamp(),
        training_config,
    )
    evaluation_config = _required_dict(document, "evaluation")
    reference_metrics = evaluation_config.get("reference_metrics")
    if reference_metrics is not None:
        if install:
            raise ValueError("重训引用评估只允许候选模式，不得自动安装")
        if not isinstance(reference_metrics, str) or not reference_metrics.strip():
            raise ValueError("evaluation.reference_metrics 必须是非空路径")
        path = _resolve_path(base_directory, reference_metrics)
        reference_document = json.loads(path.read_text(encoding="utf-8"))
        selected = str(publish_config.get("selected_model", "costar"))
        reference_model = reference_document.get("models", {}).get(selected, {})
        if (
            reference_document.get("split") != "test"
            or not reference_document.get("model_checksums", {}).get(selected)
            or reference_model.get("evaluated_subjects", 0) <= 0
        ):
            raise ValueError("重训必须引用有模型校验和及有效主体的 test 评估")
        metrics = [artifact_for(path, "evaluation_metrics", {})]
    else:
        metrics = executor.execute(
            "evaluate",
            lambda: OfflineEvaluator(
                EvaluationConfig(
                    split=str(evaluation_config.get("split", "validation")),
                    k=int(evaluation_config.get("k", 20)),
                    maximum_subjects=int(evaluation_config.get("maximum_subjects", 10_000)),
                    maximum_seeds=(
                        int(evaluation_config["maximum_seeds"])
                        if evaluation_config.get("maximum_seeds") is not None
                        else None
                    ),
                )
            ).evaluate(context, [*dataset, *models]),
        )
    publisher = ServingBundlePublisher(
        BundleConfig(
            model_version=_required_string(publish_config, "model_version"),
            selected_model=str(publish_config.get("selected_model", "costar")),
            metric_gates={
                str(key): float(value)
                for key, value in _optional_dict(publish_config, "metric_gates").items()
            },
            git_commit=_git_commit(),
            install=install,
            metrics_scope="pre_refit" if reference_metrics is not None else "same_model",
            training=training,
            evaluation_window={
                "train_end": train_end.isoformat(),
                "validation_end": validation_end.isoformat(),
            },
        ),
        LocalModelRegistry(registry_root),
    )
    bundle = executor.execute(
        "publish",
        lambda: publisher.publish(context, [*dataset, *models, *metrics, *content_indexes]),
    )[0]
    delivery: Artifact | None = None
    recommend_api_config = _optional_dict(publish_config, "recommend_api")
    if recommend_api_config:
        delivery_publisher = RecommendAPIBundlePublisher(
            RecommendAPIPublishConfig(
                base_url=_required_string(recommend_api_config, "base_url"),
                publish_key_env=str(
                    recommend_api_config.get("publish_key_env", "STARCAT_RECOMMEND_PUBLISH_KEY")
                ),
                timeout_seconds=float(recommend_api_config.get("timeout_seconds", 120)),
                maximum_bundle_bytes=int(
                    recommend_api_config.get("maximum_bundle_bytes", 512 * 1024 * 1024)
                ),
                activate=bool(recommend_api_config.get("activate", True)),
            )
        )
        delivery = executor.execute(
            "publish:recommend-api",
            lambda: delivery_publisher.publish(context, [bundle]),
        )[0]
    write_json_atomic(
        context.workspace / "run-manifest.json",
        {
            "schema_version": 1,
            "run_id": run_id,
            "config_hash": config_hash,
            # HEAD 不包含本地未提交实现；记录实际运行源码与锁文件摘要以复现实验。
            "source_checksums": source_checksums,
            "tasks": [asdict(item) for item in executor.executions],
            "bundle": {"uri": bundle.uri, "checksum": bundle.checksum, **bundle.metadata},
            "delivery": (
                {"uri": delivery.uri, "checksum": delivery.checksum, **delivery.metadata}
                if delivery is not None
                else None
            ),
        },
    )
    return bundle


def _load_canonical_artifacts(context: TaskContext, manifest_path: Path) -> list[Artifact]:
    """校验并复用已完成 run 的 Canonical，避免长任务失败后重扫不可变 Raw。"""

    if not manifest_path.is_file():
        raise FileNotFoundError(f"Canonical manifest 不存在：{manifest_path}")
    document = json.loads(manifest_path.read_text(encoding="utf-8"))
    rows = document.get("artifacts")
    if not isinstance(rows, list):
        raise ValueError("Canonical manifest 缺少 artifacts[]")

    artifacts: list[Artifact] = []
    for row in rows:
        if not isinstance(row, dict):
            raise ValueError("Canonical manifest artifacts[] 必须是对象")
        kind = row.get("kind")
        uri = row.get("uri")
        checksum = row.get("checksum")
        metadata = row.get("metadata")
        if not isinstance(kind, str) or not isinstance(uri, str) or not uri.startswith("file://"):
            raise ValueError("Canonical artifact 的 kind/uri 无效")
        if not isinstance(checksum, str) or not isinstance(metadata, dict):
            raise ValueError("Canonical artifact 的 checksum/metadata 无效")
        path = Path(uri.removeprefix("file://"))
        if not path.is_file():
            raise FileNotFoundError(f"Canonical artifact 不存在：{path}")
        actual_checksum = sha256_file(path)
        if actual_checksum != checksum:
            raise ValueError(f"Canonical artifact checksum 不匹配：{path}")
        artifacts.append(Artifact(kind, uri, checksum, metadata))

    expected_kinds = {"canonical_interactions", "canonical_repositories"}
    actual_kinds = {artifact.kind for artifact in artifacts}
    if len(artifacts) != 2 or actual_kinds != expected_kinds:
        raise ValueError(f"Canonical manifest artifact 类型不完整：{sorted(actual_kinds)}")

    # 新 run 只记录来源和摘要，不复制可能达到数十 GiB 的 Canonical 文件。
    write_json_atomic(
        context.workspace / "canonical-resume.json",
        {
            "schema_version": 1,
            "run_id": context.run_id,
            "source_manifest": manifest_path.resolve().as_uri(),
            "source_manifest_checksum": sha256_file(manifest_path),
            "source_run_id": document.get("run_id"),
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


def _source_connector(
    config: dict[str, Any], base: Path, *, repo_ids: list[int] | None = None
) -> SourceConnector:
    source_type = _required_string(config, "type")
    if source_type == "local_file":
        return LocalFileSource(
            _resolve_path(base, _required_string(config, "path")),
            source=_required_string(config, "source"),
            kind=_required_string(config, "kind"),
        )
    if source_type == "local_lake_partitions":
        return LocalLakePartitionSource(
            LocalLakeConfig(
                workspace=_resolve_path(base, _required_string(config, "workspace")),
                dataset=_required_string(config, "dataset"),
                start_date=date.fromisoformat(_required_string(config, "start_date")),
                end_date=date.fromisoformat(_required_string(config, "end_date")),
                verify_checksums=bool(config.get("verify_checksums", True)),
            )
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
    if source_type == "starcat_collection_api":
        return StarcatCollectionAPISource(
            StarcatCollectionConfig(
                base_url=_required_string(config, "base_url"),
                admin_key_env=str(config.get("admin_key_env", "STARCAT_COLLECTION_ADMIN_KEY")),
                timeout_seconds=float(config.get("timeout_seconds", 60)),
                maximum_snapshots=int(config.get("maximum_snapshots", 100_000)),
                maximum_repositories_per_snapshot=int(
                    config.get("maximum_repositories_per_snapshot", 100_000)
                ),
                maximum_response_bytes=int(config.get("maximum_response_bytes", 512 * 1024 * 1024)),
            )
        )
    if source_type == "github_api":
        return GitHubRepositoryMetadataSource(
            repo_ids
            if repo_ids is not None
            else [int(value) for value in _required_list(config, "repo_ids")],
            GitHubMetadataConfig(
                token_env=str(config.get("token_env", "GITHUB_TOKEN")),
                api_version=str(config.get("api_version", "2026-03-10")),
                timeout_seconds=float(config.get("timeout_seconds", 30)),
                maximum_requests=int(config.get("maximum_requests", 5_000)),
                cache_directory=(
                    _resolve_path(base, str(config["cache_directory"]))
                    if config.get("cache_directory")
                    else None
                ),
                reuse_cached_payloads=bool(config.get("reuse_cached_payloads", False)),
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
                maximum_total_bytes_billed=(
                    int(config["maximum_total_bytes_billed"])
                    if config.get("maximum_total_bytes_billed") is not None
                    else None
                ),
                retry_attempts=int(config.get("retry_attempts", 5)),
                retry_initial_delay_seconds=float(config.get("retry_initial_delay_seconds", 1.0)),
            )
        )
    if source_type == "bigquery_push_metadata":
        return BigQueryPushEventMetadataSource(
            BigQueryPushMetadataConfig(
                billing_project=_required_string(config, "billing_project"),
                start_year=int(config["start_year"]),
                end_year=int(config["end_year"]),
                maximum_bytes_billed=int(config["maximum_bytes_billed"]),
                minimum_push_events=int(config.get("minimum_push_events", 1)),
                location=str(config.get("location", "US")),
            )
        )
    raise ValueError(f"不支持的 source type：{source_type}")


def _costar_config(
    config: dict[str, Any],
    model_name: str,
    top_k: int,
    reference_time: datetime,
    *,
    no_decay: bool = False,
    no_shrinkage: bool = False,
) -> CoStarConfig:
    return CoStarConfig(
        reference_time=reference_time,
        half_life_days=1_000_000_000.0 if no_decay else float(config.get("half_life_days", 365)),
        legacy_time_weight=1.0 if no_decay else float(config.get("legacy_time_weight", 0.35)),
        shrinkage=0.0 if no_shrinkage else float(config.get("shrinkage", 10)),
        minimum_support=float(config.get("minimum_support", 0)),
        maximum_repositories_per_subject=int(config.get("maximum_repositories_per_subject", 2_000)),
        maximum_pair_observations=int(config.get("maximum_pair_observations", 100_000_000)),
        require_full_pair_budget=bool(config.get("require_full_pair_budget", False)),
        top_k=top_k,
        model_name=model_name,
    )


def _training_reference_time(config: dict[str, Any], train_end: datetime) -> datetime:
    """保证时间衰减只以训练截止点为基准，避免配置引入未来信息。"""

    reference_time = _datetime(_required_string(config, "reference_time"))
    if reference_time != train_end:
        raise ValueError("training.reference_time 必须等于 dataset.train_end")
    return reference_time


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
    repository_root = Path(__file__).resolve().parents[2]
    result = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        capture_output=True,
        check=False,
        cwd=repository_root,
        text=True,
        timeout=5,
    )
    return result.stdout.strip() if result.returncode == 0 else "unknown"


def _repository_ids_from_canonical(
    inputs: list[Artifact], limit: int, workspace: Path | None = None
) -> list[int]:
    if limit <= 0:
        raise ValueError("repository_limit 必须大于 0")
    matches = [item for item in inputs if item.kind == "canonical_interactions"]
    if len(matches) != 1:
        raise ValueError("延迟 GitHub metadata 采集需要一个 canonical_interactions")
    path = Path(matches[0].uri.removeprefix("file://"))
    connection = connect_for_workspace(workspace or path.parent, "repository-ranking")
    try:
        rows = connection.execute(
            """
            SELECT repo_id
            FROM read_parquet(?)
            GROUP BY repo_id
            ORDER BY count(*) DESC, repo_id
            LIMIT ?
            """,
            [path.as_posix(), limit],
        ).fetchall()
    finally:
        connection.close()
    return [int(row[0]) for row in rows]
