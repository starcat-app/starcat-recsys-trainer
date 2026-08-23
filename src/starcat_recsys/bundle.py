"""ServingBundle 构建、本地 Model Registry、校验和查询。

Bundle 是训练管道与 Starcat / 未来 REST API 的稳定边界。它只包含公开仓库、
预计算 Top-K 和模型元数据，不包含训练主体或原始快照。
"""

from __future__ import annotations

import json
import shutil
import sqlite3
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq

from starcat_recsys.artifacts import artifact_for, sha256_file, write_json_atomic
from starcat_recsys.interfaces import Artifact, TaskContext
from starcat_recsys.recommendations import artifact_path


@dataclass(frozen=True, slots=True)
class BundleConfig:
    """发布版本、选定模型和指标门禁。"""

    model_version: str
    selected_model: str = "costar"
    metric_gates: dict[str, float] = field(default_factory=dict)
    git_commit: str = "unknown"

    def __post_init__(self) -> None:
        _validate_version(self.model_version)
        if not self.selected_model:
            raise ValueError("selected_model 不能为空")


class ServingBundlePublisher:
    """构建 Bundle 并安装到不可变本地 Registry。"""

    def __init__(self, config: BundleConfig, registry: LocalModelRegistry) -> None:
        self._config = config
        self._registry = registry

    def publish(self, context: TaskContext, inputs: list[Artifact]) -> list[Artifact]:
        model = _selected_model(inputs, self._config.selected_model)
        repositories = _single_artifact(inputs, "dataset_repositories")
        metrics_artifact = _single_artifact(inputs, "evaluation_metrics")
        metrics_document = json.loads(artifact_path(metrics_artifact).read_text(encoding="utf-8"))
        metrics = metrics_document["models"].get(self._config.selected_model)
        if metrics is None:
            raise ValueError(f"评估结果不包含模型 {self._config.selected_model}")
        _check_metric_gates(metrics, self._config.metric_gates)

        staging = context.workspace / "published" / (self._config.model_version + ".staging")
        if staging.exists():
            shutil.rmtree(staging)
        staging.mkdir(parents=True)
        database_path = staging / "recommendations.sqlite"
        _build_database(database_path, artifact_path(model), artifact_path(repositories))
        manifest = {
            "schema_version": 1,
            "model_version": self._config.model_version,
            "selected_model": self._config.selected_model,
            "created_at": datetime.now(UTC).isoformat(),
            "git_commit": self._config.git_commit,
            "config_hash": context.config_hash,
            "random_seed": context.random_seed,
            "metrics": metrics,
            "input_artifacts": [
                {"kind": item.kind, "checksum": item.checksum}
                for item in sorted(inputs, key=lambda item: (item.kind, item.checksum))
            ],
        }
        write_json_atomic(staging / "manifest.json", manifest)
        checksums = {
            path.name: sha256_file(path)
            for path in sorted(staging.iterdir())
            if path.is_file() and path.name != "checksums.json"
        }
        write_json_atomic(staging / "checksums.json", checksums)
        verify_bundle(staging)
        installed = self._registry.install(staging, self._config.model_version)
        return [
            artifact_for(
                installed / "manifest.json",
                "serving_bundle",
                {
                    "model_version": self._config.model_version,
                    "bundle_uri": installed.as_uri(),
                },
            )
        ]


class LocalModelRegistry:
    """不可变版本目录 + 原子 active pointer 的单机 Registry。"""

    def __init__(self, root: Path) -> None:
        self.root = root

    def install(self, staging: Path, version: str) -> Path:
        versions = self.root / "versions"
        versions.mkdir(parents=True, exist_ok=True)
        target = versions / version
        if target.exists():
            raise FileExistsError(f"模型版本已存在，不允许覆盖：{version}")
        temporary = versions / f".{version}.installing"
        if temporary.exists():
            shutil.rmtree(temporary)
        shutil.copytree(staging, temporary)
        verify_bundle(temporary)
        temporary.replace(target)
        write_json_atomic(
            self.root / "active.json",
            {"model_version": version, "bundle_uri": target.as_uri()},
        )
        return target

    def activate(self, version: str) -> None:
        _validate_version(version)
        target = self.root / "versions" / version
        verify_bundle(target)
        write_json_atomic(
            self.root / "active.json",
            {"model_version": version, "bundle_uri": target.as_uri()},
        )


def verify_bundle(bundle: Path) -> dict[str, Any]:
    """校验必要文件、checksum 和 SQLite 完整性。"""

    manifest_path = bundle / "manifest.json"
    checksums_path = bundle / "checksums.json"
    database_path = bundle / "recommendations.sqlite"
    for path in [manifest_path, checksums_path, database_path]:
        if not path.is_file():
            raise ValueError(f"ServingBundle 缺少文件：{path.name}")
    checksums = json.loads(checksums_path.read_text(encoding="utf-8"))
    for filename, expected in checksums.items():
        if not isinstance(filename, str) or Path(filename).name != filename:
            raise ValueError(f"ServingBundle checksum 文件名不合法：{filename}")
        candidate = bundle / filename
        if candidate.is_symlink() or not candidate.is_file():
            raise ValueError(f"ServingBundle checksum 目标不合法：{filename}")
        if not isinstance(expected, str) or not expected.startswith("sha256:"):
            raise ValueError(f"ServingBundle checksum 格式不合法：{filename}")
        actual = sha256_file(candidate)
        if actual != expected:
            raise ValueError(f"ServingBundle checksum 不匹配：{filename}")
    with sqlite3.connect(f"file:{database_path}?mode=ro", uri=True) as database:
        result = database.execute("PRAGMA quick_check").fetchone()
        if result is None or result[0] != "ok":
            raise ValueError("ServingBundle SQLite quick_check 未通过")
    manifest: dict[str, Any] = json.loads(manifest_path.read_text(encoding="utf-8"))
    if manifest.get("schema_version") != 1:
        raise ValueError("不支持的 ServingBundle schema")
    return manifest


def query_bundle(
    bundle: Path,
    seed_repo_ids: list[int],
    *,
    limit: int = 20,
    exclude_repo_ids: list[int] | None = None,
) -> list[dict[str, Any]]:
    """将多个 seed 的 item-to-item 分数相加，返回可直接供 API 使用的公开仓库。"""

    if not seed_repo_ids or any(repo_id <= 0 for repo_id in seed_repo_ids):
        raise ValueError("seed_repo_ids 必须是非空正整数列表")
    if any(repo_id <= 0 for repo_id in (exclude_repo_ids or [])):
        raise ValueError("exclude_repo_ids 必须全部为正整数")
    if limit <= 0 or limit > 500:
        raise ValueError("limit 必须在 1..500 范围")
    verify_bundle(bundle)
    excluded = set(seed_repo_ids) | set(exclude_repo_ids or [])
    placeholders = ",".join("?" for _ in seed_repo_ids)
    excluded_placeholders = ",".join("?" for _ in excluded)
    query = f"""
        SELECT
            r.target_repo_id AS repo_id,
            p.full_name,
            p.description,
            p.primary_language,
            sum(r.score) AS score,
            min(r.rank) AS best_rank,
            min(r.signals) AS signals
        FROM recommendations r
        INNER JOIN repositories p ON p.repo_id = r.target_repo_id
        WHERE r.source_repo_id IN ({placeholders})
          AND r.target_repo_id NOT IN ({excluded_placeholders})
        GROUP BY r.target_repo_id, p.full_name, p.description, p.primary_language
        ORDER BY score DESC, best_rank, r.target_repo_id
        LIMIT ?
    """
    parameters = [*seed_repo_ids, *sorted(excluded), limit]
    database_path = bundle / "recommendations.sqlite"
    with sqlite3.connect(f"file:{database_path}?mode=ro", uri=True) as database:
        database.row_factory = sqlite3.Row
        return [dict(row) for row in database.execute(query, parameters).fetchall()]


def _build_database(database_path: Path, model_path: Path, repository_path: Path) -> None:
    model_rows = pq.read_table(model_path).to_pylist()
    repository_rows = pq.read_table(repository_path).to_pylist()
    public_repo_ids = {int(row["repo_id"]) for row in repository_rows}
    with sqlite3.connect(database_path) as database:
        database.executescript(
            """
            PRAGMA journal_mode = DELETE;
            CREATE TABLE repositories (
                repo_id INTEGER PRIMARY KEY,
                full_name TEXT NOT NULL,
                description TEXT,
                topics_json TEXT NOT NULL,
                primary_language TEXT,
                license_key TEXT,
                stars INTEGER NOT NULL,
                forks INTEGER NOT NULL,
                pushed_at TEXT
            );
            CREATE TABLE recommendations (
                source_repo_id INTEGER NOT NULL,
                target_repo_id INTEGER NOT NULL,
                score REAL NOT NULL,
                rank INTEGER NOT NULL,
                model TEXT NOT NULL,
                signals TEXT NOT NULL,
                PRIMARY KEY (source_repo_id, target_repo_id)
            );
            CREATE INDEX recommendations_target_idx ON recommendations(target_repo_id);
            """
        )
        database.executemany(
            "INSERT INTO repositories VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
                (
                    row["repo_id"],
                    row["full_name"],
                    row["description"],
                    json.dumps(row["topics"], ensure_ascii=False, separators=(",", ":")),
                    row["primary_language"],
                    row["license_key"],
                    row["stars"],
                    row["forks"],
                    row["pushed_at"].isoformat() if row["pushed_at"] else None,
                )
                for row in repository_rows
            ],
        )
        database.executemany(
            "INSERT INTO recommendations VALUES (?, ?, ?, ?, ?, ?)",
            [
                (
                    row["source_repo_id"],
                    row["target_repo_id"],
                    row["score"],
                    row["rank"],
                    row["model"],
                    row["signals"],
                )
                for row in model_rows
                if int(row["source_repo_id"]) in public_repo_ids
                and int(row["target_repo_id"]) in public_repo_ids
            ],
        )


def _selected_model(inputs: list[Artifact], name: str) -> Artifact:
    matches = [
        artifact
        for artifact in inputs
        if artifact.kind == "model_recommendations" and artifact.metadata.get("model") == name
    ]
    if len(matches) != 1:
        raise ValueError(f"需要一个 {name} 模型 artifact，实际为 {len(matches)}")
    return matches[0]


def _single_artifact(inputs: list[Artifact], kind: str) -> Artifact:
    matches = [artifact for artifact in inputs if artifact.kind == kind]
    if len(matches) != 1:
        raise ValueError(f"需要一个 {kind} artifact，实际为 {len(matches)}")
    return matches[0]


def _check_metric_gates(metrics: dict[str, Any], gates: dict[str, float]) -> None:
    failures = [
        f"{name}={metrics.get(name)} < {minimum}"
        for name, minimum in gates.items()
        if name not in metrics or float(metrics[name]) < minimum
    ]
    if failures:
        raise RuntimeError("模型指标门禁未通过：" + "; ".join(failures))


def _validate_version(version: str) -> None:
    if (
        not version
        or any(
            character not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789._-"
            for character in version
        )
        or ".." in version
    ):
        raise ValueError("model_version 只能包含字母、数字、点、下划线和连字符，且不能包含 ..")
