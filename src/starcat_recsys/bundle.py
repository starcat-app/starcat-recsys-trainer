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
    install: bool = True
    metrics_scope: str = "same_model"
    training: dict[str, Any] | None = None
    evaluation_window: dict[str, str | None] = field(default_factory=dict)

    def __post_init__(self) -> None:
        _validate_version(self.model_version)
        if not self.selected_model:
            raise ValueError("selected_model 不能为空")
        if self.metrics_scope not in {"same_model", "pre_refit"}:
            raise ValueError("metrics_scope 必须是 same_model 或 pre_refit")
        if self.metrics_scope == "pre_refit" and self.install:
            raise ValueError("引用重训前评估的候选禁止自动安装")


class ServingBundlePublisher:
    """构建 Bundle；候选模式只在 run 内落盘，不触碰共享 Registry。"""

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
        evaluated_checksum = metrics_document.get("model_checksums", {}).get(
            self._config.selected_model
        )
        if self._config.metrics_scope == "pre_refit" and not evaluated_checksum:
            raise ValueError("重训引用评估缺少原模型校验和")
        if (
            self._config.metrics_scope == "same_model"
            and evaluated_checksum
            and evaluated_checksum != model.checksum
        ):
            raise ValueError("评估模型与待构建模型校验和不一致")
        _check_metric_gates(metrics, self._config.metric_gates)

        staging = context.workspace / "published" / (self._config.model_version + ".staging")
        if staging.exists():
            shutil.rmtree(staging)
        staging.mkdir(parents=True)
        database_path = staging / "recommendations.sqlite"
        _build_database(database_path, artifact_path(model), artifact_path(repositories))
        content_indexes = [item for item in inputs if item.kind == "content_index"]
        if len(content_indexes) > 1:
            raise ValueError("最多提供一个内容索引")
        if content_indexes:
            _attach_content_index(database_path, artifact_path(content_indexes[0]))
        manifest = {
            "schema_version": 1,
            "model_version": self._config.model_version,
            "selected_model": self._config.selected_model,
            "created_at": datetime.now(UTC).isoformat(),
            "git_commit": self._config.git_commit,
            "config_hash": context.config_hash,
            "random_seed": context.random_seed,
            "metrics": metrics,
            "metrics_scope": self._config.metrics_scope,
            "training": self._config.training,
            "evaluation": {
                **{key: metrics_document.get(key) for key in ("split", "k", "maximum_seeds")},
                "dataset_checksum": metrics_document.get(
                    "evaluation_dataset_checksum", metrics_document.get("dataset_checksum")
                ),
                **self._config.evaluation_window,
            },
            "evaluation_checksum": metrics_artifact.checksum,
            "evaluated_model_checksum": metrics_document.get("model_checksums", {}).get(
                self._config.selected_model
            ),
            "input_artifacts": [
                {"kind": item.kind, "checksum": item.checksum}
                for item in sorted(inputs, key=lambda item: (item.kind, item.checksum))
            ],
        }
        if content_indexes:
            manifest["content_fallback"] = content_indexes[0].metadata
        write_json_atomic(staging / "manifest.json", manifest)
        # 外接卷可能自动生成 AppleDouble 文件；文件系统元数据不属于模型校验契约。
        checksums = {
            name: sha256_file(staging / name)
            for name in ("manifest.json", "recommendations.sqlite")
        }
        write_json_atomic(staging / "checksums.json", checksums)
        verify_bundle(staging)
        if self._config.install:
            installed = self._registry.install(staging, self._config.model_version)
        else:
            # 不使用临时 Registry 的 active 冒充“不激活”；候选根本不执行 install。
            installed = staging.with_name(self._config.model_version)
            if installed.exists():
                raise FileExistsError("候选版本已存在，不允许覆盖")
            staging.rename(installed)
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
    # 百分位只校准单条 item-to-item 边。多 seed 会叠加正负权重，已经不再服从
    # 训练时的单边分布，因此明确返回 NULL，避免伪造可比较的展示分。
    display_score_expression = "max(r.display_score)" if len(seed_repo_ids) == 1 else "NULL"
    query = f"""
        SELECT
            r.target_repo_id AS repo_id,
            p.full_name,
            p.description,
            p.primary_language,
            sum(r.score) AS score,
            {display_score_expression} AS display_score,
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


def _attach_content_index(database_path: Path, index_path: Path) -> None:
    """把已训练索引封装进不可变 Bundle，发布端仍只有原来的三个白名单文件。"""
    with sqlite3.connect(database_path) as database:
        database.execute("ATTACH DATABASE ? AS content_index", (str(index_path),))
        database.executescript(
            """
            CREATE TABLE content_vocabulary (
                term TEXT PRIMARY KEY, idf REAL NOT NULL
            ) WITHOUT ROWID;
            INSERT INTO content_vocabulary SELECT * FROM content_index.content_vocabulary;
            CREATE TABLE content_postings (
                term TEXT NOT NULL, repo_id INTEGER NOT NULL, weight REAL NOT NULL,
                PRIMARY KEY(term, repo_id)
            ) WITHOUT ROWID;
            INSERT INTO content_postings SELECT * FROM content_index.content_postings;
            """
        )
        invalid = database.execute(
            "SELECT COUNT(*) FROM content_postings "
            "WHERE repo_id NOT IN (SELECT repo_id FROM repositories)"
        ).fetchone()[0]
        if invalid:
            raise ValueError("内容索引引用了目录外目标")


def _build_database(database_path: Path, model_path: Path, repository_path: Path) -> None:
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
                display_score REAL NOT NULL CHECK(display_score >= 0 AND display_score <= 1),
                rank INTEGER NOT NULL,
                model TEXT NOT NULL,
                signals TEXT NOT NULL,
                PRIMARY KEY (source_repo_id, target_repo_id)
            );
            CREATE INDEX recommendations_target_idx ON recommendations(target_repo_id);
            """
        )
        for batch in pq.ParquetFile(repository_path).iter_batches(batch_size=10_000):
            database.executemany(
                "INSERT INTO repositories VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
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
                    for row in batch.to_pylist()
                ),
            )
        for batch in pq.ParquetFile(model_path).iter_batches(batch_size=50_000):
            database.executemany(
                "INSERT INTO recommendations VALUES (?, ?, ?, ?, ?, ?, ?)",
                (
                    (
                        row["source_repo_id"],
                        row["target_repo_id"],
                        row["score"],
                        # 先写原始分，全部公开边落库并完成边界清理后再统一校准。
                        # 这样无需把全部模型行加载进 Python 内存。
                        row["score"],
                        row["rank"],
                        row["model"],
                        row["signals"],
                    )
                    for row in batch.to_pylist()
                ),
            )
        # 模型只应包含 DatasetBuilder 筛选后的公开仓库，但 Bundle 边界仍做一次
        # SQLite 内部校验，避免为了构造 Python repo_id 集合而让数百万仓库常驻内存。
        database.execute(
            """
            DELETE FROM recommendations
            WHERE source_repo_id NOT IN (SELECT repo_id FROM repositories)
               OR target_repo_id NOT IN (SELECT repo_id FROM repositories)
            """
        )
        # 原始 co-star 分数与 SimRepo/Qdrant 的向量分数不在同一尺度。使用最终
        # ServingBundle 内全部有效边的经验累积分布做单调校准：排序保持不变，
        # 0.05 这类全局高分边会显示为其真实模型百分位。最高值封顶 0.99，避免
        # UI 把有限样本中的第一名表达成绝对的“100% 匹配”。
        database.executescript(
            """
            CREATE TEMP TABLE recommendation_display_scores AS
            SELECT
                source_repo_id,
                target_repo_id,
                min(0.99, cume_dist() OVER (ORDER BY score)) AS display_score
            FROM recommendations;

            -- 这里有上百万条内容融合边；相关子查询必须按复合键查找，不能为每条
            -- 推荐重新扫描整个临时校准表，否则构建复杂度会退化为 O(N²)。
            CREATE UNIQUE INDEX recommendation_display_scores_pair_idx
                ON recommendation_display_scores(source_repo_id, target_repo_id);

            UPDATE recommendations
            SET display_score = (
                SELECT calibrated.display_score
                FROM recommendation_display_scores calibrated
                WHERE calibrated.source_repo_id = recommendations.source_repo_id
                  AND calibrated.target_repo_id = recommendations.target_repo_id
            );

            DROP TABLE recommendation_display_scores;
            """
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
