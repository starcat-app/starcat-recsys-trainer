"""验证外部用户的合成训练、可分发产物及私有周配置初始化，不访问外部服务。"""

from __future__ import annotations

import json
import shutil
import sqlite3
from dataclasses import asdict
from datetime import date
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import yaml

from starcat_recsys.artifacts import artifact_for, write_json_atomic
from starcat_recsys.model_release import prepare_release
from starcat_recsys.pipeline import run_pipeline
from starcat_recsys.weekly_init import initialize_weekly


def test_hybrid_quickstart_runs_without_operator_data(tmp_path: Path) -> None:
    """原样使用公开入门配置及 fixtures，确认模型含融合边、内容索引及真实元数据。"""
    repository = Path(__file__).parents[1]
    (tmp_path / "configs").mkdir()
    shutil.copytree(repository / "examples", tmp_path / "examples")
    configuration = tmp_path / "configs/example-hybrid.yaml"
    shutil.copyfile(repository / "configs/example-hybrid.yaml", configuration)
    bundle = Path(run_pipeline(configuration).uri.removeprefix("file://")).parent
    manifest = json.loads((bundle / "manifest.json").read_text())
    assert manifest["selected_model"] == "hybrid_25"
    assert manifest["training"]["interactions"] > 0
    assert manifest["training"]["subjects"] > 0
    assert manifest["training"]["started_at"] <= manifest["training"]["completed_at"]
    with sqlite3.connect(bundle / "recommendations.sqlite") as database:
        assert database.execute("SELECT count(*) FROM recommendations").fetchone()[0] > 0
        assert database.execute("SELECT count(*) FROM content_postings").fetchone()[0] > 0
    package = prepare_release(bundle, tmp_path / "release")
    assert (package / "example-hybrid-v1.zip").is_file()
    assert "private" not in (package / "model-card.md").read_text().split("## Metrics")[0]


def test_weekly_initialization_preserves_own_inputs_and_rejects_invalid_source(
    tmp_path: Path,
) -> None:
    """配置属于用户自己的首次训练；拒绝混源、摘要变化和覆盖，且不安装调度或发布。"""
    run = tmp_path / "run"
    dataset = run / "dataset"
    dataset.mkdir(parents=True)
    interactions = dataset / "interactions.parquet"
    pq.write_table(pa.table({"source": ["gh_archive"]}), interactions)
    pq.write_table(pa.table({"repo_id": [1]}), dataset / "repositories.parquet")
    bundle = tmp_path / "bundle"
    bundle.mkdir()
    with sqlite3.connect(bundle / "recommendations.sqlite") as database:
        database.execute("CREATE TABLE repositories(repo_id)")
    write_json_atomic(bundle / "manifest.json", {"schema_version": 1, "model_version": "own-v1"})
    from starcat_recsys.artifacts import sha256_file

    write_json_atomic(
        bundle / "checksums.json",
        {name: sha256_file(bundle / name) for name in ("manifest.json", "recommendations.sqlite")},
    )
    artifact = artifact_for(bundle / "manifest.json", "serving_bundle", {})
    write_json_atomic(run / "run-manifest.json", {"bundle": asdict(artifact)})
    raw = tmp_path / "raw"
    write_json_atomic(raw / "download-state.json", {"completed_partitions": {"20261031": {}}})
    output = tmp_path / "operator/weekly.yaml"
    initialize_weekly(run, raw, date(2026, 10, 31), output, tmp_path / "api")
    config: dict[str, Any] = yaml.safe_load(output.read_text())
    assert config["frozen_training_dataset"] == str(interactions)
    assert config["initial_bundle"] == str(bundle)
    assert config["public_distribution"] == {"enabled": False}
    assert output.stat().st_mode & 0o777 == 0o600
    assert not Path(config["state_file"]).exists()
    with pytest.raises(FileExistsError):
        initialize_weekly(run, raw, date(2026, 10, 31), output, tmp_path / "api")
    pq.write_table(pa.table({"source": ["starcat_snapshot"]}), interactions)
    with pytest.raises(ValueError, match="纯 GH Archive"):
        initialize_weekly(run, raw, date(2026, 10, 31), tmp_path / "other.yaml", tmp_path / "api")
    (bundle / "manifest.json").write_text("{}")
    with pytest.raises(ValueError, match="摘要"):
        initialize_weekly(run, raw, date(2026, 10, 31), tmp_path / "other.yaml", tmp_path / "api")
