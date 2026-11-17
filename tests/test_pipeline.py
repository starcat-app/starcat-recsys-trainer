from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import yaml

from starcat_recsys.artifacts import artifact_for
from starcat_recsys.cli import main
from starcat_recsys.interfaces import Artifact, TaskContext
from starcat_recsys.pipeline import (
    _repository_ids_from_canonical,
    _source_connector,
    _training_reference_time,
    run_pipeline,
)
from starcat_recsys.sources.bigquery import BigQueryConfig
from starcat_recsys.sources.bigquery_push import BigQueryPushMetadataConfig
from starcat_recsys.sources.local_lake import LocalLakePartitionSource


def test_runs_end_to_end_pipeline_and_cli_query(tmp_path: Path, capsys: object) -> None:
    interactions = tmp_path / "interactions.jsonl"
    rows: list[dict[str, object]] = []
    subject_rows = {
        "u1": [
            (1, "2025-01-01T00:00:00Z"),
            (2, "2025-02-01T00:00:00Z"),
            (3, "2025-09-01T00:00:00Z"),
        ],
        "u2": [
            (1, "2025-01-01T00:00:00Z"),
            (2, "2025-02-01T00:00:00Z"),
            (3, "2025-09-01T00:00:00Z"),
        ],
        "u3": [
            (1, "2025-01-01T00:00:00Z"),
            (4, "2025-03-01T00:00:00Z"),
            (3, "2025-09-01T00:00:00Z"),
        ],
    }
    for subject, subject_interactions in subject_rows.items():
        rows.extend(
            _interaction(subject, repo_id, occurred_at)
            for repo_id, occurred_at in subject_interactions
        )
    interactions.write_text("".join(json.dumps(row) + "\n" for row in rows))
    repositories = tmp_path / "repositories.jsonl"
    repositories.write_text(
        "".join(
            json.dumps(
                {
                    "fetched_at": "2026-01-01T00:00:00Z",
                    "repository": {
                        "id": repo_id,
                        "full_name": f"owner/repo-{repo_id}",
                        "topics": ["demo"],
                        "visibility": "public",
                    },
                }
            )
            + "\n"
            for repo_id in [1, 2, 3, 4]
        )
    )
    config = {
        "run_id": "demo-run",
        "workspace": "workspace",
        "registry": "registry",
        "random_seed": 7,
        "sources": [
            {
                "type": "local_file",
                "path": "interactions.jsonl",
                "source": "starcat_snapshot",
                "kind": "raw_interactions",
            },
            {
                "type": "local_file",
                "path": "repositories.jsonl",
                "source": "github_api",
                "kind": "raw_repositories",
            },
        ],
        "dataset": {
            "train_end": "2025-06-01T00:00:00Z",
            "validation_end": "2025-12-01T00:00:00Z",
        },
        "training": {
            "reference_time": "2025-06-01T00:00:00Z",
            "top_k": 2,
            "svd_components": 2,
            "run_ablations": True,
        },
        "evaluation": {"split": "validation", "k": 2},
        "publish": {"model_version": "demo-v1", "selected_model": "costar"},
    }
    config_path = tmp_path / "pipeline.yaml"
    config_path.write_text(yaml.safe_dump(config, sort_keys=False))

    artifact = run_pipeline(config_path)

    bundle = Path(artifact.metadata["bundle_uri"].removeprefix("file://"))
    assert bundle.is_dir()
    bundle_manifest = json.loads((bundle / "manifest.json").read_text())
    assert bundle_manifest["training"]["interactions"] == 6
    assert bundle_manifest["training"]["subjects"] == 3
    assert bundle_manifest["training"]["completed_at"] <= bundle_manifest["created_at"]
    assert bundle_manifest["evaluation"]["train_end"] == "2025-06-01T00:00:00+00:00"
    manifest = json.loads((tmp_path / "workspace/runs/demo-run/run-manifest.json").read_text())
    assert all(task["status"] == "completed" for task in manifest["tasks"])
    assert main(["bundle", "verify", bundle.as_posix()]) == 0
    assert main(["bundle", "query", bundle.as_posix(), "--repo-id", "1"]) == 0
    # capsys 的类型由 pytest 提供，此处只需证明 CLI 输出了 JSON。
    output = capsys.readouterr().out  # type: ignore[attr-defined]
    assert "model_version" in output
    assert "repo_id" in output

    with pytest.raises(FileExistsError, match="run_id 已存在"):
        run_pipeline(config_path)

    # 长任务后续阶段失败时，可以校验并复用已落盘 Canonical，而不重复采集与标准化。
    resume_config = {
        **config,
        "run_id": "resume-run",
        "workspace": "resume-workspace",
        "resume_canonical_manifest": (
            tmp_path / "workspace/runs/demo-run/canonical/manifest.json"
        ).as_posix(),
        "sources": [],
        "publish": {"model_version": "demo-v2", "selected_model": "costar"},
    }
    resume_config_path = tmp_path / "resume-pipeline.yaml"
    resume_config_path.write_text(yaml.safe_dump(resume_config, sort_keys=False))

    resumed_artifact = run_pipeline(resume_config_path)

    assert Path(resumed_artifact.metadata["bundle_uri"].removeprefix("file://")).is_dir()
    resume_manifest = json.loads(
        (tmp_path / "resume-workspace/runs/resume-run/run-manifest.json").read_text()
    )
    assert resume_manifest["tasks"][0]["task_name"] == "resume:canonical"
    assert (tmp_path / "resume-workspace/runs/resume-run/canonical-resume.json").is_file()

    # 候选模式不能安装到原 Registry；SVD 可显式跳过以避免无关重训。
    active = tmp_path / "registry/active.json"
    before = active.read_bytes()
    candidate_config = {
        **resume_config,
        "run_id": "candidate-run",
        "workspace": "candidate",
        "training": {**config["training"], "train_svd": False},
        "publish": {"model_version": "candidate-v1", "install": False},
    }
    candidate_path = tmp_path / "candidate.yaml"
    candidate_path.write_text(yaml.safe_dump(candidate_config))
    candidate = run_pipeline(candidate_path)
    assert active.read_bytes() == before
    assert not (tmp_path / "registry/versions/candidate-v1").exists()
    candidate_bundle = Path(candidate.metadata["bundle_uri"].removeprefix("file://"))
    assert (
        json.loads((candidate_bundle / "manifest.json").read_text())["metrics_scope"]
        == "same_model"
    )
    metrics_path = tmp_path / "candidate/runs/candidate-run/evaluation/validation-metrics.json"
    reference = json.loads(metrics_path.read_text())
    reference["split"] = "test"
    reference["models"]["costar"]["evaluated_subjects"] = 1
    reference_path = tmp_path / "reference-test.json"
    reference_path.write_text(json.dumps(reference))
    refit_config = {
        **candidate_config,
        "run_id": "refit-run",
        "workspace": "refit",
        "evaluation": {"reference_metrics": str(reference_path)},
        "publish": {"model_version": "refit-v1", "install": False},
    }
    refit_path = tmp_path / "refit.yaml"
    refit_path.write_text(yaml.safe_dump(refit_config))
    refit = run_pipeline(refit_path)
    manifest_path = Path(refit.uri.removeprefix("file://"))
    assert json.loads(manifest_path.read_text())["metrics_scope"] == "pre_refit"
    assert active.read_bytes() == before


def test_candidate_rejects_remote_delivery_before_collection(tmp_path: Path) -> None:
    """候选模式禁止远端交付，必须在训练或外部副作用之前失败。"""
    path = tmp_path / "invalid.yaml"
    path.write_text(
        yaml.safe_dump(
            {"publish": {"install": False, "recommend_api": {"base_url": "https://invalid"}}}
        )
    )
    with pytest.raises(ValueError, match="禁止向 Recommend API"):
        run_pipeline(path)


def test_single_subject_skips_svd_and_publishes_costar_bundle(tmp_path: Path) -> None:
    interactions = tmp_path / "interactions.jsonl"
    interactions.write_text(
        "".join(
            json.dumps(_interaction("u1", repo_id, "2025-01-01T00:00:00Z")) + "\n"
            for repo_id in [1, 2, 3]
        )
    )
    repositories = tmp_path / "repositories.jsonl"
    repositories.write_text(
        "".join(
            json.dumps(
                {
                    "fetched_at": "2026-01-01T00:00:00Z",
                    "repository": {
                        "id": repo_id,
                        "full_name": f"owner/repo-{repo_id}",
                        "topics": ["demo"],
                        "visibility": "public",
                    },
                }
            )
            + "\n"
            for repo_id in [1, 2, 3]
        )
    )
    config = {
        "run_id": "single-subject-run",
        "workspace": "workspace",
        "registry": "registry",
        "sources": [
            {
                "type": "local_file",
                "path": "interactions.jsonl",
                "source": "starcat_snapshot",
                "kind": "raw_interactions",
            },
            {
                "type": "local_file",
                "path": "repositories.jsonl",
                "source": "github_api",
                "kind": "raw_repositories",
            },
        ],
        "dataset": {
            "train_end": "2025-06-01T00:00:00Z",
            "validation_end": "2025-12-01T00:00:00Z",
        },
        "training": {
            "reference_time": "2025-06-01T00:00:00Z",
            "top_k": 2,
            "svd_components": 2,
            "run_ablations": False,
        },
        "evaluation": {"split": "validation", "k": 2},
        "publish": {"model_version": "single-subject-v1", "selected_model": "costar"},
    }
    config_path = tmp_path / "pipeline.yaml"
    config_path.write_text(yaml.safe_dump(config, sort_keys=False))

    artifact = run_pipeline(config_path)

    assert Path(artifact.metadata["bundle_uri"].removeprefix("file://")).is_dir()
    manifest = json.loads(
        (tmp_path / "workspace/runs/single-subject-run/run-manifest.json").read_text()
    )
    svd_task = next(task for task in manifest["tasks"] if task["task_name"] == "train:svd")
    assert svd_task["status"] == "skipped"
    assert "至少需要 2 个主体" in svd_task["detail"]
    assert all(task["status"] != "failed" for task in manifest["tasks"])


def _interaction(subject: str, repo_id: int, occurred_at: str) -> dict[str, object]:
    return {
        "source_record_id": f"{subject}:{repo_id}",
        "source_subject_id": subject,
        "repo_id": repo_id,
        "starred_at": occurred_at,
        "captured_at": "2026-01-01T00:00:00Z",
        "snapshot_id": f"snapshot-{subject}",
    }


def test_selects_bounded_popular_repositories_for_deferred_metadata(tmp_path: Path) -> None:
    path = tmp_path / "interactions.parquet"
    pq.write_table(pa.table({"repo_id": [3, 2, 2, 1, 1, 1]}), path)

    repo_ids = _repository_ids_from_canonical([artifact_for(path, "canonical_interactions", {})], 2)

    assert repo_ids == [1, 2]


def test_builds_bigquery_push_metadata_connector(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    captured: list[BigQueryPushMetadataConfig] = []
    marker = object()
    monkeypatch.setattr(
        "starcat_recsys.pipeline.BigQueryPushEventMetadataSource",
        lambda config: captured.append(config) or marker,
    )
    connector = _source_connector(
        {
            "type": "bigquery_push_metadata",
            "billing_project": "starcat-test",
            "start_year": 2016,
            "end_year": 2025,
            "maximum_bytes_billed": 800_000_000_000,
            "minimum_push_events": 2,
        },
        tmp_path,
    )

    assert connector is marker
    assert captured[0].start_year == 2016
    assert captured[0].minimum_push_events == 2


def test_builds_bigquery_connector_with_resume_configuration(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    captured: list[BigQueryConfig] = []
    marker = object()
    monkeypatch.setattr(
        "starcat_recsys.pipeline.BigQueryWatchEventSource",
        lambda config: captured.append(config) or marker,
    )

    connector = _source_connector(
        {
            "type": "bigquery",
            "billing_project": "starcat-test",
            "start_date": "2026-08-22",
            "end_date": "2026-08-23",
            "maximum_bytes_billed_per_day": 1_000,
            "maximum_total_bytes_billed": 2_000,
            "retry_attempts": 7,
            "retry_initial_delay_seconds": 2,
        },
        tmp_path,
    )

    assert connector is marker
    assert captured[0].maximum_total_bytes_billed == 2_000
    assert captured[0].retry_attempts == 7
    assert captured[0].retry_initial_delay_seconds == 2


def test_builds_local_lake_partition_connector(tmp_path: Path) -> None:
    connector = _source_connector(
        {
            "type": "local_lake_partitions",
            "workspace": "/Volumes/T0/Starcat/bigquery/watch-events-2016-2026",
            "dataset": "watch_events",
            "start_date": "2016-01-01",
            "end_date": "2026-08-25",
            "verify_checksums": False,
        },
        tmp_path,
    )

    assert isinstance(connector, LocalLakePartitionSource)


def test_download_watch_events_cli_reports_summary(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: object
) -> None:
    captured: list[tuple[BigQueryConfig, TaskContext]] = []
    raw = tmp_path / "fixture.parquet"
    raw.write_bytes(b"fixture")

    class FakeWatchSource:
        def __init__(self, config: BigQueryConfig) -> None:
            self.config = config

        def collect(self, context: TaskContext) -> list[Artifact]:
            captured.append((self.config, context))
            return [
                Artifact(
                    kind="raw_interactions",
                    uri=raw.resolve().as_uri(),
                    checksum="sha256:fixture",
                    metadata={"row_count": 123, "estimated_bytes": 456},
                )
            ]

    monkeypatch.setattr("starcat_recsys.cli.BigQueryWatchEventSource", FakeWatchSource)

    assert (
        main(
            [
                "bigquery",
                "download-watch-events",
                "--workspace",
                (tmp_path / "download").as_posix(),
                "--billing-project",
                "starcat-test",
                "--start-date",
                "2026-08-22",
                "--end-date",
                "2026-08-23",
                "--maximum-bytes-billed-per-day",
                "1000",
                "--maximum-total-bytes-billed",
                "2000",
                "--run-id",
                "watch-test",
                "--append-existing",
            ]
        )
        == 0
    )

    output = json.loads(capsys.readouterr().out)  # type: ignore[attr-defined]
    assert output["partition_count"] == 1
    assert output["row_count"] == 123
    assert captured[0][0].maximum_total_bytes_billed == 2_000
    assert captured[0][0].append_existing is True
    assert captured[0][1].run_id == "watch-test"


def test_download_push_events_cli_uses_independent_source(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: object
) -> None:
    captured: list[tuple[BigQueryConfig, TaskContext]] = []
    raw = tmp_path / "push.parquet"
    raw.write_bytes(b"fixture")

    class FakePushSource:
        def __init__(self, config: BigQueryConfig) -> None:
            self.config = config

        def collect(self, context: TaskContext) -> list[Artifact]:
            captured.append((self.config, context))
            return [
                Artifact(
                    kind="raw_push_events",
                    uri=raw.resolve().as_uri(),
                    checksum="sha256:push",
                    metadata={"row_count": 321, "estimated_bytes": 654},
                )
            ]

    monkeypatch.setattr("starcat_recsys.cli.BigQueryPushEventSource", FakePushSource)

    assert (
        main(
            [
                "bigquery",
                "download-push-events",
                "--workspace",
                (tmp_path / "push-download").as_posix(),
                "--billing-project",
                "starcat-test",
                "--start-date",
                "2026-08-22",
                "--end-date",
                "2026-08-23",
                "--maximum-bytes-billed-per-day",
                "1000",
                "--maximum-total-bytes-billed",
                "2000",
                "--run-id",
                "push-test",
            ]
        )
        == 0
    )

    output = json.loads(capsys.readouterr().out)  # type: ignore[attr-defined]
    assert output["partition_count"] == 1
    assert output["row_count"] == 321
    assert captured[0][0].maximum_total_bytes_billed == 2_000
    assert captured[0][1].run_id == "push-test"


def test_repair_push_checkpoint_cli_requires_explicit_apply_count(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: object
) -> None:
    captured: list[tuple[BigQueryConfig, TaskContext, bool, int | None]] = []

    class FakePushSource:
        @classmethod
        def repair_checkpoint(
            cls,
            config: BigQueryConfig,
            context: TaskContext,
            *,
            apply: bool,
            expected_mismatch_count: int | None = None,
        ) -> dict[str, object]:
            captured.append((config, context, apply, expected_mismatch_count))
            return {"status": "repaired", "checksum_mismatch_count": 230}

    monkeypatch.setattr("starcat_recsys.cli.BigQueryPushEventSource", FakePushSource)

    assert (
        main(
            [
                "bigquery",
                "repair-push-events-checkpoint",
                "--workspace",
                tmp_path.as_posix(),
                "--billing-project",
                "starcat-test",
                "--start-date",
                "2016-01-01",
                "--end-date",
                "2026-08-25",
                "--maximum-bytes-billed-per-day",
                "1000",
                "--maximum-total-bytes-billed",
                "2000",
                "--run-id",
                "push-test",
                "--apply",
                "--expected-mismatch-count",
                "230",
            ]
        )
        == 0
    )

    output = json.loads(capsys.readouterr().out)  # type: ignore[attr-defined]
    assert output["status"] == "repaired"
    assert captured[0][1].workspace == tmp_path.resolve()
    assert captured[0][2:] == (True, 230)


def test_rejects_training_reference_time_different_from_train_end() -> None:
    with pytest.raises(ValueError, match="reference_time"):
        _training_reference_time(
            {"reference_time": "2025-06-02T00:00:00Z"},
            datetime.fromisoformat("2025-06-01T00:00:00+00:00"),
        )


def test_accepts_equivalent_training_reference_time_with_different_offset() -> None:
    reference_time = _training_reference_time(
        {"reference_time": "2025-06-01T08:00:00+08:00"},
        datetime.fromisoformat("2025-06-01T00:00:00+00:00"),
    )

    assert reference_time.isoformat() == "2025-06-01T08:00:00+08:00"
