"""本机 BigQuery SQL Lab 的安全、预算和结果边界测试。"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

from starcat_recsys.bigquery_lab import (
    BigQueryDryRun,
    BigQueryField,
    BigQueryQueryPage,
    _bounded_rows,
    dry_run_sql_lab,
    query_sql_lab,
    read_sql_file,
)
from starcat_recsys.cli import main


class FakeLabGateway:
    def __init__(
        self,
        *,
        plan: BigQueryDryRun | None = None,
        page: BigQueryQueryPage | None = None,
    ) -> None:
        self.plan = plan or BigQueryDryRun(
            statement_type="SELECT",
            estimated_bytes=128,
            fields=(BigQueryField("repo_id", "INTEGER", "NULLABLE"),),
            referenced_tables=("githubarchive.day.20240101",),
        )
        self.page = page or BigQueryQueryPage(
            job_id="job-test",
            processed_bytes=128,
            billed_bytes=10_000_000,
            total_rows=1,
            fields=self.plan.fields,
            rows=({"repo_id": 1},),
            truncated=False,
        )
        self.dry_runs: list[str] = []
        self.queries: list[tuple[str, int, int, int]] = []

    def dry_run(self, sql: str) -> BigQueryDryRun:
        self.dry_runs.append(sql)
        return self.plan

    def query(
        self,
        sql: str,
        maximum_bytes_billed: int,
        maximum_result_rows: int,
        maximum_result_bytes: int,
    ) -> BigQueryQueryPage:
        self.queries.append((sql, maximum_bytes_billed, maximum_result_rows, maximum_result_bytes))
        return self.page


def test_dry_run_returns_hash_schema_and_budget() -> None:
    gateway = FakeLabGateway()

    document = dry_run_sql_lab(
        "SELECT repo.id FROM `githubarchive.day.20240101`;",
        1_000,
        gateway,
    )

    assert document["operation"] == "dry_run"
    assert document["sql_sha256"].startswith("sha256:")
    assert document["estimated_bytes"] == 128
    assert document["referenced_tables"] == ["githubarchive.day.20240101"]
    assert gateway.dry_runs == ["SELECT repo.id FROM `githubarchive.day.20240101`"]


@pytest.mark.parametrize(
    "sql",
    [
        "DELETE FROM `githubarchive.day.20240101` WHERE TRUE",
        "SELECT 1; SELECT 2",
        "EXPORT DATA OPTIONS(uri='gs://bucket/a') AS SELECT 1",
        "SELECT * FROM `another-project.dataset.table`",
    ],
)
def test_rejects_unsafe_or_out_of_scope_sql_before_dry_run(sql: str) -> None:
    gateway = FakeLabGateway()

    with pytest.raises(ValueError):
        dry_run_sql_lab(sql, 1_000, gateway)

    assert gateway.dry_runs == []


def test_rejects_non_select_plan_and_budget_overrun() -> None:
    with pytest.raises(ValueError, match="不是只读 SELECT"):
        dry_run_sql_lab(
            "SELECT 1",
            1_000,
            FakeLabGateway(
                plan=BigQueryDryRun("SCRIPT", 1, (), ()),
            ),
        )

    with pytest.raises(RuntimeError, match="超过上限"):
        dry_run_sql_lab(
            "SELECT * FROM `githubarchive.day.20240101`",
            100,
            FakeLabGateway(
                plan=BigQueryDryRun(
                    "SELECT",
                    101,
                    (),
                    ("githubarchive.day.20240101",),
                )
            ),
        )


def test_query_requires_same_dry_run_hash_and_returns_bounded_page() -> None:
    sql = "SELECT repo.id AS repo_id FROM `githubarchive.day.20240101`"
    gateway = FakeLabGateway()
    preview = dry_run_sql_lab(sql, 1_000, gateway)

    document = query_sql_lab(
        sql,
        1_000,
        str(preview["sql_sha256"]),
        50,
        gateway,
    )

    assert document["job_id"] == "job-test"
    assert document["rows"] == [{"repo_id": 1}]
    assert document["returned_rows"] == 1
    assert gateway.queries[0][1:3] == (1_000, 50)

    with pytest.raises(ValueError, match="必须重新 dry run"):
        query_sql_lab(sql + " LIMIT 1", 1_000, str(preview["sql_sha256"]), 50, gateway)


def test_bounded_rows_serializes_bigquery_values_and_stops_by_size() -> None:
    rows: list[dict[str, Any]] = [
        {"created_at": datetime(2026, 1, 1, tzinfo=UTC), "payload": b"abc"},
        {"created_at": datetime(2026, 1, 2, tzinfo=UTC), "payload": b"def"},
    ]

    accepted, truncated = _bounded_rows(rows, maximum_rows=2, maximum_bytes=80)

    assert len(accepted) == 1
    assert accepted[0]["payload"] == "YWJj"
    assert truncated is True


def test_sql_file_and_cli_dry_run_use_file_instead_of_argv(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    sql_file = tmp_path / "query.sql"
    sql_file.write_text("SELECT 1", encoding="utf-8")
    gateway = FakeLabGateway(
        plan=BigQueryDryRun("SELECT", 0, (BigQueryField("f0_", "INTEGER", "NULLABLE"),), ())
    )
    monkeypatch.setattr(
        "starcat_recsys.cli.GoogleBigQueryLabGateway",
        lambda _project, _location: gateway,
    )

    assert read_sql_file(sql_file) == "SELECT 1"
    assert (
        main(
            [
                "bigquery",
                "sql-lab-dry-run",
                "--sql-file",
                sql_file.as_posix(),
                "--billing-project",
                "starcat-test",
                "--maximum-bytes-billed",
                "1000",
            ]
        )
        == 0
    )
    document = json.loads(capsys.readouterr().out)
    assert document["operation"] == "dry_run"
    assert document["estimated_bytes"] == 0
