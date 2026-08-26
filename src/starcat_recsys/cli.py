"""命令行入口。"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
from datetime import date
from pathlib import Path

from starcat_recsys.bigquery_quota import (
    QUOTA_STOP_EXIT_CODE,
    format_quota_usage,
    read_bigquery_quota,
)
from starcat_recsys.bundle import LocalModelRegistry, query_bundle, verify_bundle
from starcat_recsys.interfaces import TaskContext
from starcat_recsys.pipeline import run_pipeline
from starcat_recsys.sources.bigquery import BigQueryConfig, BigQueryWatchEventSource
from starcat_recsys.sources.bigquery_push import BigQueryPushEventSource


def _add_daily_download_arguments(parser: argparse.ArgumentParser) -> None:
    """为 WatchEvent 与 PushEvent 下载命令维护同一组预算和恢复参数。"""

    parser.add_argument("--workspace", type=Path, required=True)
    parser.add_argument("--billing-project", required=True)
    parser.add_argument("--start-date", type=date.fromisoformat, required=True)
    parser.add_argument("--end-date", type=date.fromisoformat, required=True)
    parser.add_argument("--maximum-bytes-billed-per-day", type=int, required=True)
    parser.add_argument("--maximum-total-bytes-billed", type=int, required=True)
    parser.add_argument("--location", default="US")
    parser.add_argument("--retry-attempts", type=int, default=5)
    parser.add_argument("--retry-initial-delay-seconds", type=float, default=1.0)
    parser.add_argument("--run-id", required=True)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Starcat 推荐数据与离线训练管道")
    parser.add_argument("--version", action="version", version="%(prog)s 0.1.0")
    commands = parser.add_subparsers(dest="command", required=True)

    pipeline = commands.add_parser("pipeline", help="执行训练管道")
    pipeline_commands = pipeline.add_subparsers(dest="pipeline_command", required=True)
    run = pipeline_commands.add_parser("run", help="按 YAML 配置执行完整管道")
    run.add_argument("--config", type=Path, required=True)

    bigquery = commands.add_parser("bigquery", help="管理 GH Archive BigQuery 原始数据")
    bigquery_commands = bigquery.add_subparsers(dest="bigquery_command", required=True)
    download_watch = bigquery_commands.add_parser(
        "download-watch-events",
        help="按 UTC 日分区下载 WatchEvent，支持断点续传",
    )
    _add_daily_download_arguments(download_watch)
    download_push = bigquery_commands.add_parser(
        "download-push-events",
        help="按 UTC 日分区下载精简 PushEvent，支持断点续传",
    )
    _add_daily_download_arguments(download_push)
    quota_usage = bigquery_commands.add_parser(
        "quota-usage",
        help="输出当前 Project 本月 BigQuery 查询计费额度",
    )
    quota_usage.add_argument("--billing-project", required=True)
    quota_usage.add_argument("--location", default="US")
    quota_usage.add_argument("--warning-percent", type=float, default=80)
    quota_usage.add_argument("--stop-percent", type=float, default=90)

    bundle = commands.add_parser("bundle", help="校验或查询 ServingBundle")
    bundle_commands = bundle.add_subparsers(dest="bundle_command", required=True)
    verify = bundle_commands.add_parser("verify", help="校验 Bundle")
    verify.add_argument("path", type=Path)
    query = bundle_commands.add_parser("query", help="查询 Bundle")
    query.add_argument("path", type=Path)
    query.add_argument("--repo-id", action="append", type=int, required=True)
    query.add_argument("--exclude-repo-id", action="append", type=int, default=[])
    query.add_argument("--limit", type=int, default=20)

    registry = commands.add_parser("registry", help="管理本地 Model Registry")
    registry_commands = registry.add_subparsers(dest="registry_command", required=True)
    activate = registry_commands.add_parser("activate", help="回滚或切换激活版本")
    activate.add_argument("path", type=Path)
    activate.add_argument("version")
    return parser


def main(argv: list[str] | None = None) -> int:
    """解析命令并返回进程退出码。"""

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )
    parser = build_parser()
    arguments = parser.parse_args(argv)
    if arguments.command == "pipeline" and arguments.pipeline_command == "run":
        artifact = run_pipeline(arguments.config)
        print(json.dumps(artifact.metadata, ensure_ascii=False, indent=2))
    elif arguments.command == "bundle" and arguments.bundle_command == "verify":
        print(json.dumps(verify_bundle(arguments.path), ensure_ascii=False, indent=2))
    elif arguments.command == "bundle" and arguments.bundle_command == "query":
        print(
            json.dumps(
                query_bundle(
                    arguments.path,
                    arguments.repo_id,
                    limit=arguments.limit,
                    exclude_repo_ids=arguments.exclude_repo_id,
                ),
                ensure_ascii=False,
                indent=2,
            )
        )
    elif arguments.command == "registry" and arguments.registry_command == "activate":
        LocalModelRegistry(arguments.path).activate(arguments.version)
        print(json.dumps({"active": arguments.version}, ensure_ascii=False))
    elif arguments.command == "bigquery" and arguments.bigquery_command in {
        "download-watch-events",
        "download-push-events",
    }:
        config_document = {
            "billing_project": arguments.billing_project,
            "start_date": arguments.start_date.isoformat(),
            "end_date": arguments.end_date.isoformat(),
            "maximum_bytes_billed_per_day": arguments.maximum_bytes_billed_per_day,
            "maximum_total_bytes_billed": arguments.maximum_total_bytes_billed,
            "location": arguments.location,
        }
        config_hash = (
            "sha256:"
            + hashlib.sha256(json.dumps(config_document, sort_keys=True).encode()).hexdigest()
        )
        workspace = arguments.workspace.resolve()
        workspace.mkdir(parents=True, exist_ok=True)
        source_type = (
            BigQueryWatchEventSource
            if arguments.bigquery_command == "download-watch-events"
            else BigQueryPushEventSource
        )
        artifacts = source_type(
            BigQueryConfig(
                billing_project=arguments.billing_project,
                start_date=arguments.start_date,
                end_date=arguments.end_date,
                maximum_bytes_billed_per_day=arguments.maximum_bytes_billed_per_day,
                maximum_total_bytes_billed=arguments.maximum_total_bytes_billed,
                location=arguments.location,
                retry_attempts=arguments.retry_attempts,
                retry_initial_delay_seconds=arguments.retry_initial_delay_seconds,
            )
        ).collect(
            TaskContext(
                run_id=arguments.run_id,
                workspace=workspace,
                config_hash=config_hash,
                random_seed=0,
            )
        )
        print(
            json.dumps(
                {
                    "workspace": workspace.as_posix(),
                    "partition_count": len(artifacts),
                    "row_count": sum(int(item.metadata["row_count"]) for item in artifacts),
                    "estimated_total_bytes": sum(
                        int(item.metadata["estimated_bytes"]) for item in artifacts
                    ),
                },
                ensure_ascii=False,
                indent=2,
            )
        )
    elif arguments.command == "bigquery" and arguments.bigquery_command == "quota-usage":
        usage = read_bigquery_quota(arguments.billing_project, arguments.location)
        line, should_stop = format_quota_usage(
            usage,
            warning_percent=arguments.warning_percent,
            stop_percent=arguments.stop_percent,
        )
        print(line)
        return QUOTA_STOP_EXIT_CODE if should_stop else 0
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
