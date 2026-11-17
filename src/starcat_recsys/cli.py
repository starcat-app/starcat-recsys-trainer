"""命令行入口。"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import sys
from datetime import date
from pathlib import Path

from starcat_recsys.bigquery_lab import (
    GoogleBigQueryLabGateway,
    dry_run_sql_lab,
    query_sql_lab,
    read_sql_file,
)
from starcat_recsys.bigquery_quota import (
    QUOTA_STOP_EXIT_CODE,
    BigQueryQuotaConnectionError,
    format_quota_usage,
    quota_usage_document,
    read_bigquery_quota,
)
from starcat_recsys.bundle import LocalModelRegistry, query_bundle, verify_bundle
from starcat_recsys.history_backfill import backfill_history
from starcat_recsys.interfaces import TaskContext
from starcat_recsys.lake_catalog import inspect_bigquery_dataset
from starcat_recsys.model_history import bundle_record, record_result, render_history
from starcat_recsys.model_release import (
    GitHubDistribution,
    prepare_release,
    read_release_package,
)
from starcat_recsys.pipeline import run_pipeline
from starcat_recsys.sources.bigquery import BigQueryConfig, BigQueryWatchEventSource
from starcat_recsys.sources.bigquery_push import BigQueryPushEventSource
from starcat_recsys.watch_catchup import plan_watch_catch_up
from starcat_recsys.weekly_init import initialize_weekly
from starcat_recsys.weekly_reporting import github_client


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
    parser.add_argument(
        "--append-existing",
        action="store_true",
        help="只校验请求日期，并连续追加到同一 checkpoint",
    )


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
    repair_push = bigquery_commands.add_parser(
        "repair-push-events-checkpoint",
        help="验证现有 PushEvent Parquet 后受控修复 checkpoint checksum",
    )
    _add_daily_download_arguments(repair_push)
    repair_push.add_argument("--apply", action="store_true")
    repair_push.add_argument("--expected-mismatch-count", type=int)
    plan_watch_catch_up_command = bigquery_commands.add_parser(
        "plan-watch-events-catch-up",
        help="根据 checkpoint 计算 WatchEvent 到最新完整 UTC 日的追赶范围",
    )
    plan_watch_catch_up_command.add_argument("--workspace", type=Path, required=True)
    plan_watch_catch_up_command.add_argument("--target-date", type=date.fromisoformat)
    quota_usage = bigquery_commands.add_parser(
        "quota-usage",
        help="输出当前 Project 本月 BigQuery 查询计费额度",
    )
    quota_usage.add_argument("--billing-project", required=True)
    quota_usage.add_argument("--location", default="US")
    quota_usage.add_argument("--warning-percent", type=float, default=80)
    quota_usage.add_argument("--stop-percent", type=float, default=90)
    quota_usage.add_argument("--format", choices=("text", "json"), default="text")
    sql_dry_run = bigquery_commands.add_parser(
        "sql-lab-dry-run",
        help="对本机 SQL Lab 的只读查询执行 dry run",
    )
    _add_sql_lab_arguments(sql_dry_run)
    sql_query = bigquery_commands.add_parser(
        "sql-lab-query",
        help="执行已经 dry run 且预算一致的 SQL Lab 查询",
    )
    _add_sql_lab_arguments(sql_query)
    sql_query.add_argument("--expected-sql-sha256", required=True)
    sql_query.add_argument("--maximum-result-rows", type=int, default=200)

    lake = commands.add_parser("lake", help="检查并登记本地数据湖产物")
    lake_commands = lake.add_subparsers(dest="lake_command", required=True)
    for command, event, help_text in (
        ("inspect-watch-events", "watch-events", "只读检查既有 WatchEvent Raw"),
        ("inspect-push-events", "push-events", "只读检查既有 PushEvent Raw"),
    ):
        inspect = lake_commands.add_parser(command, help=help_text)
        inspect.add_argument("--workspace", type=Path, required=True)
        inspect.set_defaults(lake_event=event)

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

    history = commands.add_parser("history", help="维护公开训练成果")
    history_commands = history.add_subparsers(dest="history_command", required=True)
    history_record = history_commands.add_parser("record", help="从 Bundle 归档聚合成果")
    history_record.add_argument("--bundle", type=Path, required=True)
    history_record.add_argument("--catalog", type=Path, required=True)
    history_render = history_commands.add_parser("render", help="从索引生成历史页和模型说明")
    history_render.add_argument("--catalog", type=Path, required=True)
    history_render.add_argument("--output", type=Path, required=True)
    history_backfill = history_commands.add_parser("backfill", help="只读历史元数据并回补公开档案")
    history_backfill.add_argument("--root", type=Path, required=True)
    history_backfill.add_argument("--catalog", type=Path, required=True)
    history_backfill.add_argument("--output", type=Path, required=True)
    history_sync = history_commands.add_parser("sync", help="同步生成文档到 GitHub")
    history_sync.add_argument("--catalog", type=Path, required=True)
    history_sync.add_argument("--repository", required=True)
    history_sync.add_argument("--branch", default="main")
    history_sync.add_argument("--apply", action="store_true")
    history_sync.add_argument("--token-env", default="STARCAT_MODELS_GITHUB_TOKEN")

    release = commands.add_parser("release", help="准备或分发预训练模型")
    release_commands = release.add_subparsers(dest="release_command", required=True)
    release_prepare = release_commands.add_parser("prepare", help="仅生成本地发布资产")
    release_prepare.add_argument("--bundle", type=Path, required=True)
    release_prepare.add_argument("--output", type=Path, required=True)
    release_publish = release_commands.add_parser("publish", help="发布校验后的模型资产")
    release_publish.add_argument("--package", type=Path, required=True)
    release_publish.add_argument("--repository", required=True)
    release_publish.add_argument("--target", default="main")
    release_publish.add_argument("--apply", action="store_true")
    release_publish.add_argument("--token-env", default="STARCAT_MODELS_GITHUB_TOKEN")

    weekly_init = commands.add_parser("weekly-init", help="由首次训练结果生成私有周任务配置")
    weekly_init.add_argument("--run", type=Path, required=True)
    weekly_init.add_argument("--raw-root", type=Path, required=True)
    weekly_init.add_argument("--watermark", type=date.fromisoformat, required=True)
    weekly_init.add_argument("--output", type=Path, required=True)
    weekly_init.add_argument("--api-root", type=Path, required=True)
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
    elif arguments.command == "lake" and arguments.lake_command in {
        "inspect-watch-events",
        "inspect-push-events",
    }:
        # Catalog 快照可能包含数千个分区，使用紧凑 JSON 控制 BFF 子进程输出上限。
        print(
            json.dumps(
                inspect_bigquery_dataset(arguments.workspace, arguments.lake_event),
                ensure_ascii=False,
                separators=(",", ":"),
            )
        )
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
    elif arguments.command == "history":
        if arguments.history_command == "record":
            record_result(arguments.catalog, bundle_record(arguments.bundle))
            print(json.dumps({"status": "recorded"}))
        elif arguments.history_command == "render":
            files = render_history(arguments.catalog, arguments.output)
            print(json.dumps({"status": "rendered", "files": len(files)}))
        elif arguments.history_command == "backfill":
            count = backfill_history(arguments.root, arguments.catalog, arguments.output)
            print(json.dumps({"status": "backfilled", "records": count}))
        elif not arguments.apply:
            print(
                json.dumps(
                    {
                        "status": "preview",
                        "repository": arguments.repository,
                        "branch": arguments.branch,
                    }
                )
            )
        else:
            with github_client(arguments.token_env) as client:
                commit = GitHubDistribution(arguments.repository, client).sync_history(
                    arguments.catalog,
                    branch=arguments.branch,
                )
            print(json.dumps({"status": "published", "commit": commit}))
    elif arguments.command == "release":
        if arguments.release_command == "prepare":
            prepare_release(arguments.bundle, arguments.output)
            print(json.dumps({"status": "prepared"}))
        elif not arguments.apply:
            package = read_release_package(arguments.package)
            print(
                json.dumps(
                    {
                        "status": "preview",
                        "repository": arguments.repository,
                        "target": arguments.target,
                        "tag": package["tag"],
                        "assets": package["assets"],
                    }
                )
            )
        else:
            with github_client(arguments.token_env) as client:
                url = GitHubDistribution(arguments.repository, client).publish(
                    arguments.package,
                    target=arguments.target,
                )
            print(json.dumps({"status": "published", "release_url": url}))
    elif arguments.command == "weekly-init":
        initialize_weekly(
            arguments.run,
            arguments.raw_root,
            arguments.watermark,
            arguments.output,
            arguments.api_root,
        )
        print(json.dumps({"status": "configured", "scheduler_installed": False}))
    elif arguments.command == "bigquery" and arguments.bigquery_command in {
        "download-watch-events",
        "download-push-events",
    }:
        config = _daily_download_config(arguments)
        context = _daily_download_context(arguments)
        workspace = context.workspace
        workspace.mkdir(parents=True, exist_ok=True)
        source_type = (
            BigQueryWatchEventSource
            if arguments.bigquery_command == "download-watch-events"
            else BigQueryPushEventSource
        )
        artifacts = source_type(config).collect(context)
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
    elif (
        arguments.command == "bigquery"
        and arguments.bigquery_command == "repair-push-events-checkpoint"
    ):
        document = BigQueryPushEventSource.repair_checkpoint(
            _daily_download_config(arguments),
            _daily_download_context(arguments),
            apply=arguments.apply,
            expected_mismatch_count=arguments.expected_mismatch_count,
        )
        print(json.dumps(document, ensure_ascii=False, indent=2))
    elif (
        arguments.command == "bigquery"
        and arguments.bigquery_command == "plan-watch-events-catch-up"
    ):
        print(
            json.dumps(
                plan_watch_catch_up(arguments.workspace, arguments.target_date),
                ensure_ascii=False,
                separators=(",", ":"),
            )
        )
    elif arguments.command == "bigquery" and arguments.bigquery_command == "quota-usage":
        try:
            usage = read_bigquery_quota(arguments.billing_project, arguments.location)
        except BigQueryQuotaConnectionError as error:
            # 启动脚本会捕获此输出写入长期日志；只暴露可操作原因，不打印代理地址、
            # GCP 响应正文或整段 traceback。
            print(f"错误：{error}", file=sys.stderr)
            return 1
        line, should_stop = format_quota_usage(
            usage,
            warning_percent=arguments.warning_percent,
            stop_percent=arguments.stop_percent,
        )
        if arguments.format == "json":
            print(
                json.dumps(
                    quota_usage_document(
                        usage,
                        warning_percent=arguments.warning_percent,
                        stop_percent=arguments.stop_percent,
                    ),
                    ensure_ascii=False,
                )
            )
        else:
            print(line)
        return QUOTA_STOP_EXIT_CODE if should_stop else 0
    elif arguments.command == "bigquery" and arguments.bigquery_command in {
        "sql-lab-dry-run",
        "sql-lab-query",
    }:
        sql = read_sql_file(arguments.sql_file)
        gateway = GoogleBigQueryLabGateway(arguments.billing_project, arguments.location)
        if arguments.bigquery_command == "sql-lab-dry-run":
            document = dry_run_sql_lab(sql, arguments.maximum_bytes_billed, gateway)
        else:
            quota = read_bigquery_quota(arguments.billing_project, arguments.location)
            _, should_stop = format_quota_usage(quota, warning_percent=80, stop_percent=90)
            if should_stop:
                raise RuntimeError("BigQuery 月度用量已达到 90% 停止阈值")
            document = query_sql_lab(
                sql,
                arguments.maximum_bytes_billed,
                arguments.expected_sql_sha256,
                arguments.maximum_result_rows,
                gateway,
            )
        print(json.dumps(document, ensure_ascii=False))
    return 0


def _daily_download_config(arguments: argparse.Namespace) -> BigQueryConfig:
    """从下载或 repair 命令构造同一份 checkpoint 任务配置。"""

    return BigQueryConfig(
        billing_project=arguments.billing_project,
        start_date=arguments.start_date,
        end_date=arguments.end_date,
        maximum_bytes_billed_per_day=arguments.maximum_bytes_billed_per_day,
        maximum_total_bytes_billed=arguments.maximum_total_bytes_billed,
        location=arguments.location,
        retry_attempts=arguments.retry_attempts,
        retry_initial_delay_seconds=arguments.retry_initial_delay_seconds,
        append_existing=arguments.append_existing,
    )


def _daily_download_context(arguments: argparse.Namespace) -> TaskContext:
    """固定下载与 repair 共用的 config hash，避免修错其他任务的 checkpoint。"""

    config_document = {
        "billing_project": arguments.billing_project,
        "start_date": arguments.start_date.isoformat(),
        "end_date": arguments.end_date.isoformat(),
        "maximum_bytes_billed_per_day": arguments.maximum_bytes_billed_per_day,
        "maximum_total_bytes_billed": arguments.maximum_total_bytes_billed,
        "location": arguments.location,
    }
    config_hash = (
        "sha256:" + hashlib.sha256(json.dumps(config_document, sort_keys=True).encode()).hexdigest()
    )
    return TaskContext(
        run_id=arguments.run_id,
        workspace=arguments.workspace.resolve(),
        config_hash=config_hash,
        random_seed=0,
    )


def _add_sql_lab_arguments(parser: argparse.ArgumentParser) -> None:
    """SQL 文本只通过文件传递，避免出现在 argv 和进程列表。"""

    parser.add_argument("--sql-file", type=Path, required=True)
    parser.add_argument("--billing-project", required=True)
    parser.add_argument("--location", default="US")
    parser.add_argument("--maximum-bytes-billed", type=int, required=True)


if __name__ == "__main__":
    raise SystemExit(main())
