"""命令行入口。"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from starcat_recsys.bundle import LocalModelRegistry, query_bundle, verify_bundle
from starcat_recsys.pipeline import run_pipeline


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Starcat 推荐数据与离线训练管道")
    parser.add_argument("--version", action="version", version="%(prog)s 0.1.0")
    commands = parser.add_subparsers(dest="command", required=True)

    pipeline = commands.add_parser("pipeline", help="执行训练管道")
    pipeline_commands = pipeline.add_subparsers(dest="pipeline_command", required=True)
    run = pipeline_commands.add_parser("run", help="按 YAML 配置执行完整管道")
    run.add_argument("--config", type=Path, required=True)

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
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
