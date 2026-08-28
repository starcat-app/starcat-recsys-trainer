# Starcat 推荐数据与离线训练管道

`starcat-recsys-trainer` 负责从多种数据源采集公开仓库行为和 metadata，转换为统一 Parquet 格式，完成数据质量检查、离线训练、时间留出评估和 ServingBundle 发布。

当前仓库为私有项目，不提供生产 REST API。训练完成后可把已校验 Bundle 发布到 `starcat-recommend-api /api/v2`；在线服务只读 ServingBundle，与训练数据和匿名主体隔离。

## 已实现能力

- GH Archive BigQuery `WatchEvent` 使用显式 UTC 日表采集，支持断点续传、原子 checkpoint、瞬时错误重试、分区与总扫描预算门禁。
- GH Archive BigQuery `PushEvent` 使用独立显式 UTC 日表下载精简原始事件，只保留事件 ID、actor ID、repo ID、仓库名和事件时间。
- GH Archive BigQuery `PushEvent` 跨年份聚合仓库基础目录，提供最新名称、首次/末次 Push 和事件数，并使用独立总预算门禁。
- 只读检查既有 WatchEvent / PushEvent Raw，重新校验 Parquet footer、字段、行数和 SHA-256，并输出不含真实路径的 Catalog 快照。
- 通过 `local_lake_partitions` 直接登记本地 WatchEvent / PushEvent 日分区，训练 run 不复制长期 Raw。
- 面向本机 Admin Console 的受控 SQL Lab，强制只读 `githubarchive`、dry run、SQL hash 绑定、10 GiB 查询预算和 200 行 / 2 MiB 结果上限。
- GitHub Repository REST metadata 采集，支持 ETag cache、请求上限和限流识别。
- Starcat schema v1 匿名完整 Star 快照校验、HMAC 脱敏和导入。
- `starcat-collection-api` Admin 导出 Pull，校验响应 checksum、字节上限、schema 和 active 快照唯一性。
- DuckDB 流式生成 `InteractionRecord` / `RepositoryRecord` Canonical Parquet；十年事件不进入 Python 行对象列表，内存固定限制为 4 GiB，排序和聚合产生的临时文件写入训练 workspace 下的 `duckdb-temp/`。
- 最新完整快照选择、同源去重、公开仓库过滤、主体权重和时间切分。
- train-only 主体权重、subject-repo 唯一隐式反馈和 validation/test 未来数据隔离。
- Popular、TruncatedSVD、时间衰减 co-star 和两个 ablation 变体。
- Dataset 稳定主体抽样、Top 仓库目录、co-star pair 预算、SVD 非零项预算和评估主体预算，适配单机全量 Raw 扫描。
- 单主体冷启动时明确跳过不满足数学条件的 SVD，继续交付可用的 Popular/co-star。
- Recall@K、NDCG@K、MRR@K、Coverage 离线评估和指标发布门禁。
- 不可变本地 Model Registry、SQLite ServingBundle、checksum/quick-check 校验和查询 CLI。
- `RecommendAPIBundlePublisher` 使用独立发布密钥上传、激活 Bundle，并生成不含凭据的交付回执。
- YAML 驱动的单机端到端管道，节点通过 artifact 接口可继续扩展数据源和训练器。

## 快速开始

```bash
uv sync --all-groups
make check
make demo
```

`make demo` 使用本地 fixture 完整执行一次管道，不访问 BigQuery 或 GitHub。查询已发布产物：

```bash
uv run starcat-recsys bundle verify \
  workspace/registry/versions/example-local-v1

uv run starcat-recsys bundle query \
  workspace/registry/versions/example-local-v1 \
  --repo-id 1 \
  --limit 10
```

Registry 版本不可覆盖。重跑时同时修改 YAML 中的 `run_id` 和 `publish.model_version`。

已下载的十年 WatchEvent 使用 `configs/example-local-lake.yaml`。该配置把 Raw、Canonical、
训练 workspace 和 Registry 都留在 `/Volumes/T0`，并通过有界抽样完成第一版单机训练；运行前
只需提供 `GITHUB_TOKEN`，不会再次查询 BigQuery。

十年级 WatchEvent 与 PushEvent 首次采集使用两个可独立运行、独立续传的下载任务：

```bash
./scripts/download-watch-events.sh start
./scripts/download-watch-events.sh status
./scripts/download-watch-events.sh logs

./scripts/download-push-events.sh start
./scripts/download-push-events.sh status
./scripts/download-push-events.sh logs
```

本机 Admin Console 使用结构化状态和固定动作接口：

```bash
./scripts/download-watch-events.sh status --json
./scripts/download-push-events.sh status --json --skip-quota
./scripts/download-watch-events.sh start --json
```

把现有 Raw 登记到本地数据平台前，可独立执行同一只读检查：

```bash
uv run starcat-recsys lake inspect-watch-events \
  --workspace /Volumes/T0/Starcat/bigquery/watch-events-2016-2026
uv run starcat-recsys lake inspect-push-events \
  --workspace /Volumes/T0/Starcat/bigquery/push-events-2016-2026
```

命令不会移动、复制或改写 Raw；输出只包含 `lake://` / `storage://` 逻辑 URI、分区状态和
聚合统计，不包含真实数据目录。只有重新通过 Parquet 与 checksum 校验的分区才标记为 ready。

`--skip-quota` 只用于同一控制台批次的第二个状态请求；两个任务共享同一 GCP Project 月度
额度，控制台会复用第一个状态响应中的 quota。它不关闭下载 worker 自身的后台额度保护。

脚本通过 `screen + caffeinate` 在后台运行并阻止 macOS 自动休眠，不依赖 `launchd`。停止或重启使用 `stop` / `restart`。两个任务分别使用 `/Volumes/T0/Starcat/bigquery/watch-events-2016-2026` 和 `push-events-2016-2026`，不会共用 Parquet 或 checkpoint。脚本只允许在 `/Volumes/T0` 已真实挂载时启动，避免移动硬盘掉线后误写本机磁盘。每 30 分钟记录当前 GCP Project 的月度 BigQuery 计费量，达到 80% 警告、90% 停止对应下载任务；启动时额度查询失败也会拒绝运行。阈值和间隔可通过 `STARCAT_BQ_QUOTA_WARNING_PERCENT`、`STARCAT_BQ_QUOTA_STOP_PERCENT`、`STARCAT_BQ_QUOTA_LOG_INTERVAL_SECONDS` 覆盖。相同任务可安全续传，已通过 Parquet footer 和 SHA-256 校验的日期不会再次查询。

## 管道结构

```text
SourceConnector
  -> WatchEvent 行为 raw + PushEvent 精简事件 raw / 仓库目录 raw
  -> CanonicalNormalizer（DuckDB 流式扫描外部 Parquet）
  -> GitHub API Top N 权威 metadata 覆盖
  -> canonical Parquet
  -> DuckDBDatasetBuilder
  -> train / validation / test
  -> Popular / SVD / co-star
  -> OfflineEvaluator
  -> ServingBundlePublisher
  -> LocalModelRegistry
  -> RecommendAPIBundlePublisher（可选）
  -> starcat-recommend-api /api/v2
```

数据源、标准化、数据集、训练、评估和发布都通过稳定 Protocol 连接。当前 `LocalExecutor` 在单机编排；后续多机执行可替换 executor 和 artifact storage，不需要把 BigQuery、GitHub 或 Starcat DTO 带入训练器。

## 安全边界

- 不导入 Private/Internal repo、login、email、Starcat 标签、笔记、搜索和 AI/RAG 内容。
- 本地 Starcat 文件源在 raw landing 读取后立即 HMAC 转换 `participant_id`；Collection 源只接收服务端已 HMAC 的 `subject_id`，后续产物都不保留原值。
- GH Archive 和 Starcat 的主体标识不跨来源连接。
- Token、GCP 凭据和 HMAC key 只通过环境提供，不写入配置或 artifact。
- Recommend API 发布密钥只从环境变量读取，交付回执不记录密钥或服务端错误正文。
- 在线查询只读 ServingBundle，不读取 raw、canonical 或 dataset。

## 文档

- [完整使用说明](docs/使用说明.md)
- [Collection 本机 E2E 配置](configs/example-collection-local.yaml)
- [架构设计](docs/架构设计.md)
- [实施清单](docs/实施清单.md)
- [BigQuery PushEvent 扩展完成结果报告](docs/BigQuery-PushEvent元数据扩展完成结果报告.md)
- [贡献指南](CONTRIBUTING.md)
- [安全说明](SECURITY.md)
- [支持说明](SUPPORT.md)
- [变更记录](CHANGELOG.md)

## 开发验证

```bash
make format
make check
```

项目按要求不包含 CI workflow，所有质量门禁在本地执行。

## License

[MIT License](LICENSE)
