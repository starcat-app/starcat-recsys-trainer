# Starcat 推荐数据与离线训练管道

`starcat-recsys-trainer` 负责从多种数据源采集公开仓库行为和 metadata，转换为统一 Parquet 格式，完成数据质量检查、离线训练、时间留出评估和 ServingBundle 发布。

当前仓库为私有项目，不提供生产 REST API。未来 Starcat 和 `starcat-recsys-api` 通过只读 ServingBundle 接入，与训练数据和匿名主体隔离。

## 已实现能力

- GH Archive BigQuery `WatchEvent` 使用显式 UTC 日表采集，dry run 和 `maximum_bytes_billed` 双重预算门禁。
- GitHub Repository REST metadata 采集，支持 ETag cache、请求上限和限流识别。
- Starcat schema v1 匿名完整 Star 快照校验、HMAC 脱敏和导入。
- `starcat-collection-api` Admin 导出 Pull，校验响应 checksum、字节上限、schema 和 active 快照唯一性。
- `InteractionRecord` / `RepositoryRecord` Canonical Parquet 与全链路 manifest。
- 最新完整快照选择、同源去重、公开仓库过滤、主体权重和时间切分。
- train-only 主体权重、subject-repo 唯一隐式反馈和 validation/test 未来数据隔离。
- Popular、TruncatedSVD、时间衰减 co-star 和两个 ablation 变体。
- Recall@K、NDCG@K、MRR@K、Coverage 离线评估和指标发布门禁。
- 不可变本地 Model Registry、SQLite ServingBundle、checksum/quick-check 校验和查询 CLI。
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

## 管道结构

```text
SourceConnector
  -> raw
  -> CanonicalNormalizer
  -> canonical Parquet
  -> DuckDBDatasetBuilder
  -> train / validation / test
  -> Popular / SVD / co-star
  -> OfflineEvaluator
  -> ServingBundlePublisher
  -> LocalModelRegistry
```

数据源、标准化、数据集、训练、评估和发布都通过稳定 Protocol 连接。当前 `LocalExecutor` 在单机编排；后续多机执行可替换 executor 和 artifact storage，不需要把 BigQuery、GitHub 或 Starcat DTO 带入训练器。

## 安全边界

- 不导入 Private/Internal repo、login、email、Starcat 标签、笔记、搜索和 AI/RAG 内容。
- 本地 Starcat 文件源在 raw landing 读取后立即 HMAC 转换 `participant_id`；Collection 源只接收服务端已 HMAC 的 `subject_id`，后续产物都不保留原值。
- GH Archive 和 Starcat 的主体标识不跨来源连接。
- Token、GCP 凭据和 HMAC key 只通过环境提供，不写入配置或 artifact。
- 在线查询只读 ServingBundle，不读取 raw、canonical 或 dataset。

## 文档

- [完整使用说明](docs/使用说明.md)
- [架构设计](docs/架构设计.md)
- [实施清单](docs/实施清单.md)
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
