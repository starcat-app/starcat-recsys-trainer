# Starcat 推荐数据与离线训练管道

`starcat-recsys-trainer` 是 Starcat 自研仓库推荐系统的数据与离线训练管道。它负责从多种来源采集公开数据，将来源各异的数据转换为版本化的内部标准格式，完成数据质量检查、训练集构建、离线训练、评估和 ServingBundle 发布。

当前仓库为私有项目，尚未对外发布，也不提供生产 REST API。

## 当前能力

- 从 GH Archive BigQuery 采集公开 `WatchEvent`。
- 从 GitHub REST API 补齐公开仓库 metadata。
- 导入 Starcat 匿名公开 Star 全量快照。
- 将所有来源转换为 `InteractionRecord` 和 `RepositoryRecord`。
- 构建可复现、可追踪的数据集。
- 训练 Popular、TruncatedSVD 和时间衰减 co-star 基线。
- 输出离线评估、模型 manifest、数据质量报告和 ServingBundle。
- 使用本地查询命令验证 ServingBundle。

## 架构

```text
Push / Pull SourceConnector
    -> Raw Zone
    -> Normalizer
    -> Canonical Parquet
    -> Validator
    -> DatasetBuilder
    -> Trainer
    -> Evaluator
    -> Publisher
    -> ServingBundle
```

训练管道不承担公网查询。未来独立的 `starcat-recsys-api` 只读已发布 ServingBundle，为 Starcat 和经授权的第三方客户端提供 REST API。

详细设计见 [docs/架构设计.md](docs/架构设计.md)，完整使用方法见 [docs/使用说明.md](docs/使用说明.md)。

## 环境要求

- Python 3.12
- macOS 或 Linux
- 推荐使用 `uv`
- BigQuery 数据源需要 Google Application Default Credentials
- GitHub metadata 数据源建议设置 `GITHUB_TOKEN`

## 快速开始

```bash
uv sync --all-groups
uv run starcat-recsys --help
make check
```

使用本地 fixture 运行完整示例：

```bash
uv run starcat-recsys pipeline run \
  --config configs/example-local.yaml
```

查看产物：

```bash
uv run starcat-recsys bundle verify \
  workspace/published/latest

uv run starcat-recsys bundle query \
  workspace/published/latest \
  --repo-id 41881900 \
  --limit 10
```

## 数据安全边界

- 不上传或保存 GitHub Token、login、email、Private/Internal repo 和 Starcat 本地标签、笔记、搜索、AI/RAG 内容。
- Starcat `participant_id` 在标准化阶段通过 HMAC 转换为来源内训练标识，标准数据集中不保留原值。
- GH Archive、Starcat 和其他来源的用户标识不做跨来源连接。
- 日志、指标和审查报告不得包含完整用户 Star 集合。
- BigQuery 正式查询必须先 dry run，并受到 `maximum_bytes_billed` 限制。

## 开发与验证

```bash
make format
make check
make test
```

项目当前不包含 GitHub Actions。所有质量门禁先在本地执行。

## 文档

- [架构设计](docs/架构设计.md)
- [使用说明](docs/使用说明.md)
- [实施清单](docs/实施清单.md)
- [安全说明](SECURITY.md)
- [贡献指南](CONTRIBUTING.md)
- [支持说明](SUPPORT.md)
- [变更记录](CHANGELOG.md)

## License

[MIT License](LICENSE)
