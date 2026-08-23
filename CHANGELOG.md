# Changelog

All notable changes to Starcat Recommendation Pipeline are documented here.

## Unreleased

- 新增 GH Archive BigQuery `WatchEvent` 按日采集，在查询前执行 dry run 并限制每日最大扫描字节数。
- 新增 GitHub Repository metadata 采集，支持 ETag 缓存、请求数上限和限流信息识别。
- 新增 Starcat schema v1 完整快照导入，校验 canonical hash 并在落地前脱敏 participant ID。
- 新增统一 `InteractionRecord` 和 `RepositoryRecord` Parquet schema，保留行为来源语义与 lineage manifest。
- 新增 DuckDB 数据集构建，执行最新快照选择、去重、公开仓库过滤、主体权重和时间切分。
- 新增 Popular 离线基线，作为冷启动 fallback 和评估下限。
- 新增基于稀疏隐式反馈矩阵的 TruncatedSVD item-to-item 推荐基线。
- 新增时间衰减 co-star 核心训练器，包含主体权重、半衰期、cosine 归一化、shrinkage 和 pair 上限。
- 新增 Recall、NDCG、MRR 和 Coverage 时间留出评估，支持多模型与 co-star ablation 变体对比。
- 新增不可变本地 Model Registry 和可校验 SQLite ServingBundle，发布前执行指标门禁。
- 新增 YAML 端到端单机管道、任务执行记录、Bundle 校验/查询和 Registry 激活 CLI。
- 新增可直接运行的本地完整示例、BigQuery 配置样例和中文操作手册。
