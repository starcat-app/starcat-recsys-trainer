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
