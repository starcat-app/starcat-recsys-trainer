# Changelog

All notable changes to Starcat Recommendation Pipeline are documented here.

## Unreleased

- 新增 WatchEvent 独立下载 CLI、分区断点续传、原子 checkpoint、瞬时错误重试与累计扫描预算门禁。
- 新增 GH Archive BigQuery `PushEvent` 仓库基础目录采集，并由 GitHub API 对 Top N 候选执行权威 metadata 覆盖。
- 新增查询窗口内仓库观察支持度、主体最小/最大仓库数过滤，适配扩大后的 BigQuery 训练数据。
- 新增 `starcat_collection_api` Pull Connector，校验 Admin 导出 checksum、schema、数量上限与 active 主体唯一性，并复用现有 Starcat canonical 训练链路。
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
- 完成 Starcat `supports/` 独立私有项目登记。
- 第二轮审查修复 run/Registry 不可变与路径安全、跨 run ETag cache、GitHub 403 分类和 SVD 退化输入。
- 第三轮审查将 warning 纳入默认门禁，补齐 GitHub HTTP Adapter 测试，完成构建和进度收口。
- 修复 GH Archive 通配符匹配 `yesterday` View 和空预算序列化问题，并完成 ADC、真实查询与 Arrow 转换验证。
- 第四轮审查完成 BigQuery 生产链路、测试、文档和工程进度收口。
- 第五轮审查修复训练权重、重复隐式反馈、Popular seed、Coverage 和时间衰减基准的未来数据污染。
- 第六轮审查完成训练逻辑、测试、构建、本地 demo、文档和工程进度收口。
