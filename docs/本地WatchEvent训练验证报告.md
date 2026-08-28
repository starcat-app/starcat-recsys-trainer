# 本地 WatchEvent 训练验证报告

## 1. 验证目标

验证已经下载到 `/Volumes/T0` 的 BigQuery Raw 可以在不重复查询 BigQuery、不复制 Raw、
不占用系统盘的前提下，完成 Canonical、Dataset、Popular/SVD/co-star、离线评估和
ServingBundle 查询全链路。

验证时间：2026-08-27

## 2. 数据流向

```text
/Volumes/T0/Starcat/bigquery/*/raw（日分区，只读）
  -> LocalLakePartitionSource（checkpoint、schema、行数、checksum）
  -> DuckDB Canonical（WatchEvent 主体 SHA-256、PushEvent 仓库目录）
  -> DuckDB Dataset（去重、时间切分、支持度、Top 仓库、稳定主体抽样）
  -> Popular / bounded SVD / bounded co-star
  -> bounded OfflineEvaluator
  -> batch SQLite ServingBundle
  -> bundle verify / bundle query
```

三个 run 均没有配置 `type: bigquery`，因此没有执行 BigQuery 查询。烟测使用同范围的
PushEvent 日分区生成基础仓库目录，不依赖 GitHub Token。

## 3. 分级验证结果

| 范围 | WatchEvent 输入 | Dataset | 评估主体 | 结果 |
|---|---:|---:|---:|---|
| 2016-01-01～2016-01-14 | 900,149 | 20,110 | 738 | 九个节点完成，Bundle 校验和查询通过 |
| 2016-01-01～2016-01-31 | 2,030,368 | 23,173 | 770 | 九个节点完成，Bundle 校验和查询通过 |
| 2016-01-01～2016-12-31 | 26,130,723 | 136,699 | 1,919 | 九个节点完成，Bundle 校验和查询通过 |

全年 run 的主要配置：Top 5,000 仓库、2% 稳定主体抽样、单主体最多 200 个仓库、
co-star 最多 10,000,000 次 pair 展开、SVD 最多 2,000,000 个非零项、评估最多 2,000
个主体。

## 4. 2016 全年结果

任务耗时：

| 节点 | 秒 |
|---|---:|
| 两个本地分区源 | 5.13 |
| Canonical | 18.47 |
| Dataset | 5.75 |
| Popular | 0.04 |
| SVD | 1.70 |
| co-star | 0.08 |
| Evaluation | 0.56 |
| Bundle | 0.42 |

指标：

| 模型 | Recall@20 | NDCG@20 | MRR@20 | Coverage |
|---|---:|---:|---:|---:|
| co-star | 0.06009 | 0.03928 | 0.06167 | 0.07074 |
| Popular | 0.04748 | 0.02696 | 0.03870 | 0.00421 |
| SVD | 0.03966 | 0.02313 | 0.03533 | 0.74669 |

本次 co-star 的 Recall/NDCG/MRR 均高于 Popular 和 SVD，可以作为第一版候选模型；
Coverage 仍低于 SVD，后续可通过候选融合改进，但不影响当前离线基线可用性。

## 5. 产物

- 全年 workspace：`/Volumes/T0/Starcat/training/gharchive-smoke-2016-v3`
- 全年 Bundle：`/Volumes/T0/Starcat/training/registry/versions/gharchive-smoke-2016-v3`
- Workspace 大小：约 1.4 GiB
- Bundle 大小：约 1.5 MiB

Bundle 查询已经返回 `ruby/ruby`、`FreeCodeCamp/FreeCodeCamp` 等真实仓库；SQLite
`quick_check`、文件 checksum 和 manifest schema 全部通过。

## 6. 当前结论

本地 WatchEvent 已经可以训练。2016 全年真实数据证明 Raw 接入、流式 Canonical、训练预算、
评估和 Bundle 交付逻辑有效。2016～2026 全量 run 使用
`configs/example-local-lake.yaml`，其最终统计在运行完成后补入本报告。
