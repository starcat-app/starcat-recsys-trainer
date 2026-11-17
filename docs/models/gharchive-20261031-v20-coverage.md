# gharchive-20261031-v20-coverage

- Status / 状态: `evaluated`
- Algorithm / 算法: `hybrid_25`
- Bundle created / 打包时间: `2026-11-02T23:23:09.351855+00:00`
- Raw watermark / 数据水位: `unknown`
- Metrics scope / 指标口径: `pre_refit`
- Source commit / 源码提交: `working-tree-checksums-in-training-start`

Missing historical values are unknown, not zero. / 历史缺失值未知，不代表零。
Different evaluation datasets are not directly comparable. / 不同评估集不可直接比较。
pre_refit means these metrics describe the evaluated candidate, not the final refit.
`pre_refit` 表示重训前候选的成绩，不是最终重训模型再次实测。

Coverage expansion and content fallback; metrics are inherited from v19 before refit, not a new quality measurement. 扩容与内容补充；继承 v19 重训前指标，不代表新增效果实测。

## Training / 训练

```json
null
```

## Evaluation protocol / 评估协议

```json
null
```

## Metrics / 指标

```json
{
  "catalog_recall": 0.041144184503069524,
  "catalog_truth_count": 7942,
  "coverage": 0.1613589668329909,
  "empty_result_rate": 0.0,
  "evaluated_subjects": 8395,
  "mrr@20": 0.00838934770716828,
  "ndcg@20": 0.013851370702961559,
  "recall@20": 0.03508551008253212,
  "skipped_subjects": 0,
  "truth_catalog_coverage": 0.8626982402780795,
  "truth_count": 9206
}
```

## Decision / 检查结论

```json
null
```

## Data processing / 数据处理

```json
null
```

## Artifact / 产物

- Repositories / 仓库: 18839
- Edges / 推荐边: 1350586
- SQLite bytes: 239292416
- Manifest SHA-256: `sha256:8d9d9dfeca60a165d093dcd9414c342a3a544e34cfe82b013a0bcc46b28d34c0`
- Release: not published / 未公开发布
- Evidence / 证据: manifest.json, checksums.json, recommendations.sqlite

The bundle contains public repository metadata, aggregate edges and a content index.
It excludes raw events, training subjects and private Starcat snapshots.
模型只含公开仓库元数据、聚合推荐边和内容索引，不含原始事件、训练主体或私有快照。
