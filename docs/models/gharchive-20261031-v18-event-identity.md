# gharchive-20261031-v18-event-identity

- Status / 状态: `data_only`
- Algorithm / 算法: `unknown`
- Bundle created / 打包时间: `unknown`
- Raw watermark / 数据水位: `unknown`
- Metrics scope / 指标口径: `unknown`
- Source commit / 源码提交: `unknown`

Missing historical values are unknown, not zero. / 历史缺失值未知，不代表零。
Different evaluation datasets are not directly comparable. / 不同评估集不可直接比较。
pre_refit means these metrics describe the evaluated candidate, not the final refit.
`pre_refit` 表示重训前候选的成绩，不是最终重训模型再次实测。

Event identity repair and dataset rebuild only; no new quality claim. 事件身份修复与 Dataset 重建，不代表推荐质量提升。

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
{}
```

## Decision / 检查结论

```json
null
```

## Data processing / 数据处理

```json
{
  "candidate_repository_count": 13638,
  "final_rows": 2061840,
  "input_interaction_rows": 511284530,
  "public_repository_count": 13638,
  "split_counts": {
    "test": 291,
    "train": 2060069,
    "validation": 1480
  },
  "training_eligible_subject_count": 59940
}
```

## Artifact / 产物

- Repositories / 仓库: unknown
- Edges / 推荐边: unknown
- SQLite bytes: unknown
- Manifest SHA-256: `unknown`
- Release: not published / 未公开发布
- Evidence / 证据: dataset/quality-report.json

The bundle contains public repository metadata, aggregate edges and a content index.
It excludes raw events, training subjects and private Starcat snapshots.
模型只含公开仓库元数据、聚合推荐边和内容索引，不含原始事件、训练主体或私有快照。
