# gharchive-20261031-v17-recent

- Status / 状态: `blocked`
- Algorithm / 算法: `costar`
- Bundle created / 打包时间: `unknown`
- Raw watermark / 数据水位: `unknown`
- Metrics scope / 指标口径: `same_model`
- Source commit / 源码提交: `unknown`

Missing historical values are unknown, not zero. / 历史缺失值未知，不代表零。
Different evaluation datasets are not directly comparable. / 不同评估集不可直接比较。
pre_refit means these metrics describe the evaluated candidate, not the final refit.
`pre_refit` 表示重训前候选的成绩，不是最终重训模型再次实测。

The v17 recent holdout gate failed; final refit was stopped. v17 近期留出未通过，停止最终 refit；详见 v17 结果报告。

## Training / 训练

```json
null
```

## Evaluation protocol / 评估协议

```json
{
  "dataset_checksum": "sha256:48d46b9320d6bf8aa2d5be980f7298ab23b776d29cf98c77d16f3d4b2e34d204",
  "k": 20,
  "split": "test"
}
```

## Metrics / 指标

```json
{
  "coverage": 0.03228646903434106,
  "evaluated_subjects": 274,
  "mrr@20": 0.001694473409801877,
  "ndcg@20": 0.004453361721586155,
  "recall@20": 0.014598540145985401,
  "skipped_subjects": 0
}
```

## Decision / 检查结论

```json
{
  "candidate": {
    "coverage": 0.03228646903434106,
    "evaluated_subjects": 274,
    "mrr@20": 0.001694473409801877,
    "ndcg@20": 0.004453361721586155,
    "recall@20": 0.014598540145985401,
    "skipped_subjects": 0
  },
  "incumbent": {
    "coverage": 0.025242148517757558,
    "evaluated_subjects": 274,
    "mrr@20": 0.0018242250450279647,
    "ndcg@20": 0.005882304614524603,
    "recall@20": 0.021897810218978103,
    "skipped_subjects": 0
  },
  "passed": false
}
```

## Data processing / 数据处理

```json
{
  "candidate_repository_count": 13638,
  "final_rows": 2061640,
  "input_interaction_rows": 511284530,
  "public_repository_count": 13638,
  "split_counts": {
    "test": 291,
    "train": 2059869,
    "validation": 1480
  },
  "training_eligible_subject_count": 59934
}
```

## Artifact / 产物

- Repositories / 仓库: unknown
- Edges / 推荐边: unknown
- SQLite bytes: unknown
- Manifest SHA-256: `unknown`
- Release: not published / 未公开发布
- Evidence / 证据: dataset/quality-report.json, v17 controls decision.json

The bundle contains public repository metadata, aggregate edges and a content index.
It excludes raw events, training subjects and private Starcat snapshots.
模型只含公开仓库元数据、聚合推荐边和内容索引，不含原始事件、训练主体或私有快照。
