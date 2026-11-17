# gharchive-local-2016-2026-v12

- Status / 状态: `unverified`
- Algorithm / 算法: `costar`
- Bundle created / 打包时间: `2026-08-28T03:24:03.121528+00:00`
- Raw watermark / 数据水位: `unknown`
- Metrics scope / 指标口径: `unknown`
- Source commit / 源码提交: `unknown`

Missing historical values are unknown, not zero. / 历史缺失值未知，不代表零。
Different evaluation datasets are not directly comparable. / 不同评估集不可直接比较。
pre_refit means these metrics describe the evaluated candidate, not the final refit.
`pre_refit` 表示重训前候选的成绩，不是最终重训模型再次实测。

Historical Bundle validation failed; metrics are unverified manifest records. 历史 Bundle 校验失败，指标仅为未核实的 manifest 记录，不可直接分发。

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
  "coverage": 0.19956663055254606,
  "evaluated_subjects": 9536,
  "mrr@20": 0.02451401643857282,
  "ndcg@20": 0.025841390836662297,
  "recall@20": 0.0550445000067207,
  "skipped_subjects": 182
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

- Repositories / 仓库: unknown
- Edges / 推荐边: unknown
- SQLite bytes: unknown
- Manifest SHA-256: `unknown`
- Release: not published / 未公开发布
- Evidence / 证据: manifest.json (Bundle validation failed)

The bundle contains public repository metadata, aggregate edges and a content index.
It excludes raw events, training subjects and private Starcat snapshots.
模型只含公开仓库元数据、聚合推荐边和内容索引，不含原始事件、训练主体或私有快照。
