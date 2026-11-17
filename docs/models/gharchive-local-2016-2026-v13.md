# gharchive-local-2016-2026-v13

- Status / 状态: `evaluated`
- Algorithm / 算法: `costar`
- Bundle created / 打包时间: `2026-08-28T12:18:46.659641+00:00`
- Raw watermark / 数据水位: `unknown`
- Metrics scope / 指标口径: `unknown`
- Source commit / 源码提交: `7b7629b534c2a34d264c3b658388009a8eab7cde`

Missing historical values are unknown, not zero. / 历史缺失值未知，不代表零。
Different evaluation datasets are not directly comparable. / 不同评估集不可直接比较。
pre_refit means these metrics describe the evaluated candidate, not the final refit.
`pre_refit` 表示重训前候选的成绩，不是最终重训模型再次实测。

Recovered from an immutable Bundle; local installation is not deployment evidence. 从不可变 Bundle 恢复；本地安装不代表曾上线。

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

- Repositories / 仓库: 4619
- Edges / 推荐边: 22530
- SQLite bytes: 5357568
- Manifest SHA-256: `sha256:1a4d29ff4f4e4efec149ba009da84bea13d7a81808177feefa021eb0b5460076`
- Release: not published / 未公开发布
- Evidence / 证据: manifest.json, checksums.json, recommendations.sqlite

The bundle contains public repository metadata, aggregate edges and a content index.
It excludes raw events, training subjects and private Starcat snapshots.
模型只含公开仓库元数据、聚合推荐边和内容索引，不含原始事件、训练主体或私有快照。
