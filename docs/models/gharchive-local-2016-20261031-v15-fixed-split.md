# gharchive-local-2016-20261031-v15-fixed-split

- Status / 状态: `evaluated`
- Algorithm / 算法: `costar`
- Bundle created / 打包时间: `2026-11-02T02:41:19.148210+00:00`
- Raw watermark / 数据水位: `unknown`
- Metrics scope / 指标口径: `unknown`
- Source commit / 源码提交: `f651e9bac97707e084ae3e1a1d30ce7d4a0a89a1`

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
  "coverage": 0.19878075332026998,
  "evaluated_subjects": 9532,
  "mrr@20": 0.024404948716831517,
  "ndcg@20": 0.025843263098556574,
  "recall@20": 0.05513557870851581,
  "skipped_subjects": 189
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

- Repositories / 仓库: 4597
- Edges / 推荐边: 22502
- SQLite bytes: 5349376
- Manifest SHA-256: `sha256:1c729abe48273830ac8be6eff4253a25896762dd792de9bd2b303283ca75a4ac`
- Release: not published / 未公开发布
- Evidence / 证据: manifest.json, checksums.json, recommendations.sqlite

The bundle contains public repository metadata, aggregate edges and a content index.
It excludes raw events, training subjects and private Starcat snapshots.
模型只含公开仓库元数据、聚合推荐边和内容索引，不含原始事件、训练主体或私有快照。
