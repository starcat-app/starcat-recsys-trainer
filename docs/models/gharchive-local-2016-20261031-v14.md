# gharchive-local-2016-20261031-v14

- Status / 状态: `evaluated`
- Algorithm / 算法: `costar`
- Bundle created / 打包时间: `2026-11-02T02:25:26.089200+00:00`
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
  "coverage": 0.13346354166666666,
  "evaluated_subjects": 1347,
  "mrr@20": 0.014133443170277692,
  "ndcg@20": 0.022523166068385628,
  "recall@20": 0.05567398451585534,
  "skipped_subjects": 17
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

- Repositories / 仓库: 4611
- Edges / 推荐边: 15870
- SQLite bytes: 4198400
- Manifest SHA-256: `sha256:a312b7b4015140403fcb7901f75e0c5c33d4bbc719ee3292ca0b74ecc35f2eb0`
- Release: not published / 未公开发布
- Evidence / 证据: manifest.json, checksums.json, recommendations.sqlite

The bundle contains public repository metadata, aggregate edges and a content index.
It excludes raw events, training subjects and private Starcat snapshots.
模型只含公开仓库元数据、聚合推荐边和内容索引，不含原始事件、训练主体或私有快照。
