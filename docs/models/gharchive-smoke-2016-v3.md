# gharchive-smoke-2016-v3

- Status / 状态: `evaluated`
- Algorithm / 算法: `costar`
- Bundle created / 打包时间: `2026-08-27T14:18:44.162406+00:00`
- Raw watermark / 数据水位: `unknown`
- Metrics scope / 指标口径: `unknown`
- Source commit / 源码提交: `547e50b0d5c71c26ba084c1381af40e3f0a84ce5`

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
  "coverage": 0.07074148296593187,
  "evaluated_subjects": 1919,
  "mrr@20": 0.06167496125206781,
  "ndcg@20": 0.03928312909183402,
  "recall@20": 0.06009384078995894,
  "skipped_subjects": 81
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

- Repositories / 仓库: 5000
- Edges / 推荐边: 2219
- SQLite bytes: 712704
- Manifest SHA-256: `sha256:ee8268566a4c80fa46d83900619cea7a0d96511d5bf32f112ca1f32abbff40ca`
- Release: not published / 未公开发布
- Evidence / 证据: manifest.json, checksums.json, recommendations.sqlite

The bundle contains public repository metadata, aggregate edges and a content index.
It excludes raw events, training subjects and private Starcat snapshots.
模型只含公开仓库元数据、聚合推荐边和内容索引，不含原始事件、训练主体或私有快照。
