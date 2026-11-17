# ServingBundle and version contract / 模型与版本契约

## Immutable archive / 不可变产物

The model ZIP has exactly three root files:

```text
manifest.json
recommendations.sqlite
checksums.json
```

`checksums.json` maps file names to `sha256:<hex>`. The release-level `SHA256SUMS`
checks the ZIP and public sidecars; it does not replace the internal checksums.
SQLite stores public repositories, aggregate recommendations and, when present,
the TF-IDF content index. The default compressed upload limit is 512 MiB.

包内 checksum 校验原始文件，Release 的 SHA256SUMS 校验下载资产。原始事件、
训练主体、Collection 快照、机器路径和凭据均不属于分发包。旧模型保持字节不变，
历史补充信息放在成果索引中，不回写旧 manifest。
旧包经完整校验的 AppleDouble `._*` 项可从传输校验清单中移除；原始模型文件及源包
不变，ZIP 内仍严格只有上述三个文件。

## Manifest schema 1

The additive metadata fields below do not change the existing SQLite contract.
Legacy Bundles remain readable. Missing metadata is `null`, not inferred.

| Field | Meaning / 含义 |
| --- | --- |
| `schema_version` | Bundle format version, currently 1 / 文件格式版本 |
| `model_version` | Immutable model identity / 不可变模型版本 |
| `selected_model` | Algorithm identifier, e.g. `hybrid_25` / 算法标识 |
| `created_at` | Bundle construction time, not training completion / 打包时间 |
| `training.started_at`, `completed_at` | Actual trainer execution interval / 真实训练执行区间 |
| `training.data_start`, `data_end` | Earliest/latest observed training-event time / 实际训练事件范围 |
| `training.interactions`, `subjects` | Training-eligible train rows and distinct subjects / 实际参与训练规模 |
| `training.parameters` | Public numeric recipe parameters / 数值配方参数 |
| `raw_watermark` | Complete UTC date consumed by the weekly run / 周任务完整数据水位 |
| `evaluation.split`, `k`, `maximum_seeds` | Evaluation protocol / 评估口径 |
| `evaluation.train_end`, `validation_end`, `data_end` | Exclusive split boundaries where recorded / 已记录的切分边界 |
| `evaluation.dataset_checksum` | Evaluation dataset identity / 评估数据身份 |
| `metrics` | Numeric aggregate Recall, NDCG, MRR, coverage, empty rate and counts |
| `metrics_scope` | `same_model` or `pre_refit`; missing in some historical models |
| `evaluation_checksum`, `evaluated_model_checksum` | Exact assessment/model provenance / 评估与模型摘要 |
| `git_commit`, `config_hash` | Source HEAD and configuration digest / 源码提交与配置摘要 |

Source HEAD does not include uncommitted changes. A run's private source snapshot
and source checksums remain the detailed execution evidence. The public model
version must not be advertised as a bit-for-bit reproduction unless all inputs,
metadata snapshots, code and dependencies were frozen.

`training.data_end` is an observed timestamp, while `raw_watermark` is a complete
input date. They differ intentionally. `pre_refit` means the final model uses more
recent training data but cites the selected candidate's evaluation.

## API / 对外查询

`GET /api/v2/version` uses the normal query Bearer key, not the publish key.
It returns the existing envelope:

```json
{
  "schema_version": 1,
  "data": {
    "service": {"name": "recommend", "version": "0.0.0-dev"},
    "model": null
  }
}
```

With an active Bundle, `model` contains the supported manifest fields plus
`repositories`, `recommendation_edges`, and `database_bytes`. With no model it is
`null` and the endpoint still returns HTTP 200. Recommendation requests remain
unavailable until a model is installed.

The endpoint returns `Cache-Control: private, no-store`. Uploading a candidate with
`activate=false` does not change the reported model. Counts and metadata are read
from one active snapshot. File paths, publish keys and unknown manifest fields
are not exposed.

`service.version` describes the executable; `model.model_version` describes the
Bundle. A single-repository content-fallback response may append a metadata
digest to its effective `model_version`; `/version` always reports the base Bundle.

Historical release URLs and delivery state live in `docs/models/history.json`
and the GitHub Release, not in a mutable field added to an already published ZIP.

## CLI / MCP scope

Model operations use `starcat-recsys` CLI (`history`, `release`, `weekly-init`) and
the independent HTTP API. GitHub writes require explicit `--apply` or enabled
operator configuration. App CLI/MCP integration is deferred: this project has
no App account, Pro entitlement or App execution context, and does not register
placeholder App capabilities.
