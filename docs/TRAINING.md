# Training and local reproduction

[简体中文](TRAINING-ZH.md)

## Environment and resource boundaries

Use Python 3.12, uv and Git on macOS or Linux:

```bash
uv sync --locked --all-groups
```

No GPU is required. DuckDB currently uses a 6 GiB working-memory cap and one
thread; NumPy/SciPy/scikit-learn and the OS need additional memory. The cap is not
the total machine requirement. The weekly operator checks for 20 GiB free space,
but that is a starting guard, not an estimate for the raw archive or every run.
Start with the synthetic example and a bounded data window, then measure elapsed
time, RSS, temporary spill and output sizes on your hardware. Historical official
metrics do not imply the same runtime or quality on a smaller dataset.

## Offline synthetic example

```bash
uv run starcat-recsys pipeline run --config configs/example-hybrid.yaml
uv run starcat-recsys bundle query \
  workspace/registry/versions/example-hybrid-v1 --repo-id 1
```

This exercises co-star + 25% content rank fusion and a TF-IDF serving index.
`configs/example-local.yaml` retains the original Popular/SVD/co-star baseline
demo. Both use synthetic records checked into this repository.

## Public-data training

1. Follow [data acquisition](DATA_SOURCES.md) to download October 2026 WatchEvent
   partitions into `workspace/watch-events`, or choose your own explicit window.
2. Set your GitHub metadata token: `export GITHUB_TOKEN='your-token'`.
3. Review `configs/example-public-training.yaml`. Paths are relative to the YAML.
   Change all dates consistently when changing the window. The starter bounds the
   catalog to 5,000 repositories and samples 2% of subjects.
4. Run:

```bash
uv run starcat-recsys pipeline run --config configs/example-public-training.yaml
uv run starcat-recsys bundle verify workspace/registry/versions/public-202610-v1
uv run starcat-recsys bundle query \
  workspace/registry/versions/public-202610-v1 --repo-id YOUR_REPO_ID
```

The pipeline creates its own canonical data, training cohort, weights, repository
catalog and evaluation data. It needs no maintainer dataset or private Collection
service. It publishes only to the configured **local** registry unless you add
`publish.recommend_api`.

The starter's minimal `evaluated_subjects: 1` check only demonstrates that
evaluation ran. It is not the official weekly 5,000-subject production gate.
Choose/freeze your protocol before test evaluation. Do not repeatedly tune against
the same test observations. `training.reference_time` must equal `dataset.train_end`.

Useful outputs:

```text
workspace/public-training/runs/public-202610-v1/
  dataset/interactions.parquet
  dataset/repositories.parquet
  dataset/quality-report.json
  evaluation/test-metrics.json
  run-manifest.json
workspace/registry/versions/public-202610-v1/
  manifest.json
  recommendations.sqlite
  checksums.json
```

Use new run and model IDs for a second experiment. Freeze inputs, lockfile,
configuration and source revision to reproduce an experiment. Refreshed GitHub
metadata means a later run is not necessarily byte-identical.

## Package and use the model

```bash
uv run starcat-recsys release prepare \
  --bundle workspace/registry/versions/public-202610-v1 \
  --output workspace/releases/public-202610-v1
```

The output ZIP, public metrics, model card and SHA256SUMS can be imported into
your local recommendation service using its `scripts/install-model.sh`.
This command does not upload to GitHub or activate any service.

## Weekly training

Initialize a private operator config from your own completed public-data run:

```bash
uv run starcat-recsys weekly-init \
  --run workspace/public-training/runs/public-202610-v1 \
  --raw-root workspace/watch-events/raw/gh_archive \
  --watermark 2026-10-31 \
  --api-root /path/to/starcat-recommend-api \
  --output workspace/operator/weekly.yaml
```

The initial watermark records the already-observed input window. First update the
Raw dataset beyond that date; weekly evaluation must use newer observations.
The command does not train, install a scheduler or contact production.
Keep the original dataset/catalog/Bundle immutable. The generated catalog starts
with your own bounded candidates; it does not copy the official expanded catalog.

Edit the generated config with your own service URL, query/publish key file paths,
and `volume_root` when using an external disk. Put each key in its own mode-600
file. Import the initial Bundle into that service first so its active version
matches `initial_bundle`.

```bash
uv run python -m starcat_recsys.weekly status --config workspace/operator/weekly.yaml
uv run python -m starcat_recsys.weekly run --config workspace/operator/weekly.yaml
```

The second command is a real training and deployment command. It retains the
5,000-subject quality, coverage and API gates. Small or sparse datasets may stay
blocked; the evaluation window accumulates rather than weakening the gate.
No sibling `starcat-api-kit` checkout is required for the isolated Go API test.

The first Sunday of each month also tests a roster with up to 10% of accounts
replaced. It must beat a same-cutoff retrain of the current roster on a shared,
independent holdout and pass the production gates. The `status` command reports
the last roster decision under `monthly_roster`.

On macOS, set `STARCAT_WEEKLY_RECOMMENDATION_CONFIG` to the absolute config path,
then use the existing launch-agent installer. On Linux use the Python command
from your scheduler; do not call the macOS caffeinate wrapper. The due-slot
calculation uses Asia/Shanghai. Installation/first scheduled execution may
immediately perform an overdue run.

For public history and GitHub delivery, see [model distribution](MODEL_RELEASES.md).
