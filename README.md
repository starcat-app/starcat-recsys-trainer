# Starcat recommendation trainer

[简体中文](README-ZH.md)

Train repository recommendations from public GitHub activity and ship an immutable
ServingBundle to [starcat-recommend-api](https://github.com/starcat-app/starcat-recommend-api).
This is an offline Python project; the separate Go service answers recommendations.

## Choose your starting point

| Goal | Start here | Requires |
| --- | --- | --- |
| Use recommendations without training | [Pretrained models and deployment](https://github.com/starcat-app/starcat-recommend-api/blob/main/docs/SELF_HOSTING.md) | Go or Docker; a model ZIP |
| Try training without network access | [Local synthetic example](#local-example) | Python 3.12, uv |
| Train on public data | [Training guide](docs/TRAINING.md) | GH Archive data, GitHub metadata |
| Understand the results | [Model history](docs/MODEL_HISTORY.md) | No account or training environment |
| Publish or automate model delivery | [Model distribution](docs/MODEL_RELEASES.md) | Maintainer credentials for your own repository |

Pretrained models are assets in this repository's
[GitHub Releases](https://github.com/starcat-app/starcat-recsys-trainer/releases), with tags
`model-<model_version>`. If no model release has been published yet, use the local
synthetic example or a model ZIP supplied by your deployment operator. Source-code
ZIP downloads are not trained models.

## Local example

```bash
git clone https://github.com/starcat-app/starcat-recsys-trainer.git
cd starcat-recsys-trainer
uv sync --locked --all-groups
uv run starcat-recsys pipeline run --config configs/example-hybrid.yaml
uv run starcat-recsys bundle verify workspace/registry/versions/example-hybrid-v1
uv run starcat-recsys bundle query workspace/registry/versions/example-hybrid-v1 --repo-id 1
```

The sample data is synthetic. It needs no Starcat account, Collection service, GCP
credentials or network requests. It exercises co-star, TF-IDF ranking fusion,
evaluation and the serving content index. Change both `run_id` and
`publish.model_version` before repeating a run; versions are immutable.

## Data and model

- [GH Archive](https://www.gharchive.org/) WatchEvent observations provide public
  Star events. They are not a complete current-Star or Unstar snapshot.
- Public GitHub repository metadata supplies descriptions, topics and languages.
  PushEvent-derived metadata is an optional additional catalog source.
- Training uses time-decayed co-star and TF-IDF content ranking; SVD and popularity
  are available baselines. This is not a large language model.
- A Bundle contains `recommendations.sqlite`, `manifest.json` and `checksums.json`.
  It excludes raw events, training identities and private Starcat snapshots.
- `pre_refit` metrics describe the evaluated candidate before the final refit.
  Different datasets and protocols must not be treated as a continuous benchmark.

See [data acquisition and provenance](docs/DATA_SOURCES.md), the
[Bundle/version contract](docs/MODEL_FORMAT.md) and [recorded results](docs/MODEL_HISTORY.md).
Public-data training is independent of the optional opt-in Starcat Collection connector.

## Weekly operation

The existing Mac schedule runs on Sunday at 11:00 Asia/Shanghai. It checks data
integrity, at least 5,000 independent evaluation subjects, quality, coverage and an
isolated API before deployment. Blocked and failed runs also receive public
aggregate reports. GitHub distribution is disabled by default and can be retried
without training or redeploying.

[Weekly setup](docs/TRAINING.md#weekly-training) ·
[Detailed operator guide / 中文](docs/每周自动训练运维指南.md)

## Development

```bash
make check
git diff --check
```

Checks include Ruff, strict mypy and pytest coverage. The trainer uses local
quality gates; it does not require a training CI workflow. Python CLI execution
supports macOS/Linux; the provided launchd and caffeinate wrappers are macOS-only.
Serving requires neither Python nor a GPU.

[Contributing](CONTRIBUTING.md) · [Security](SECURITY.md) · [Support](SUPPORT.md) ·
[MIT code license](LICENSE)
