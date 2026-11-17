# Model results and GitHub Releases

[中文](MODEL_RELEASES-ZH.md)

## Public artifacts

Browse [MODEL_HISTORY.md](MODEL_HISTORY.md); `models/history.json` is the machine-readable
catalog. Records distinguish evaluated, deployed, blocked, failed, data-only and unverified
results. `published` means serving deployment passed; `release_url` separately records
GitHub distribution. Missing historical values remain `null`. Bundle creation is not the
training completion time.

Model tags use `model-<model_version>`, independently of code versions. Download these
assets rather than GitHub's generated Source code archive:

| Asset | Purpose |
| --- | --- |
| `<model_version>.zip` | ServingBundle accepted by the recommendation service |
| `model-card.md` | Algorithm, inputs, evaluation scope, size and provenance |
| `evaluation.json` | Aggregate metrics and evaluation checksums |
| `SHA256SUMS` | SHA-256 for those three assets |

The ZIP contains exactly `manifest.json`, `recommendations.sqlite` and `checksums.json`
at its root. It excludes raw events, training subjects, private configuration and
credentials. Local `release.json` supports retries and is not uploaded.
See [the format contract](MODEL_FORMAT.md).

## Prepare and publish

Validate a local bundle and prepare deterministic assets without contacting GitHub:

```bash
uv run starcat-recsys release prepare \
  --bundle workspace/registry/versions/YOUR_MODEL \
  --output workspace/releases/YOUR_MODEL
```

Repeated packaging of the same bundle produces identical ZIP bytes. Changed contents
require a new model version. Blocked, failed or invalid bundles cannot become model
Releases. Investigate historical checksum failures and produce a new valid version;
do not edit an immutable old bundle and reuse its version.

Inspect the assets, then supply a GitHub token with **Contents: write** for your repository.
This credential is separate from both the serving query key and the activation key:

```bash
export STARCAT_MODELS_GITHUB_TOKEN='your-repository-token'
uv run starcat-recsys release publish \
  --package workspace/releases/YOUR_MODEL --repository YOUR_OWNER/YOUR_REPO
# Explicitly upload after reviewing the preview:
uv run starcat-recsys release publish \
  --package workspace/releases/YOUR_MODEL --repository YOUR_OWNER/YOUR_REPO --apply
```

The default target is `main`. Use `--target` for an existing branch or commit containing
the training source. Publication creates a draft, uploads and verifies all four assets,
then makes it public. Retry the same command after interruption. Conflicting assets
are never replaced or deleted. Publishing a Release does not activate a serving model.

Record and synchronize results separately:

```bash
uv run starcat-recsys history record \
  --bundle workspace/registry/versions/YOUR_MODEL --catalog workspace/history.json
uv run starcat-recsys history render --catalog workspace/history.json --output workspace/docs
uv run starcat-recsys history sync \
  --catalog workspace/history.json --repository YOUR_OWNER/YOUR_REPO --branch main
```

`history sync` previews by default; `--apply` permits writes. It merges remote records
and commits only generated `docs/MODEL_HISTORY.md` and `docs/models/` files in one normal
commit without forcing the branch. Protected branches may require your review workflow.
Standalone `release publish` does not link its URL into this catalog; the weekly workflow does.

## Automatic weekly distribution

Every terminal weekly run generates aggregate results under `<workspace>/public/docs/`
without changing the source checkout. Failed and blocked runs are documented too; only
successfully deployed final bundles are automatically distributed.
Explicitly enable remote writes in the private weekly configuration:

```yaml
public_distribution:
  enabled: true
  repository: YOUR_OWNER/YOUR_REPO
  branch: main
  token_env: STARCAT_MODELS_GITHUB_TOKEN
  # Optional for launchd: a private token file with mode 600. Environment takes precedence.
  # token_file: /path/to/private/models-github.key
```

The actual scheduler process must receive that environment variable. A shell `export`
does not automatically reach launchd or systemd. Alternatively, set `token_file` to a
private plain text token file with mode `600`; Python reads it directly and YAML stores
only its path. Keep token values out of YAML, plists, logs and Git.
See the [operator guide](每周自动训练运维指南.md) for the real weekly command.

Distribution has a separate status: `prepared` means local reports only, `published`
means GitHub synchronization completed, and `pending` requires a retry. A GitHub failure
does not invalidate training, redeploy a model or trigger a rollback.

```bash
uv run python -m starcat_recsys.weekly sync-publication --config /path/to/weekly.yaml
# Retry distribution of the current run after inspecting local output:
uv run python -m starcat_recsys.weekly sync-publication --config /path/to/weekly.yaml --apply
```

Resolve pending distribution before the next weekly run. For older runs, retry
`release publish` with its saved package and `history sync` with its saved catalog.

## Historical backfill

Maintainers can recover public records from existing training metadata:

```bash
uv run starcat-recsys history backfill --root /path/to/training \
  --catalog docs/models/history.json --output docs
```

This reads manifests, aggregate reports and checksums at known directory levels. It does
not scan raw events, retrain, package or upload old models. Full run IDs distinguish
separate experimental series. A directory alone does not prove completed training, and
registry installation does not prove deployment. The v17 block, v18 data repair and
v20 inherited evaluation scope remain explicit.
