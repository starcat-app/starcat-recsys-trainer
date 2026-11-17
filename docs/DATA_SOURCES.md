# Data acquisition and provenance

[简体中文](DATA_SOURCES-ZH.md)

## What the model uses

| Source | Purpose | Obtaining it |
| --- | --- | --- |
| GH Archive `WatchEvent` | Public Star observations | BigQuery daily download below |
| GitHub Repository REST API | Public names, descriptions, topics, language and availability | `github_api` source; your server-side token |
| GH Archive `PushEvent` | Optional activity/catalog metadata | Separate PushEvent CLI; not required by the starter recipe |
| Starcat opt-in snapshots | Optional contributed snapshots | Your own Collection instance and admin credentials |

The starter recipe does not use Starcat user contributions. Raw actor IDs are
converted into source-scoped hashes locally. Neither identities nor raw snapshots
are included in a serving model.

WatchEvent is an event observation, not a current stargazer list. Unstars are not
reconstructed. Missing upstream events, renamed/deleted repositories, frozen
metadata and sampled cohorts limit coverage and evaluation conclusions.

## Supported download path: BigQuery

Install Google Cloud CLI, enable BigQuery in **your** project, and configure ADC:

```bash
gcloud auth application-default login
gcloud auth application-default set-quota-project YOUR_GCP_PROJECT
uv sync --locked --all-groups
uv run starcat-recsys bigquery download-watch-events \
  --workspace workspace/watch-events \
  --billing-project YOUR_GCP_PROJECT \
  --start-date 2026-10-01 --end-date 2026-10-31 \
  --maximum-bytes-billed-per-day 1073741824 \
  --maximum-total-bytes-billed 40000000000 \
  --run-id public-watch-202610
```

The dates are an explicit example, not “latest”. Start with one day to validate
your environment. Budgets are hard ceilings, not guaranteed estimates or a promise
of free execution. Each partition is dry-run checked before the real query.

The connector queries `githubarchive.day.YYYYMMDD`, selecting WatchEvent ID,
actor ID, repository ID and event time. It writes:

```text
workspace/watch-events/raw/gh_archive/
  download-state.json
  watch-events-YYYYMMDD.parquet
```

Repeat exactly the same command to resume verified partitions. To extend the same
dataset, use the next consecutive complete UTC date, the same workspace/run ID
and budgets, and `--append-existing`. Do not use today's unfinished UTC partition.

```bash
uv run starcat-recsys bigquery plan-watch-events-catch-up \
  --workspace workspace/watch-events
uv run starcat-recsys lake inspect-watch-events --workspace workspace/watch-events
```

The first command plans only; it does not download. The second verifies the local
checkpoint, schema, rows and checksums. A ready local partition is not proof that
the upstream archive captured every GitHub event.

## HTTP archives are a different input format

GH Archive also supplies hourly `https://data.gharchive.org/YYYY-MM-DD-H.json.gz`.
Those files contain all event types as JSON, whereas this project's supported
lake loader expects the normalized Parquet layout and checkpoint above.
There is currently **no built-in HTTP JSON.gz-to-lake converter**. Do not rename
the downloaded file to `.parquet` or advertise it as directly trainable.
Using pretrained models avoids both the raw download and GCP setup entirely.

## Repository metadata

Set `GITHUB_TOKEN` to your own token. The starter configuration resolves a bounded
catalog through public repository IDs and maintains an ETag cache. Private,
disabled and otherwise unavailable repositories are filtered. Token access does
not authorize training on private repositories. Repository text and its original
provenance remain associated with the originating public project.

Raw data, credentials, private run/config snapshots, dataset identities and
subject-level observations stay outside Git and Releases. The code license is
in `LICENSE`; upstream data and repository content retain their source terms.
See each model card for the dataset window and evaluation limitations.

## Existing Mac operator scripts

`scripts/download-{watch,push}-events.sh` are optional Mac wrappers for external
volumes, screen and caffeinate. They require `STARCAT_BQ_BILLING_PROJECT` or an
ignored plain-text `configs/billing-project.local`. Override their documented
`STARCAT_WATCH_*` / `STARCAT_PUSH_*` paths before using them on another machine.
The portable commands above do not depend on these scripts or maintainer disks.

## Primary references

- [GH Archive formats and public dataset](https://www.gharchive.org/)
- [BigQuery cost controls](https://cloud.google.com/bigquery/docs/best-practices-costs)
- [GitHub repository API](https://docs.github.com/en/rest/repos/repos#get-a-repository)
