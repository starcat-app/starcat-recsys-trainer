# 训练数据来源与下载

[English](DATA_SOURCES.md)

## 数据用途

| 来源 | 用途 | 获取方式 |
| --- | --- | --- |
| GH Archive WatchEvent | 公开 Star 行为观测 | 下述 BigQuery 日分区下载 |
| GitHub Repository REST API | 名称、描述、topics、语言和仓库有效性 | `github_api` 数据源与自己的 GitHub Token |
| GH Archive PushEvent | 可选的活跃度、目录补充 | 独立 PushEvent CLI，入门配方不依赖 |
| Starcat 用户主动贡献的快照 | 可选补充来源 | 自己部署 Collection 并使用管理凭据 |

公开数据入门配置不依赖 Starcat 用户贡献。Raw 中的 actor ID 在本地转换为来源内哈希；
原始身份、训练主体和私有快照都不会进入推荐模型包。

WatchEvent 是历史事件观测，不是当前完整 Star 列表；本项目不重建 Unstar。
上游缺失、仓库更名或删除、元数据时点和训练抽样都会影响覆盖，不能把本地分区完整
等同于捕获了全部 GitHub 行为。

## 已支持的下载方式：BigQuery

安装 Google Cloud CLI，在**自己的项目**启用 BigQuery，并配置 ADC：

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

这是固定历史窗口示例，不代表最新数据。首次建议先下载一天确认环境；预算是硬上限，
不是容量估算或“保证免费”。每个日分区都会先 dry run，实际查询继续携带费用上限。

Connector 从 `githubarchive.day.YYYYMMDD` 读取 WatchEvent 的原始 ID、actor ID、
repo ID 和事件时间，生成：

```text
workspace/watch-events/raw/gh_archive/
  download-state.json
  watch-events-YYYYMMDD.parquet
```

断点续跑使用完全相同的命令；追加日期保持同一 workspace、run ID 和预算，指定下一
个连续完整 UTC 日期并增加 `--append-existing`。不要下载仍在更新的 UTC 当天。

```bash
uv run starcat-recsys bigquery plan-watch-events-catch-up \
  --workspace workspace/watch-events
uv run starcat-recsys lake inspect-watch-events --workspace workspace/watch-events
```

第一个命令只规划日期，不会下载；第二个检查本地 schema、行数、校验和与 checkpoint。
遇到上游不存在的日表，现有采集器会记录空分区；应保留这些来源限制，不能将空分区
宣传为当日没有 Star 行为。

## HTTP 原始归档与项目输入格式不同

GH Archive 也提供 `https://data.gharchive.org/YYYY-MM-DD-H.json.gz` 小时归档。
它包含所有事件类型的 JSON；本项目的数据湖加载器要求上述 Parquet 与 checkpoint。
当前**没有内置 HTTP JSON.gz 转数据湖的转换器**，不能把下载文件改后缀后直接训练。
只使用预训练模型的用户无需下载这些 Raw，也不需要配置 GCP。

## 仓库元数据

设置自己的 `GITHUB_TOKEN`。公开训练配置按仓库 ID 获取有界目录并维护 ETag 缓存，
过滤私有、disabled 及其他不可用仓库。Token 能访问某个私有仓库，不代表可以把它
加入公开模型。仓库文本保留其来源，代码的 MIT License 不替代上游数据或文本条款。
公开描述和 topics 可能包含作者主动公布的联系邮箱；这些上游文本不属于训练用户身份信息，
模型包不承诺任意公开文本中都没有邮箱。

Raw、凭据、私有运行配置、训练主体和逐主体评估结果不进入 Git 或 Releases。
公开成果只记录聚合计数、指标、时间范围及摘要；每个模型的限制见模型说明。

## 既有 Mac 运维脚本

`scripts/download-{watch,push}-events.sh` 是面向外接卷、screen、caffeinate 的可选封装。
它们要求 `STARCAT_BQ_BILLING_PROJECT` 或被 Git 忽略的纯文本
`configs/billing-project.local`。数据卷默认使用通用示例 `/Volumes/StarcatData`；
用 `STARCAT_WATCH_VOLUME_ROOT` / `STARCAT_PUSH_VOLUME_ROOT` 覆盖，或在被 Git 忽略的
`configs/data-volume.local` 中保存一行自己的卷路径。环境变量优先，私有配置文件建议设为
`600`。脚本只读取纯文本，不执行其中的 shell 内容，启动前仍检查独立磁盘是否挂载。
其他目录可通过 `STARCAT_WATCH_*` / `STARCAT_PUSH_*` 覆盖。
上面的通用 CLI 不依赖这些脚本或维护者的磁盘布局。

## 官方资料

- [GH Archive 数据格式与公共数据集](https://www.gharchive.org/)
- [BigQuery 费用控制](https://cloud.google.com/bigquery/docs/best-practices-costs)
- [GitHub 仓库元数据 API](https://docs.github.com/en/rest/repos/repos#get-a-repository)
