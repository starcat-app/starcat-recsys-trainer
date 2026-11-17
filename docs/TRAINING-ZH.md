# 本地训练与复现指南

[English](TRAINING.md)

## 环境与资源

macOS 或 Linux 上安装 Python 3.12、uv 和 Git：

```bash
uv sync --locked --all-groups
```

不需要 GPU。DuckDB 当前使用 6 GiB 工作内存上限和单线程；NumPy、SciPy、
scikit-learn 与操作系统还会使用额外内存，不能把 6 GiB 当整机最低配置。
每周运维入口检查至少 20 GiB 空闲空间，这也不是 Raw 或所有任务的容量估算。
先用合成样例和小窗口测量耗时、峰值内存、临时文件与产物大小，再决定扩大范围。

## 离线合成样例

```bash
uv run starcat-recsys pipeline run --config configs/example-hybrid.yaml
uv run starcat-recsys bundle query \
  workspace/registry/versions/example-hybrid-v1 --repo-id 1
```

此配置执行 co-star 与 25% 内容排名融合，并生成服务端 TF-IDF 索引。
原 `example-local.yaml` 保留 Popular/SVD/co-star 基线演示。两者都只使用仓库内
合成数据，不访问网络，不能以样例指标代表正式质量。

## 从公开数据训练

1. 按[数据下载指南](DATA_SOURCES-ZH.md)将示例 2026 年 10 月 WatchEvent
   下载到 `workspace/watch-events`，或选定自己的明确窗口。
2. 设置 `export GITHUB_TOKEN='your-token'`，用于补公开仓库元数据。
3. 检查 `configs/example-public-training.yaml`。路径相对 YAML；更改时间范围时同步
   更改训练、验证与测试切分。示例限制 5000 个仓库并抽样 2% 主体。
4. 执行：

```bash
uv run starcat-recsys pipeline run --config configs/example-public-training.yaml
uv run starcat-recsys bundle verify workspace/registry/versions/public-202610-v1
uv run starcat-recsys bundle query \
  workspace/registry/versions/public-202610-v1 --repo-id YOUR_REPO_ID
```

流程会从输入生成自己的 Canonical、训练主体、权重、目录与评估数据，不依赖维护者的
冻结样本或私有 Collection。未配置 `publish.recommend_api` 时只安装本地 Registry。

入门配置的 `evaluated_subjects: 1` 只检查评估确实有样本，不是正式周训练的 5000 人
质量门槛。先固定评估协议，再看 test；不要不断按同一 test 调参。
`training.reference_time` 必须等于 `dataset.train_end`。

主要产物：

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

每次新实验更换 run ID 和模型版本；固定输入、源码、lockfile、参数及 metadata 快照
后才谈可复现。重新抓取 GitHub metadata 的结果可能变化。

## 打包后交给推荐服务

```bash
uv run starcat-recsys release prepare \
  --bundle workspace/registry/versions/public-202610-v1 \
  --output workspace/releases/public-202610-v1
```

输出 ZIP、指标 JSON、模型说明与 SHA256SUMS。使用推荐服务的 `scripts/install-model.sh`
导入自己的实例；这里的 prepare 只生成本地文件，不上传 GitHub，也不激活线上模型。

## 初始化每周训练

从自己的公开数据训练结果生成私有配置：

```bash
uv run starcat-recsys weekly-init \
  --run workspace/public-training/runs/public-202610-v1 \
  --raw-root workspace/watch-events/raw/gh_archive \
  --watermark 2026-10-31 \
  --api-root /path/to/starcat-recommend-api \
  --output workspace/operator/weekly.yaml
```

初始水位记录已经观察过的数据窗口。先追加该日期之后的 Raw，周评估才有新的观测。
初始化只写配置，不训练、不安装调度、不连接生产。保留首次训练的 Dataset、目录与
Bundle 不变；初始扩展目录复用用户自己的候选，不复制官方训练主体或扩容目录。

修改配置中的服务地址、两类密钥文件路径，以及外接盘场景的 `volume_root`。
两个密钥分别保存为权限 600 的文件。先把初始 Bundle 导入该服务，使其 active 与
配置的 `initial_bundle` 一致。

```bash
uv run python -m starcat_recsys.weekly status --config workspace/operator/weekly.yaml
uv run python -m starcat_recsys.weekly run --config workspace/operator/weekly.yaml
```

第二条是真实训练与服务发布命令，继续执行 5000 人、同输入质量、覆盖和接口检查。
小数据集可能长期受阻；系统累积留出窗口，不降低门槛。隔离 API 验收使用其已锁定
Go 依赖，不要求额外克隆同级 `starcat-api-kit`。

macOS 将 `STARCAT_WEEKLY_RECOMMENDATION_CONFIG` 设置为配置绝对路径后，可使用原
LaunchAgent 安装脚本。Linux 定时器直接调用 Python 入口，不运行 caffeinate 封装。
到期判断使用 Asia/Shanghai；安装或首次加载可能立即补跑到期任务。

成果自动更新、GitHub 发布与失败补传见[模型分发指南](MODEL_RELEASES-ZH.md)。
