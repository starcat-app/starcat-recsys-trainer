# AGENTS.md

本文档是 `starcat-recsys-trainer` 的 AI 协作规则唯一维护源。

## 仓库边界

- 本目录是推荐数据与离线训练管道的独立 Git 仓库，不提供生产 REST API；在线
  查询由独立的 `starcat-recommend-api` 读取 ServingBundle。
- 本仓库拥有独立分支、remote、提交、数据和发布边界；不得把改动并入外层 Starcat
  主仓库或顺带修改相邻 `supports/*` 项目。
- 开工前核对 `git status --short` 与当前分支，保留用户已有改动；未经明确要求不
  commit、push、切分支或修改 remote。

## 用途、技术栈与目录

- 使用 Python 3.12、uv、DuckDB、PyArrow、BigQuery SDK、NumPy、SciPy 与
  scikit-learn 完成数据采集、Canonical Parquet、训练、评估和 Bundle 交付。
- `src/starcat_recsys/` 是管道、数据源、训练器、评估、Bundle 与 publisher 实现；
  `tests/` 是回归测试；`configs/` 是显式运行配置；`scripts/` 是 BigQuery 下载等
  运维入口；`docs/` 记录架构、使用和已完成验证。
- 中英文 README 导向本地训练、数据来源和模型分发指南；公开成果由
  `docs/models/history.json` 生成。周运行默认仅写 workspace 下的公开报告，
  `public_distribution.enabled` 或 CLI `--apply` 才允许写 GitHub。
- Raw、Canonical、workspace、Registry、checkpoint 与凭据都是运行时数据，不得提交；
  Registry 版本不可覆盖，重跑必须使用新的 `run_id` 和 `model_version`。
- 只处理公开仓库和已脱敏主体；不同数据源的主体标识不得关联，Token、GCP 凭据、
  HMAC key 与发布密钥只能从环境或权限为 600 的私有凭据文件读取。

## 开发与验证

- 代码变更必须运行完整本地质量门禁：

```bash
make check
git diff --check
```

- `make check` 包含 Ruff 格式检查、lint、mypy strict 与 pytest coverage；不要用单个
  子命令的成功代替完整门禁。
- 只有明确需要更新依赖环境时才运行 `uv sync --all-groups`；不要擅自升级依赖或改写
  lockfile。

## 高风险与副作用禁令

- BigQuery 查询会消耗配额并可能产生费用。未经 dong4j 针对本次运行明确授权，不得
  执行 BigQuery 采集、SQL Lab 实查或 `download-*-events.sh start/restart`。
- 未经明确授权不得运行 `make demo`、任何 `pipeline run`、正式训练、全量 Raw 扫描，
  也不得在 `/Volumes/T0` 或其他外接存储创建、移动、覆盖数据。
- 未经明确授权不得生成、替换、上传或激活 ServingBundle，不得调用
  `RecommendAPIBundlePublisher`，不得向 `starcat-recommend-api /api/v2` 发布。
- 不得打印或提交 Token、ADC、HMAC key、发布密钥、真实数据路径、匿名主体原值或服务端
  错误正文；只读检查也必须遵守既有路径脱敏与预算门禁。
