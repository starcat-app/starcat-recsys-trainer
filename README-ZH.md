# Starcat 推荐模型训练

[English](README.md)

本项目从公开 GitHub 行为与仓库元数据生成推荐模型，由独立的
[starcat-recommend-api](https://github.com/starcat-app/starcat-recommend-api)
提供在线查询。训练和服务可以部署在不同机器。

## 选择入口

| 目标 | 入口 | 环境 |
| --- | --- | --- |
| 不下载训练数据，直接使用模型 | [推荐服务部署](https://github.com/starcat-app/starcat-recommend-api/blob/main/docs/SELF_HOSTING-ZH.md) | Docker 或 Go、模型 ZIP |
| 不联网体验训练全流程 | 本页快速开始 | Python 3.12、uv |
| 用公开数据训练自己的模型 | [训练指南](docs/TRAINING-ZH.md) | GH Archive 数据、GitHub 元数据 |
| 查看从早期版本到每周模型的成果 | [训练历史](docs/MODEL_HISTORY.md) | 无需运行环境 |
| 发布模型、自动同步成果 | [模型分发](docs/MODEL_RELEASES-ZH.md) | 自己仓库的发布权限 |

预训练模型在本仓库 [GitHub Releases](https://github.com/starcat-app/starcat-recsys-trainer/releases)
中以 `model-<model_version>` 发布。若尚无模型 Release，可先跑合成样例，或导入维护者
提供的模型 ZIP。GitHub 自动生成的 Source code ZIP 不是训练好的模型。

## 快速开始

```bash
git clone https://github.com/starcat-app/starcat-recsys-trainer.git
cd starcat-recsys-trainer
uv sync --locked --all-groups
uv run starcat-recsys pipeline run --config configs/example-hybrid.yaml
uv run starcat-recsys bundle verify workspace/registry/versions/example-hybrid-v1
uv run starcat-recsys bundle query workspace/registry/versions/example-hybrid-v1 --repo-id 1
```

样例全部使用仓库内的合成数据，不访问网络，不需要 Starcat 账号、Collection、GCP 或
GitHub 凭据。它验证共现训练、内容融合、评估与内容索引；样例指标不能用来宣传正式
模型效果。重跑时同时更换 `run_id` 与 `publish.model_version`，已有版本不能覆盖。

## 数据、成果和边界

- GH Archive WatchEvent 提供公开 Star 事件，不等于当前完整 Stars 或 Unstar 数据。
- GitHub Repository API 提供公开名称、描述、topics、语言等；PushEvent 目录可选。
- 主要算法为时间衰减 co-star 与 TF-IDF 内容排名融合，另提供 Popular/SVD 基线。
- ServingBundle 只包含公开仓库元数据、聚合推荐边、内容索引和模型说明，不包含训练主体。
- `metrics_scope=pre_refit` 表示指标来自最终重训前的候选，不能冒充最终模型再次实测。
- 历史失败、仅数据修复和缺少完成证据的版本均如实列出；不同评估集不能直接比较。

[数据从哪里来、怎么下载](docs/DATA_SOURCES-ZH.md) ·
[模型文件与 version 契约](docs/MODEL_FORMAT.md) ·
[训练成果](docs/MODEL_HISTORY.md)

## 每周自动更新

Mac 定时入口为每周日北京时间 11:00。保持现有 5000 人、同输入质量、覆盖与隔离接口
检查，通过后才上线；受阻和失败也会归档聚合报告。GitHub 自动写入默认关闭，
启用后同步模型 Release 和成果文档，失败可独立补传，不必重新训练。

[首次训练与周任务初始化](docs/TRAINING-ZH.md) ·
[详细运维与恢复](docs/每周自动训练运维指南.md)

## 开发与支持

```bash
make check
git diff --check
```

本地质量门禁包含 Ruff、mypy strict 和 pytest coverage。Python CLI 可在 macOS/Linux
运行；launchd、caffeinate 运维封装仅适用于 macOS，不是 Linux 的必需依赖。
推荐服务不需要 Python 或 GPU。

[贡献说明](CONTRIBUTING.md) · [安全](SECURITY.md) · [支持](SUPPORT.md) ·
[代码 MIT License](LICENSE)
