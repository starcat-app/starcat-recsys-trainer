# 模型成果与 GitHub Releases

[English](MODEL_RELEASES.md)

## 发布内容

训练成果在 [MODEL_HISTORY.md](MODEL_HISTORY.md) 中浏览，机器可读索引是
`models/history.json`。每条记录区分已评估、已上线、受阻、失败、仅数据处理和缺少
证据；`published` 指推荐服务上线验收成功，是否已在 GitHub 分发另看 `release_url`。
旧版本缺少的训练时间或指标保留 `null`，不会用打包时间替代。

模型 Release 使用 `model-<model_version>` tag，代码版本与模型版本独立。
下载 Release 的模型资产，而不是 GitHub 自动生成的 Source code：

| 资产 | 用途 |
| --- | --- |
| `<model_version>.zip` | 可直接交给推荐服务的 ServingBundle |
| `model-card.md` | 算法、输入范围、评估口径、规模和来源说明 |
| `evaluation.json` | 聚合指标及评估来源摘要 |
| `SHA256SUMS` | 以上三个资产的 SHA-256 |

ZIP 根目录只有 `manifest.json`、`recommendations.sqlite` 和 `checksums.json`。
不分发 Raw、逐主体行为、冻结训练主体、私有配置或凭据。`release.json` 是本地续传
凭据清单，不是上传资产。[模型格式](MODEL_FORMAT.md)解释指标与元数据语义。

## 准备并发布一个模型

先验证本地 Bundle，生成可复现 ZIP。此步骤不访问 GitHub：

```bash
uv run starcat-recsys release prepare \
  --bundle workspace/registry/versions/YOUR_MODEL \
  --output workspace/releases/YOUR_MODEL
```

同一个 Bundle 重试打包的 ZIP 字节保持一致；不同内容必须用新版本。被阻止、失败或
未完成校验的产物不能作为正式模型发布。旧 Bundle 若含有失效的额外校验项，应先
调查并产出新的合法版本，不能改写旧版本后冒用原编号。

检查资产和模型卡后，使用自己的仓库及具有该仓库 **Contents: write** 权限的
GitHub Token。Token 从环境读取，不复用推荐查询 Key 或模型激活 Key：

```bash
export STARCAT_MODELS_GITHUB_TOKEN='your-repository-token'
uv run starcat-recsys release publish \
  --package workspace/releases/YOUR_MODEL --repository YOUR_OWNER/YOUR_REPO
# 上条只预览；明确执行上传时添加：
uv run starcat-recsys release publish \
  --package workspace/releases/YOUR_MODEL --repository YOUR_OWNER/YOUR_REPO --apply
```

默认目标分支 `main`；可用 `--target` 指定已经存在、包含本次训练源码的分支或提交。
先创建草稿、上传四个资产并校验摘要，完整后才公开。重试补齐缺少资产；同名不同内容
直接报错，不覆盖、不删除。首次上传失败后保留草稿，重新执行相同命令即可。
发布 Release 不会自动激活推荐服务。

单独发布后归档成果：

```bash
uv run starcat-recsys history record \
  --bundle workspace/registry/versions/YOUR_MODEL --catalog workspace/history.json
uv run starcat-recsys history render --catalog workspace/history.json --output workspace/docs
uv run starcat-recsys history sync \
  --catalog workspace/history.json --repository YOUR_OWNER/YOUR_REPO --branch main
```

`history sync` 默认只预览，添加 `--apply` 后才写远端。它读取远端历史合并记录，
只更新 `docs/MODEL_HISTORY.md` 与 `docs/models/` 中的生成文件，并创建一个普通提交；
不会强推。分支保护若拒绝直接更新，需要维护者按自己的审核流程合入文档。
单独 `release publish` 的 URL 不会自动回写该索引；周流程会自动关联。

## 每周完成后自动更新

周任务结束时自动生成聚合成果，默认放在 `<workspace>/public/docs/`，不改源码
checkout。失败和受阻也留记录，只有成功上线的最终 Bundle 才自动分发。
在私有周配置中显式启用 GitHub 写入：

```yaml
public_distribution:
  enabled: true
  repository: YOUR_OWNER/YOUR_REPO
  branch: main
  token_env: STARCAT_MODELS_GITHUB_TOKEN
  # launchd 可改用权限为 600 的私有文件；环境变量存在时优先使用环境值。
  # token_file: /path/to/private/models-github.key
```

确保真实调度进程能读到该环境变量；交互 shell 的 `export` 不会自动进入 launchd
或 systemd。也可设置 `token_file` 指向权限为 `600` 的私有纯文本 Token 文件，
由 Python 直接读取；YAML 只保存路径。Token 不写入 YAML、plist、日志或 Git。
再使用[运维指南](每周自动训练运维指南.md)中的真实周任务入口。

分发独立记账：`distribution.status=prepared` 表示仅本地生成，
`published` 表示 GitHub 同步完成，`pending` 表示需要补传。GitHub 失败不会把已上线
模型改成训练失败，也不会触发重新训练或回退。

```bash
uv run python -m starcat_recsys.weekly sync-publication --config /path/to/weekly.yaml
# 检查本地结果后补传当前运行，不训练、不调用生产发布接口：
uv run python -m starcat_recsys.weekly sync-publication --config /path/to/weekly.yaml --apply
```

补传针对当前运行。开始下次周任务前处理 `pending`；更早的资产可用对应本地 package
重试 `release publish`，成果可用对应本地 catalog 重试 `history sync`。

## 历史回补

维护者可从既有训练目录只读恢复公开成果：

```bash
uv run starcat-recsys history backfill --root /path/to/training \
  --catalog docs/models/history.json --output docs
```

回补读取固定层级的 manifest、评估摘要和校验信息，不重训、不扫描 Raw、不打包或
上传旧模型。完整 run ID 区分不同实验系列的同名 v1/v2。仅有目录不证明训练完成，
本地 Registry 安装也不证明曾上线。v17 受阻、v18 数据修复和 v20 继承评估口径均
分别呈现，避免把版本增加误读成效果提高。
