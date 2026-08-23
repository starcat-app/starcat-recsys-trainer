# 安全说明

## 报告漏洞

请通过仓库的 GitHub Security Advisory 私下报告。报告中可以包含受影响的 commit、运行环境、复现步骤和预期影响，但不得提交真实 token、原始 Starcat 快照或其他用户数据。

## 安全边界

- BigQuery 使用 Application Default Credentials，GitHub token 和 Starcat HMAC key 只从环境变量读取。
- 正式 BigQuery 查询必须先 dry run，并对真实任务设置 `maximum_bytes_billed`。
- 不采集 Private/Internal repo、login、email、标签、笔记、搜索或 AI/RAG 内容。
- Starcat `participant_id` 在导入时转换为 source-local HMAC 标识，不进入 canonical 之后的对外产物。
- ServingBundle 仅包含公开仓库 metadata 和预计算聚合边，必须通过 checksum 和 SQLite `quick_check` 才能激活。
- Model Registry 中的版本不可覆盖，防止同版本产物被静默替换。

## 支持范围

尚未发布稳定版本时，只支持当前默认分支。公开后只为最新稳定版本和当前默认分支提供安全修复。
