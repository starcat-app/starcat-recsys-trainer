# 安全说明

## 报告漏洞

请通过仓库的 GitHub Security Advisory 私下报告。报告中可以包含受影响的 commit、运行环境、复现步骤和预期影响，但不得提交真实 token、原始 Starcat 快照或其他用户数据。

## 安全边界

- BigQuery 使用 Application Default Credentials；GitHub token、Starcat HMAC key 和发布凭据只从环境变量或权限为 `600` 的私有凭据文件读取，不写入仓库。
- 正式 BigQuery 查询必须先 dry run，并对真实任务设置 `maximum_bytes_billed`。
- 不采集 Private/Internal repo、训练用户的 login、email、标签、笔记、搜索或 AI/RAG 内容。
- 公开仓库的描述与 topics 属于上游元数据，可能包含作者主动公布的联系邮箱。保留这些文本不代表采集了训练用户邮箱，也不承诺所有公开文本都不含邮箱。
- Starcat `participant_id` 在导入时转换为 source-local HMAC 标识，不进入 canonical 之后的对外产物。
- ServingBundle 仅包含公开仓库 metadata、预计算聚合边和内容索引，必须通过 checksum 和 SQLite `quick_check` 才能激活。
- Model Registry 中的版本不可覆盖，防止同版本产物被静默替换。

## 支持范围

尚未发布稳定版本时，只支持当前默认分支。公开后只为最新稳定版本和当前默认分支提供安全修复。
