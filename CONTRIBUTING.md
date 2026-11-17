# 贡献指南

欢迎通过 Issue 和 Pull Request 贡献。涉及数据源、训练或发布的变更，请先通过 Issue 对齐需求和数据安全边界。

## 本地开发

```bash
uv sync --all-groups
make check
```

修改训练或发布逻辑时，还应用新的 `run_id` 和 `model_version` 执行本地端到端示例。

## Pull Request

- 一个 PR 只处理一类问题。
- 行为变更必须增加或更新测试。
- 同步更新 `README.md`、`docs/使用说明.md`、`docs/架构设计.md` 和 `CHANGELOG.md` 中与本次变更相关的内容。
- PR 说明中列出执行过的验证命令和结果。
- 不得提交 token、`.env`、GCP 凭据、HMAC key、原始用户快照、本地 workspace 或模型 Registry。

安全漏洞按 [SECURITY.md](SECURITY.md) 私下报告，不要提交公开 Issue。
