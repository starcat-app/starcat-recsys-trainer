# Changelog

All notable changes to Starcat Recommendation Pipeline are documented here.

## Unreleased

- 新增 GH Archive BigQuery `WatchEvent` 按日采集，在查询前执行 dry run 并限制每日最大扫描字节数。
- 新增 GitHub Repository metadata 采集，支持 ETag 缓存、请求数上限和限流信息识别。
