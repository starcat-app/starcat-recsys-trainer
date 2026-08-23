.DEFAULT_GOAL := help

.PHONY: help sync format format-check lint typecheck test check

help: ## 显示可用命令
	@awk 'BEGIN {FS = ":.*?## "}; /^[a-zA-Z_-]+:.*?## / {printf "  %-18s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

sync: ## 安装运行和开发依赖
	uv sync --all-groups

format: ## 格式化代码
	uv run ruff format .
	uv run ruff check --fix .

format-check: ## 检查格式
	uv run ruff format --check .

lint: ## 执行静态检查
	uv run ruff check .

typecheck: ## 执行类型检查
	uv run mypy

test: ## 执行单元测试
	uv run pytest --cov=starcat_recsys --cov-report=term-missing

check: format-check lint typecheck test ## 执行全部质量门禁
	@echo "全部检查通过"
