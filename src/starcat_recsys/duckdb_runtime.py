"""统一创建训练阶段使用的 DuckDB 连接。

全量 GH Archive 查询可能产生大量排序、聚合临时文件。DuckDB 默认把这些文件写到
系统临时目录，会让训练数据虽然位于外置盘，系统盘仍被意外占满。因此生产管道必须把
每个任务的 spill 目录固定到本次训练 workspace 内；调用方不能直接使用默认连接。
"""

from __future__ import annotations

from pathlib import Path

import duckdb

DUCKDB_MEMORY_LIMIT = "4GiB"
DUCKDB_THREADS = 1


def connect_for_workspace(workspace: Path, task_name: str) -> duckdb.DuckDBPyConnection:
    """创建临时文件受控的内存 DuckDB 连接。

    ``task_name`` 同时用于隔离并行或连续任务的临时文件。限制名称字符可以避免配置值
    被解释为路径，从而确保所有 spill 文件都留在 ``workspace/duckdb-temp`` 下。
    """

    if not task_name or not task_name.replace("-", "").replace("_", "").isalnum():
        raise ValueError("DuckDB task_name 只能包含字母、数字、连字符和下划线")
    temp_directory = (workspace / "duckdb-temp" / task_name).resolve()
    temp_directory.mkdir(parents=True, exist_ok=True)
    connection = duckdb.connect()
    connection.execute(f"SET temp_directory = {_sql_literal(temp_directory)}")
    # DuckDB 默认可使用大部分物理内存；在桌面 Mac 上会与系统和其他应用争抢内存，
    # 极端情况下触发 swap 占满系统盘。十年 Dataset 实测 8 GiB / 2 线程仍会快速扩大
    # macOS swap；因此固定为 4 GiB / 1 线程，用更长运行时间换取桌面机器的系统稳定性。
    connection.execute(f"SET memory_limit = '{DUCKDB_MEMORY_LIMIT}'")
    connection.execute(f"SET threads = {DUCKDB_THREADS}")
    connection.execute("SET preserve_insertion_order = false")
    return connection


def _sql_literal(path: Path) -> str:
    """转义仅由本地路径生成的 SQL 字符串字面量。"""

    return "'" + path.as_posix().replace("'", "''") + "'"
