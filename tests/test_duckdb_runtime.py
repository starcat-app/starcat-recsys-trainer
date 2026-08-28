"""DuckDB 运行时目录约束测试。"""

from pathlib import Path

import pytest

from starcat_recsys.duckdb_runtime import (
    DUCKDB_MEMORY_LIMIT,
    DUCKDB_THREADS,
    connect_for_workspace,
)


def test_connect_for_workspace_binds_temp_directory(tmp_path: Path) -> None:
    """spill 目录必须位于调用方指定的训练 workspace。"""

    connection = connect_for_workspace(tmp_path, "dataset-build")
    try:
        row = connection.execute(
            "SELECT current_setting('temp_directory'), current_setting('memory_limit'), "
            "current_setting('threads'), current_setting('preserve_insertion_order')"
        ).fetchone()
    finally:
        connection.close()

    assert row is not None
    assert Path(str(row[0])) == (tmp_path / "duckdb-temp" / "dataset-build").resolve()
    assert str(row[1]) == "4.0 GiB"
    assert int(row[2]) == 1
    assert row[3] is False
    assert DUCKDB_MEMORY_LIMIT == "4GiB"
    assert DUCKDB_THREADS == 1


@pytest.mark.parametrize("task_name", ["", "../escape", "task/name", "task name"])
def test_connect_for_workspace_rejects_unsafe_task_name(tmp_path: Path, task_name: str) -> None:
    """任务名不能逃逸统一临时目录。"""

    with pytest.raises(ValueError, match="task_name"):
        connect_for_workspace(tmp_path, task_name)
