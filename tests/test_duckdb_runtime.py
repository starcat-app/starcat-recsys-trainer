"""DuckDB 运行时目录约束测试。"""

from pathlib import Path

import pytest

from starcat_recsys.duckdb_runtime import connect_for_workspace


def test_connect_for_workspace_binds_temp_directory(tmp_path: Path) -> None:
    """spill 目录必须位于调用方指定的训练 workspace。"""

    connection = connect_for_workspace(tmp_path, "dataset-build")
    try:
        row = connection.execute("SELECT current_setting('temp_directory')").fetchone()
    finally:
        connection.close()

    assert row is not None
    assert Path(str(row[0])) == (tmp_path / "duckdb-temp" / "dataset-build").resolve()


@pytest.mark.parametrize("task_name", ["", "../escape", "task/name", "task name"])
def test_connect_for_workspace_rejects_unsafe_task_name(tmp_path: Path, task_name: str) -> None:
    """任务名不能逃逸统一临时目录。"""

    with pytest.raises(ValueError, match="task_name"):
        connect_for_workspace(tmp_path, task_name)
