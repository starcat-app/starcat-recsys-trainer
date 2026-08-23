"""本地文件数据源，用于导入已经落地的 raw 分区和可复现 fixture。"""

from __future__ import annotations

import shutil
from pathlib import Path

from starcat_recsys.artifacts import artifact_for, write_json_atomic
from starcat_recsys.interfaces import Artifact, TaskContext


class LocalFileSource:
    """将一个明确类型的本地文件复制到 raw zone。"""

    def __init__(self, path: Path, *, source: str, kind: str) -> None:
        self._path = path
        self._source = source
        self._kind = kind
        self.name = f"local_file_{source}"

    def collect(self, context: TaskContext) -> list[Artifact]:
        if not self._path.is_file():
            raise FileNotFoundError(f"本地数据文件不存在：{self._path}")
        output_directory = context.workspace / "raw" / "local" / self._source
        output_directory.mkdir(parents=True, exist_ok=True)
        target = output_directory / self._path.name
        shutil.copy2(self._path, target)
        artifact = artifact_for(target, self._kind, {"source": self._source})
        write_json_atomic(
            output_directory / f"{self._path.name}.manifest.json",
            {
                "schema_version": 1,
                "run_id": context.run_id,
                "source": self.name,
                "input_path": self._path.as_posix(),
                "artifact_checksum": artifact.checksum,
            },
        )
        return [artifact]
