"""单机管道任务执行器。"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from time import monotonic
from typing import TypeVar

Result = TypeVar("Result")


@dataclass(frozen=True, slots=True)
class TaskExecution:
    """一个节点的最小可观测记录。"""

    task_name: str
    duration_seconds: float
    status: str


class LocalExecutor:
    """串行执行节点，但不改变 artifact 边界。"""

    def __init__(self) -> None:
        self.executions: list[TaskExecution] = []

    def execute(self, task_name: str, callback: Callable[[], Result]) -> Result:
        started = monotonic()
        try:
            result = callback()
        except Exception:
            self.executions.append(TaskExecution(task_name, monotonic() - started, "failed"))
            raise
        self.executions.append(TaskExecution(task_name, monotonic() - started, "completed"))
        return result
