"""管道节点接口。

接口以不可变 artifact 路径为边界，使同一节点既能由单机执行器串行运行，也能在未来
交给远程 worker。这里不提供动态第三方代码加载，避免训练环境执行不受信任插件。
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol, runtime_checkable


class InsufficientTrainingDataError(ValueError):
    """候选模型因样本数学条件不足而无法训练，但不代表整条管道失败。"""


@dataclass(frozen=True, slots=True)
class Artifact:
    """一个可校验、可追踪的管道产物。"""

    kind: str
    uri: str
    checksum: str
    metadata: dict[str, Any]


@dataclass(frozen=True, slots=True)
class TaskContext:
    """单次可复现任务共享的运行上下文。"""

    run_id: str
    workspace: Path
    config_hash: str
    random_seed: int


@runtime_checkable
class SourceConnector(Protocol):
    """把外部数据采集到原始数据区。"""

    @property
    def name(self) -> str: ...

    def collect(self, context: TaskContext) -> list[Artifact]: ...


@runtime_checkable
class Normalizer(Protocol):
    """把来源数据转换为管道标准记录。"""

    def normalize(self, context: TaskContext, inputs: list[Artifact]) -> list[Artifact]: ...


@runtime_checkable
class DatasetBuilder(Protocol):
    """从标准记录构建训练、验证和测试数据集。"""

    def build(self, context: TaskContext, inputs: list[Artifact]) -> list[Artifact]: ...


@runtime_checkable
class Trainer(Protocol):
    """根据固定配置生成候选模型产物。"""

    @property
    def name(self) -> str: ...

    def train(self, context: TaskContext, inputs: list[Artifact]) -> list[Artifact]: ...


@runtime_checkable
class Evaluator(Protocol):
    """评估候选模型并生成可比较指标。"""

    def evaluate(self, context: TaskContext, inputs: list[Artifact]) -> list[Artifact]: ...


@runtime_checkable
class Publisher(Protocol):
    """把通过门禁的模型转换为只读 ServingBundle。"""

    def publish(self, context: TaskContext, inputs: list[Artifact]) -> list[Artifact]: ...


@runtime_checkable
class TaskExecutor(Protocol):
    """执行一个无共享内存依赖的管道任务。"""

    def execute(self, task_name: str, callback: Any) -> Any: ...
