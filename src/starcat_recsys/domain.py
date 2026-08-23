"""管道内部的稳定领域模型。

外部数据源必须先转换为这些模型，训练器不能读取 BigQuery、GitHub 或 Starcat 的
原始 DTO。行为关系和仓库特征故意拆成两类记录，避免抹平事件与当前快照的语义差异。
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any


class SourceKind(StrEnum):
    """第一版支持的数据来源。"""

    GH_ARCHIVE = "gh_archive"
    STARCAT_SNAPSHOT = "starcat_snapshot"
    GITHUB_METADATA = "github_metadata"


class RelationType(StrEnum):
    """用户与仓库之间的正向关系语义。"""

    STAR_EVENT = "star_event"
    CURRENT_STAR_MEMBERSHIP = "current_star_membership"


@dataclass(frozen=True, slots=True)
class InteractionRecord:
    """训练可使用的标准 user-repo 关系记录。"""

    source: SourceKind
    source_record_id: str
    source_subject_id: str
    repo_id: int
    relation_type: RelationType
    occurred_at: datetime | None
    observed_at: datetime
    source_weight: float = 1.0
    snapshot_id: str | None = None
    schema_version: int = 1

    def __post_init__(self) -> None:
        if self.repo_id <= 0:
            raise ValueError("repo_id 必须为正整数")
        if not self.source_record_id or not self.source_subject_id:
            raise ValueError("来源记录和主体标识不能为空")
        if self.source_weight <= 0:
            raise ValueError("source_weight 必须大于 0")
        if self.observed_at.tzinfo is None:
            raise ValueError("observed_at 必须包含时区")
        if self.occurred_at is not None and self.occurred_at.tzinfo is None:
            raise ValueError("occurred_at 必须包含时区")

    def to_dict(self) -> dict[str, Any]:
        return {
            "source": self.source.value,
            "source_record_id": self.source_record_id,
            "source_subject_id": self.source_subject_id,
            "repo_id": self.repo_id,
            "relation_type": self.relation_type.value,
            "occurred_at": _isoformat(self.occurred_at),
            "observed_at": _isoformat(self.observed_at),
            "source_weight": self.source_weight,
            "snapshot_id": self.snapshot_id,
            "schema_version": self.schema_version,
        }


@dataclass(frozen=True, slots=True)
class RepositoryRecord:
    """训练、过滤和 Serving 共用的公开仓库特征。"""

    repo_id: int
    full_name: str
    description: str | None
    topics: tuple[str, ...]
    primary_language: str | None
    license_key: str | None
    stars: int
    forks: int
    archived: bool
    disabled: bool
    visibility: str
    pushed_at: datetime | None
    fetched_at: datetime
    metadata_version: int = 1

    def __post_init__(self) -> None:
        if self.repo_id <= 0:
            raise ValueError("repo_id 必须为正整数")
        if not self.full_name:
            raise ValueError("full_name 不能为空")
        if self.stars < 0 or self.forks < 0:
            raise ValueError("stars 和 forks 不能为负数")
        if self.fetched_at.tzinfo is None:
            raise ValueError("fetched_at 必须包含时区")

    @property
    def is_public_trainable(self) -> bool:
        return self.visibility == "public" and not self.archived and not self.disabled

    def to_dict(self) -> dict[str, Any]:
        return {
            "repo_id": self.repo_id,
            "full_name": self.full_name,
            "description": self.description,
            "topics": list(self.topics),
            "primary_language": self.primary_language,
            "license_key": self.license_key,
            "stars": self.stars,
            "forks": self.forks,
            "archived": self.archived,
            "disabled": self.disabled,
            "visibility": self.visibility,
            "pushed_at": _isoformat(self.pushed_at),
            "fetched_at": _isoformat(self.fetched_at),
            "metadata_version": self.metadata_version,
        }


def parse_datetime(value: str | datetime | None) -> datetime | None:
    """把外部 ISO-8601 时间统一转换为 UTC aware datetime。"""

    if value is None:
        return None
    if isinstance(value, datetime):
        parsed = value
    else:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("时间必须包含时区")
    return parsed.astimezone(UTC)


def _isoformat(value: datetime | None) -> str | None:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z") if value else None
