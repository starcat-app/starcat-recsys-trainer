"""标准领域模型测试。"""

from datetime import UTC, datetime

import pytest

from starcat_recsys.domain import (
    InteractionRecord,
    RelationType,
    RepositoryRecord,
    SourceKind,
    parse_datetime,
)


def test_interaction_requires_positive_repo_id() -> None:
    with pytest.raises(ValueError, match="repo_id"):
        InteractionRecord(
            source=SourceKind.GH_ARCHIVE,
            source_record_id="event-1",
            source_subject_id="gh:1",
            repo_id=0,
            relation_type=RelationType.STAR_EVENT,
            occurred_at=datetime.now(UTC),
            observed_at=datetime.now(UTC),
        )


def test_repository_trainable_only_when_public_and_active() -> None:
    repository = RepositoryRecord(
        repo_id=1,
        full_name="owner/repo",
        description=None,
        topics=(),
        primary_language="Python",
        license_key="mit",
        stars=10,
        forks=1,
        archived=False,
        disabled=False,
        visibility="public",
        pushed_at=None,
        fetched_at=datetime.now(UTC),
    )
    assert repository.is_public_trainable


def test_parse_datetime_normalizes_utc() -> None:
    assert parse_datetime("2026-08-23T08:00:00+08:00") == datetime(2026, 8, 23, tzinfo=UTC)
