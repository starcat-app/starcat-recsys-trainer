"""本机 BigQuery SQL Lab 的只读查询、预算与结果边界。

SQL Lab 只服务管理员本机的数据验证，不是通用 SQL 网关。模块把 SQL 校验、
dry run、引用数据集限制和结果截断放在 Trainer 内，避免 Node BFF 复制 BigQuery
业务逻辑，也避免浏览器绕过同一组成本护栏。
"""

from __future__ import annotations

import base64
import hashlib
import json
import math
import re
from collections.abc import Iterable
from dataclasses import asdict, dataclass
from datetime import date, datetime, time
from decimal import Decimal
from pathlib import Path
from typing import Any, Protocol

from google.cloud import bigquery

SQL_LAB_SCHEMA_VERSION = 1
SQL_LAB_MAXIMUM_SQL_BYTES = 100 * 1024
SQL_LAB_MAXIMUM_BYTES_BILLED = 10 * (1 << 30)
SQL_LAB_MAXIMUM_RESULT_ROWS = 200
SQL_LAB_MAXIMUM_RESULT_BYTES = 2 * (1 << 20)
_ALLOWED_PROJECTS = frozenset({"githubarchive"})
_FORBIDDEN_SQL = re.compile(
    r"\b(?:ALTER|CALL|CREATE|DELETE|DROP|EXECUTE\s+IMMEDIATE|EXPORT\s+DATA|"
    r"EXTERNAL_QUERY|GRANT|INSERT|LOAD\s+DATA|MERGE|REMOTE|REVOKE|TRUNCATE|UPDATE)\b",
    re.IGNORECASE,
)


@dataclass(frozen=True, slots=True)
class BigQueryField:
    """SQL Lab 可以返回浏览器的 BigQuery 字段描述。"""

    name: str
    field_type: str
    mode: str


@dataclass(frozen=True, slots=True)
class BigQueryDryRun:
    """Google dry run 返回的只读查询计划摘要。"""

    statement_type: str
    estimated_bytes: int
    fields: tuple[BigQueryField, ...]
    referenced_tables: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class BigQueryQueryPage:
    """有严格行数与序列化字节上限的临时查询结果。"""

    job_id: str
    processed_bytes: int
    billed_bytes: int
    total_rows: int
    fields: tuple[BigQueryField, ...]
    rows: tuple[dict[str, Any], ...]
    truncated: bool


class BigQueryLabGateway(Protocol):
    """隔离 Google SDK，让 SQL 安全和预算门禁可以离线测试。"""

    def dry_run(self, sql: str) -> BigQueryDryRun: ...

    def query(
        self,
        sql: str,
        maximum_bytes_billed: int,
        maximum_result_rows: int,
        maximum_result_bytes: int,
    ) -> BigQueryQueryPage: ...


class GoogleBigQueryLabGateway:
    """使用 ADC 执行 BigQuery dry run 和有界只读查询。"""

    def __init__(self, billing_project: str, location: str = "US") -> None:
        if not billing_project:
            raise ValueError("billing_project 不能为空")
        if not re.fullmatch(r"[A-Za-z0-9-]+", location):
            raise ValueError("BigQuery location 格式不合法")
        self._client = bigquery.Client(project=billing_project, location=location)

    def dry_run(self, sql: str) -> BigQueryDryRun:
        job = self._client.query(
            sql,
            job_config=bigquery.QueryJobConfig(dry_run=True, use_query_cache=False),
        )
        return BigQueryDryRun(
            statement_type=str(job.statement_type or ""),
            estimated_bytes=int(job.total_bytes_processed or 0),
            fields=_fields(job.schema or ()),
            referenced_tables=tuple(
                sorted(
                    f"{table.project}.{table.dataset_id}.{table.table_id}"
                    for table in (job.referenced_tables or ())
                )
            ),
        )

    def query(
        self,
        sql: str,
        maximum_bytes_billed: int,
        maximum_result_rows: int,
        maximum_result_bytes: int,
    ) -> BigQueryQueryPage:
        job = self._client.query(
            sql,
            job_config=bigquery.QueryJobConfig(
                use_query_cache=False,
                maximum_bytes_billed=maximum_bytes_billed,
            ),
        )
        result = job.result(max_results=maximum_result_rows + 1)
        rows, size_truncated = _bounded_rows(
            result,
            maximum_result_rows,
            maximum_result_bytes,
        )
        total_rows = int(result.total_rows or 0)
        return BigQueryQueryPage(
            job_id=job.job_id,
            processed_bytes=int(job.total_bytes_processed or 0),
            billed_bytes=int(job.total_bytes_billed or 0),
            total_rows=total_rows,
            fields=_fields(result.schema),
            rows=rows,
            truncated=size_truncated or total_rows > len(rows),
        )


def dry_run_sql_lab(
    sql: str,
    maximum_bytes_billed: int,
    gateway: BigQueryLabGateway,
) -> dict[str, Any]:
    """校验 SQL 并返回可绑定正式执行的 dry run 文档。"""

    normalized = _validate_input(sql, maximum_bytes_billed)
    plan = gateway.dry_run(normalized)
    _validate_plan(plan, maximum_bytes_billed)
    return {
        "schema_version": SQL_LAB_SCHEMA_VERSION,
        "operation": "dry_run",
        "sql_sha256": _sql_sha256(normalized),
        "statement_type": plan.statement_type,
        "estimated_bytes": plan.estimated_bytes,
        "maximum_bytes_billed": maximum_bytes_billed,
        "fields": [asdict(field) for field in plan.fields],
        "referenced_tables": list(plan.referenced_tables),
    }


def query_sql_lab(
    sql: str,
    maximum_bytes_billed: int,
    expected_sql_sha256: str,
    maximum_result_rows: int,
    gateway: BigQueryLabGateway,
) -> dict[str, Any]:
    """重新 dry run 同一 SQL 后执行，防止预览与正式查询之间发生替换。"""

    normalized = _validate_input(sql, maximum_bytes_billed)
    sql_sha256 = _sql_sha256(normalized)
    if expected_sql_sha256 != sql_sha256:
        raise ValueError("SQL 已变化，必须重新 dry run")
    if not 1 <= maximum_result_rows <= SQL_LAB_MAXIMUM_RESULT_ROWS:
        raise ValueError(f"maximum_result_rows 必须在 1..{SQL_LAB_MAXIMUM_RESULT_ROWS} 之间")
    plan = gateway.dry_run(normalized)
    _validate_plan(plan, maximum_bytes_billed)
    page = gateway.query(
        normalized,
        maximum_bytes_billed,
        maximum_result_rows,
        SQL_LAB_MAXIMUM_RESULT_BYTES,
    )
    return {
        "schema_version": SQL_LAB_SCHEMA_VERSION,
        "operation": "query",
        "sql_sha256": sql_sha256,
        "job_id": page.job_id,
        "statement_type": plan.statement_type,
        "estimated_bytes": plan.estimated_bytes,
        "processed_bytes": page.processed_bytes,
        "billed_bytes": page.billed_bytes,
        "maximum_bytes_billed": maximum_bytes_billed,
        "total_rows": page.total_rows,
        "returned_rows": len(page.rows),
        "truncated": page.truncated,
        "fields": [asdict(field) for field in page.fields],
        "rows": list(page.rows),
    }


def read_sql_file(path: Path) -> str:
    """从受限输入文件读取 SQL，避免 SQL 文本进入进程 argv。"""

    if not path.is_file():
        raise ValueError("SQL 输入文件不存在或不是普通文件")
    size = path.stat().st_size
    if size <= 0 or size > SQL_LAB_MAXIMUM_SQL_BYTES:
        raise ValueError(f"SQL 文件必须在 1..{SQL_LAB_MAXIMUM_SQL_BYTES} 字节之间")
    return path.read_text(encoding="utf-8")


def _validate_input(sql: str, maximum_bytes_billed: int) -> str:
    normalized = sql.strip()
    if not normalized:
        raise ValueError("SQL 不能为空")
    if len(normalized.encode("utf-8")) > SQL_LAB_MAXIMUM_SQL_BYTES:
        raise ValueError(f"SQL 不能超过 {SQL_LAB_MAXIMUM_SQL_BYTES} 字节")
    if not 0 < maximum_bytes_billed <= SQL_LAB_MAXIMUM_BYTES_BILLED:
        raise ValueError(f"maximum_bytes_billed 必须在 1..{SQL_LAB_MAXIMUM_BYTES_BILLED} 之间")
    statement = normalized[:-1].rstrip() if normalized.endswith(";") else normalized
    if ";" in statement:
        raise ValueError("SQL Lab 只允许一条语句")
    if _FORBIDDEN_SQL.search(statement):
        raise ValueError("SQL Lab 只允许只读 SELECT 查询")
    if not re.match(r"^(?:SELECT|WITH)\b", statement, re.IGNORECASE):
        raise ValueError("SQL Lab 只允许 SELECT 或 WITH ... SELECT")
    for identifier in re.findall(r"`([^`]+)`", statement):
        project = identifier.split(".", 1)[0]
        if project not in _ALLOWED_PROJECTS:
            raise ValueError("SQL Lab 首期只允许引用 githubarchive 公共数据集")
    return statement


def _validate_plan(plan: BigQueryDryRun, maximum_bytes_billed: int) -> None:
    if plan.statement_type.upper() != "SELECT":
        raise ValueError("BigQuery dry run 表明该语句不是只读 SELECT")
    for table in plan.referenced_tables:
        if table.split(".", 1)[0] not in _ALLOWED_PROJECTS:
            raise ValueError("SQL Lab 首期只允许引用 githubarchive 公共数据集")
    if plan.estimated_bytes > maximum_bytes_billed:
        raise RuntimeError(
            f"BigQuery dry run 预计扫描 {plan.estimated_bytes} 字节，超过上限 "
            f"{maximum_bytes_billed}"
        )


def _sql_sha256(sql: str) -> str:
    return "sha256:" + hashlib.sha256(sql.encode("utf-8")).hexdigest()


def _fields(schema: Iterable[bigquery.SchemaField]) -> tuple[BigQueryField, ...]:
    return tuple(
        BigQueryField(name=field.name, field_type=field.field_type, mode=field.mode)
        for field in schema
    )


def _bounded_rows(
    rows: Iterable[Any],
    maximum_rows: int,
    maximum_bytes: int,
) -> tuple[tuple[dict[str, Any], ...], bool]:
    accepted: list[dict[str, Any]] = []
    serialized_bytes = 2  # JSON 数组的中括号。
    truncated = False
    for raw_row in rows:
        if len(accepted) >= maximum_rows:
            truncated = True
            break
        row = {str(key): _json_value(value) for key, value in raw_row.items()}
        row_bytes = len(
            json.dumps(
                row,
                ensure_ascii=False,
                separators=(",", ":"),
                allow_nan=False,
            ).encode("utf-8")
        )
        separator_bytes = 1 if accepted else 0
        if serialized_bytes + separator_bytes + row_bytes > maximum_bytes:
            truncated = True
            break
        accepted.append(row)
        serialized_bytes += separator_bytes + row_bytes
    return tuple(accepted), truncated


def _json_value(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, bool)):
        return value
    if isinstance(value, float):
        return value if math.isfinite(value) else str(value)
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, (datetime, date, time)):
        return value.isoformat()
    if isinstance(value, bytes):
        return base64.b64encode(value).decode("ascii")
    if isinstance(value, dict):
        return {str(key): _json_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_value(item) for item in value]
    return str(value)
