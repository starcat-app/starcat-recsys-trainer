"""内置数据源 Connector。"""

from starcat_recsys.sources.bigquery import BigQueryConfig, BigQueryWatchEventSource
from starcat_recsys.sources.starcat_collection import (
    StarcatCollectionAPISource,
    StarcatCollectionConfig,
)

__all__ = [
    "BigQueryConfig",
    "BigQueryWatchEventSource",
    "StarcatCollectionAPISource",
    "StarcatCollectionConfig",
]
