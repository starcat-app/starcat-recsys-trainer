"""内置数据源 Connector。"""

from starcat_recsys.sources.bigquery import BigQueryConfig, BigQueryWatchEventSource

__all__ = ["BigQueryConfig", "BigQueryWatchEventSource"]
