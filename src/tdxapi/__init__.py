from tdxapi.cache import TdxCache
from tdxapi.network.client import TdxClient
from tdxapi.async_client import AsyncTdxClient, BatchResult
from tdxapi.connection_pool import TdxConnectionPool, PooledClient, PoolStats
from tdxapi.models import StockQuote, Bar, Tick
from tdxapi.protocol.constants import Market
from tdxapi.subscription import (
    QuoteSubscription,
    MultiQuoteSubscription,
    SubscriptionConfig,
    SubscriptionStats,
)

__version__ = "0.1.0"
__all__ = [
    "TdxClient",
    "AsyncTdxClient",
    "BatchResult",
    "TdxCache",
    "TdxConnectionPool",
    "PooledClient",
    "PoolStats",
    "StockQuote",
    "Bar",
    "Tick",
    "Market",
    "QuoteSubscription",
    "MultiQuoteSubscription",
    "SubscriptionConfig",
    "SubscriptionStats",
]

# 导出功能在导入时按需加载
# from tdxapi.export import to_dataframe, to_parquet, to_csv, to_excel, read_parquet
