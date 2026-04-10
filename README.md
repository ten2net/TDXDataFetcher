# TDXAPI - 通达信行情数据自研接口

不依赖通达信客户端，直接通过 TCP 协议连接行情服务器获取数据。

**协议已与 pytdx 源码逐命令校准，二进制完全一致。**

## 功能特性

- **实时行情**: 5档盘口、实时价格、成交量
- **历史数据**: K线（日/周/月/年/分钟）、分笔成交
- **异步支持**: AsyncTdxClient、批量并发、流式数据
- **数据存储**: SQLite本地缓存、Parquet/CSV/Excel导出
- **技术指标**: MA/MACD/KDJ/RSI/BOLL等完整指标库
- **批量下载**: 全市场数据批量下载、断点续传
- **数据质量**: 复权计算、缺失检测、异常校验
- **高级功能**: 股票筛选器、告警系统、Pandas/Polars集成

## 快速开始

```bash
pip install -e .
# 或带可选依赖
pip install -e ".[export,compression]"
```

### 基础用法

```python
from tdxapi import TdxClient

with TdxClient() as client:
    # 实时行情
    q = client.get_quote("600519", "SH")
    print(f"贵州茅台: {q.price}元")

    # K线
    bars = client.get_bars("600519", "SH", period="1d", count=100)
    print(f"收盘价: {[b.close for b in bars]}")

    # 分钟K线
    bars = client.get_bars("600519", "SH", period="1min", count=240)
```

### 异步客户端

```python
from tdxapi import AsyncTdxClient

async with AsyncTdxClient() as client:
    # 批量并发获取
    codes = [(1, "600519"), (0, "000001"), (1, "600036")]
    quotes = await client.get_quotes(codes)
    
    # 流式数据
    async for quote in client.stream_quotes(["600519"], interval=1):
        print(f"价格: {quote.price}")
```

### 本地缓存

```python
from tdxapi import TdxClient, TdxCache

cache = TdxCache()

with TdxClient() as client:
    # 下载并缓存
    bars = client.get_bars("600519", "SH", "1d", count=500)
    cache.save_bars("600519", "SH", "1d", bars)
    
    # 从缓存读取
    cached = cache.get_bars("600519", "SH", "1d", 
                            start_date="2024-01-01", 
                            end_date="2024-12-31")
```

### 技术指标

```python
from tdxapi import TdxClient
from tdxapi.indicators import calculate_all, MACD, KDJ

with TdxClient() as client:
    bars = client.get_bars("600519", "SH", "1d", count=100)
    
    # 计算所有指标
    df = calculate_all(bars)
    print(df[["close", "ma5", "ma20", "macd_histogram", "kdj_k"]])
    
    # 单独计算MACD
    macd_result = MACD.calculate([b.close for b in bars])
    print(f"DIF: {macd_result.dif[-1]}, DEA: {macd_result.dea[-1]}")
```

### 批量下载

```python
from tdxapi import BulkDownloader

async with BulkDownloader() as downloader:
    # 下载全市场日线数据
    progress = await downloader.download_all_stocks_bars(
        period="1d",
        count=500,
        save_dir="./data"
    )
    print(f"下载完成: {progress.completed}/{progress.total}")
```

### 数据导出

```python
from tdxapi import TdxClient
from tdxapi.export import to_parquet, to_csv, to_dataframe

with TdxClient() as client:
    bars = client.get_bars("600519", "SH", "1d", count=100)
    
    # 导出为 Parquet
    to_parquet(bars, "600519_daily.parquet")
    
    # 导出为 CSV
    to_csv(bars, "600519_daily.csv")
    
    # 转为 DataFrame
    df = to_dataframe(bars)
```

## 核心功能

| 功能 | 类/模块 | 说明 |
|------|---------|------|
| **同步客户端** | `TdxClient` | 实时行情、K线、分笔数据 |
| **异步客户端** | `AsyncTdxClient` | asyncio支持、批量并发、流式接口 |
| **连接池** | `TdxConnectionPool` | 多连接管理、负载均衡 |
| **本地缓存** | `TdxCache` | SQLite存储、LRU策略、TTL过期、压缩 |
| **数据导出** | `export` | Parquet/CSV/Excel/DataFrame |
| **技术指标** | `indicators` | MA/MACD/KDJ/RSI/BOLL等完整指标库 |
| **批量下载** | `BulkDownloader` | 全市场数据、断点续传、进度显示 |
| **数据质量** | `data_quality` | 复权计算、缺失检测、异常校验 |
| **实时订阅** | `QuoteSubscription` | 轮询订阅、回调机制、多股票管理 |
| **股票筛选** | `StockScreener` | 多条件筛选、技术指标过滤 |
| **告警系统** | `AlertSystem` | 价格/指标告警、金叉死叉检测 |

## K线周期

| period | 说明 |
|--------|------|
| `1d` | 日线 |
| `1w` | 周线 |
| `1m` | 月线 |
| `1min` | 1分钟 |
| `5m` | 5分钟 |
| `15m` | 15分钟 |
| `30m` | 30分钟 |
| `60m` | 60分钟 |

## 验证结果

- **单元测试**: 496+ 通过，覆盖率 90%+
- **协议包**: 18/18 与 pytdx 源码一致
- **数据验证**: 与东方财富、新浪等公开数据对比一致

### 测试覆盖

| 模块 | 测试数 | 状态 |
|------|--------|------|
| 协议层 | 28 | ✅ |
| 解析器 | 14 | ✅ |
| 同步客户端 | 24 | ✅ |
| 异步客户端 | 85 | ✅ |
| 连接池 | 25 | ✅ |
| 本地缓存 | 82 | ✅ |
| 数据导出 | 12 | ✅ |
| 技术指标 | 98 | ✅ |
| 批量下载 | 30 | ✅ |
| 数据质量 | 48 | ✅ |
| 实时订阅 | 41 | ✅ |
| 高级功能 | 60 | ✅ |

### 600519 贵州茅台验证

| 数据 | TdxAPI | 参考值 |
|------|--------|--------|
| 当前价 | 1460.00 | ~1460 |
| 2026-04-03 收盘 | 1460.00 | 1460.00 |
| 1分钟K线 | 正常 | 正常 |
| 5分钟K线 | 正常 | 正常 |

## 项目结构

```
tdxapi/
├── src/tdxapi/
│   ├── protocol/         # 协议层（包头、请求构造）
│   ├── parser/           # 解析器（二进制→结构化数据）
│   ├── network/          # 网络层（TdxClient、AsyncTdxClient）
│   ├── cache.py          # 本地缓存（SQLite、LRU、压缩）
│   ├── export.py         # 数据导出（Parquet/CSV/Excel）
│   ├── indicators.py     # 技术指标（MA/MACD/KDJ/RSI/BOLL）
│   ├── bulk_download.py  # 批量下载
│   ├── data_quality.py   # 数据质量（复权、对齐、校验）
│   ├── subscription.py   # 实时订阅
│   ├── advanced.py       # 高级功能（筛选器、告警）
│   ├── connection_pool.py # 连接池管理
│   ├── models/           # 数据模型
│   └── utils/            # 工具函数
├── tests/               # 单元测试 (496+，100%通过)
└── examples/            # 使用示例
```

## License

MIT
