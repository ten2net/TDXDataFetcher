# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

## [Unreleased]

## [0.2.0] - 2026-04-10

### 主题: 完整功能扩展 - Phases 1-8

**本次发布包含 39 个任务的完整实现，新增约 15,000 行代码，496+ 单元测试通过。**

---

#### 1. 数据存储与缓存 (Phase 1)

**今まで**: 所有数据需要实时从服务器获取，无本地存储能力，重复请求相同数据浪费带宽和时间。

**今後**: 支持 SQLite 本地缓存，智能缓存策略，数据压缩存储，多格式导出。

```python
from tdxapi import TdxCache

cache = TdxCache()
# 自动缓存和过期管理
bars = cache.get_bars("600519", "SH", "1d", start_date="2024-01-01")
# 支持压缩存储 (zlib/lz4)
# 支持 LRU 内存缓存 + TTL 过期机制
```

- 新增 `TdxCache` 类，支持 K线/分笔数据本地存储
- 支持 Parquet、CSV、Excel 格式导出
- 智能缓存策略：LRU 缓存、TTL 过期、增量更新
- 数据压缩存储：支持 zlib/lz4

#### 2. 异步API支持 (Phase 2)

**今まで**: 仅支持同步阻塞式请求，批量获取多只股票时效率低下。

**今後**: 完整的异步客户端，支持并发批量请求和流式数据。

```python
from tdxapi import AsyncTdxClient

async with AsyncTdxClient() as client:
    # 批量并发获取
    codes = [(1, "600519"), (0, "000001"), (1, "600036")]
    quotes = await client.get_quotes(codes)
    
    # 流式实时数据
    async for quote in client.stream_quotes(["600519"], interval=1):
        print(f"价格: {quote.price}")
```

- 新增 `AsyncTdxClient` 类，基于 asyncio
- 批量并发请求：`batch_get_quotes()`、`batch_get_bars()`
- 流式接口：`stream_quotes()`、`stream_bars()`、`stream_transactions()`
- 连接池管理：`TdxConnectionPool`，支持负载均衡和连接复用

#### 3. 实时数据订阅 (Phase 3)

**今まで**: 需要手动轮询获取实时数据，无法方便地监控多只股票。

**今後**: 支持实时订阅模式，自动轮询，回调机制，变化检测。

```python
from tdxapi import QuoteSubscription

subscription = QuoteSubscription()
subscription.subscribe_quotes(["600519", "000001"], interval=1)

@subscription.register_callback
def on_quote(quote):
    print(f"{quote.code}: {quote.price}")

subscription.start()
```

- 新增 `QuoteSubscription` 类，支持轮询订阅
- 回调接口设计：支持注册多种回调函数
- 变化检测机制：只在价格变动时触发回调
- 多股票订阅管理：`MultiQuoteSubscription` 支持多客户端并发

#### 4. 技术指标计算 (Phase 4)

**今まで**: 需要自行实现技术指标计算，或使用第三方库。

**今後**: 内置完整的技术指标库，支持一键计算所有指标。

```python
from tdxapi.indicators import calculate_all, MACD, KDJ, RSI

# 一键计算所有指标
df = calculate_all(bars)
print(df[["close", "ma5", "ma20", "macd_histogram", "kdj_k"]])

# 单独指标
macd_result = MACD.calculate([b.close for b in bars])
rsi_values = RSI.rsi14([b.close for b in bars])
```

- 移动平均线 (MA)：MA5/10/20/60/120/250，支持 SMA/EMA/WMA
- MACD 指标：DIF/DEA/MACD 柱状图
- KDJ 随机指标：K/D/J 值，支持超买超卖判断
- RSI 相对强弱指标：RSI6/12/24
- 布林带 (BOLL)：上轨/中轨/下轨
- 成交量指标：VOL、OBV
- 指标组合输出：返回包含所有指标的 DataFrame

#### 5. 批量数据下载 (Phase 5)

**今まで**: 需要手动逐个下载股票数据，无法批量获取全市场数据。

**今後**: 支持全市场数据批量下载，断点续传，进度显示。

```python
from tdxapi import BulkDownloader

async with BulkDownloader() as downloader:
    # 下载全市场日线数据
    progress = await downloader.download_all_stocks_bars(
        period="1d", count=500, save_dir="./data"
    )
```

- 全市场股票列表自动获取
- 历史K线批量下载：支持日期范围
- 分笔数据批量下载：支持多日数据
- 下载进度显示：tqdm 进度条
- 断点续传机制：支持中断后恢复

#### 6. 数据质量与工具 (Phase 6)

**今まで**: 需要自行处理除权除息、数据缺失等问题。

**今後**: 内置数据质量工具，支持复权计算、缺失检测、异常校验。

```python
from tdxapi.data_quality import PriceAdjuster, DataValidator

# 前复权/后复权
adjuster = PriceAdjuster()
adjusted = adjuster.adjust_forward(bars, xdxr_info)

# 数据校验
validator = DataValidator()
issues = validator.validate_bars(bars)
```

- 数据校验工具：检查价格/成交量异常值
- 缺失数据检测：识别并补全缺失的K线数据
- 除权除息复权：实现前复权/后复权计算
- 数据对齐工具：多股票数据时间对齐

#### 7. 高级功能 (Phase 7)

**今まで**: 数据使用需要自行处理格式转换和筛选逻辑。

**今後**: 内置 DataFrame 转换、股票筛选器、告警系统。

```python
from tdxapi import StockScreener, AlertSystem

# 股票筛选
screener = StockScreener()
screener.add_rule(ScreenerRule.undervalued())
results = screener.screen(stocks)

# 告警系统
alert_system = AlertSystem()
alert_system.add_alert(create_price_alert("600519", 1500.0, "above"))
```

- Pandas 集成：返回 DataFrame 格式数据
- Polars 支持：支持 Polars DataFrame 导出
- 股票筛选器：基于财务/技术指标筛选股票
- 告警系统：价格突破/指标金叉死叉告警

#### 8. 代码质量改进 (Phase 8)

- 修复测试警告
- 清理未使用导入
- 更新 README 文档

### 新增模块

| 模块 | 说明 |
|------|------|
| `tdxapi.cache` | SQLite本地缓存、LRU策略、TTL过期 |
| `tdxapi.export` | Parquet/CSV/Excel导出 |
| `tdxapi.async_client` | 异步客户端、批量并发、流式接口 |
| `tdxapi.connection_pool` | 连接池管理、负载均衡 |
| `tdxapi.subscription` | 实时数据订阅 |
| `tdxapi.indicators` | 技术指标计算 |
| `tdxapi.bulk_download` | 批量数据下载 |
| `tdxapi.data_quality` | 数据质量工具、复权计算 |
| `tdxapi.advanced` | 高级功能、筛选器、告警 |

### 测试覆盖

- 总测试数：496+
- 测试通过率：100%
- 新增测试文件：10个

## [0.1.0] - 2026-04-09

### Initial Release

- 通达信行情数据自研接口
- 支持实时行情、K线、分时、分笔数据
- 支持股票列表、财务数据、除权除息
- 协议已与 pytdx 源码逐命令校准
