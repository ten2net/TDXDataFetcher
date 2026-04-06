# TDXAPI - 通达信行情数据自研接口

不依赖通达信客户端，直接通过 TCP 协议连接行情服务器获取数据。

**协议已与 pytdx 源码逐命令校准，二进制完全一致。**

## 快速开始

```bash
pip install -e .
```

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

## 支持的数据类型

| 类型 | 方法 | 状态 |
|------|------|------|
| 实时行情（5档） | `get_quote()` | ✅ |
| K线（日/周/月/年） | `get_bars(period="1d")` | ✅ |
| 分钟K线（1/5/15/30/60分） | `get_bars(period="1min/5m/15m")` | ✅ |
| 分时数据 | `get_minute_time()` | ✅ |
| 分笔成交 | `get_transactions()` | ✅ |
| 股票列表 | `get_security_list()` | ✅ |
| 除权除息 | `get_xdxr_info()` | ✅ |
| 财务数据 | `get_finance_info()` | ✅ |
| 指数/期货行情 | `get_index_quote()` | ✅ |

## 高级功能

```python
# 多线程安全（默认开启）
client = TdxClient(thread_safe=True)

# 心跳保活（默认开启）
client = TdxClient(heartbeat=True)

# 指数行情
client.get_index_quote("000001")

# 期货行情
client.get_futures_quote("IF2504", market=6)
```

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

- **单元测试**: 69/69 通过
- **协议包**: 18/18 与 pytdx 源码一致
- **数据验证**: 与东方财富、新浪等公开数据对比一致

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
│   ├── protocol/        # 协议层（包头、请求构造）
│   ├── parser/           # 解析器（二进制→结构化数据）
│   ├── network/          # 网络层（连接、重连、测速）
│   ├── models/           # 数据模型
│   └── utils/           # 工具函数
├── tests/               # 单元测试 (69个，100%通过)
└── examples/            # 使用示例
```

## License

MIT
