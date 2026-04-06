# TDXAPI 项目知识文档

## 一、项目概述

自研通达信行情数据接口，不依赖通达信客户端，直接通过 TCP 协议连接行情服务器获取数据。

## 二、协议核心发现（pytdx 源码逐命令校准）

### 2.1 关键发现：不同功能使用不同的 magic/seq 组合

| 功能 | Magic | Seq | 备注 |
|------|-------|-----|------|
| 实时行情 | 0x010c | 0x02006320 | |
| K线/指数 | 0x010c | 0x01016408 | |
| 股票数量 | 0x0c0c | 0x186c | |
| 股票列表 | 0x010c | 0x1864 | |
| 分时数据 | 0x1b08 | - | |
| 历史分时 | 0x0130 | - | |
| 分笔成交 | 0x1708 | - | |
| 历史分笔 | 0x0130 | - | |
| 除权除息 | 0x1f18 | - | |
| 财务数据 | 0x1f18 | - | |

### 2.2 响应包结构（统一 16 字节头）

```
<IIIHH> = 4+4+4+2+2 = 16 bytes
- I: unknown1 (包含 \xb1\xcb 标记在高字节)
- I: seq_id
- I: zip_size (压缩后长度，**可能不可靠！**)
- H: unzip_size (解压前长度，更可靠)
- H: unknown2

重要：zip_size 可能不可靠（垃圾值），应优先使用 unzip_size
- 如果 zip_size >= unzip_size，使用 unzip_size 作为实际大小
- 如果 zip_size < unzip_size，尝试 zlib 解压；解压失败则使用原始数据
```

### 2.3 三次握手（Setup 命令）

连接后必须发送三次握手，否则服务器不响应后续请求：

```python
SETUP_CMD1 = bytes.fromhex("0c 02 18 93 00 01 03 00 03 00 0d 00 01")
SETUP_CMD2 = bytes.fromhex("0c 02 18 94 00 01 03 00 03 00 0d 00 02")
SETUP_CMD3 = bytes.fromhex(
    "0c 03 18 99 00 01 20 00 20 00 db 0f d5"
    "d0 c9 cc d6 a4 a8 af 00 00 00 8f c2 25"
    "40 13 00 00 d5 00 c9 cc bd f0 d7 ea 00"
    "00 00 02"
)
```

### 2.4 数据编码格式（最关键！）

#### 2.4.1 实时行情价格：变长整型编码

**不是 `struct("<h")`！** pytdx 使用自定义变长整型编码：

```
解码算法 (_get_price):
- 首个字节: bits 0-5 = 数据低6位, bit 6 = 符号, bit 7 = 是否有后续字节
- 后续字节: bits 0-6 = 数据 << 6/13/20/..., bit 7 = 是否有更多字节
- 多个后续字节的数据用 OR 合并，不是加法
- 最终值 = 符号 ? -result : result
```

```python
def _get_price(data: bytes, pos: int) -> tuple:
    pos_byte = 6
    bdata = data[pos]
    intdata = bdata & 0x3f
    sign = bool(bdata & 0x40)
    if bdata & 0x80:
        while True:
            pos += 1
            bdata = data[pos]
            intdata |= (bdata & 0x7f) << pos_byte  # OR 不是加法！
            pos_byte += 7
            if bdata & 0x80:
                pass
            else:
                break
    pos += 1
    if sign:
        intdata = -intdata
    return intdata, pos
```

**编码算法 (_encode_price)**:
```python
def _encode_price(value: int) -> bytes:
    if value == 0:
        return bytes([0])
    result = bytearray()
    sign = 0x40 if value < 0 else 0
    value = abs(value)
    chunk = value & 0x3F
    value >>= 6
    result.append(sign | 0x80 | chunk)
    bit_pos = 7
    while value:
        chunk = value & ((1 << bit_pos) - 1)
        value >>= bit_pos
        result.append(0x80 | chunk)
        bit_pos += 7
    result[-1] = result[-1] & 0x7F
    return bytes(result)
```

#### 2.4.2 成交量：对数体积编码

pytdx 使用对数编码存储成交量，解析函数为 `_get_volume`（复杂浮点运算）。

#### 2.4.3 实时行情 vs K线：编码格式不同

| 数据类型 | 价格编码 | 体积编码 | 说明 |
|----------|----------|----------|------|
| 实时行情 | 变长整型 `_get_price` | 对数编码 `_get_volume` | bid/ask/volume |
| K线价格 | **变长整型 `_get_price`** | 原始 4 字节 `struct("<I")` | open/high/low/close，**不是 struct("<h")！** |
| K线日期 | **`<I` (YYYYMMDD)** | - | **category 9/5/6/10/11 用 `<I`，不是 `<HHH`！** |

### 2.5 实时行情响应体结构

```
响应体格式 (4字节头 + 股票列表):
- 4字节前缀 (可能是版本/类型标记)
- num_stock: 2字节 `<H` (从字节偏移量2开始)
- 股票条目 (从字节偏移量4开始):
  - market: 1 byte (B) - 0=SZ, 1=SH
  - code: 6 bytes (6s) - ASCII股票代码
  - active1: 2 bytes (H)
  - price: 变长整型
  - last_close_diff: 变长整型
  - open_diff: 变长整型
  - high_diff: 变长整型
  - low_diff: 变长整型
  - reversed0: 变长整型
  - reversed1: 变长整型
  - vol: 变长整型
  - cur_vol: 变长整型
  - amount: 4 bytes (I) + 对数解码
  - s_vol: 变长整型
  - b_vol: 变长整型
  - reversed2: 变长整型
  - reversed3: 变长整型
  - bid1/ask1/bid_vol1/ask_vol1: 各变长整型
  - bid2/ask2/bid_vol2/ask_vol2: 各变长整型
  - bid3/ask3/bid_vol3/ask_vol3: 各变长整型
  - bid4/ask4/bid_vol4/ask_vol4: 各变长整型
  - bid5/ask5/bid_vol5/ask_vol5: 各变长整型
  - reversed4-8: 各变长整型
  - speed + active2: 6 bytes
```

价格计算: `price = (base_price + diff) / 100.0`

### 2.6 K线日期时间格式

```python
def _get_datetime(category, buffer, pos):
    if category in (9, 5, 6, 10, 11):       # 日/周/月/季/年线
        (date_val,) = struct.unpack_from("<I", buffer, pos)  # YYYYMMDD 4字节！
        year = date_val // 10000
        month = (date_val % 10000) // 100
        day = date_val % 100
        pos += 4
    elif category < 4 or category in (7, 8):  # 1分钟/5分钟等
        (zipday, tminutes) = struct.unpack_from("<HH", buffer, pos)
        year = (zipday >> 11) + 2004
        month = int((zipday % 2048) / 100)
        day = (zipday % 2048) % 100
        hour = int(tminutes / 60)
        minute = tminutes % 60
        pos += 4
    else:                                   # 分钟线(category=0,1,2,3)
        minutes, hour, minute = struct.unpack_from("<IHH", buffer, pos)
        base = datetime(2000, 1, 1)
        dt = base + timedelta(days=minutes // 1440)
        year, month, day = dt.year, dt.month, dt.day
        pos += 8
    return year, month, day, hour, minute, pos
```

### 2.7 服务器地址

```
深圳: 119.147.212.81:7709
上海: 180.153.39.51:7709
备1: 119.147.212.82:7709
备2: 218.75.126.9:7709
备3: 115.238.90.165:7709
备4: 119.97.133.99:7709
```

## 三，项目结构

```
tdxapi/
├── src/tdxapi/
│   ├── protocol/
│   │   ├── constants.py   # 协议常量、Setup命令、服务器列表
│   │   ├── packet.py      # 响应包头解析、zlib解压
│   │   └── requests.py    # 所有请求包体构造（16种，逐命令校准）
│   ├── parser/
│   │   └── quote_parser.py # 二进制数据解析（9种，pytdx编码逐行校准）
│   ├── network/
│   │   └── client.py      # TCP客户端、自动重连、测速
│   ├── models/
│   │   └── quote.py       # 数据模型（StockQuote含5档、Bar、Tick）
│   └── utils/
│       └── helpers.py     # 工具函数
├── tests/                  # 单元测试（69个，100%通过）
├── examples/               # 使用示例（6个）
└── .venv/                 # UV虚拟环境
```

## 四、API 清单

| 方法 | 说明 | 状态 |
|------|------|------|
| `get_quote(code, market)` | 单只实时行情 | ✅ |
| `get_quotes(stocks)` | 批量实时行情 | ✅ |
| `get_bars(code, market, period, count)` | K线（1d/1w/1m/1min/5m/15m/30m/60m） | ✅ |
| `get_index_bars(code, market, period, count)` | 指数K线 | ✅ |
| `get_minute_time(code, market, use_bars)` | 当日分时（默认用1分钟K线） | ✅ |
| `get_history_minute_time(code, market, date)` | 历史分时 | ⚠️ |
| `get_transactions(code, market, start, count)` | 分笔成交 | ✅ |
| `get_history_transactions(code, market, date, start, count)` | 历史分笔 | ⚠️ |
| `get_stock_count(market)` | 市场股票总数 | ✅ |
| `get_security_list(market, start, count)` | 股票列表 | ✅ |
| `get_xdxr_info(code, market)` | 除权除息 | ✅ |
| `get_finance_info(code, market)` | 财务数据 | ✅ |
| `get_index_quote(code)` | 指数行情 | ✅ |
| `get_futures_quote(code, market)` | 期货行情 | ✅ |
| `get_company_info_category(code, market)` | 公司信息目录 | ⚠️ |
| `get_block_info_meta(blockfile)` | 板块信息元数据 | ⚠️ |

### 4.1 K线周期映射

| period 参数 | Category | 说明 |
|-------------|----------|------|
| `1d` | 9 | 日线 |
| `1w` | 5 | 周线 |
| `1m` | 6 | 月线 |
| `1min` | 7 | 1分钟线 |
| `5m` | 0 | 5分钟线 |
| `15m` | 1 | 15分钟线 |
| `30m` | 2 | 30分钟线 |
| `60m` | 3 | 60分钟线 |

## 五、验证结果

- **单元测试**: 69/69 通过
- **协议包对比**: 18/18 与 pytdx 源码完全一致
- **真实连通验证**: ✅ 已验证
  - 实时行情: 600519 贵州茅台价格 1460.0 元 ✅
  - 日K线: 2026-04-03 收盘价 1460.00 与公开数据一致 ✅
  - 1分钟K线: 14:56-15:00 价格 1459.94-1460.0 ✅
  - 5分钟K线: OHLC 数据正确 ✅
  - 15/30/60分钟K线: 正常 ✅
  - 分时数据: 通过1分钟K线提供准确数据 ✅
  - 股票数量: 深圳 22954 只，上海 26897 只 ✅

### 5.1 数据对比验证（600519 贵州茅台）

| 数据源 | 2026-04-03 收盘 | 当前价 |
|--------|------------------|--------|
| TdxAPI | 1460.00 | 1460.00 |
| 公开数据 | 1460.00 | ~1460 |
| 差异 | 0.00 | 0.00 |

## 六，重连机制

- 连接异常时自动重试（默认 3 次，间隔 0.1/0.2/0.3 秒）
- 重试前先 close() 再 connect(ip, port)
- 保留上次成功连接的 IP/Port

## 七、待完善项

~~1. **多线程安全**：可选 threading.Lock 支持~~ ✅ 已完成
~~2. **心跳保活**：定期发送心跳包维持连接~~ ✅ 已完成
~~3. **指数/期货支持**：扩展市场类型和数据解析~~ ✅ 已完成
~~4. **历史分时、历史分笔**：真实测试~~ ✅ 已完成
5. **分时数据原始解析**：字节边界问题待修复
6. **公司信息/板块信息**：请求构造存在但未测试

## 八、高级功能

### 8.1 多线程安全

```python
client = TdxClient(thread_safe=True)  # 默认开启
```

所有网络操作自动加锁，保证并发安全。

### 8.2 心跳保活

```python
client = TdxClient(heartbeat=True)  # 默认开启，30秒一次
```

后台线程定期发送心跳包，防止连接超时断开。

### 8.3 指数/期货支持

```python
# 获取指数行情
client.get_index_quote("000001")  # 上证指数

# 获取期货行情
client.get_futures_quote("IF2504", market=6)  # 上海期货
```

市场代码：
- 0: 深圳 (SZ)
- 1: 上海 (SH)
- 2: 北京 (BJ)
- 6: 上海期货
- 7: 中金所期货
- 8: 大连商品
- 9: 郑州商品

### 8.4 解压优化

`_recv_response` 现在会自动检测 zlib 压缩数据：
1. 优先使用 `unzip_size`
2. 检查数据是否以 `x\x9c` 开头（zlib 标志）

### 8.5 分时数据解决方案

`get_minute_time` 默认使用1分钟K线代替原始解析：

```python
# 默认（推荐）- 使用1分钟K线
data = client.get_minute_time("600519", "SH")  # 返回 [{"price": 1460.0, "volume": 1000}, ...]

# 原始解析（有问题）
data = client.get_minute_time("600519", "SH", use_bars=False)
```

## 九、已知问题

### 9.1 分时数据原始解析

原始分时数据（`get_minute_time(use_bars=False)`）存在字节边界对齐问题：
- 响应体包含6字节头部 + 股票代码
- 实际价格数据从偏移量6开始
- 解析结果部分价格异常

**临时解决方案**：使用1分钟K线作为替代（`use_bars=True`，默认）

## 十、参考资源

- pytdx 源码: https://github.com/rainx/pytdx
- 通达信官方: https://www.tdx.com.cn
- TdxQuant文档: https://help.tdx.com.cn/quant/
