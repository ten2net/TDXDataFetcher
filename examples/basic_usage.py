"""
使用示例
"""

from tdxapi import TdxClient


def example_basic():
    """基础用法：获取实时行情和K线"""
    with TdxClient() as client:
        print(f"已连接: {client._ip}:{client._port}")

        # 单只股票行情
        q = client.get_quote("600519", "SH")
        print(f"贵州茅台: 现价={q.price}, 最高={q.high}, 最低={q.low}")

        # 批量行情
        quotes = client.get_quotes([(1, "600519"), (0, "000001"), (0, "300750")])
        for q in quotes:
            print(f"{q.code}: {q.price}")

        # K线
        bars = client.get_bars("600519", "SH", period="1d", count=10)
        for b in bars:
            print(f"{b.datetime.date()} 开:{b.open} 收:{b.close} 量:{b.volume}")


def example_kline():
    """K线数据获取"""
    with TdxClient() as client:
        # 日线
        daily = client.get_bars("000001", "SZ", period="1d", count=100)
        # 5分钟线
        m5 = client.get_bars("000001", "SZ", period="5m", count=100)
        # 周线
        weekly = client.get_bars("000001", "SZ", period="1w", count=52)
        print(f"日线 {len(daily)} 根, 5分钟线 {len(m5)} 根, 周线 {len(weekly)} 根")


def example_minute():
    """分时数据"""
    with TdxClient() as client:
        # 当日分时
        minute = client.get_minute_time("000001", "SZ")
        print(f"分时数据 {len(minute)} 条")

        # 历史分时
        hist = client.get_history_minute_time("000001", "SZ", date=20250101)
        print(f"历史分时 {len(hist)} 条")


def example_transactions():
    """分笔成交"""
    with TdxClient() as client:
        ticks = client.get_transactions("000001", "SZ", start=0, count=50)
        for t in ticks:
            print(
                f"{t.time} {t.price} {t.volume}手 {'买' if t.direction == 1 else '卖'}"
            )


def example_list():
    """股票列表"""
    with TdxClient() as client:
        count = client.get_stock_count("SH")
        print(f"上海股票总数: {count}")

        # 获取前20只
        stocks = client.get_security_list("SH", start=0, count=20)
        for s in stocks:
            print(f"{s['code']} {s['name']}")


def example_finance():
    """财务数据"""
    with TdxClient() as client:
        # 除权除息
        xdxr = client.get_xdxr_info("600519", "SH")
        print(f"除权除息记录: {len(xdxr)} 条")

        # 财务数据
        fin = client.get_finance_info("000001", "SZ")
        print(f"总资产: {fin.get('total_assets', 0)}")


def example_reconnect():
    """自动重连"""
    client = TdxClient(auto_reconnect=True, max_retries=3)
    try:
        client.connect()
        q = client.get_quote("600519", "SH")
        print(f"行情: {q.price}")
    finally:
        client.close()


if __name__ == "__main__":
    example_basic()
