"""
数据模型测试
"""

import pytest
from datetime import datetime
from tdxapi.models import StockQuote, Bar, Tick


class TestStockQuote:
    def test_create(self):
        q = StockQuote(
            code="600519",
            market="SH",
            name="贵州茅台",
            price=1800.0,
            last_close=1790.0,
            open=1785.0,
            high=1810.0,
            low=1780.0,
            volume=5000000,
            amount=85000.0,
            bid1=1800.0,
            bid1_vol=100,
            ask1=1801.0,
            ask1_vol=50,
            datetime=datetime.now(),
        )
        assert q.code == "600519"
        assert q.price == 1800.0
        assert q.volume == 5000000

    def test_repr(self):
        q = StockQuote(
            code="600519",
            market="SH",
            name="",
            price=1800.0,
            last_close=0,
            open=0,
            high=0,
            low=0,
            volume=5000000,
            amount=0,
            bid1=0,
            bid1_vol=0,
            ask1=0,
            ask1_vol=0,
            datetime=datetime.now(),
        )
        assert "600519" in repr(q)

    def test_five_level_bids(self):
        q = StockQuote(
            code="600519",
            market="SH",
            name="",
            price=1800.0,
            last_close=0,
            open=0,
            high=0,
            low=0,
            volume=0,
            amount=0,
            bid1=1799.0,
            bid1_vol=100,
            ask1=1800.0,
            ask1_vol=100,
            datetime=datetime.now(),
            bid2=1798.0,
            bid2_vol=200,
            bid3=1797.0,
            bid3_vol=300,
            bid4=1796.0,
            bid4_vol=400,
            bid5=1795.0,
            bid5_vol=500,
            ask2=1801.0,
            ask2_vol=150,
            ask3=1802.0,
            ask3_vol=250,
            ask4=1803.0,
            ask4_vol=350,
            ask5=1804.0,
            ask5_vol=450,
        )
        assert q.bid5 == 1795.0
        assert q.ask5_vol == 450


class TestBar:
    def test_create(self):
        bar = Bar(
            code="600519",
            market="SH",
            datetime=datetime(2025, 1, 1),
            open=100.0,
            high=105.0,
            low=99.0,
            close=103.0,
            volume=1000000,
            amount=100000000.0,
        )
        assert bar.close == 103.0
        assert bar.volume == 1000000

    def test_repr(self):
        bar = Bar(
            code="600519",
            market="SH",
            datetime=datetime(2025, 1, 1),
            open=100.0,
            high=105.0,
            low=99.0,
            close=103.0,
            volume=1000000,
            amount=100000000.0,
        )
        assert "600519" in repr(bar)


class TestTick:
    def test_create(self):
        tick = Tick(
            code="600519",
            market="SH",
            time="09:30:15",
            price=100.5,
            volume=100,
            amount=10050.0,
            direction=1,
        )
        assert tick.price == 100.5
        assert tick.direction == 1
        assert tick.time == "09:30:15"
