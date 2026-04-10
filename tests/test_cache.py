"""
TdxCache缓存模块测试
"""

import os
import tempfile
from datetime import datetime
from pathlib import Path

import pytest

from tdxapi.cache import TdxCache
from tdxapi.models import Bar, Tick


class TestTdxCache:
    """TdxCache类测试"""

    @pytest.fixture
    def temp_db(self):
        """创建临时数据库文件"""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name
        yield db_path
        # 清理
        if os.path.exists(db_path):
            os.unlink(db_path)

    @pytest.fixture
    def cache(self, temp_db):
        """创建缓存实例"""
        return TdxCache(temp_db)

    @pytest.fixture
    def sample_bars(self):
        """创建示例K线数据"""
        return [
            Bar(
                code="600519",
                market="SH",
                datetime=datetime(2025, 1, 15, 10, 30, 0),
                open=100.0,
                high=105.0,
                low=99.0,
                close=103.0,
                volume=1000000,
                amount=100000000.0,
            ),
            Bar(
                code="600519",
                market="SH",
                datetime=datetime(2025, 1, 15, 11, 30, 0),
                open=103.0,
                high=106.0,
                low=102.0,
                close=105.0,
                volume=1200000,
                amount=126000000.0,
            ),
            Bar(
                code="600519",
                market="SH",
                datetime=datetime(2025, 1, 16, 10, 30, 0),
                open=105.0,
                high=108.0,
                low=104.0,
                close=107.0,
                volume=1500000,
                amount=160500000.0,
            ),
            Bar(
                code="000001",
                market="SZ",
                datetime=datetime(2025, 1, 15, 10, 30, 0),
                open=10.0,
                high=11.0,
                low=9.5,
                close=10.5,
                volume=5000000,
                amount=52500000.0,
            ),
        ]

    @pytest.fixture
    def sample_ticks(self):
        """创建示例分笔数据"""
        return [
            Tick(
                code="600519",
                market="SH",
                time="09:30:15",
                price=100.5,
                volume=100,
                amount=10050.0,
                direction=1,
            ),
            Tick(
                code="600519",
                market="SH",
                time="09:30:20",
                price=100.6,
                volume=200,
                amount=20120.0,
                direction=0,
            ),
            Tick(
                code="600519",
                market="SH",
                time="09:31:00",
                price=100.8,
                volume=150,
                amount=15120.0,
                direction=1,
            ),
            Tick(
                code="000001",
                market="SZ",
                time="09:30:15",
                price=10.5,
                volume=1000,
                amount=10500.0,
                direction=0,
            ),
        ]

    def test_init_creates_db(self, temp_db):
        """测试初始化创建数据库文件"""
        cache = TdxCache(temp_db)
        assert Path(temp_db).exists()

    def test_init_default_path(self):
        """测试默认数据库路径"""
        cache = TdxCache()
        assert cache.db_path == Path("tdx_cache.db")

    def test_save_bars(self, cache, sample_bars):
        """测试保存K线数据"""
        saved = cache.save_bars(sample_bars)
        assert saved == 4

        # 验证重复保存不会重复插入
        saved = cache.save_bars(sample_bars)
        assert saved == 0

    def test_save_bars_empty_list(self, cache):
        """测试保存空列表抛出异常"""
        with pytest.raises(ValueError, match="bars列表不能为空"):
            cache.save_bars([])

    def test_get_bars_all(self, cache, sample_bars):
        """测试查询所有K线数据"""
        cache.save_bars(sample_bars)

        bars = cache.get_bars("600519", "SH")
        assert len(bars) == 3
        assert bars[0].code == "600519"
        assert bars[0].market == "SH"
        assert bars[0].close == 103.0

    def test_get_bars_date_range(self, cache, sample_bars):
        """测试按日期范围查询K线数据"""
        cache.save_bars(sample_bars)

        # 查询特定日期范围
        start = datetime(2025, 1, 15, 0, 0, 0)
        end = datetime(2025, 1, 15, 23, 59, 59)
        bars = cache.get_bars("600519", "SH", start_date=start, end_date=end)

        assert len(bars) == 2
        assert bars[0].datetime.day == 15
        assert bars[1].datetime.day == 15

    def test_get_bars_start_date_only(self, cache, sample_bars):
        """测试只指定开始日期"""
        cache.save_bars(sample_bars)

        start = datetime(2025, 1, 16, 0, 0, 0)
        bars = cache.get_bars("600519", "SH", start_date=start)

        assert len(bars) == 1
        assert bars[0].datetime.day == 16

    def test_get_bars_end_date_only(self, cache, sample_bars):
        """测试只指定结束日期"""
        cache.save_bars(sample_bars)

        end = datetime(2025, 1, 15, 23, 59, 59)
        bars = cache.get_bars("600519", "SH", end_date=end)

        assert len(bars) == 2

    def test_get_bars_no_result(self, cache, sample_bars):
        """测试查询无结果"""
        cache.save_bars(sample_bars)

        bars = cache.get_bars("999999", "SH")
        assert len(bars) == 0

    def test_get_bars_ordering(self, cache, sample_bars):
        """测试查询结果按时间排序"""
        cache.save_bars(sample_bars)

        bars = cache.get_bars("600519", "SH")
        assert len(bars) == 3
        # 验证按时间升序排列
        assert bars[0].datetime < bars[1].datetime < bars[2].datetime

    def test_save_ticks(self, cache, sample_ticks):
        """测试保存分笔数据"""
        saved = cache.save_ticks(sample_ticks)
        assert saved == 4

        # 验证重复保存不会重复插入
        saved = cache.save_ticks(sample_ticks)
        assert saved == 0

    def test_save_ticks_empty_list(self, cache):
        """测试保存空列表抛出异常"""
        with pytest.raises(ValueError, match="ticks列表不能为空"):
            cache.save_ticks([])

    def test_get_ticks_all(self, cache, sample_ticks):
        """测试查询所有分笔数据"""
        cache.save_ticks(sample_ticks)

        ticks = cache.get_ticks("600519", "SH")
        assert len(ticks) == 3
        assert ticks[0].code == "600519"
        assert ticks[0].market == "SH"

    def test_get_ticks_time_range(self, cache, sample_ticks):
        """测试按时间范围查询分笔数据"""
        cache.save_ticks(sample_ticks)

        ticks = cache.get_ticks(
            "600519", "SH", start_time="09:30:00", end_time="09:30:30"
        )

        assert len(ticks) == 2
        assert ticks[0].time == "09:30:15"
        assert ticks[1].time == "09:30:20"

    def test_get_ticks_start_time_only(self, cache, sample_ticks):
        """测试只指定开始时间"""
        cache.save_ticks(sample_ticks)

        ticks = cache.get_ticks("600519", "SH", start_time="09:31:00")

        assert len(ticks) == 1
        assert ticks[0].time == "09:31:00"

    def test_get_ticks_end_time_only(self, cache, sample_ticks):
        """测试只指定结束时间"""
        cache.save_ticks(sample_ticks)

        ticks = cache.get_ticks("600519", "SH", end_time="09:30:15")

        assert len(ticks) == 1
        assert ticks[0].time == "09:30:15"

    def test_get_ticks_no_result(self, cache, sample_ticks):
        """测试查询无结果"""
        cache.save_ticks(sample_ticks)

        ticks = cache.get_ticks("999999", "SH")
        assert len(ticks) == 0

    def test_get_ticks_ordering(self, cache, sample_ticks):
        """测试查询结果按时间排序"""
        cache.save_ticks(sample_ticks)

        ticks = cache.get_ticks("600519", "SH")
        assert len(ticks) == 3
        # 验证按时间升序排列
        assert ticks[0].time < ticks[1].time < ticks[2].time

    def test_clear_cache_all(self, cache, sample_bars, sample_ticks):
        """测试清除所有缓存"""
        cache.save_bars(sample_bars)
        cache.save_ticks(sample_ticks)

        deleted = cache.clear_cache()
        assert deleted == 8  # 4 bars + 4 ticks

        bars = cache.get_bars("600519", "SH")
        ticks = cache.get_ticks("600519", "SH")
        assert len(bars) == 0
        assert len(ticks) == 0

    def test_clear_cache_by_code(self, cache, sample_bars, sample_ticks):
        """测试按股票代码清除缓存"""
        cache.save_bars(sample_bars)
        cache.save_ticks(sample_ticks)

        deleted = cache.clear_cache(code="600519")
        assert deleted == 6  # 3 bars + 3 ticks for 600519

        bars = cache.get_bars("600519", "SH")
        ticks = cache.get_ticks("600519", "SH")
        assert len(bars) == 0
        assert len(ticks) == 0

        # 验证其他股票数据还在
        bars = cache.get_bars("000001", "SZ")
        ticks = cache.get_ticks("000001", "SZ")
        assert len(bars) == 1
        assert len(ticks) == 1

    def test_clear_cache_by_market(self, cache, sample_bars, sample_ticks):
        """测试按市场清除缓存"""
        cache.save_bars(sample_bars)
        cache.save_ticks(sample_ticks)

        deleted = cache.clear_cache(market="SH")
        assert deleted == 6  # 3 bars + 3 ticks for SH market

        bars = cache.get_bars("600519", "SH")
        assert len(bars) == 0

        # 验证SZ市场数据还在
        bars = cache.get_bars("000001", "SZ")
        assert len(bars) == 1

    def test_clear_cache_by_code_and_market(self, cache, sample_bars, sample_ticks):
        """测试按股票代码和市场清除缓存"""
        cache.save_bars(sample_bars)
        cache.save_ticks(sample_ticks)

        deleted = cache.clear_cache(code="600519", market="SH")
        assert deleted == 6  # 3 bars + 3 ticks

        bars = cache.get_bars("600519", "SH")
        assert len(bars) == 0

        # 验证其他数据还在
        bars = cache.get_bars("000001", "SZ")
        assert len(bars) == 1

    def test_get_cache_info(self, cache, sample_bars, sample_ticks):
        """测试获取缓存统计信息"""
        cache.save_bars(sample_bars)
        cache.save_ticks(sample_ticks)

        info = cache.get_cache_info()

        assert "db_path" in info
        assert info["bars_count"] == 4
        assert info["ticks_count"] == 4
        assert info["bars_stocks"] == 2  # 600519, 000001
        assert info["ticks_stocks"] == 2

    def test_bar_data_integrity(self, cache, sample_bars):
        """测试K线数据完整性"""
        cache.save_bars(sample_bars)

        bars = cache.get_bars("600519", "SH")
        assert len(bars) == 3

        # 验证所有字段正确保存
        bar = bars[0]
        assert bar.code == "600519"
        assert bar.market == "SH"
        assert isinstance(bar.datetime, datetime)
        assert bar.open == 100.0
        assert bar.high == 105.0
        assert bar.low == 99.0
        assert bar.close == 103.0
        assert bar.volume == 1000000
        assert bar.amount == 100000000.0

    def test_tick_data_integrity(self, cache, sample_ticks):
        """测试分笔数据完整性"""
        cache.save_ticks(sample_ticks)

        ticks = cache.get_ticks("600519", "SH")
        assert len(ticks) == 3

        # 验证所有字段正确保存
        tick = ticks[0]
        assert tick.code == "600519"
        assert tick.market == "SH"
        assert tick.time == "09:30:15"
        assert tick.price == 100.5
        assert tick.volume == 100
        assert tick.amount == 10050.0
        assert tick.direction == 1

    def test_mixed_operations(self, cache, sample_bars, sample_ticks):
        """测试混合操作"""
        # 保存数据
        cache.save_bars(sample_bars)
        cache.save_ticks(sample_ticks)

        # 查询
        bars = cache.get_bars("600519", "SH")
        ticks = cache.get_ticks("600519", "SH")
        assert len(bars) == 3
        assert len(ticks) == 3

        # 清除部分数据
        cache.clear_cache(code="600519", market="SH")

        # 再次查询
        bars = cache.get_bars("600519", "SH")
        ticks = cache.get_ticks("600519", "SH")
        assert len(bars) == 0
        assert len(ticks) == 0

        # 其他数据应该还在
        bars = cache.get_bars("000001", "SZ")
        ticks = cache.get_ticks("000001", "SZ")
        assert len(bars) == 1
        assert len(ticks) == 1
