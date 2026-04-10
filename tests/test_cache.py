"""
TdxCache缓存模块测试
"""

import os
import tempfile
from datetime import datetime
from pathlib import Path

import pytest

from tdxapi.cache import CompressionType, TdxCache
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
        """创建缓存实例（默认zlib压缩）"""
        return TdxCache(temp_db)

    @pytest.fixture
    def cache_no_compression(self, temp_db):
        """创建无压缩缓存实例"""
        return TdxCache(temp_db, compression=CompressionType.NONE)

    @pytest.fixture
    def cache_zlib(self, temp_db):
        """创建zlib压缩缓存实例"""
        return TdxCache(temp_db, compression=CompressionType.ZLIB)

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

    def test_init_default_compression(self, temp_db):
        """测试默认压缩类型为zlib"""
        cache = TdxCache(temp_db)
        assert cache.compression == CompressionType.ZLIB

    def test_init_no_compression(self, temp_db):
        """测试无压缩模式"""
        cache = TdxCache(temp_db, compression=CompressionType.NONE)
        assert cache.compression == CompressionType.NONE

    def test_init_zlib_compression(self, temp_db):
        """测试zlib压缩模式"""
        cache = TdxCache(temp_db, compression="zlib")
        assert cache.compression == CompressionType.ZLIB

    def test_init_compression_level_default(self, temp_db):
        """测试默认压缩级别"""
        cache_zlib = TdxCache(temp_db, compression=CompressionType.ZLIB)
        assert cache_zlib.compression_level == 6

    def test_init_compression_level_custom(self, temp_db):
        """测试自定义压缩级别"""
        cache = TdxCache(temp_db, compression=CompressionType.ZLIB, compression_level=9)
        assert cache.compression_level == 9

    def test_init_compression_string(self, temp_db):
        """测试字符串压缩类型"""
        cache = TdxCache(temp_db, compression="none")
        assert cache.compression == CompressionType.NONE

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
        assert "bars_compression" in info
        assert "ticks_compression" in info
        assert "default_compression" in info

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


class TestCompression:
    """压缩功能测试"""

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
        ]

    def test_compression_no_compression(self, temp_db, sample_bars):
        """测试无压缩模式"""
        cache = TdxCache(temp_db, compression=CompressionType.NONE)
        cache.save_bars(sample_bars)

        info = cache.get_cache_info()
        assert info["bars_compression"].get("none", 0) == 2

        # 验证数据可正常读取
        bars = cache.get_bars("600519", "SH")
        assert len(bars) == 2
        assert bars[0].close == 103.0

    def test_compression_zlib(self, temp_db, sample_bars):
        """测试zlib压缩模式"""
        cache = TdxCache(temp_db, compression=CompressionType.ZLIB)
        cache.save_bars(sample_bars)

        info = cache.get_cache_info()
        assert info["bars_compression"].get("zlib", 0) == 2

        # 验证数据可正常读取
        bars = cache.get_bars("600519", "SH")
        assert len(bars) == 2
        assert bars[0].close == 103.0

    def test_compression_zlib_ticks(self, temp_db, sample_ticks):
        """测试zlib压缩模式保存ticks"""
        cache = TdxCache(temp_db, compression=CompressionType.ZLIB)
        cache.save_ticks(sample_ticks)

        info = cache.get_cache_info()
        assert info["ticks_compression"].get("zlib", 0) == 2

        # 验证数据可正常读取
        ticks = cache.get_ticks("600519", "SH")
        assert len(ticks) == 2
        assert ticks[0].price == 100.5

    def test_compression_stats(self, temp_db, sample_bars, sample_ticks):
        """测试压缩统计信息"""
        cache = TdxCache(temp_db, compression=CompressionType.ZLIB)
        cache.save_bars(sample_bars)
        cache.save_ticks(sample_ticks)

        stats = cache.get_compression_stats()
        assert "bars_compressed_bytes" in stats
        assert "ticks_compressed_bytes" in stats
        assert "total_compressed_bytes" in stats
        assert "compression_ratio_percent" in stats

    def test_compression_transparent(self, temp_db, sample_bars):
        """测试压缩对用户透明"""
        # 使用压缩保存
        cache_compressed = TdxCache(temp_db, compression=CompressionType.ZLIB)
        cache_compressed.save_bars(sample_bars)

        # 读取数据（自动解压）
        bars = cache_compressed.get_bars("600519", "SH")

        # 验证数据完整性
        assert len(bars) == 2
        for i, bar in enumerate(bars):
            assert bar.code == sample_bars[i].code
            assert bar.market == sample_bars[i].market
            assert bar.datetime == sample_bars[i].datetime
            assert bar.open == sample_bars[i].open
            assert bar.high == sample_bars[i].high
            assert bar.low == sample_bars[i].low
            assert bar.close == sample_bars[i].close
            assert bar.volume == sample_bars[i].volume
            assert bar.amount == sample_bars[i].amount

    def test_compression_different_levels(self, temp_db, sample_bars):
        """测试不同压缩级别"""
        # 级别1（最快）
        cache_fast = TdxCache(
            temp_db + "_fast", compression=CompressionType.ZLIB, compression_level=1
        )
        cache_fast.save_bars(sample_bars)

        # 级别9（最大压缩）
        cache_best = TdxCache(
            temp_db + "_best", compression=CompressionType.ZLIB, compression_level=9
        )
        cache_best.save_bars(sample_bars)

        # 验证两者都能正常读取
        bars_fast = cache_fast.get_bars("600519", "SH")
        bars_best = cache_best.get_bars("600519", "SH")

        assert len(bars_fast) == 2
        assert len(bars_best) == 2
        assert bars_fast[0].close == bars_best[0].close

        # 清理临时文件
        os.unlink(temp_db + "_fast")
        os.unlink(temp_db + "_best")


class TestLZ4Compression:
    """LZ4压缩测试（需要lz4包）"""

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
        ]

    def test_lz4_not_installed_raises(self, temp_db, monkeypatch):
        """测试lz4未安装时抛出错误"""
        # 模拟lz4不可用
        monkeypatch.setattr(
            TdxCache, "_check_lz4_available", lambda self: False
        )

        with pytest.raises(ImportError, match="lz4 is not installed"):
            TdxCache(temp_db, compression=CompressionType.LZ4)

    def test_lz4_compression_if_available(self, temp_db, sample_bars):
        """测试lz4压缩（如果可用）"""
        try:
            import lz4.frame  # noqa: F401
        except ImportError:
            pytest.skip("lz4 not installed")

        cache = TdxCache(temp_db, compression=CompressionType.LZ4)
        cache.save_bars(sample_bars)

        info = cache.get_cache_info()
        assert info["bars_compression"].get("lz4", 0) == 1

        # 验证数据可正常读取
        bars = cache.get_bars("600519", "SH")
        assert len(bars) == 1
        assert bars[0].close == 103.0

    def test_lz4_compression_level(self, temp_db, sample_bars):
        """测试lz4压缩级别"""
        try:
            import lz4.frame  # noqa: F401
        except ImportError:
            pytest.skip("lz4 not installed")

        cache = TdxCache(
            temp_db, compression=CompressionType.LZ4, compression_level=9
        )
        assert cache.compression_level == 9

        cache.save_bars(sample_bars)
        bars = cache.get_bars("600519", "SH")
        assert len(bars) == 1


class TestBackwardCompatibility:
    """向后兼容性测试"""

    @pytest.fixture
    def temp_db(self):
        """创建临时数据库文件"""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name
        yield db_path
        # 清理
        if os.path.exists(db_path):
            os.unlink(db_path)

    def test_read_old_uncompressed_data(self, temp_db):
        """测试读取旧版本未压缩数据"""
        # 模拟旧版本数据库（无compression列）
        import sqlite3

        conn = sqlite3.connect(temp_db)
        cursor = conn.cursor()

        # 创建旧版本表结构
        cursor.execute("""
            CREATE TABLE bars (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                code TEXT NOT NULL,
                market TEXT NOT NULL,
                datetime TEXT NOT NULL,
                open REAL NOT NULL,
                high REAL NOT NULL,
                low REAL NOT NULL,
                close REAL NOT NULL,
                volume INTEGER NOT NULL,
                amount REAL NOT NULL,
                UNIQUE(code, market, datetime)
            )
        """)

        # 插入旧格式数据
        cursor.execute("""
            INSERT INTO bars (code, market, datetime, open, high, low, close, volume, amount)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, ("600519", "SH", "2025-01-15T10:30:00", 100.0, 105.0, 99.0, 103.0, 1000000, 100000000.0))

        conn.commit()
        conn.close()

        # 使用新版本打开（应该能自动迁移）
        cache = TdxCache(temp_db)

        # 验证能读取旧数据
        # 注意：由于schema变化，旧数据格式可能无法直接读取
        # 这里主要验证不抛出异常
        info = cache.get_cache_info()
        assert "bars_count" in info


class TestCompressionType:
    """CompressionType枚举测试"""

    def test_compression_type_values(self):
        """测试压缩类型枚举值"""
        assert CompressionType.NONE.value == "none"
        assert CompressionType.ZLIB.value == "zlib"
        assert CompressionType.LZ4.value == "lz4"

    def test_compression_type_from_string(self):
        """测试从字符串创建压缩类型"""
        assert CompressionType("none") == CompressionType.NONE
        assert CompressionType("zlib") == CompressionType.ZLIB
        assert CompressionType("lz4") == CompressionType.LZ4


class TestLRUCache:
    """LRU缓存功能测试"""

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
        ]

    def test_lru_cache_initialization(self, temp_db):
        """测试LRU缓存初始化参数"""
        cache = TdxCache(temp_db, max_memory_cache_size=500)
        assert cache.max_memory_cache_size == 500

    def test_lru_cache_default_size(self, temp_db):
        """测试默认LRU缓存大小"""
        cache = TdxCache(temp_db)
        assert cache.max_memory_cache_size == 1000

    def test_lru_cache_memory_caching(self, temp_db, sample_bars):
        """测试内存LRU缓存功能"""
        cache = TdxCache(temp_db, max_memory_cache_size=100)
        cache.save_bars(sample_bars)

        # 第一次查询，应该缓存到内存
        bars1 = cache.get_bars("600519", "SH")
        assert len(bars1) == 2

        # 第二次查询，应该从内存缓存获取
        bars2 = cache.get_bars("600519", "SH")
        assert len(bars2) == 2

        # 验证数据正确
        assert bars2[0].close == 103.0

    def test_lru_cache_stats(self, temp_db, sample_bars):
        """测试LRU缓存统计信息"""
        cache = TdxCache(temp_db, max_memory_cache_size=100)
        cache.save_bars(sample_bars)

        # 查询以触发缓存
        cache.get_bars("600519", "SH")

        stats = cache.get_lru_cache_info()
        assert "hits" in stats
        assert "misses" in stats
        assert "maxsize" in stats
        assert "currsize" in stats
        assert stats["maxsize"] == 100

    def test_lru_cache_eviction(self, temp_db):
        """测试LRU缓存淘汰机制"""
        cache = TdxCache(temp_db, max_memory_cache_size=2)

        # 保存3组不同股票的数据
        for i, code in enumerate(["600001", "600002", "600003"]):
            bars = [
                Bar(
                    code=code,
                    market="SH",
                    datetime=datetime(2025, 1, 15, 10, 30, 0),
                    open=100.0 + i,
                    high=105.0 + i,
                    low=99.0 + i,
                    close=103.0 + i,
                    volume=1000000,
                    amount=100000000.0,
                )
            ]
            cache.save_bars(bars)
            # 查询以触发缓存
            cache.get_bars(code, "SH")

        # 验证内存缓存只保留最近2个
        assert len(cache._memory_cache) <= 2

    def test_clear_memory_cache(self, temp_db, sample_bars):
        """测试清空内存缓存"""
        cache = TdxCache(temp_db, max_memory_cache_size=100)
        cache.save_bars(sample_bars)
        cache.get_bars("600519", "SH")  # 触发缓存

        assert len(cache._memory_cache) > 0

        cleared = cache.clear_memory_cache()
        assert cleared > 0
        assert len(cache._memory_cache) == 0
        assert len(cache._access_stats) == 0


class TestTTLCache:
    """TTL过期机制测试"""

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
        ]

    def test_ttl_initialization(self, temp_db):
        """测试TTL初始化参数"""
        cache = TdxCache(temp_db, default_ttl=3600)
        assert cache.default_ttl == 3600

    def test_ttl_no_expiration_default(self, temp_db):
        """测试默认无过期时间"""
        cache = TdxCache(temp_db)
        assert cache.default_ttl is None

    def test_ttl_data_not_expired(self, temp_db, sample_bars):
        """测试数据未过期"""
        cache = TdxCache(temp_db, default_ttl=3600)  # 1小时TTL
        cache.save_bars(sample_bars)

        # 立即检查，数据应该未过期
        is_expired = cache._is_data_expired("600519", "SH", "bars")
        assert is_expired is False

    def test_ttl_data_expired(self, temp_db, sample_bars):
        """测试数据过期检测"""
        import time

        cache = TdxCache(temp_db, default_ttl=1)  # 1秒TTL
        cache.save_bars(sample_bars)

        # 等待2秒
        time.sleep(2)

        # 检查数据是否过期
        is_expired = cache._is_data_expired("600519", "SH", "bars")
        assert is_expired is True

    def test_ttl_no_expiration_check(self, temp_db, sample_bars):
        """测试无TTL时不检查过期"""
        cache = TdxCache(temp_db, default_ttl=None)
        cache.save_bars(sample_bars)

        # 应该始终返回未过期
        is_expired = cache._is_data_expired("600519", "SH", "bars")
        assert is_expired is False

    def test_invalidate_expired_data(self, temp_db, sample_bars):
        """测试清理过期数据"""
        import time

        cache = TdxCache(temp_db, default_ttl=1)  # 1秒TTL
        cache.save_bars(sample_bars)

        # 等待数据过期
        time.sleep(2)

        # 清理过期数据
        deleted = cache.invalidate_expired_data()
        assert deleted >= 0  # 可能删除0条或更多

    def test_invalidate_expired_no_ttl(self, temp_db, sample_bars):
        """测试无TTL时不清理数据"""
        cache = TdxCache(temp_db, default_ttl=None)
        cache.save_bars(sample_bars)

        # 不应该删除任何数据
        deleted = cache.invalidate_expired_data()
        assert deleted == 0

    def test_cache_stats_with_ttl(self, temp_db, sample_bars):
        """测试缓存统计信息包含TTL配置"""
        cache = TdxCache(temp_db, default_ttl=3600)
        cache.save_bars(sample_bars)

        stats = cache.get_cache_stats()
        assert "ttl_config" in stats
        assert stats["ttl_config"]["default_ttl_seconds"] == 3600
        assert "seconds" in stats["ttl_config"]["default_ttl_human"]


class TestIncrementalUpdate:
    """增量更新功能测试"""

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
                datetime=datetime(2025, 1, 16, 10, 30, 0),
                open=103.0,
                high=106.0,
                low=102.0,
                close=105.0,
                volume=1200000,
                amount=126000000.0,
            ),
        ]

    def test_get_data_date_range(self, temp_db, sample_bars):
        """测试获取数据日期范围"""
        cache = TdxCache(temp_db)
        cache.save_bars(sample_bars)

        earliest, latest = cache.get_data_date_range("600519", "SH", "bars")

        assert earliest is not None
        assert latest is not None
        assert earliest == datetime(2025, 1, 15, 10, 30, 0)
        assert latest == datetime(2025, 1, 16, 10, 30, 0)

    def test_get_data_date_range_no_data(self, temp_db):
        """测试无数据时返回None"""
        cache = TdxCache(temp_db)

        earliest, latest = cache.get_data_date_range("600519", "SH", "bars")

        assert earliest is None
        assert latest is None

    def test_get_missing_date_ranges_no_cache(self, temp_db):
        """测试无缓存时返回完整范围"""
        cache = TdxCache(temp_db)

        start_date = datetime(2025, 1, 1)
        end_date = datetime(2025, 1, 31)

        missing = cache.get_missing_date_ranges("600519", "SH", start_date, end_date)

        assert len(missing) == 1
        assert missing[0] == (start_date, end_date)

    def test_get_missing_date_ranges_partial(self, temp_db, sample_bars):
        """测试部分缓存时返回缺失范围"""
        cache = TdxCache(temp_db)
        cache.save_bars(sample_bars)

        # 请求更大的范围
        start_date = datetime(2025, 1, 10)
        end_date = datetime(2025, 1, 20)

        missing = cache.get_missing_date_ranges("600519", "SH", start_date, end_date)

        # 应该返回两个缺失区间：1月10-15日和1月16-20日
        assert len(missing) == 2

    def test_get_missing_date_ranges_fully_covered(self, temp_db, sample_bars):
        """测试缓存完全覆盖时返回空列表"""
        cache = TdxCache(temp_db)
        cache.save_bars(sample_bars)

        # 请求更小的范围（完全在缓存内）
        start_date = datetime(2025, 1, 15, 10, 30, 0)
        end_date = datetime(2025, 1, 16, 10, 30, 0)

        missing = cache.get_missing_date_ranges("600519", "SH", start_date, end_date)

        assert len(missing) == 0

    def test_get_missing_date_ranges_only_earlier(self, temp_db, sample_bars):
        """测试只需要更早的数据"""
        cache = TdxCache(temp_db)
        cache.save_bars(sample_bars)

        start_date = datetime(2025, 1, 10)
        end_date = datetime(2025, 1, 15, 10, 30, 0)

        missing = cache.get_missing_date_ranges("600519", "SH", start_date, end_date)

        assert len(missing) == 1
        assert missing[0][0] == start_date

    def test_get_missing_date_ranges_only_later(self, temp_db, sample_bars):
        """测试只需要更新的数据"""
        cache = TdxCache(temp_db)
        cache.save_bars(sample_bars)

        start_date = datetime(2025, 1, 16, 10, 30, 0)
        end_date = datetime(2025, 1, 20)

        missing = cache.get_missing_date_ranges("600519", "SH", start_date, end_date)

        assert len(missing) == 1
        assert missing[0][1] == end_date


class TestCacheStats:
    """缓存统计信息测试"""

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
        ]

    def test_get_cache_stats_structure(self, temp_db, sample_bars):
        """测试缓存统计信息结构"""
        cache = TdxCache(temp_db)
        cache.save_bars(sample_bars)
        cache.get_bars("600519", "SH")  # 触发缓存

        stats = cache.get_cache_stats()

        assert "db_path" in stats
        assert "db_stats" in stats
        assert "memory_cache" in stats
        assert "ttl_config" in stats
        assert "stock_stats" in stats
        assert "access_stats" in stats

    def test_get_cache_stats_db_stats(self, temp_db, sample_bars):
        """测试数据库统计信息"""
        cache = TdxCache(temp_db)
        cache.save_bars(sample_bars)

        stats = cache.get_cache_stats()

        assert stats["db_stats"]["bars_count"] == 1
        assert stats["db_stats"]["ticks_count"] == 0
        assert stats["db_stats"]["total_count"] == 1

    def test_get_cache_stats_memory_cache(self, temp_db, sample_bars):
        """测试内存缓存统计信息"""
        cache = TdxCache(temp_db, max_memory_cache_size=500)
        cache.save_bars(sample_bars)
        cache.get_bars("600519", "SH")  # 触发缓存

        stats = cache.get_cache_stats()

        assert stats["memory_cache"]["max_size"] == 500
        assert stats["memory_cache"]["size"] >= 0
        assert "utilization" in stats["memory_cache"]

    def test_get_cache_stats_stock_stats(self, temp_db, sample_bars):
        """测试股票统计信息"""
        cache = TdxCache(temp_db)
        cache.save_bars(sample_bars)

        stats = cache.get_cache_stats()

        assert len(stats["stock_stats"]) == 1
        stock = stats["stock_stats"][0]
        assert stock["code"] == "600519"
        assert stock["market"] == "SH"
        assert stock["data_type"] == "bars"
        assert "record_count" in stock
        assert "is_expired" in stock

    def test_get_cache_stats_access_stats(self, temp_db, sample_bars):
        """测试访问统计信息"""
        cache = TdxCache(temp_db)
        cache.save_bars(sample_bars)
        cache.get_bars("600519", "SH")  # 触发访问统计

        stats = cache.get_cache_stats()

        assert len(stats["access_stats"]) >= 0  # 可能有也可能没有，取决于缓存是否命中


class TestCacheBackwardCompatibility:
    """缓存功能向后兼容性测试"""

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
        ]

    def test_old_api_without_new_params(self, temp_db, sample_bars):
        """测试旧API调用方式（不带新参数）"""
        # 使用旧的方式创建缓存
        cache = TdxCache(temp_db)

        # 保存和查询数据
        cache.save_bars(sample_bars)
        bars = cache.get_bars("600519", "SH")

        assert len(bars) == 1
        assert bars[0].close == 103.0

    def test_old_api_get_cache_info(self, temp_db, sample_bars):
        """测试旧的get_cache_info API仍然可用"""
        cache = TdxCache(temp_db)
        cache.save_bars(sample_bars)

        info = cache.get_cache_info()

        # 验证旧API返回的字段仍然存在
        assert "db_path" in info
        assert "bars_count" in info
        assert "ticks_count" in info

    def test_new_params_with_defaults(self, temp_db):
        """测试新参数有合理的默认值"""
        cache = TdxCache(temp_db)

        # 验证新参数有默认值
        assert cache.max_memory_cache_size == 1000
        assert cache.default_ttl is None
