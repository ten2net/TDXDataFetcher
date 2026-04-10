"""
数据导出模块测试
"""

import os
import tempfile
from datetime import datetime
from pathlib import Path

import pytest

from tdxapi.cache import TdxCache
from tdxapi.export import to_csv, to_excel, to_dataframe, to_parquet, read_parquet
from tdxapi.models import Bar, Tick


class TestExportCSV:
    """CSV导出功能测试"""

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

    def test_to_csv_bars(self, sample_bars, tmp_path):
        """测试K线数据导出为CSV"""
        filepath = tmp_path / "test_bars.csv"
        result = to_csv(sample_bars, filepath)

        assert result.exists()
        assert result.suffix == ".csv"

        # 读取CSV验证内容
        import pandas as pd
        df = pd.read_csv(filepath, encoding="utf-8-sig")
        assert len(df) == 2
        assert "code" in df.columns
        assert "market" in df.columns
        assert "open" in df.columns
        assert "close" in df.columns
        assert str(df.iloc[0]["code"]) == "600519"
        assert df.iloc[0]["open"] == 100.0

    def test_to_csv_ticks(self, sample_ticks, tmp_path):
        """测试分笔数据导出为CSV"""
        filepath = tmp_path / "test_ticks.csv"
        result = to_csv(sample_ticks, filepath)

        assert result.exists()

        # 读取CSV验证内容
        import pandas as pd
        df = pd.read_csv(filepath, encoding="utf-8-sig")
        assert len(df) == 2
        assert "code" in df.columns
        assert "time" in df.columns
        assert "direction" in df.columns
        assert df.iloc[0]["price"] == 100.5

    def test_to_csv_empty_list(self):
        """测试导出空列表抛出异常"""
        with pytest.raises(ValueError, match="数据不能为空列表"):
            to_csv([], "test.csv")

    def test_to_csv_creates_parent_dir(self, sample_bars, tmp_path):
        """测试自动创建父目录"""
        filepath = tmp_path / "subdir" / "test.csv"
        result = to_csv(sample_bars, filepath)
        assert result.exists()

    def test_to_csv_custom_encoding(self, sample_bars, tmp_path):
        """测试自定义编码"""
        filepath = tmp_path / "test_utf8.csv"
        result = to_csv(sample_bars, filepath, encoding="utf-8")
        assert result.exists()


class TestExportExcel:
    """Excel导出功能测试"""

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
        ]

    def test_to_excel_bars(self, sample_bars, tmp_path):
        """测试K线数据导出为Excel"""
        filepath = tmp_path / "test_bars.xlsx"
        result = to_excel(sample_bars, filepath)

        assert result.exists()
        assert result.suffix == ".xlsx"

        # 读取Excel验证内容
        import pandas as pd
        df = pd.read_excel(filepath)
        assert len(df) == 1
        assert "code" in df.columns
        assert str(df.iloc[0]["code"]) == "600519"

    def test_to_excel_ticks(self, sample_ticks, tmp_path):
        """测试分笔数据导出为Excel"""
        filepath = tmp_path / "test_ticks.xlsx"
        result = to_excel(sample_ticks, filepath, sheet_name="分笔数据")

        assert result.exists()

        # 读取Excel验证内容
        import pandas as pd
        df = pd.read_excel(filepath, sheet_name="分笔数据")
        assert len(df) == 1
        assert df.iloc[0]["price"] == 100.5

    def test_to_excel_empty_list(self):
        """测试导出空列表抛出异常"""
        with pytest.raises(ValueError, match="数据不能为空列表"):
            to_excel([], "test.xlsx")

    def test_to_excel_creates_parent_dir(self, sample_bars, tmp_path):
        """测试自动创建父目录"""
        filepath = tmp_path / "subdir" / "test.xlsx"
        result = to_excel(sample_bars, filepath)
        assert result.exists()


class TestExportCacheIntegration:
    """从TdxCache导出数据的集成测试"""

    @pytest.fixture
    def temp_db(self):
        """创建临时数据库文件"""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name
        yield db_path
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
                datetime=datetime(2025, 1, 16, 10, 30, 0),
                open=105.0,
                high=108.0,
                low=104.0,
                close=107.0,
                volume=1500000,
                amount=160500000.0,
            ),
        ]

    def test_export_cache_bars_to_csv(self, cache, sample_bars, tmp_path):
        """测试从缓存导出K线数据到CSV"""
        cache.save_bars(sample_bars)

        bars = cache.get_bars("600519", "SH")
        filepath = tmp_path / "exported_bars.csv"
        result = to_csv(bars, filepath)

        assert result.exists()

        import pandas as pd
        df = pd.read_csv(filepath, encoding="utf-8-sig")
        assert len(df) == 2

    def test_export_cache_bars_to_excel(self, cache, sample_bars, tmp_path):
        """测试从缓存导出K线数据到Excel"""
        cache.save_bars(sample_bars)

        bars = cache.get_bars("600519", "SH")
        filepath = tmp_path / "exported_bars.xlsx"
        result = to_excel(bars, filepath)

        assert result.exists()

        import pandas as pd
        df = pd.read_excel(filepath)
        assert len(df) == 2

    def test_export_cache_with_date_filter(self, cache, sample_bars, tmp_path):
        """测试带日期过滤的导出"""
        cache.save_bars(sample_bars)

        start_date = datetime(2025, 1, 16, 0, 0, 0)
        bars = cache.get_bars("600519", "SH", start_date=start_date)

        filepath = tmp_path / "filtered_bars.csv"
        result = to_csv(bars, filepath)

        import pandas as pd
        df = pd.read_csv(filepath, encoding="utf-8-sig")
        assert len(df) == 1
        assert df.iloc[0]["close"] == 107.0
