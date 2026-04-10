"""
技术指标计算模块的单元测试
"""

import unittest
from datetime import datetime
from tdxapi.models import Bar
from tdxapi.indicators import vol, obv, vol_ma


class TestVol(unittest.TestCase):
    """测试成交量指标 (vol)"""

    def setUp(self):
        """设置测试数据"""
        self.bars = [
            Bar(code="000001", market="SZ", datetime=datetime(2024, 1, 1), open=10.0, high=11.0, low=9.0, close=10.5, volume=1000, amount=10500.0),
            Bar(code="000001", market="SZ", datetime=datetime(2024, 1, 2), open=10.5, high=11.5, low=10.0, close=11.0, volume=2000, amount=22000.0),
            Bar(code="000001", market="SZ", datetime=datetime(2024, 1, 3), open=11.0, high=11.5, low=10.5, close=10.8, volume=1500, amount=16200.0),
        ]

    def test_vol_basic(self):
        """测试基本的成交量提取"""
        result = vol(self.bars)
        self.assertEqual(len(result), 3)
        self.assertEqual(result, [1000, 2000, 1500])

    def test_vol_empty(self):
        """测试空列表"""
        result = vol([])
        self.assertEqual(result, [])

    def test_vol_single_bar(self):
        """测试单根K线"""
        single_bar = [self.bars[0]]
        result = vol(single_bar)
        self.assertEqual(result, [1000])


class TestOBV(unittest.TestCase):
    """测试能量潮指标 (OBV)"""

    def setUp(self):
        """设置测试数据"""
        # 上涨情况
        self.rising_bars = [
            Bar(code="000001", market="SZ", datetime=datetime(2024, 1, 1), open=10.0, high=11.0, low=9.0, close=10.0, volume=1000, amount=10000.0),
            Bar(code="000001", market="SZ", datetime=datetime(2024, 1, 2), open=10.0, high=11.0, low=9.0, close=11.0, volume=2000, amount=22000.0),  # 上涨
            Bar(code="000001", market="SZ", datetime=datetime(2024, 1, 3), open=11.0, high=12.0, low=10.5, close=12.0, volume=1500, amount=18000.0),  # 上涨
        ]

        # 下跌情况
        self.falling_bars = [
            Bar(code="000001", market="SZ", datetime=datetime(2024, 1, 1), open=12.0, high=12.0, low=11.0, close=12.0, volume=1000, amount=12000.0),
            Bar(code="000001", market="SZ", datetime=datetime(2024, 1, 2), open=12.0, high=12.0, low=11.0, close=11.0, volume=2000, amount=22000.0),  # 下跌
            Bar(code="000001", market="SZ", datetime=datetime(2024, 1, 3), open=11.0, high=11.0, low=10.0, close=10.0, volume=1500, amount=15000.0),  # 下跌
        ]

        # 平盘情况
        self.flat_bars = [
            Bar(code="000001", market="SZ", datetime=datetime(2024, 1, 1), open=10.0, high=11.0, low=9.0, close=10.0, volume=1000, amount=10000.0),
            Bar(code="000001", market="SZ", datetime=datetime(2024, 1, 2), open=10.0, high=11.0, low=9.0, close=10.0, volume=2000, amount=20000.0),  # 平盘
            Bar(code="000001", market="SZ", datetime=datetime(2024, 1, 3), open=10.0, high=11.0, low=9.0, close=10.0, volume=1500, amount=15000.0),  # 平盘
        ]

        # 混合情况：上涨、下跌、平盘
        self.mixed_bars = [
            Bar(code="000001", market="SZ", datetime=datetime(2024, 1, 1), open=10.0, high=11.0, low=9.0, close=10.0, volume=1000, amount=10000.0),
            Bar(code="000001", market="SZ", datetime=datetime(2024, 1, 2), open=10.0, high=11.0, low=9.0, close=11.0, volume=2000, amount=22000.0),  # 上涨: +2000
            Bar(code="000001", market="SZ", datetime=datetime(2024, 1, 3), open=11.0, high=11.0, low=10.0, close=10.5, volume=1500, amount=15750.0),  # 下跌: -1500
            Bar(code="000001", market="SZ", datetime=datetime(2024, 1, 4), open=10.5, high=11.0, low=10.5, close=10.5, volume=800, amount=8400.0),   # 平盘: 0
            Bar(code="000001", market="SZ", datetime=datetime(2024, 1, 5), open=10.5, high=11.5, low=10.5, close=11.0, volume=1200, amount=13200.0),  # 上涨: +1200
        ]

    def test_obv_rising(self):
        """测试上涨情况的OBV计算"""
        result = obv(self.rising_bars)
        # OBV: [1000, 1000+2000=3000, 3000+1500=4500]
        self.assertEqual(result, [1000, 3000, 4500])

    def test_obv_falling(self):
        """测试下跌情况的OBV计算"""
        result = obv(self.falling_bars)
        # OBV: [1000, 1000-2000=-1000, -1000-1500=-2500]
        self.assertEqual(result, [1000, -1000, -2500])

    def test_obv_flat(self):
        """测试平盘情况的OBV计算"""
        result = obv(self.flat_bars)
        # OBV: [1000, 1000, 1000] (平盘OBV不变)
        self.assertEqual(result, [1000, 1000, 1000])

    def test_obv_mixed(self):
        """测试混合情况的OBV计算"""
        result = obv(self.mixed_bars)
        # OBV: [1000, 3000, 1500, 1500, 2700]
        self.assertEqual(result, [1000, 3000, 1500, 1500, 2700])

    def test_obv_empty(self):
        """测试空列表"""
        result = obv([])
        self.assertEqual(result, [])

    def test_obv_single_bar(self):
        """测试单根K线"""
        single_bar = [self.rising_bars[0]]
        result = obv(single_bar)
        self.assertEqual(result, [1000])


class TestVolMA(unittest.TestCase):
    """测试成交量移动平均线 (vol_ma)"""

    def setUp(self):
        """设置测试数据"""
        self.bars = [
            Bar(code="000001", market="SZ", datetime=datetime(2024, 1, 1), open=10.0, high=11.0, low=9.0, close=10.0, volume=1000, amount=10000.0),
            Bar(code="000001", market="SZ", datetime=datetime(2024, 1, 2), open=10.0, high=11.0, low=9.0, close=11.0, volume=2000, amount=22000.0),
            Bar(code="000001", market="SZ", datetime=datetime(2024, 1, 3), open=11.0, high=12.0, low=10.5, close=12.0, volume=1500, amount=18000.0),
            Bar(code="000001", market="SZ", datetime=datetime(2024, 1, 4), open=12.0, high=12.0, low=11.0, close=11.5, volume=3000, amount=34500.0),
            Bar(code="000001", market="SZ", datetime=datetime(2024, 1, 5), open=11.5, high=12.0, low=11.0, close=11.8, volume=2500, amount=29500.0),
        ]

    def test_vol_ma_period_3(self):
        """测试3日成交量均线"""
        result = vol_ma(self.bars, 3)
        # 期望: [None, None, (1000+2000+1500)/3=1500.0, (2000+1500+3000)/3=2166.67..., (1500+3000+2500)/3=2333.33...]
        self.assertEqual(len(result), 5)
        self.assertIsNone(result[0])
        self.assertIsNone(result[1])
        self.assertAlmostEqual(result[2], 1500.0, places=2)
        self.assertAlmostEqual(result[3], 2166.67, places=2)
        self.assertAlmostEqual(result[4], 2333.33, places=2)

    def test_vol_ma_period_5(self):
        """测试5日成交量均线"""
        result = vol_ma(self.bars, 5)
        # 期望: [None, None, None, None, (1000+2000+1500+3000+2500)/5=2000.0]
        self.assertEqual(len(result), 5)
        self.assertIsNone(result[0])
        self.assertIsNone(result[1])
        self.assertIsNone(result[2])
        self.assertIsNone(result[3])
        self.assertAlmostEqual(result[4], 2000.0, places=2)

    def test_vol_ma_period_1(self):
        """测试1日成交量均线（即原值）"""
        result = vol_ma(self.bars, 1)
        # 期望: [1000.0, 2000.0, 1500.0, 3000.0, 2500.0]
        self.assertEqual(len(result), 5)
        self.assertAlmostEqual(result[0], 1000.0, places=2)
        self.assertAlmostEqual(result[1], 2000.0, places=2)
        self.assertAlmostEqual(result[2], 1500.0, places=2)
        self.assertAlmostEqual(result[3], 3000.0, places=2)
        self.assertAlmostEqual(result[4], 2500.0, places=2)

    def test_vol_ma_empty(self):
        """测试空列表"""
        result = vol_ma([], 5)
        self.assertEqual(result, [])

    def test_vol_ma_invalid_period_zero(self):
        """测试无效周期（0）"""
        with self.assertRaises(ValueError) as context:
            vol_ma(self.bars, 0)
        self.assertIn("period must be positive", str(context.exception))

    def test_vol_ma_invalid_period_negative(self):
        """测试无效周期（负数）"""
        with self.assertRaises(ValueError) as context:
            vol_ma(self.bars, -1)
        self.assertIn("period must be positive", str(context.exception))

    def test_vol_ma_single_bar(self):
        """测试单根K线"""
        single_bar = [self.bars[0]]
        result = vol_ma(single_bar, 3)
        self.assertEqual(len(result), 1)
        self.assertIsNone(result[0])


if __name__ == "__main__":
    unittest.main()
