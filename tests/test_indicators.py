"""
技术指标计算模块的单元测试
"""

import unittest
from datetime import datetime
from tdxapi.models import Bar
from tdxapi.indicators import (
    vol, obv, vol_ma,
    ma, ema, wma, MA, calculate_all_ma
)


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


class TestMAFunction(unittest.TestCase):
    """测试 ma() 函数 - 简单移动平均线"""

    def test_ma_basic(self):
        """测试基本的 MA 计算"""
        prices = [10.0, 11.0, 12.0, 13.0, 14.0]
        result = ma(prices, 3)
        # 前2个为 None，后面是移动平均
        self.assertIsNone(result[0])
        self.assertIsNone(result[1])
        self.assertAlmostEqual(result[2], 11.0)  # (10+11+12)/3
        self.assertAlmostEqual(result[3], 12.0)  # (11+12+13)/3
        self.assertAlmostEqual(result[4], 13.0)  # (12+13+14)/3

    def test_ma_with_bars(self):
        """测试使用 Bar 列表计算 MA"""
        bars = [
            Bar("000001", "SZ", datetime(2024, 1, i+1), 10.0+i, 11.0+i, 9.0+i, 10.5+i, 1000, 10000.0)
            for i in range(5)
        ]
        result = ma(bars, 3, "close")
        self.assertIsNone(result[0])
        self.assertIsNone(result[1])
        # (10.5 + 11.5 + 12.5) / 3 = 34.5 / 3 = 11.5
        self.assertAlmostEqual(result[2], 11.5)

    def test_ma_different_price_types(self):
        """测试不同的价格类型"""
        bars = [
            Bar("000001", "SZ", datetime(2024, 1, 1), 10.0, 12.0, 8.0, 11.0, 1000, 10000.0),
            Bar("000001", "SZ", datetime(2024, 1, 2), 11.0, 13.0, 9.0, 12.0, 1000, 10000.0),
            Bar("000001", "SZ", datetime(2024, 1, 3), 12.0, 14.0, 10.0, 13.0, 1000, 10000.0),
        ]

        ma_open = ma(bars, 2, "open")
        self.assertAlmostEqual(ma_open[1], 10.5)  # (10+11)/2

        ma_high = ma(bars, 2, "high")
        self.assertAlmostEqual(ma_high[1], 12.5)  # (12+13)/2

        ma_low = ma(bars, 2, "low")
        self.assertAlmostEqual(ma_low[1], 8.5)  # (8+9)/2

    def test_ma_empty_list(self):
        """测试空列表"""
        result = ma([], 5)
        self.assertEqual(result, [])

    def test_ma_insufficient_data(self):
        """测试数据不足的情况"""
        prices = [10.0, 11.0]
        result = ma(prices, 5)
        self.assertEqual(result, [None, None])

    def test_ma_invalid_period(self):
        """测试无效的周期"""
        with self.assertRaises(ValueError):
            ma([10.0, 11.0], 0)
        with self.assertRaises(ValueError):
            ma([10.0, 11.0], -1)

    def test_ma_period_1(self):
        """测试周期为1的情况"""
        prices = [10.0, 11.0, 12.0]
        result = ma(prices, 1)
        self.assertEqual(result, [10.0, 11.0, 12.0])


class TestEMAFunction(unittest.TestCase):
    """测试 ema() 函数 - 指数移动平均线"""

    def test_ema_basic(self):
        """测试基本的 EMA 计算"""
        prices = [10.0, 11.0, 12.0, 13.0, 14.0]
        result = ema(prices, 3)
        # EMA 公式: alpha = 2/(3+1) = 0.5
        # EMA[0] = 10.0
        # EMA[1] = 0.5*11 + 0.5*10 = 10.5
        # EMA[2] = 0.5*12 + 0.5*10.5 = 11.25
        self.assertAlmostEqual(result[0], 10.0)
        self.assertAlmostEqual(result[1], 10.5)
        self.assertAlmostEqual(result[2], 11.25)
        self.assertAlmostEqual(result[3], 12.125)
        self.assertAlmostEqual(result[4], 13.0625)

    def test_ema_with_bars(self):
        """测试使用 Bar 列表计算 EMA"""
        bars = [
            Bar("000001", "SZ", datetime(2024, 1, i+1), 10.0+i, 11.0+i, 9.0+i, 10.5+i, 1000, 10000.0)
            for i in range(5)
        ]
        result = ema(bars, 3, "close")
        self.assertAlmostEqual(result[0], 10.5)
        self.assertAlmostEqual(result[1], 11.0)

    def test_ema_empty_list(self):
        """测试空列表"""
        result = ema([], 5)
        self.assertEqual(result, [])

    def test_ema_invalid_period(self):
        """测试无效的周期"""
        with self.assertRaises(ValueError):
            ema([10.0, 11.0], 0)


class TestWMAFunction(unittest.TestCase):
    """测试 wma() 函数 - 加权移动平均线"""

    def test_wma_basic(self):
        """测试基本的 WMA 计算"""
        prices = [10.0, 11.0, 12.0, 13.0, 14.0]
        result = wma(prices, 3)
        # 权重: 1, 2, 3，总和为6
        # WMA[2] = (10*1 + 11*2 + 12*3) / 6 = 68/6 = 11.333...
        # WMA[3] = (11*1 + 12*2 + 13*3) / 6 = 74/6 = 12.333...
        # WMA[4] = (12*1 + 13*2 + 14*3) / 6 = 80/6 = 13.333...
        self.assertIsNone(result[0])
        self.assertIsNone(result[1])
        self.assertAlmostEqual(result[2], 68/6)
        self.assertAlmostEqual(result[3], 74/6)
        self.assertAlmostEqual(result[4], 80/6)

    def test_wma_empty_list(self):
        """测试空列表"""
        result = wma([], 5)
        self.assertEqual(result, [])


class TestMAClass(unittest.TestCase):
    """测试 MA 类"""

    def test_ma_class_calculate_sma(self):
        """测试 MA.calculate() 使用 SMA"""
        prices = [10.0, 11.0, 12.0, 13.0, 14.0]
        ma_obj = MA.calculate(prices, 3, "sma")
        self.assertEqual(ma_obj.period, 3)
        self.assertEqual(ma_obj.ma_type, "sma")
        self.assertAlmostEqual(ma_obj.values[2], 11.0)

    def test_ma_class_calculate_ema(self):
        """测试 MA.calculate() 使用 EMA"""
        prices = [10.0, 11.0, 12.0, 13.0, 14.0]
        ma_obj = MA.calculate(prices, 3, "ema")
        self.assertEqual(ma_obj.period, 3)
        self.assertEqual(ma_obj.ma_type, "ema")
        self.assertAlmostEqual(ma_obj.values[0], 10.0)

    def test_ma_class_invalid_type(self):
        """测试无效的 MA 类型"""
        with self.assertRaises(ValueError):
            MA.calculate([10.0, 11.0], 3, "invalid")

    def test_ma_convenience_methods(self):
        """测试便捷的 MA 计算方法"""
        prices = list(range(1, 300))  # 1到299

        ma5 = MA.ma5(prices)
        self.assertEqual(ma5.period, 5)

        ma10 = MA.ma10(prices)
        self.assertEqual(ma10.period, 10)

        ma20 = MA.ma20(prices)
        self.assertEqual(ma20.period, 20)

        ma60 = MA.ma60(prices)
        self.assertEqual(ma60.period, 60)

        ma120 = MA.ma120(prices)
        self.assertEqual(ma120.period, 120)

        ma250 = MA.ma250(prices)
        self.assertEqual(ma250.period, 250)

    def test_ma_class_indexing(self):
        """测试 MA 类的索引访问"""
        prices = [10.0, 11.0, 12.0, 13.0, 14.0]
        ma_obj = MA.calculate(prices, 3)
        self.assertIsNone(ma_obj[0])
        self.assertAlmostEqual(ma_obj[2], 11.0)

    def test_ma_class_len(self):
        """测试 MA 类的长度"""
        prices = [10.0, 11.0, 12.0]
        ma_obj = MA.calculate(prices, 2)
        self.assertEqual(len(ma_obj), 3)

    def test_ma_class_last(self):
        """测试 MA.last() 方法"""
        prices = [10.0, 11.0, 12.0, 13.0, 14.0]
        ma_obj = MA.calculate(prices, 3)
        self.assertAlmostEqual(ma_obj.last(), 13.0)

        # 测试全为 None 的情况
        ma_obj2 = MA.calculate([10.0], 5)
        self.assertIsNone(ma_obj2.last())

    def test_ma_class_valid_values(self):
        """测试 MA.valid_values() 方法"""
        prices = [10.0, 11.0, 12.0, 13.0, 14.0]
        ma_obj = MA.calculate(prices, 3)
        valid = ma_obj.valid_values()
        self.assertEqual(len(valid), 3)
        self.assertAlmostEqual(valid[0], 11.0)

    def test_ma_class_repr(self):
        """测试 MA 类的字符串表示"""
        prices = [10.0, 11.0, 12.0, 13.0, 14.0]
        ma_obj = MA.calculate(prices, 3)
        repr_str = repr(ma_obj)
        self.assertIn("MA(3", repr_str)
        self.assertIn("sma", repr_str)
        self.assertIn("3/5", repr_str)  # 3个有效值，总共5个


class TestCalculateAllMA(unittest.TestCase):
    """测试 calculate_all_ma() 函数"""

    def test_calculate_all_ma_default(self):
        """测试默认计算所有常用周期"""
        prices = list(range(1, 300))
        result = calculate_all_ma(prices)

        self.assertIn(5, result)
        self.assertIn(10, result)
        self.assertIn(20, result)
        self.assertIn(60, result)
        self.assertIn(120, result)
        self.assertIn(250, result)

        self.assertIsInstance(result[5], MA)

    def test_calculate_all_ma_custom_periods(self):
        """测试自定义周期"""
        prices = [10.0, 11.0, 12.0, 13.0, 14.0]
        result = calculate_all_ma(prices, periods=[2, 3])

        self.assertIn(2, result)
        self.assertIn(3, result)
        self.assertNotIn(5, result)

    def test_calculate_all_ma_ema(self):
        """测试使用 EMA 类型"""
        prices = [10.0, 11.0, 12.0, 13.0, 14.0]
        result = calculate_all_ma(prices, periods=[3], ma_type="ema")

        self.assertEqual(result[3].ma_type, "ema")


if __name__ == "__main__":
    unittest.main()
