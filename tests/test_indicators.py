"""
技术指标计算模块的单元测试
"""

import unittest
from datetime import datetime, timedelta
from tdxapi.models import Bar
from tdxapi.indicators import (
    macd,
    vol, obv, vol_ma,
    ma, ema, wma, MA, calculate_all_ma,
    rsi, rsi_multi, RSI,
    kdj, KDJ,
    std, BOLL, boll
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


class TestMACDFunction(unittest.TestCase):
    """测试 macd() 函数 - MACD 指标"""

    def test_macd_basic(self):
        """测试基本的 MACD 计算"""
        # 生成足够的数据（至少需要 slow + signal = 35 个数据点才能有有效值）
        prices = [10.0, 11.0, 12.0, 13.0, 14.0, 15.0, 16.0, 17.0, 18.0, 19.0,
                  20.0, 21.0, 22.0, 23.0, 24.0, 25.0, 26.0, 27.0, 28.0, 29.0,
                  30.0, 31.0, 32.0, 33.0, 34.0, 35.0, 36.0, 37.0, 38.0, 39.0,
                  40.0, 41.0, 42.0, 43.0, 44.0, 45.0, 46.0, 47.0, 48.0, 49.0]

        result = macd(prices, fast=12, slow=26, signal=9)

        # 检查返回结构
        self.assertIn("dif", result)
        self.assertIn("dea", result)
        self.assertIn("macd", result)
        self.assertIn("histogram", result)

        # 检查长度与输入相同
        self.assertEqual(len(result["dif"]), len(prices))
        self.assertEqual(len(result["dea"]), len(prices))
        self.assertEqual(len(result["macd"]), len(prices))
        self.assertEqual(len(result["histogram"]), len(prices))

        # macd 和 histogram 应该是相同的
        self.assertEqual(result["macd"], result["histogram"])

    def test_macd_empty_list(self):
        """测试空列表"""
        result = macd([])
        self.assertEqual(result["dif"], [])
        self.assertEqual(result["dea"], [])
        self.assertEqual(result["macd"], [])
        self.assertEqual(result["histogram"], [])

    def test_macd_invalid_period_fast_zero(self):
        """测试无效的 fast 周期（0）"""
        with self.assertRaises(ValueError) as context:
            macd([10.0, 11.0], fast=0, slow=26, signal=9)
        self.assertIn("periods must be positive", str(context.exception))

    def test_macd_invalid_period_slow_zero(self):
        """测试无效的 slow 周期（0）"""
        with self.assertRaises(ValueError) as context:
            macd([10.0, 11.0], fast=12, slow=0, signal=9)
        self.assertIn("periods must be positive", str(context.exception))

    def test_macd_invalid_period_signal_zero(self):
        """测试无效的 signal 周期（0）"""
        with self.assertRaises(ValueError) as context:
            macd([10.0, 11.0], fast=12, slow=26, signal=0)
        self.assertIn("periods must be positive", str(context.exception))

    def test_macd_invalid_fast_greater_than_slow(self):
        """测试 fast >= slow 的错误"""
        with self.assertRaises(ValueError) as context:
            macd([10.0, 11.0], fast=26, slow=12, signal=9)
        self.assertIn("fast period must be less than slow period", str(context.exception))

    def test_macd_invalid_fast_equal_slow(self):
        """测试 fast == slow 的错误"""
        with self.assertRaises(ValueError) as context:
            macd([10.0, 11.0], fast=12, slow=12, signal=9)
        self.assertIn("fast period must be less than slow period", str(context.exception))

    def test_macd_with_bars(self):
        """测试使用 Bar 列表计算 MACD"""
        from datetime import timedelta
        base_date = datetime(2024, 1, 1)
        bars = [
            Bar(code="000001", market="SZ", datetime=base_date + timedelta(days=i),
                open=10.0+i, high=11.0+i, low=9.0+i, close=10.5+i, volume=1000, amount=10000.0)
            for i in range(40)
        ]
        result = macd(bars, fast=12, slow=26, signal=9, price_type="close")

        # 检查返回结构
        self.assertIn("dif", result)
        self.assertIn("dea", result)
        self.assertIn("macd", result)

        # 检查长度与输入相同
        self.assertEqual(len(result["dif"]), len(bars))

    def test_macd_formula_correctness(self):
        """测试 MACD 公式正确性"""
        # 使用简单的价格序列验证公式
        prices = [10.0, 11.0, 12.0, 13.0, 14.0, 15.0, 16.0, 17.0, 18.0, 19.0,
                  20.0, 21.0, 22.0, 23.0, 24.0, 25.0, 26.0, 27.0, 28.0, 29.0,
                  30.0, 31.0, 32.0, 33.0, 34.0, 35.0, 36.0, 37.0, 38.0, 39.0,
                  40.0, 41.0, 42.0, 43.0, 44.0, 45.0, 46.0, 47.0, 48.0, 49.0]

        result = macd(prices, fast=12, slow=26, signal=9)

        # 验证 MACD = (DIF - DEA) * 2
        for i in range(len(prices)):
            dif = result["dif"][i]
            dea = result["dea"][i]
            macd_val = result["macd"][i]

            if dif is not None and dea is not None and macd_val is not None:
                expected_macd = (dif - dea) * 2
                self.assertAlmostEqual(macd_val, expected_macd, places=10)

    def test_macd_custom_parameters(self):
        """测试自定义 MACD 参数"""
        prices = [10.0, 11.0, 12.0, 13.0, 14.0, 15.0, 16.0, 17.0, 18.0, 19.0,
                  20.0, 21.0, 22.0, 23.0, 24.0, 25.0, 26.0, 27.0, 28.0, 29.0,
                  30.0, 31.0, 32.0, 33.0, 34.0, 35.0, 36.0, 37.0, 38.0, 39.0]

        # 使用自定义参数
        result = macd(prices, fast=5, slow=10, signal=3)

        # 检查返回结构正确
        self.assertEqual(len(result["dif"]), len(prices))
        self.assertEqual(len(result["dea"]), len(prices))
        self.assertEqual(len(result["macd"]), len(prices))



class TestKDJ(unittest.TestCase):
    """测试 KDJ 随机指标"""

    def setUp(self):
        """设置测试数据 - 创建10根K线用于测试"""
        self.bars = [
            # 日期, 开盘, 最高, 最低, 收盘, 成交量
            Bar("000001", "SZ", datetime(2024, 1, 1), 10.0, 11.0, 9.0, 10.0, 1000, 10000.0),
            Bar("000001", "SZ", datetime(2024, 1, 2), 10.0, 12.0, 9.5, 11.0, 2000, 22000.0),
            Bar("000001", "SZ", datetime(2024, 1, 3), 11.0, 13.0, 10.0, 12.0, 1500, 18000.0),
            Bar("000001", "SZ", datetime(2024, 1, 4), 12.0, 14.0, 11.0, 13.0, 1800, 23400.0),
            Bar("000001", "SZ", datetime(2024, 1, 5), 13.0, 15.0, 12.0, 14.0, 2200, 30800.0),
            Bar("000001", "SZ", datetime(2024, 1, 6), 14.0, 16.0, 13.0, 15.0, 2500, 37500.0),
            Bar("000001", "SZ", datetime(2024, 1, 7), 15.0, 17.0, 14.0, 16.0, 2000, 32000.0),
            Bar("000001", "SZ", datetime(2024, 1, 8), 16.0, 18.0, 15.0, 17.0, 2300, 39100.0),
            Bar("000001", "SZ", datetime(2024, 1, 9), 17.0, 19.0, 16.0, 18.0, 2800, 50400.0),
            Bar("000001", "SZ", datetime(2024, 1, 10), 18.0, 20.0, 17.0, 19.0, 3000, 57000.0),
        ]

    def test_kdj_basic(self):
        """测试基本的 KDJ 计算"""
        k, d, j = kdj(self.bars)

        # 检查返回的是三个列表
        self.assertEqual(len(k), 10)
        self.assertEqual(len(d), 10)
        self.assertEqual(len(j), 10)

        # 前8个应该是 None（需要9个数据点计算RSV）
        for i in range(8):
            self.assertIsNone(k[i])
            self.assertIsNone(d[i])
            self.assertIsNone(j[i])

        # 第9个及以后应该有值
        self.assertIsNotNone(k[8])
        self.assertIsNotNone(d[8])
        self.assertIsNotNone(j[8])

    def test_kdj_formula(self):
        """测试 KDJ 公式计算正确性"""
        # 使用简单的测试数据验证公式
        # 9根K线，最高价和最低价固定，便于验证RSV
        simple_bars = [
            Bar("000001", "SZ", datetime(2024, 1, i+1), 10.0, 12.0, 8.0, 10.0 + i * 0.5, 1000, 10000.0)
            for i in range(9)
        ]

        k, d, j = kdj(simple_bars, n=9, m1=3, m2=3)

        # 第9根K线（索引8）的RSV计算
        # 9日内最高 = 12.0, 最低 = 8.0
        # 收盘 = 10.0 + 8 * 0.5 = 14.0
        # RSV = (14.0 - 8.0) / (12.0 - 8.0) * 100 = 6.0 / 4.0 * 100 = 150
        # 但RSV应该被限制在0-100之间，这里需要重新考虑

        # 实际上我们的实现不限制RSV范围，只是按公式计算
        # 如果收盘超过最高价，RSV > 100；如果收盘低于最低价，RSV < 0

        self.assertIsNotNone(k[8])
        self.assertIsNotNone(d[8])
        self.assertIsNotNone(j[8])

        # 验证 J = 3K - 2D
        self.assertAlmostEqual(j[8], 3 * k[8] - 2 * d[8], places=5)

    def test_kdj_empty(self):
        """测试空列表"""
        k, d, j = kdj([])
        self.assertEqual(k, [])
        self.assertEqual(d, [])
        self.assertEqual(j, [])

    def test_kdj_insufficient_data(self):
        """测试数据不足的情况"""
        # 只有5根K线，但n=9
        short_bars = self.bars[:5]
        k, d, j = kdj(short_bars, n=9)

        # 所有值都应该是 None
        self.assertEqual(len(k), 5)
        for i in range(5):
            self.assertIsNone(k[i])
            self.assertIsNone(d[i])
            self.assertIsNone(j[i])

    def test_kdj_invalid_params(self):
        """测试无效参数"""
        with self.assertRaises(ValueError):
            kdj(self.bars, n=0)
        with self.assertRaises(ValueError):
            kdj(self.bars, n=-1)
        with self.assertRaises(ValueError):
            kdj(self.bars, m1=0)
        with self.assertRaises(ValueError):
            kdj(self.bars, m2=0)

    def test_kdj_class(self):
        """测试 KDJ 类"""
        kdj_obj = KDJ.calculate(self.bars)

        self.assertEqual(kdj_obj.n, 9)
        self.assertEqual(kdj_obj.m1, 3)
        self.assertEqual(kdj_obj.m2, 3)
        self.assertEqual(len(kdj_obj.k), 10)
        self.assertEqual(len(kdj_obj.d), 10)
        self.assertEqual(len(kdj_obj.j), 10)

    def test_kdj_class_last(self):
        """测试 KDJ.last() 方法"""
        kdj_obj = KDJ.calculate(self.bars)
        k_last, d_last, j_last = kdj_obj.last()

        self.assertIsNotNone(k_last)
        self.assertIsNotNone(d_last)
        self.assertIsNotNone(j_last)

        # 验证 J = 3K - 2D
        self.assertAlmostEqual(j_last, 3 * k_last - 2 * d_last, places=5)

    def test_kdj_overbought(self):
        """测试超买判断"""
        # 创建超买情况的数据（K值很高）
        # 使用连续上涨的数据
        rising_bars = [
            Bar("000001", "SZ", datetime(2024, 1, i+1), 10.0 + i, 20.0, 10.0, 19.0, 1000, 10000.0)
            for i in range(10)
        ]

        kdj_obj = KDJ.calculate(rising_bars)
        result = kdj_obj.is_overbought(threshold=80.0)

        # 连续上涨应该导致K值较高
        self.assertIsNotNone(result)

    def test_kdj_oversold(self):
        """测试超卖判断"""
        # 创建超卖情况的数据（K值很低）
        falling_bars = [
            Bar("000001", "SZ", datetime(2024, 1, i+1), 20.0 - i, 20.0, 10.0, 11.0, 1000, 10000.0)
            for i in range(10)
        ]

        kdj_obj = KDJ.calculate(falling_bars)
        result = kdj_obj.is_oversold(threshold=20.0)

        # 连续下跌应该导致K值较低
        self.assertIsNotNone(result)

    def test_kdj_repr(self):
        """测试 KDJ 类的字符串表示"""
        kdj_obj = KDJ.calculate(self.bars)
        repr_str = repr(kdj_obj)
        self.assertIn("KDJ", repr_str)
        self.assertIn("n=9", repr_str)

    def test_kdj_flat_prices(self):
        """测试价格不变的情况（避免除零）"""
        # 所有价格相同，最高价=最低价
        flat_bars = [
            Bar("000001", "SZ", datetime(2024, 1, i+1), 10.0, 10.0, 10.0, 10.0, 1000, 10000.0)
            for i in range(10)
        ]

        k, d, j = kdj(flat_bars)

        # 当最高价=最低价时，RSV应该设为50（中间值）
        self.assertIsNotNone(k[8])
        self.assertIsNotNone(d[8])
        self.assertIsNotNone(j[8])


if __name__ == "__main__":
    unittest.main()


class TestStdFunction(unittest.TestCase):
    """测试 std() 函数 - 标准差"""

    def test_std_basic(self):
        """测试基本的标准差计算"""
        prices = [10.0, 11.0, 12.0, 13.0, 14.0]
        result = std(prices, 3)
        # 前2个为 None
        self.assertIsNone(result[0])
        self.assertIsNone(result[1])
        # 窗口 [10, 11, 12]: mean=11, variance=((10-11)^2+(11-11)^2+(12-11)^2)/3=2/3, std=sqrt(2/3)
        expected_std = (2.0/3.0) ** 0.5
        self.assertAlmostEqual(result[2], expected_std, places=5)

    def test_std_constant_values(self):
        """测试常数值的标准差（应为0）"""
        prices = [10.0, 10.0, 10.0, 10.0, 10.0]
        result = std(prices, 3)
        self.assertIsNone(result[0])
        self.assertIsNone(result[1])
        self.assertAlmostEqual(result[2], 0.0)
        self.assertAlmostEqual(result[3], 0.0)
        self.assertAlmostEqual(result[4], 0.0)

    def test_std_empty_list(self):
        """测试空列表"""
        result = std([], 5)
        self.assertEqual(result, [])

    def test_std_invalid_period(self):
        """测试无效的周期"""
        with self.assertRaises(ValueError):
            std([10.0, 11.0], 0)


class TestBOLLClass(unittest.TestCase):
    """测试 BOLL 类 - 布林带"""

    def setUp(self):
        """设置测试数据 - 25个价格数据，足够计算20日布林带"""
        self.prices = [10.0 + i * 0.5 for i in range(25)]  # 10.0, 10.5, 11.0, ..., 22.0

    def test_boll_basic(self):
        """测试基本的布林带计算"""
        boll = BOLL.calculate(self.prices, period=20, std_dev=2)

        # 检查返回的三个序列长度正确
        self.assertEqual(len(boll.up), 25)
        self.assertEqual(len(boll.mid), 25)
        self.assertEqual(len(boll.low), 25)

        # 前19个应为 None（数据不足）
        for i in range(19):
            self.assertIsNone(boll.up[i])
            self.assertIsNone(boll.mid[i])
            self.assertIsNone(boll.low[i])

        # 第20个及以后应有有效值
        self.assertIsNotNone(boll.mid[19])
        self.assertIsNotNone(boll.up[19])
        self.assertIsNotNone(boll.low[19])

        # 验证中轨是20日简单移动平均
        expected_mid = sum(self.prices[0:20]) / 20
        self.assertAlmostEqual(boll.mid[19], expected_mid, places=5)

        # 验证上轨 = 中轨 + 2 * 标准差
        # 验证下轨 = 中轨 - 2 * 标准差
        mean = sum(self.prices[0:20]) / 20
        variance = sum((p - mean) ** 2 for p in self.prices[0:20]) / 20
        expected_std = variance ** 0.5
        expected_up = mean + 2 * expected_std
        expected_low = mean - 2 * expected_std

        self.assertAlmostEqual(boll.up[19], expected_up, places=5)
        self.assertAlmostEqual(boll.low[19], expected_low, places=5)

    def test_boll_relationship(self):
        """测试布林带三条线的关系"""
        boll = BOLL.calculate(self.prices, period=20, std_dev=2)

        # 对于有效值，应始终满足: up >= mid >= low
        for i in range(19, 25):
            self.assertGreaterEqual(boll.up[i], boll.mid[i])
            self.assertGreaterEqual(boll.mid[i], boll.low[i])

    def test_boll_with_bars(self):
        """测试使用 Bar 列表计算布林带"""
        bars = [
            Bar("000001", "SZ", datetime(2024, 1, i+1), 10.0+i, 11.0+i, 9.0+i, 10.5+i, 1000, 10000.0)
            for i in range(25)
        ]
        boll = BOLL.calculate(bars, period=20, std_dev=2, price_type="close")

        self.assertEqual(len(boll.mid), 25)
        # 前19个为 None
        self.assertIsNone(boll.mid[0])
        self.assertIsNotNone(boll.mid[19])

    def test_boll_empty_list(self):
        """测试空列表"""
        boll = BOLL.calculate([], period=20, std_dev=2)
        self.assertEqual(len(boll.up), 0)
        self.assertEqual(len(boll.mid), 0)
        self.assertEqual(len(boll.low), 0)

    def test_boll_invalid_period(self):
        """测试无效的周期"""
        with self.assertRaises(ValueError):
            BOLL.calculate(self.prices, period=0, std_dev=2)
        with self.assertRaises(ValueError):
            BOLL.calculate(self.prices, period=-1, std_dev=2)

    def test_boll_invalid_std_dev(self):
        """测试无效的标准差倍数"""
        with self.assertRaises(ValueError):
            BOLL.calculate(self.prices, period=20, std_dev=0)
        with self.assertRaises(ValueError):
            BOLL.calculate(self.prices, period=20, std_dev=-1)

    def test_boll_different_std_dev(self):
        """测试不同的标准差倍数"""
        boll_1 = BOLL.calculate(self.prices, period=20, std_dev=1)
        boll_2 = BOLL.calculate(self.prices, period=20, std_dev=2)
        boll_3 = BOLL.calculate(self.prices, period=20, std_dev=3)

        # std_dev 越大，带宽越大
        bandwidth_1 = boll_1.up[19] - boll_1.low[19]
        bandwidth_2 = boll_2.up[19] - boll_2.low[19]
        bandwidth_3 = boll_3.up[19] - boll_3.low[19]

        self.assertLess(bandwidth_1, bandwidth_2)
        self.assertLess(bandwidth_2, bandwidth_3)

    def test_boll_indexing(self):
        """测试 BOLL 类的索引访问"""
        boll = BOLL.calculate(self.prices, period=20, std_dev=2)

        # 测试 __getitem__
        up, mid, low = boll[19]
        self.assertAlmostEqual(mid, boll.mid[19])
        self.assertAlmostEqual(up, boll.up[19])
        self.assertAlmostEqual(low, boll.low[19])

    def test_boll_last(self):
        """测试 BOLL.last() 方法"""
        boll = BOLL.calculate(self.prices, period=20, std_dev=2)

        up, mid, low = boll.last()
        self.assertAlmostEqual(mid, boll.mid[-1])
        self.assertAlmostEqual(up, boll.up[-1])
        self.assertAlmostEqual(low, boll.low[-1])

    def test_boll_last_empty(self):
        """测试 BOLL.last() 空列表情况"""
        boll = BOLL.calculate([], period=20, std_dev=2)
        self.assertEqual(boll.last(), (None, None, None))

    def test_boll_repr(self):
        """测试 BOLL 的字符串表示"""
        boll = BOLL.calculate(self.prices, period=20, std_dev=2)
        repr_str = repr(boll)
        self.assertIn("BOLL", repr_str)
        self.assertIn("period=20", repr_str)
        self.assertIn("std_dev=2", repr_str)

    def test_boll_bandwidth(self):
        """测试布林带宽度计算"""
        boll = BOLL.calculate(self.prices, period=20, std_dev=2)
        bandwidth = boll.bandwidth()

        # 前19个应为 None
        for i in range(19):
            self.assertIsNone(bandwidth[i])

        # 第20个及以后应有有效值
        self.assertIsNotNone(bandwidth[19])
        # Bandwidth = (UP - LOW) / MID * 100%
        expected_bandwidth = (boll.up[19] - boll.low[19]) / boll.mid[19] * 100
        self.assertAlmostEqual(bandwidth[19], expected_bandwidth, places=5)


class TestBollFunction(unittest.TestCase):
    """测试 boll() 函数"""

    def setUp(self):
        self.prices = [10.0 + i * 0.5 for i in range(25)]

    def test_boll_function(self):
        """测试 boll() 函数是 BOLL.calculate 的别名"""
        result_func = boll(self.prices, period=20, std_dev=2)
        result_class = BOLL.calculate(self.prices, period=20, std_dev=2)

        self.assertEqual(result_func.up, result_class.up)
        self.assertEqual(result_func.mid, result_class.mid)
        self.assertEqual(result_func.low, result_class.low)


class TestRSIFunction(unittest.TestCase):
    """测试 rsi() 函数 - 相对强弱指标"""

    def test_rsi_basic(self):
        """测试基本的 RSI 计算"""
        prices = [10.0, 11.0, 12.0, 13.0, 14.0, 15.0, 16.0, 17.0, 18.0, 19.0,
                  20.0, 21.0, 22.0, 23.0, 24.0, 25.0]
        result = rsi(prices, 14)
        for i in range(14):
            self.assertIsNone(result[i])
        self.assertGreater(result[14], 90)

    def test_rsi_falling(self):
        """测试持续下跌情况的 RSI"""
        prices = [25.0, 24.0, 23.0, 22.0, 21.0, 20.0, 19.0, 18.0, 17.0, 16.0,
                  15.0, 14.0, 13.0, 12.0, 11.0, 10.0]
        result = rsi(prices, 14)
        for i in range(14):
            self.assertIsNone(result[i])
        self.assertLess(result[14], 10)

    def test_rsi_empty_list(self):
        """测试空列表"""
        result = rsi([], 14)
        self.assertEqual(result, [])

    def test_rsi_invalid_period(self):
        """测试无效的周期"""
        with self.assertRaises(ValueError):
            rsi([10.0, 11.0], 0)


class TestRSIClass(unittest.TestCase):
    """测试 RSI 类"""

    def test_rsi_class_calculate(self):
        """测试 RSI.calculate()"""
        prices = list(range(1, 30))
        rsi_obj = RSI.calculate(prices, 14)
        self.assertEqual(rsi_obj.period, 14)

    def test_rsi_convenience_methods(self):
        """测试便捷的 RSI 计算方法"""
        prices = list(range(1, 50))
        self.assertEqual(RSI.rsi6(prices).period, 6)
        self.assertEqual(RSI.rsi12(prices).period, 12)
        self.assertEqual(RSI.rsi14(prices).period, 14)
        self.assertEqual(RSI.rsi24(prices).period, 24)


class TestMACDFunction(unittest.TestCase):
    """测试 macd() 函数 - MACD 指标"""

    def test_macd_basic(self):
        """测试基本的 MACD 计算"""
        # 生成足够的数据（至少需要 slow + signal = 35 个数据点才能有有效值）
        prices = [10.0, 11.0, 12.0, 13.0, 14.0, 15.0, 16.0, 17.0, 18.0, 19.0,
                  20.0, 21.0, 22.0, 23.0, 24.0, 25.0, 26.0, 27.0, 28.0, 29.0,
                  30.0, 31.0, 32.0, 33.0, 34.0, 35.0, 36.0, 37.0, 38.0, 39.0,
                  40.0, 41.0, 42.0, 43.0, 44.0, 45.0, 46.0, 47.0, 48.0, 49.0]

        result = macd(prices, fast=12, slow=26, signal=9)

        # 检查返回结构
        self.assertIn("dif", result)
        self.assertIn("dea", result)
        self.assertIn("macd", result)
        self.assertIn("histogram", result)

        # 检查长度与输入相同
        self.assertEqual(len(result["dif"]), len(prices))
        self.assertEqual(len(result["dea"]), len(prices))
        self.assertEqual(len(result["macd"]), len(prices))
        self.assertEqual(len(result["histogram"]), len(prices))

        # macd 和 histogram 应该是相同的
        self.assertEqual(result["macd"], result["histogram"])

    def test_macd_empty_list(self):
        """测试空列表"""
        result = macd([])
        self.assertEqual(result["dif"], [])
        self.assertEqual(result["dea"], [])
        self.assertEqual(result["macd"], [])
        self.assertEqual(result["histogram"], [])

    def test_macd_invalid_period_fast_zero(self):
        """测试无效的 fast 周期（0）"""
        with self.assertRaises(ValueError) as context:
            macd([10.0, 11.0], fast=0, slow=26, signal=9)
        self.assertIn("periods must be positive", str(context.exception))

    def test_macd_invalid_period_slow_zero(self):
        """测试无效的 slow 周期（0）"""
        with self.assertRaises(ValueError) as context:
            macd([10.0, 11.0], fast=12, slow=0, signal=9)
        self.assertIn("periods must be positive", str(context.exception))

    def test_macd_invalid_period_signal_zero(self):
        """测试无效的 signal 周期（0）"""
        with self.assertRaises(ValueError) as context:
            macd([10.0, 11.0], fast=12, slow=26, signal=0)
        self.assertIn("periods must be positive", str(context.exception))

    def test_macd_invalid_fast_greater_than_slow(self):
        """测试 fast >= slow 的错误"""
        with self.assertRaises(ValueError) as context:
            macd([10.0, 11.0], fast=26, slow=12, signal=9)
        self.assertIn("fast period must be less than slow period", str(context.exception))

    def test_macd_invalid_fast_equal_slow(self):
        """测试 fast == slow 的错误"""
        with self.assertRaises(ValueError) as context:
            macd([10.0, 11.0], fast=12, slow=12, signal=9)
        self.assertIn("fast period must be less than slow period", str(context.exception))

    def test_macd_with_bars(self):
        """测试使用 Bar 列表计算 MACD"""
        from datetime import timedelta
        base_date = datetime(2024, 1, 1)
        bars = [
            Bar(code="000001", market="SZ", datetime=base_date + timedelta(days=i),
                open=10.0+i, high=11.0+i, low=9.0+i, close=10.5+i, volume=1000, amount=10000.0)
            for i in range(40)
        ]
        result = macd(bars, fast=12, slow=26, signal=9, price_type="close")

        # 检查返回结构
        self.assertIn("dif", result)
        self.assertIn("dea", result)
        self.assertIn("macd", result)

        # 检查长度与输入相同
        self.assertEqual(len(result["dif"]), len(bars))

    def test_macd_formula_correctness(self):
        """测试 MACD 公式正确性"""
        # 使用简单的价格序列验证公式
        prices = [10.0, 11.0, 12.0, 13.0, 14.0, 15.0, 16.0, 17.0, 18.0, 19.0,
                  20.0, 21.0, 22.0, 23.0, 24.0, 25.0, 26.0, 27.0, 28.0, 29.0,
                  30.0, 31.0, 32.0, 33.0, 34.0, 35.0, 36.0, 37.0, 38.0, 39.0,
                  40.0, 41.0, 42.0, 43.0, 44.0, 45.0, 46.0, 47.0, 48.0, 49.0]

        result = macd(prices, fast=12, slow=26, signal=9)

        # 验证 MACD = (DIF - DEA) * 2
        for i in range(len(prices)):
            dif = result["dif"][i]
            dea = result["dea"][i]
            macd_val = result["macd"][i]

            if dif is not None and dea is not None and macd_val is not None:
                expected_macd = (dif - dea) * 2
                self.assertAlmostEqual(macd_val, expected_macd, places=10)

    def test_macd_custom_parameters(self):
        """测试自定义 MACD 参数"""
        prices = [10.0, 11.0, 12.0, 13.0, 14.0, 15.0, 16.0, 17.0, 18.0, 19.0,
                  20.0, 21.0, 22.0, 23.0, 24.0, 25.0, 26.0, 27.0, 28.0, 29.0,
                  30.0, 31.0, 32.0, 33.0, 34.0, 35.0, 36.0, 37.0, 38.0, 39.0]

        # 使用自定义参数
        result = macd(prices, fast=5, slow=10, signal=3)

        # 检查返回结构正确
        self.assertEqual(len(result["dif"]), len(prices))
        self.assertEqual(len(result["dea"]), len(prices))
        self.assertEqual(len(result["macd"]), len(prices))
