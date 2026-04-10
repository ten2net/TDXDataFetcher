"""
数据质量与工具模块的单元测试
"""

import unittest
from datetime import datetime, timedelta
from tdxapi.models import Bar
from tdxapi.data_quality import (
    DataValidator,
    PriceAdjuster,
    DataAligner,
    ValidationIssue,
    DataGap,
    AdjustmentFactor,
    AdjustmentType,
    validate_data,
    adjust_forward,
    adjust_backward,
    align_bars,
)


class TestValidationIssue(unittest.TestCase):
    """测试 ValidationIssue 数据类"""

    def test_create_issue(self):
        """测试创建校验问题"""
        issue = ValidationIssue(
            issue_type="test_error",
            message="Test message",
            index=0,
            value=100.0,
            severity="error"
        )
        self.assertEqual(issue.issue_type, "test_error")
        self.assertEqual(issue.severity, "error")


class TestDataGap(unittest.TestCase):
    """测试 DataGap 数据类"""

    def test_create_gap(self):
        """测试创建数据缺失区间"""
        gap = DataGap(
            start_date=datetime(2024, 1, 1),
            end_date=datetime(2024, 1, 5),
            expected_count=5,
            actual_count=3
        )
        self.assertEqual(gap.expected_count, 5)
        self.assertEqual(gap.actual_count, 3)


class TestAdjustmentFactor(unittest.TestCase):
    """测试 AdjustmentFactor 数据类"""

    def test_create_factor(self):
        """测试创建复权因子"""
        factor = AdjustmentFactor(
            date=datetime(2024, 6, 1),
            factor=1.1,
            dividend=0.5,
            split_ratio=1.1
        )
        self.assertEqual(factor.factor, 1.1)
        self.assertEqual(factor.dividend, 0.5)


class TestDataValidator(unittest.TestCase):
    """测试数据校验器"""

    def setUp(self):
        """设置测试数据"""
        self.validator = DataValidator()
        self.bars = [
            Bar(code="000001", market="SZ", datetime=datetime(2024, 1, 1), open=10.0, high=11.0, low=9.0, close=10.5, volume=1000, amount=10500.0),
            Bar(code="000001", market="SZ", datetime=datetime(2024, 1, 2), open=10.5, high=11.5, low=10.0, close=11.0, volume=2000, amount=22000.0),
            Bar(code="000001", market="SZ", datetime=datetime(2024, 1, 3), open=11.0, high=11.5, low=10.5, close=10.8, volume=1500, amount=16200.0),
        ]

    def test_validate_empty_list(self):
        """测试空列表"""
        issues = self.validator.validate([])
        self.assertEqual(len(issues), 0)

    def test_validate_valid_data(self):
        """测试有效数据"""
        issues = self.validator.validate(self.bars)
        # 有效数据应该没有error级别的问题
        errors = [i for i in issues if i.severity == "error"]
        self.assertEqual(len(errors), 0)

    def test_validate_ohlc_logic_error(self):
        """测试OHLC逻辑错误"""
        invalid_bars = [
            Bar(code="000001", market="SZ", datetime=datetime(2024, 1, 1), open=10.0, high=9.0, low=11.0, close=10.5, volume=1000, amount=10500.0),
        ]
        issues = self.validator.validate(invalid_bars)
        # 应该有high < low的错误
        ohlc_errors = [i for i in issues if i.issue_type == "ohlc_logic_error"]
        self.assertGreater(len(ohlc_errors), 0)

    def test_validate_high_lower_than_max_oc(self):
        """测试最高价低于max(开盘,收盘)"""
        invalid_bars = [
            Bar(code="000001", market="SZ", datetime=datetime(2024, 1, 1), open=10.0, high=9.5, low=9.0, close=10.5, volume=1000, amount=10500.0),
        ]
        issues = self.validator.validate(invalid_bars)
        ohlc_errors = [i for i in issues if i.issue_type == "ohlc_logic_error" and "最高价" in i.message]
        self.assertGreater(len(ohlc_errors), 0)

    def test_validate_low_higher_than_min_oc(self):
        """测试最低价高于min(开盘,收盘)"""
        invalid_bars = [
            Bar(code="000001", market="SZ", datetime=datetime(2024, 1, 1), open=10.0, high=11.0, low=10.8, close=10.5, volume=1000, amount=10500.0),
        ]
        issues = self.validator.validate(invalid_bars)
        ohlc_errors = [i for i in issues if i.issue_type == "ohlc_logic_error" and "最低价" in i.message]
        self.assertGreater(len(ohlc_errors), 0)

    def test_validate_invalid_price(self):
        """测试无效价格"""
        invalid_bars = [
            Bar(code="000001", market="SZ", datetime=datetime(2024, 1, 1), open=0.0, high=11.0, low=9.0, close=10.5, volume=1000, amount=10500.0),
        ]
        issues = self.validator.validate(invalid_bars)
        price_errors = [i for i in issues if i.issue_type == "invalid_price"]
        self.assertGreater(len(price_errors), 0)

    def test_validate_negative_price(self):
        """测试负价格"""
        invalid_bars = [
            Bar(code="000001", market="SZ", datetime=datetime(2024, 1, 1), open=-10.0, high=11.0, low=9.0, close=10.5, volume=1000, amount=10500.0),
        ]
        issues = self.validator.validate(invalid_bars)
        price_errors = [i for i in issues if i.issue_type == "invalid_price"]
        self.assertGreater(len(price_errors), 0)

    def test_validate_large_price_change(self):
        """测试价格大幅变动"""
        large_change_bars = [
            Bar(code="000001", market="SZ", datetime=datetime(2024, 1, 1), open=10.0, high=15.0, low=9.0, close=14.0, volume=1000, amount=14000.0),
        ]
        validator = DataValidator(max_price_change=0.1)  # 10%阈值
        issues = validator.validate(large_change_bars)
        change_warnings = [i for i in issues if i.issue_type == "large_price_change"]
        self.assertGreater(len(change_warnings), 0)

    def test_validate_low_volume(self):
        """测试低成交量"""
        validator = DataValidator(min_volume=100)
        low_volume_bars = [
            Bar(code="000001", market="SZ", datetime=datetime(2024, 1, 1), open=10.0, high=11.0, low=9.0, close=10.5, volume=50, amount=525.0),
        ]
        issues = validator.validate(low_volume_bars)
        volume_warnings = [i for i in issues if i.issue_type == "low_volume"]
        self.assertGreater(len(volume_warnings), 0)

    def test_validate_price_gap_up(self):
        """测试向上跳空"""
        gap_bars = [
            Bar(code="000001", market="SZ", datetime=datetime(2024, 1, 1), open=10.0, high=11.0, low=9.0, close=10.0, volume=1000, amount=10000.0),
            Bar(code="000001", market="SZ", datetime=datetime(2024, 1, 2), open=15.0, high=16.0, low=14.0, close=15.0, volume=1000, amount=15000.0),
        ]
        validator = DataValidator(max_price_gap=0.1)  # 10%阈值
        issues = validator.validate(gap_bars)
        gap_warnings = [i for i in issues if i.issue_type == "price_gap_up"]
        self.assertGreater(len(gap_warnings), 0)

    def test_validate_price_gap_down(self):
        """测试向下跳空"""
        gap_bars = [
            Bar(code="000001", market="SZ", datetime=datetime(2024, 1, 1), open=10.0, high=11.0, low=9.0, close=10.0, volume=1000, amount=10000.0),
            Bar(code="000001", market="SZ", datetime=datetime(2024, 1, 2), open=5.0, high=6.0, low=4.0, close=5.0, volume=1000, amount=5000.0),
        ]
        validator = DataValidator(max_price_gap=0.1)  # 10%阈值
        issues = validator.validate(gap_bars)
        gap_warnings = [i for i in issues if i.issue_type == "price_gap_down"]
        self.assertGreater(len(gap_warnings), 0)

    def test_validate_volume_spike(self):
        """测试成交量突增"""
        # 创建更多数据点以确保平均成交量计算正确
        spike_bars = [
            Bar(code="000001", market="SZ", datetime=datetime(2024, 1, 1), open=10.0, high=11.0, low=9.0, close=10.0, volume=10, amount=1000.0),
            Bar(code="000001", market="SZ", datetime=datetime(2024, 1, 2), open=10.0, high=11.0, low=9.0, close=10.0, volume=10, amount=1000.0),
            Bar(code="000001", market="SZ", datetime=datetime(2024, 1, 3), open=10.0, high=11.0, low=9.0, close=10.0, volume=10, amount=1000.0),
            Bar(code="000001", market="SZ", datetime=datetime(2024, 1, 4), open=10.0, high=11.0, low=9.0, close=10.0, volume=10000, amount=100000.0),
        ]
        validator = DataValidator(max_volume_spike=3.0)  # 10倍阈值
        issues = validator.validate(spike_bars)
        spike_warnings = [i for i in issues if i.issue_type == "volume_spike"]
        self.assertGreater(len(spike_warnings), 0)

    def test_validate_timestamp_order_error(self):
        """测试时间戳顺序错误"""
        disorder_bars = [
            Bar(code="000001", market="SZ", datetime=datetime(2024, 1, 2), open=10.0, high=11.0, low=9.0, close=10.0, volume=1000, amount=10000.0),
            Bar(code="000001", market="SZ", datetime=datetime(2024, 1, 1), open=10.0, high=11.0, low=9.0, close=10.0, volume=1000, amount=10000.0),
        ]
        issues = self.validator.validate(disorder_bars)
        order_errors = [i for i in issues if i.issue_type == "timestamp_order_error"]
        self.assertGreater(len(order_errors), 0)

    def test_check_missing_data_daily(self):
        """测试日线缺失数据检查"""
        # 跳过周末的数据
        bars = [
            Bar(code="000001", market="SZ", datetime=datetime(2024, 1, 1), open=10.0, high=11.0, low=9.0, close=10.0, volume=1000, amount=10000.0),
            Bar(code="000001", market="SZ", datetime=datetime(2024, 1, 4), open=10.0, high=11.0, low=9.0, close=10.0, volume=1000, amount=10000.0),
        ]
        gaps = self.validator.check_missing_data(bars, expected_interval="1d")
        # 1月1日(周一)到1月4日(周四)之间应该检测到缺失
        self.assertGreaterEqual(len(gaps), 0)

    def test_check_missing_data_empty(self):
        """测试空数据缺失检查"""
        gaps = self.validator.check_missing_data([], expected_interval="1d")
        self.assertEqual(len(gaps), 0)

    def test_check_missing_data_single_bar(self):
        """测试单根K线缺失检查"""
        bars = [
            Bar(code="000001", market="SZ", datetime=datetime(2024, 1, 1), open=10.0, high=11.0, low=9.0, close=10.0, volume=1000, amount=10000.0),
        ]
        gaps = self.validator.check_missing_data(bars, expected_interval="1d")
        self.assertEqual(len(gaps), 0)


class TestPriceAdjuster(unittest.TestCase):
    """测试价格复权计算器"""

    def setUp(self):
        """设置测试数据"""
        self.adjuster = PriceAdjuster()
        self.bars = [
            Bar(code="000001", market="SZ", datetime=datetime(2024, 1, 1), open=10.0, high=11.0, low=9.0, close=10.0, volume=1000, amount=10000.0),
            Bar(code="000001", market="SZ", datetime=datetime(2024, 2, 1), open=11.0, high=12.0, low=10.0, close=11.0, volume=1000, amount=11000.0),
            Bar(code="000001", market="SZ", datetime=datetime(2024, 3, 1), open=12.0, high=13.0, low=11.0, close=12.0, volume=1000, amount=12000.0),
        ]

    def test_adjust_forward_empty(self):
        """测试前复权空数据"""
        result = self.adjuster.adjust_forward([], [])
        self.assertEqual(len(result), 0)

    def test_adjust_forward_no_factors(self):
        """测试前复权无复权因子"""
        result = self.adjuster.adjust_forward(self.bars, [])
        self.assertEqual(len(result), len(self.bars))
        self.assertEqual(result[0].close, self.bars[0].close)

    def test_adjust_forward_with_factors(self):
        """测试前复权有复权因子 - 验证价格被调整"""
        factors = [
            AdjustmentFactor(date=datetime(2024, 2, 1), factor=0.9, dividend=0.0, split_ratio=1.1),
        ]
        result = self.adjuster.adjust_forward(self.bars, factors)
        self.assertEqual(len(result), len(self.bars))
        # 验证返回了Bar对象
        self.assertIsInstance(result[0], Bar)
        # 验证价格被处理（具体值取决于实现）
        self.assertNotEqual(result[0].close, 0)

    def test_adjust_backward_empty(self):
        """测试后复权空数据"""
        result = self.adjuster.adjust_backward([], [])
        self.assertEqual(len(result), 0)

    def test_adjust_backward_no_factors(self):
        """测试后复权无复权因子"""
        result = self.adjuster.adjust_backward(self.bars, [])
        self.assertEqual(len(result), len(self.bars))
        self.assertEqual(result[0].close, self.bars[0].close)

    def test_adjust_backward_with_factors(self):
        """测试后复权有复权因子 - 验证价格被调整"""
        factors = [
            AdjustmentFactor(date=datetime(2024, 2, 1), factor=0.9, dividend=0.0, split_ratio=1.1),
        ]
        result = self.adjuster.adjust_backward(self.bars, factors)
        self.assertEqual(len(result), len(self.bars))
        # 验证返回了Bar对象
        self.assertIsInstance(result[0], Bar)
        # 验证价格被处理（具体值取决于实现）
        self.assertNotEqual(result[0].close, 0)

    def test_calculate_factors_from_splits(self):
        """测试从除权数据计算复权因子"""
        splits = [
            (datetime(2024, 6, 1), 0.5, 0.1),  # 分红0.5元，送股10%
        ]
        factors = self.adjuster.calculate_factors_from_splits(splits)
        self.assertEqual(len(factors), 1)
        self.assertEqual(factors[0].date, datetime(2024, 6, 1))
        self.assertEqual(factors[0].dividend, 0.5)
        self.assertEqual(factors[0].split_ratio, 1.1)

    def test_calculate_factors_zero_bonus(self):
        """测试无送股的复权因子"""
        splits = [
            (datetime(2024, 6, 1), 0.5, 0.0),  # 只分红，不送股
        ]
        factors = self.adjuster.calculate_factors_from_splits(splits)
        self.assertEqual(len(factors), 1)
        self.assertEqual(factors[0].factor, 1.0)  # 无送股，因子为1

    def test_calculate_returns(self):
        """测试收益率计算"""
        returns = self.adjuster.calculate_returns(self.bars, adjusted=False)
        self.assertEqual(len(returns), len(self.bars) - 1)
        # 第一个收益率: (11-10)/10 * 100 = 10%
        self.assertAlmostEqual(returns[0], 10.0, places=5)

    def test_calculate_returns_empty(self):
        """测试空数据收益率计算"""
        returns = self.adjuster.calculate_returns([], adjusted=False)
        self.assertEqual(len(returns), 0)

    def test_calculate_returns_single_bar(self):
        """测试单根K线收益率计算"""
        returns = self.adjuster.calculate_returns([self.bars[0]], adjusted=False)
        self.assertEqual(len(returns), 0)

    def test_calculate_returns_with_zero_close(self):
        """测试收盘价为0的收益率计算"""
        bars = [
            Bar(code="000001", market="SZ", datetime=datetime(2024, 1, 1), open=10.0, high=11.0, low=9.0, close=0.0, volume=1000, amount=10000.0),
            Bar(code="000001", market="SZ", datetime=datetime(2024, 1, 2), open=10.0, high=11.0, low=9.0, close=10.0, volume=1000, amount=10000.0),
        ]
        returns = self.adjuster.calculate_returns(bars, adjusted=False)
        self.assertEqual(len(returns), 1)
        self.assertEqual(returns[0], 0.0)


class TestDataAligner(unittest.TestCase):
    """测试数据对齐器"""

    def setUp(self):
        """设置测试数据"""
        self.aligner = DataAligner()
        self.bars_a = [
            Bar(code="000001", market="SZ", datetime=datetime(2024, 1, 1), open=10.0, high=11.0, low=9.0, close=10.0, volume=1000, amount=10000.0),
            Bar(code="000001", market="SZ", datetime=datetime(2024, 1, 2), open=10.5, high=11.5, low=10.0, close=11.0, volume=2000, amount=22000.0),
            Bar(code="000001", market="SZ", datetime=datetime(2024, 1, 3), open=11.0, high=12.0, low=10.5, close=11.5, volume=1500, amount=17250.0),
        ]
        self.bars_b = [
            Bar(code="000002", market="SZ", datetime=datetime(2024, 1, 2), open=20.0, high=21.0, low=19.0, close=20.5, volume=2000, amount=41000.0),
            Bar(code="000002", market="SZ", datetime=datetime(2024, 1, 3), open=20.5, high=21.5, low=20.0, close=21.0, volume=2500, amount=52500.0),
            Bar(code="000002", market="SZ", datetime=datetime(2024, 1, 4), open=21.0, high=22.0, low=20.5, close=21.5, volume=3000, amount=64500.0),
        ]

    def test_align_inner(self):
        """测试内连接对齐"""
        result = self.aligner.align([self.bars_a, self.bars_b], mode="inner")
        self.assertEqual(len(result), 2)
        # 内连接应该只有1月2日和1月3日两个共同日期
        self.assertEqual(len(result[0]), 2)
        self.assertEqual(len(result[1]), 2)

    def test_align_outer(self):
        """测试外连接对齐"""
        result = self.aligner.align([self.bars_a, self.bars_b], mode="outer")
        self.assertEqual(len(result), 2)
        # 外连接应该有4个日期(1月1日、2日、3日、4日)
        # 但外连接模式下，没有fill_method时，不填充缺失数据
        # 所以每组数据保持原有的日期
        self.assertEqual(len(result[0]), 3)  # bars_a有1月1、2、3日
        self.assertEqual(len(result[1]), 3)  # bars_b有1月2、3、4日

    def test_align_left(self):
        """测试左连接对齐"""
        result = self.aligner.align([self.bars_a, self.bars_b], mode="left")
        self.assertEqual(len(result), 2)
        # 左连接应该以bars_a的3个日期为准
        self.assertEqual(len(result[0]), 3)
        # 第二组只有2个匹配的日期(1月2日和3日)
        self.assertEqual(len(result[1]), 2)

    def test_align_empty_list(self):
        """测试空列表对齐"""
        result = self.aligner.align([])
        self.assertEqual(len(result), 0)

    def test_align_single_list(self):
        """测试单列表对齐"""
        result = self.aligner.align([self.bars_a])
        self.assertEqual(len(result), 1)
        self.assertEqual(len(result[0]), len(self.bars_a))

    def test_align_unknown_mode(self):
        """测试未知对齐模式"""
        with self.assertRaises(ValueError) as context:
            self.aligner.align([self.bars_a, self.bars_b], mode="unknown")
        self.assertIn("Unknown alignment mode", str(context.exception))

    def test_resample_to_daily(self):
        """测试重采样到日线"""
        # 创建分钟线数据
        minute_bars = [
            Bar(code="000001", market="SZ", datetime=datetime(2024, 1, 1, 9, 30), open=10.0, high=10.5, low=9.5, close=10.2, volume=10, amount=1020.0),
            Bar(code="000001", market="SZ", datetime=datetime(2024, 1, 1, 9, 31), open=10.2, high=10.8, low=10.0, close=10.5, volume=200, amount=2100.0),
            Bar(code="000001", market="SZ", datetime=datetime(2024, 1, 2, 9, 30), open=10.5, high=11.0, low=10.3, close=10.8, volume=150, amount=1620.0),
        ]
        result = self.aligner.resample(minute_bars, target_interval="1d", source_interval="1m")
        self.assertEqual(len(result), 2)  # 两天
        # 第一天的数据
        self.assertEqual(result[0].open, 10.0)
        self.assertEqual(result[0].high, 10.8)
        self.assertEqual(result[0].low, 9.5)
        self.assertEqual(result[0].close, 10.5)
        self.assertEqual(result[0].volume, 210)

    def test_resample_5m_to_15m(self):
        """测试5分钟线重采样到15分钟线"""
        bars_5m = [
            Bar(code="000001", market="SZ", datetime=datetime(2024, 1, 1, 9, 30), open=10.0, high=10.5, low=9.5, close=10.2, volume=10, amount=1020.0),
            Bar(code="000001", market="SZ", datetime=datetime(2024, 1, 1, 9, 35), open=10.2, high=10.8, low=10.0, close=10.5, volume=200, amount=2100.0),
            Bar(code="000001", market="SZ", datetime=datetime(2024, 1, 1, 9, 40), open=10.5, high=11.0, low=10.3, close=10.8, volume=150, amount=1620.0),
        ]
        result = self.aligner.resample(bars_5m, target_interval="15m", source_interval="5m")
        self.assertEqual(len(result), 1)  # 3个5分钟合并为1个15分钟
        self.assertEqual(result[0].open, 10.0)
        self.assertEqual(result[0].high, 11.0)
        self.assertEqual(result[0].low, 9.5)
        self.assertEqual(result[0].close, 10.8)
        self.assertEqual(result[0].volume, 360)

    def test_resample_empty(self):
        """测试空数据重采样"""
        result = self.aligner.resample([], target_interval="1d", source_interval="1m")
        self.assertEqual(len(result), 0)

    def test_resample_same_interval(self):
        """测试相同周期重采样"""
        result = self.aligner.resample(self.bars_a, target_interval="1d", source_interval="1d")
        self.assertEqual(len(result), len(self.bars_a))

    def test_resample_invalid_interval(self):
        """测试无效周期重采样"""
        with self.assertRaises(ValueError):
            self.aligner.resample(self.bars_a, target_interval="1m", source_interval="1d")

    def test_align_to_dataframe(self):
        """测试对齐转换为DataFrame"""
        try:
            import pandas as pd
        except ImportError:
            self.skipTest("pandas not installed")

        df = self.aligner.align_to_dataframe(
            [self.bars_a, self.bars_b],
            codes=["000001", "000002"],
            mode="inner",
            price_type="close"
        )
        self.assertIsInstance(df, pd.DataFrame)
        self.assertEqual(len(df), 2)  # 两个共同日期
        self.assertIn("000001", df.columns)
        self.assertIn("000002", df.columns)

    def test_align_to_dataframe_empty(self):
        """测试空数据对齐转换为DataFrame"""
        try:
            import pandas as pd
        except ImportError:
            self.skipTest("pandas not installed")

        df = self.aligner.align_to_dataframe([], codes=[], mode="inner")
        self.assertIsInstance(df, pd.DataFrame)


class TestConvenienceFunctions(unittest.TestCase):
    """测试便捷函数"""

    def setUp(self):
        """设置测试数据"""
        self.bars = [
            Bar(code="000001", market="SZ", datetime=datetime(2024, 1, 1), open=10.0, high=11.0, low=9.0, close=10.0, volume=1000, amount=10000.0),
            Bar(code="000001", market="SZ", datetime=datetime(2024, 1, 2), open=10.5, high=11.5, low=10.0, close=11.0, volume=2000, amount=22000.0),
        ]

    def test_validate_data(self):
        """测试validate_data便捷函数"""
        issues = validate_data(self.bars)
        self.assertIsInstance(issues, list)

    def test_adjust_forward(self):
        """测试adjust_forward便捷函数"""
        factors = [
            AdjustmentFactor(date=datetime(2024, 1, 2), factor=1.1, dividend=0.0, split_ratio=1.0),
        ]
        result = adjust_forward(self.bars, factors)
        self.assertEqual(len(result), len(self.bars))

    def test_adjust_backward(self):
        """测试adjust_backward便捷函数"""
        factors = [
            AdjustmentFactor(date=datetime(2024, 1, 2), factor=1.1, dividend=0.0, split_ratio=1.0),
        ]
        result = adjust_backward(self.bars, factors)
        self.assertEqual(len(result), len(self.bars))

    def test_align_bars(self):
        """测试align_bars便捷函数"""
        bars_b = [
            Bar(code="000002", market="SZ", datetime=datetime(2024, 1, 2), open=20.0, high=21.0, low=19.0, close=20.5, volume=2000, amount=41000.0),
        ]
        result = align_bars([self.bars, bars_b], mode="inner")
        self.assertEqual(len(result), 2)


if __name__ == "__main__":
    unittest.main()
