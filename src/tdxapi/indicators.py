"""
技术指标计算模块

提供常用的技术分析指标计算功能，包括成交量指标、移动平均线等。
"""

from typing import List, Optional, Union
from dataclasses import dataclass

try:
    import numpy as np
    HAS_NUMPY = True
except ImportError:
    HAS_NUMPY = False

from tdxapi.models import Bar


# 类型别名
PriceData = Union[List[float], List[Bar]]


def _extract_prices(data: PriceData, price_type: str = "close") -> List[float]:
    """
    从 Bar 列表或价格列表中提取价格数据。

    Args:
        data: Bar 列表或价格列表
        price_type: 价格类型，可选 "open", "high", "low", "close"

    Returns:
        价格列表
    """
    if not data:
        return []

    if isinstance(data[0], Bar):
        if price_type == "open":
            return [b.open for b in data]  # type: ignore
        elif price_type == "high":
            return [b.high for b in data]  # type: ignore
        elif price_type == "low":
            return [b.low for b in data]  # type: ignore
        else:  # close
            return [b.close for b in data]  # type: ignore
    else:
        return data  # type: ignore


def vol(bars: List[Bar]) -> List[int]:
    """
    计算成交量 (Volume)

    成交量指标直接返回每根K线的成交量数据。

    Args:
        bars: K线数据列表

    Returns:
        成交量列表，与输入bars长度相同

    Example:
        >>> bars = [Bar(..., volume=1000, ...), Bar(..., volume=2000, ...)]
        >>> vol(bars)
        [1000, 2000]
    """
    return [bar.volume for bar in bars]


def obv(bars: List[Bar]) -> List[int]:
    """
    计算能量潮指标 (On Balance Volume, OBV)

    OBV指标通过统计成交量变动的趋势来推测股价趋势。
    计算公式：
    - 如果当日收盘价 > 前日收盘价，则 OBV = 前日OBV + 当日成交量
    - 如果当日收盘价 < 前日收盘价，则 OBV = 前日OBV - 当日成交量
    - 如果当日收盘价 = 前日收盘价，则 OBV = 前日OBV

    Args:
        bars: K线数据列表，按时间顺序排列（从早到晚）

    Returns:
        OBV值列表，与输入bars长度相同。第一个值为第一个bar的成交量。

    Example:
        >>> bars = [
        ...     Bar(..., close=10.0, volume=1000, ...),
        ...     Bar(..., close=11.0, volume=2000, ...),  # 上涨
        ...     Bar(..., close=10.5, volume=1500, ...),  # 下跌
        ... ]
        >>> obv(bars)
        [1000, 3000, 1500]
    """
    if not bars:
        return []

    result: List[int] = [bars[0].volume]

    for i in range(1, len(bars)):
        prev_close = bars[i - 1].close
        curr_close = bars[i].close
        curr_volume = bars[i].volume

        if curr_close > prev_close:
            # 上涨，累加成交量
            result.append(result[-1] + curr_volume)
        elif curr_close < prev_close:
            # 下跌，累减成交量
            result.append(result[-1] - curr_volume)
        else:
            # 平盘，OBV不变
            result.append(result[-1])

    return result


def vol_ma(bars: List[Bar], period: int) -> List[Optional[float]]:
    """
    计算成交量移动平均线 (Volume Moving Average)

    Args:
        bars: K线数据列表
        period: 计算周期，必须大于0

    Returns:
        成交量移动平均值列表，与输入bars长度相同。
        前period-1个值为None（因为数据不足），从第period个值开始为有效平均值。

    Raises:
        ValueError: 当period <= 0时

    Example:
        >>> bars = [Bar(..., volume=1000, ...), Bar(..., volume=2000, ...), ...]
        >>> vol_ma(bars, 5)  # 5日成交量均线
        [None, None, None, None, 1500.0, ...]
    """
    if period <= 0:
        raise ValueError(f"period must be positive, got {period}")

    if not bars:
        return []

    volumes = [bar.volume for bar in bars]
    result: List[Optional[float]] = []

    for i in range(len(volumes)):
        if i < period - 1:
            # 数据不足，返回None
            result.append(None)
        else:
            # 计算移动平均
            window = volumes[i - period + 1:i + 1]
            result.append(sum(window) / period)

    return result


def ma(data: PriceData, period: int, price_type: str = "close") -> List[Optional[float]]:
    """
    计算简单移动平均线 (Simple Moving Average, SMA)。

    对于数据不足 period 个的位置，返回 None。

    Args:
        data: Bar 列表或价格列表
        period: 移动平均周期
        price_type: 价格类型，可选 "open", "high", "low", "close"（仅对 Bar 列表有效）

    Returns:
        移动平均线值列表，长度与输入相同，前 period-1 个为 None

    Example:
        >>> prices = [10.0, 11.0, 12.0, 13.0, 14.0]
        >>> ma(prices, 3)
        [None, None, 11.0, 12.0, 13.0]
    """
    if period <= 0:
        raise ValueError(f"period must be positive, got {period}")

    prices = _extract_prices(data, price_type)
    n = len(prices)

    if n == 0:
        return []

    # 纯 Python 实现（与 numpy 实现结果一致且更易理解）
    result: List[Optional[float]] = []
    for i in range(n):
        if i < period - 1:
            result.append(None)
        else:
            window = prices[i - period + 1:i + 1]
            result.append(sum(window) / period)
    return result


def ema(data: PriceData, period: int, price_type: str = "close") -> List[Optional[float]]:
    """
    计算指数移动平均线 (Exponential Moving Average, EMA)。

    EMA 公式: EMA_today = alpha * price_today + (1 - alpha) * EMA_yesterday
    其中 alpha = 2 / (period + 1)

    对于第一个数据点，使用第一个价格作为初始值。

    Args:
        data: Bar 列表或价格列表
        period: 移动平均周期
        price_type: 价格类型，可选 "open", "high", "low", "close"（仅对 Bar 列表有效）

    Returns:
        指数移动平均线值列表，长度与输入相同

    Example:
        >>> prices = [10.0, 11.0, 12.0, 13.0, 14.0]
        >>> ema(prices, 3)
        [10.0, 10.5, 11.25, 12.125, 13.0625]
    """
    if period <= 0:
        raise ValueError(f"period must be positive, got {period}")

    prices = _extract_prices(data, price_type)
    n = len(prices)

    if n == 0:
        return []

    alpha = 2.0 / (period + 1)
    result: List[Optional[float]] = []

    if HAS_NUMPY:
        # 使用 numpy 高效计算
        prices_arr = np.array(prices, dtype=np.float64)
        ema_values = np.zeros(n, dtype=np.float64)
        # 第一个值使用第一个价格
        ema_values[0] = prices_arr[0]
        for i in range(1, n):
            ema_values[i] = alpha * prices_arr[i] + (1 - alpha) * ema_values[i - 1]
        return ema_values.tolist()
    else:
        # 纯 Python 实现
        # 第一个值使用第一个价格
        result.append(prices[0])
        prev_ema = prices[0]
        for i in range(1, n):
            curr_ema = alpha * prices[i] + (1 - alpha) * prev_ema
            result.append(curr_ema)
            prev_ema = curr_ema
        return result


def wma(data: PriceData, period: int, price_type: str = "close") -> List[Optional[float]]:
    """
    计算加权移动平均线 (Weighted Moving Average, WMA)。

    权重线性递减，最近的数据权重最高。
    权重公式: weight_i = i / sum(1..period)

    Args:
        data: Bar 列表或价格列表
        period: 移动平均周期
        price_type: 价格类型，可选 "open", "high", "low", "close"（仅对 Bar 列表有效）

    Returns:
        加权移动平均线值列表，长度与输入相同，前 period-1 个为 None

    Example:
        >>> prices = [10.0, 11.0, 12.0, 13.0, 14.0]
        >>> wma(prices, 3)
        [None, None, 11.166..., 12.166..., 13.166...]
    """
    if period <= 0:
        raise ValueError(f"period must be positive, got {period}")

    prices = _extract_prices(data, price_type)
    n = len(prices)

    if n == 0:
        return []

    # 计算权重: 1, 2, ..., period（最近的数据权重最高）
    # 窗口中第一个元素（最旧）权重为1，最后一个元素（最新）权重为period
    weights = list(range(1, period + 1))
    weight_sum = sum(weights)

    result: List[Optional[float]] = []
    for i in range(n):
        if i < period - 1:
            result.append(None)
        else:
            window = prices[i - period + 1:i + 1]
            # zip 会自动按顺序配对：window[0]（最旧）对应 weights[0]=1
            # window[-1]（最新）对应 weights[-1]=period
            weighted_sum = sum(p * w for p, w in zip(window, weights))
            result.append(weighted_sum / weight_sum)
    return result


@dataclass
class MA:
    """
    移动平均线指标类。

    支持常用的移动平均线周期: MA5, MA10, MA20, MA60, MA120, MA250
    支持 SMA(简单移动平均) 和 EMA(指数移动平均) 两种计算方式。

    Attributes:
        period: 移动平均周期
        values: 计算出的移动平均数值列表
        ma_type: 移动平均类型，"sma" 或 "ema"

    Example:
        >>> prices = [10.0, 11.0, 12.0, 13.0, 14.0, 15.0, 16.0, 17.0, 18.0, 19.0]
        >>> ma5 = MA.calculate(prices, 5)
        >>> print(ma5.values)
        [None, None, None, None, 12.0, 13.0, 14.0, 15.0, 16.0, 17.0]
    """

    period: int
    values: List[Optional[float]]
    ma_type: str = "sma"

    @classmethod
    def calculate(
        cls,
        data: PriceData,
        period: int,
        ma_type: str = "sma",
        price_type: str = "close"
    ) -> "MA":
        """
        计算移动平均线。

        Args:
            data: Bar 列表或价格列表
            period: 移动平均周期
            ma_type: 移动平均类型，"sma"(简单移动平均) 或 "ema"(指数移动平均)
            price_type: 价格类型，可选 "open", "high", "low", "close"（仅对 Bar 列表有效）

        Returns:
            MA 实例

        Raises:
            ValueError: 当 ma_type 不是 "sma" 或 "ema" 时
        """
        if ma_type not in ("sma", "ema"):
            raise ValueError(f"ma_type must be 'sma' or 'ema', got '{ma_type}'")

        if ma_type == "sma":
            values = ma(data, period, price_type)
        else:
            values = ema(data, period, price_type)

        return cls(period=period, values=values, ma_type=ma_type)

    @classmethod
    def ma5(cls, data: PriceData, ma_type: str = "sma", price_type: str = "close") -> "MA":
        """计算 MA5"""
        return cls.calculate(data, 5, ma_type, price_type)

    @classmethod
    def ma10(cls, data: PriceData, ma_type: str = "sma", price_type: str = "close") -> "MA":
        """计算 MA10"""
        return cls.calculate(data, 10, ma_type, price_type)

    @classmethod
    def ma20(cls, data: PriceData, ma_type: str = "sma", price_type: str = "close") -> "MA":
        """计算 MA20"""
        return cls.calculate(data, 20, ma_type, price_type)

    @classmethod
    def ma30(cls, data: PriceData, ma_type: str = "sma", price_type: str = "close") -> "MA":
        """计算 MA30"""
        return cls.calculate(data, 30, ma_type, price_type)

    @classmethod
    def ma60(cls, data: PriceData, ma_type: str = "sma", price_type: str = "close") -> "MA":
        """计算 MA60"""
        return cls.calculate(data, 60, ma_type, price_type)

    @classmethod
    def ma120(cls, data: PriceData, ma_type: str = "sma", price_type: str = "close") -> "MA":
        """计算 MA120"""
        return cls.calculate(data, 120, ma_type, price_type)

    @classmethod
    def ma250(cls, data: PriceData, ma_type: str = "sma", price_type: str = "close") -> "MA":
        """计算 MA250 (年线)"""
        return cls.calculate(data, 250, ma_type, price_type)

    def __len__(self) -> int:
        return len(self.values)

    def __getitem__(self, index: int) -> Optional[float]:
        return self.values[index]

    def __repr__(self) -> str:
        non_none_count = sum(1 for v in self.values if v is not None)
        return f"MA({self.period}, {self.ma_type}, {non_none_count}/{len(self.values)} values)"

    def last(self) -> Optional[float]:
        """
        获取最新的移动平均数值。

        Returns:
            最新的 MA 值，如果没有有效值则返回 None
        """
        for v in reversed(self.values):
            if v is not None:
                return v
        return None

    def valid_values(self) -> List[float]:
        """
        获取所有有效的（非 None）移动平均数值。

        Returns:
            有效 MA 值列表
        """
        return [v for v in self.values if v is not None]


def calculate_all_ma(
    data: PriceData,
    periods: Optional[List[int]] = None,
    ma_type: str = "sma",
    price_type: str = "close"
) -> dict:
    """
    批量计算多个周期的移动平均线。

    Args:
        data: Bar 列表或价格列表
        periods: 周期列表，默认为 [5, 10, 20, 60, 120, 250]
        ma_type: 移动平均类型，"sma" 或 "ema"
        price_type: 价格类型，可选 "open", "high", "low", "close"（仅对 Bar 列表有效）

    Returns:
        字典，键为周期，值为 MA 实例

    Example:
        >>> prices = [10.0, 11.0, 12.0, 13.0, 14.0, 15.0]
        >>> result = calculate_all_ma(prices)
        >>> print(result[5].values)
        [None, None, None, None, 12.0, 13.0]
    """
    if periods is None:
        periods = [5, 10, 20, 60, 120, 250]

    return {
        period: MA.calculate(data, period, ma_type, price_type)
        for period in periods
    }
