"""
技术指标计算模块

提供常用的技术分析指标计算功能，包括成交量指标、移动平均线等。
"""

from typing import List, Optional
from tdxapi.models import Bar


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
