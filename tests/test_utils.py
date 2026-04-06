"""
测试工具函数
"""

import pytest
from tdxapi.utils.helpers import (
    code_to_market,
    market_to_str,
    format_volume,
    format_amount,
)


class TestCodeToMarket:
    def test_sh_stock(self):
        assert code_to_market("600519") == 1
        assert code_to_market("688001") == 1

    def test_sz_stock(self):
        assert code_to_market("000001") == 0
        assert code_to_market("002001") == 0
        assert code_to_market("300001") == 0

    def test_bj_stock(self):
        assert code_to_market("830001") == 0
        assert code_to_market("430001") == 0


class TestMarketToStr:
    def test_sh(self):
        assert market_to_str(1) == "SH"

    def test_sz(self):
        assert market_to_str(0) == "SZ"


class TestFormatVolume:
    def test_small(self):
        assert format_volume(100) == "100"

    def test_wan(self):
        assert format_volume(50000) == "5.00万"

    def test_yi(self):
        assert format_volume(150000000) == "1.50亿"


class TestFormatAmount:
    def test_small(self):
        assert format_amount(999.5) == "999.50"

    def test_wan(self):
        assert format_amount(50000.0) == "5.00万"

    def test_yi(self):
        assert format_amount(150000000.0) == "1.50亿"
