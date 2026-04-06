"""
解析器测试（使用模拟二进制数据）
"""

import struct
import zlib
import pytest
from tdxapi.parser.quote_parser import (
    parse_quotes,
    parse_bars,
    parse_ticks,
    parse_minute_time,
    parse_security_list,
    parse_stock_count,
    parse_xdxr_info,
    parse_finance_info,
    _encode_price,
)


class TestParseQuotes:
    """解析实时行情"""

    def _build_quote_response(
        self, market: int, code: str, base_price: int, last_close_diff: int
    ):
        """使用 pytdx 变长整型编码构造行情响应"""
        body = bytearray()
        body += b"\xb1\xcb"
        body += struct.pack("<H", 1)
        body += struct.pack("<B6sH", market, code.encode("ascii").ljust(6, b"\x00"), 0)
        body += _encode_price(base_price)
        body += _encode_price(last_close_diff)
        for _ in range(28):
            body += _encode_price(0)
        body += struct.pack("<I", 1000000)
        body += _encode_price(0)
        body += _encode_price(0)
        for _ in range(5):
            body += _encode_price(0)
            body += _encode_price(0)
            body += _encode_price(0)
        for _ in range(8):
            body += _encode_price(0)
        body += b"\x00" * 6
        return bytes(body)

    def test_parse_single_stock(self):
        body = self._build_quote_response(1, "600519", 180000, -1000)
        results = parse_quotes(body)
        assert len(results) == 1
        assert results[0].code == "600519"
        assert results[0].market == "SH"
        assert results[0].price == 1800.0
        assert results[0].last_close == 1790.0

    def test_parse_sz_stock(self):
        body = self._build_quote_response(0, "000001", 12000, -50)
        results = parse_quotes(body)
        assert len(results) == 1
        assert results[0].market == "SZ"
        assert results[0].price == 120.0
        assert results[0].last_close == 119.5

    def test_empty_response(self):
        body = b"\xb1\xcb\x00\x00"
        results = parse_quotes(body)
        assert len(results) == 0

    def test_multi_byte_code(self):
        body = self._build_quote_response(1, "688001", 5000, 0)
        results = parse_quotes(body)
        assert results[0].code == "688001"
        assert results[0].price == 50.0


class TestParseBars:
    """解析K线数据"""

    def _build_bars_response(self, bars_data: list, category: int = 9) -> bytes:
        body = struct.pack("<H", len(bars_data))
        pre_diff_base = 0
        for year, month, day, open_p, close_p in bars_data:
            if category in (9, 5, 6, 10, 11):
                date_val = year * 10000 + month * 100 + day
                body += struct.pack("<I", date_val)
            else:
                body += struct.pack("<IHH", year, month, day)
            open_diff_total = pre_diff_base + open_p
            close_diff = open_p + close_p - open_diff_total
            high_diff = close_diff + 10
            low_diff = close_diff - 10
            body += _encode_price(open_diff_total)
            body += _encode_price(close_diff)
            body += _encode_price(high_diff)
            body += _encode_price(low_diff)
            body += struct.pack("<I", 1000000)
            body += struct.pack("<I", 100000000)
            pre_diff_base = open_diff_total
        return bytes(body)

    def test_parse_daily_bars(self):
        bars = [(2025, 1, 1, 1000, 10), (2025, 1, 2, 1010, 20)]
        body = self._build_bars_response(bars, category=9)
        results = parse_bars(body, category=9)
        assert len(results) == 2
        assert results[0].datetime.year == 2025
        assert results[0].close == pytest.approx(1.01, rel=0.1)

    def test_empty_bars(self):
        body = struct.pack("<H", 0)
        results = parse_bars(body, category=9)
        assert len(results) == 0

    def test_minute_bars(self):
        bars = [(930, 0, 0, 1000, 1010)]
        body = self._build_bars_response(bars, category=0)
        results = parse_bars(body, category=0)
        assert len(results) == 1


class TestParseSecurityList:
    def _build_list_response(self, stocks: list) -> bytes:
        body = struct.pack("<H", len(stocks))
        for code, name, decimal, pre_close in stocks:
            body += code.encode("ascii").ljust(6, b"\x00")
            body += name.encode("gbk").ljust(8, b"\x00")
            body += b"\x00" * 12
            body += struct.pack("<B", decimal)
            body += struct.pack("<H", pre_close)
        return bytes(body)

    def test_parse_stock_list(self):
        stocks = [
            ("600519", "贵州茅台", 2, 18000),
            ("000001", "平安银行", 2, 12000),
        ]
        body = self._build_list_response(stocks)
        results = parse_security_list(body, market=1)
        assert len(results) == 2
        assert results[0]["code"] == "600519"
        assert results[0]["name"] == "贵州茅台"
        assert results[0]["pre_close"] == 180.0

    def test_empty_list(self):
        body = struct.pack("<H", 0)
        results = parse_security_list(body, market=1)
        assert len(results) == 0


class TestParseStockCount:
    def test_parse_count(self):
        body = struct.pack("<H", 4500)
        assert parse_stock_count(body) == 4500

    def test_parse_zero(self):
        body = struct.pack("<H", 0)
        assert parse_stock_count(body) == 0


class TestParseMinuteTime:
    def _build_minute_response(self, data: list) -> bytes:
        body = struct.pack("<H", len(data))
        body += b"\x00\x00\x00\x00"
        for price_diff, vol in data:
            body += _encode_price(price_diff * 100)
            body += _encode_price(0)
            body += _encode_price(vol)
        return bytes(body)

    def test_parse_minute_time(self):
        data = [(18, 100), (19, 150)]
        body = self._build_minute_response(data)
        results = parse_minute_time(body)
        assert len(results) == 2
        assert results[0]["price"] == 18.0
        assert results[0]["volume"] == 100


class TestParseXdXr:
    def _build_xdxr_response(self, records: list) -> bytes:
        body = struct.pack("<H", len(records))
        for date, add_count, add_price in records:
            body += struct.pack("<III", date, add_count, add_price)
        return bytes(body)

    def test_parse_xdxr(self):
        records = [(20230101, 100, 1500), (20230601, 50, 2000)]
        body = self._build_xdxr_response(records)
        results = parse_xdxr_info(body)
        assert len(results) == 2
        assert results[0]["date"] == "20230101"
        assert results[0]["add_price"] == 1.5


class TestParseFinance:
    def test_parse_finance(self):
        body = struct.pack("<15I", *[1000000] * 15)
        result = parse_finance_info(body)
        assert "total_shares" in result
        assert "eps" in result
        assert result["total_shares"] == 1000000
