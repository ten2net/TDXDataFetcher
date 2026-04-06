"""
测试协议数据包（基于 pytdx 校准后的真实协议）
"""

import struct
import pytest
from tdxapi.protocol.packet import RspHeader, decode_response_body
from tdxapi.protocol.requests import (
    build_quote_request,
    build_bars_request,
    build_stock_count_request,
    build_security_list_request,
)
from tdxapi.protocol.constants import RSP_HEADER_LEN


class TestRspHeader:
    def test_pack_unpack(self):
        header = RspHeader(
            unknown1=0x0074CBB1,
            seq_id=0x0C010864,
            zip_size=100,
            unzip_size=100,
            unknown2=0x00AA,
        )
        packed = header.pack()
        assert len(packed) == RSP_HEADER_LEN

        unpacked = RspHeader.unpack(packed)
        assert unpacked.unknown1 == 0x0074CBB1
        assert unpacked.seq_id == 0x0C010864
        assert unpacked.zip_size == 100
        assert unpacked.unzip_size == 100

    def test_header_size(self):
        """响应包头固定 16 字节"""
        header = RspHeader()
        assert len(header.pack()) == 16


class TestDecodeResponse:
    def test_no_decompression(self):
        """zip_size == unzip_size 时不解压"""
        body = b"\x00\x01\x02\x03"
        header = RspHeader(zip_size=4, unzip_size=4)
        result = decode_response_body(body, header)
        assert result == body

    def test_with_decompression(self):
        """zip_size != unzip_size 时需要 zlib 解压"""
        import zlib

        original = b"hello world" * 100
        compressed = zlib.compress(original)
        header = RspHeader(zip_size=len(compressed), unzip_size=len(original))
        result = decode_response_body(compressed, header)
        assert result == original


class TestBuildQuoteRequest:
    def test_single_stock(self):
        """单只股票行情请求"""
        pkg = build_quote_request([(1, "600519")])
        assert len(pkg) > 0
        # 包头 10 字节 + 包体
        magic = struct.unpack_from("<H", pkg, 0)[0]
        assert magic == 0x010C

    def test_multiple_stocks(self):
        """多只股票行情请求"""
        pkg = build_quote_request(
            [
                (1, "600519"),
                (0, "000001"),
                (0, "300750"),
            ]
        )
        assert len(pkg) > 0

    def test_pkg_len_consistency(self):
        """pkg_len = stock_len * 7 + 12（协议定义值，非 body 字节数）"""
        stocks = [(1, "600519"), (0, "000001")]
        pkg = build_quote_request(stocks)
        pkg_len = struct.unpack_from("<H", pkg, 6)[0]
        expected_len = len(stocks) * 7 + 12
        assert pkg_len == expected_len


class TestBuildBarsRequest:
    def test_daily_bars(self):
        """日线请求包"""
        pkg = build_bars_request(
            category=9, market=1, code="600519", start=0, count=100
        )
        assert len(pkg) > 0
        # 验证 magic
        magic = struct.unpack_from("<H", pkg, 0)[0]
        assert magic == 0x010C

    def test_minute_bars(self):
        """5分钟线请求包"""
        pkg = build_bars_request(category=0, market=0, code="000001", start=0, count=50)
        assert len(pkg) > 0

    def test_fixed_size(self):
        """K线请求包长度固定"""
        pkg = build_bars_request(
            category=9, market=1, code="600519", start=0, count=100
        )
        # <HIHHHH6sHHHHIIH> = 2+4+2+2+2+2+6+2+2+2+2+4+4+2 = 38
        assert len(pkg) == 38


class TestBuildStockCountRequest:
    def test_sh_count(self):
        pkg = build_stock_count_request(1)
        assert len(pkg) > 0

    def test_sz_count(self):
        pkg = build_stock_count_request(0)
        assert len(pkg) > 0


class TestBuildSecurityListRequest:
    def test_sh_list(self):
        pkg = build_security_list_request(1, 0)
        assert len(pkg) > 0

    def test_sz_list(self):
        pkg = build_security_list_request(0, 0)
        assert len(pkg) > 0
