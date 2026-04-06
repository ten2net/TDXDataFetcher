"""
边界测试和集成测试（不含网络）
"""

import pytest
from tdxapi.protocol.constants import DEFAULT_SERVERS, Category, Market


class TestConstants:
    def test_default_servers_not_empty(self):
        assert len(DEFAULT_SERVERS) > 0
        for ip, port in DEFAULT_SERVERS:
            assert isinstance(ip, str)
            assert isinstance(port, int)
            assert port > 0
            assert "." in ip

    def test_category_values(self):
        assert Category.RI == 9
        assert Category.ZHOU == 5
        assert Category.YUE == 6
        assert Category.FENZHONG_5 == 0

    def test_market_values(self):
        assert Market.SH.value == 1
        assert Market.SZ.value == 0


class TestPacketHeader:
    def test_rsp_header_pack_unpack_roundtrip(self):
        from tdxapi.protocol.packet import RspHeader

        h = RspHeader(
            unknown1=0x12345678,
            seq_id=0x87654321,
            zip_size=1000,
            unzip_size=800,
            unknown2=0xABCD,
        )
        data = h.pack()
        h2 = RspHeader.unpack(data)
        assert h2.unknown1 == 0x12345678
        assert h2.seq_id == 0x87654321
        assert h2.zip_size == 1000
        assert h2.unzip_size == 800

    def test_decode_no_compress(self):
        from tdxapi.protocol.packet import decode_response_body, RspHeader

        body = b"hello world"
        header = RspHeader(zip_size=len(body), unzip_size=len(body))
        result = decode_response_body(body, header)
        assert result == body

    def test_decode_with_zlib(self):
        import zlib
        from tdxapi.protocol.packet import decode_response_body, RspHeader

        original = b"hello world" * 100
        compressed = zlib.compress(original)
        header = RspHeader(zip_size=len(compressed), unzip_size=len(original))
        result = decode_response_body(bytes(compressed), header)
        assert result == original


class TestClientInit:
    def test_default_init(self):
        from tdxapi.network.client import TdxClient

        client = TdxClient()
        assert client._ip is None
        assert client._port == 7709
        assert client._timeout == 5
        assert client._auto_reconnect is True
        assert client._max_retries == 3

    def test_custom_init(self):
        from tdxapi.network.client import TdxClient

        client = TdxClient(
            ip="1.2.3.4", port=8888, timeout=10, auto_reconnect=False, max_retries=5
        )
        assert client._ip == "1.2.3.4"
        assert client._port == 8888
        assert client._timeout == 10
        assert client._auto_reconnect is False
        assert client._max_retries == 5

    def test_context_manager_without_connect(self):
        from tdxapi.network.client import TdxClient

        client = TdxClient()
        try:
            with client:
                pass
        except Exception:
            pass


class TestHelpers:
    def test_code_to_market_6(self):
        from tdxapi.utils.helpers import code_to_market

        assert code_to_market("600519") == 1
        assert code_to_market("601888") == 1

    def test_code_to_market_0(self):
        from tdxapi.utils.helpers import code_to_market

        assert code_to_market("000001") == 0
        assert code_to_market("002001") == 0
        assert code_to_market("300750") == 0

    def test_code_to_market_8(self):
        from tdxapi.utils.helpers import code_to_market

        assert code_to_market("830001") == 0

    def test_market_to_str(self):
        from tdxapi.utils.helpers import market_to_str

        assert market_to_str(1) == "SH"
        assert market_to_str(0) == "SZ"

    def test_format_volume_edge(self):
        from tdxapi.utils.helpers import format_volume

        assert format_volume(0) == "0"
        assert format_volume(9999) == "9999"
        assert format_volume(10000) == "1.00万"
        assert format_volume(100000000) == "1.00亿"

    def test_format_amount_edge(self):
        from tdxapi.utils.helpers import format_amount

        assert format_amount(0.0) == "0.00"
        assert format_amount(9999.5) == "9999.50"


class TestEncodingEdgeCases:
    def test_quote_request_all_markets(self):
        from tdxapi.protocol.requests import build_quote_request

        stocks = [(0, "000001"), (1, "600519"), (0, "300750")]
        pkg = build_quote_request(stocks)
        assert len(pkg) > 0

    def test_quote_request_empty(self):
        from tdxapi.protocol.requests import build_quote_request

        pkg = build_quote_request([])
        assert len(pkg) > 0

    def test_bars_request_all_periods(self):
        from tdxapi.protocol.requests import build_bars_request

        for cat in [0, 1, 2, 3, 5, 6, 9, 10, 11]:
            pkg = build_bars_request(
                category=cat, market=0, code="000001", start=0, count=10
            )
            assert len(pkg) == 38

    def test_long_code(self):
        from tdxapi.protocol.requests import build_quote_request

        pkg = build_quote_request([(1, "6019888")])
        assert len(pkg) > 0


class TestModuleImports:
    def test_import_tdxapi(self):
        import tdxapi

        assert hasattr(tdxapi, "TdxClient")
        assert hasattr(tdxapi, "StockQuote")
        assert hasattr(tdxapi, "Bar")
        assert hasattr(tdxapi, "Tick")

    def test_import_protocol(self):
        from tdxapi.protocol import constants, packet, requests

        assert hasattr(constants, "DEFAULT_SERVERS")
        assert hasattr(packet, "RspHeader")
        assert hasattr(requests, "build_quote_request")

    def test_import_parser(self):
        from tdxapi.parser import parse_quotes, parse_bars, parse_ticks

    def test_import_models(self):
        from tdxapi.models import StockQuote, Bar, Tick

    def test_version(self):
        import tdxapi

        assert tdxapi.__version__ == "0.1.0"
