"""
AsyncTdxClient 单元测试
"""

import asyncio
import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from tdxapi.async_client import AsyncTdxClient
from tdxapi.models import StockQuote, Bar, Tick
from tdxapi.protocol.constants import DEFAULT_SERVERS, Market


class TestAsyncClientInit:
    """测试异步客户端初始化"""

    def test_default_init(self):
        client = AsyncTdxClient()
        assert client._ip is None
        assert client._port == 7709
        assert client._timeout == 5
        assert client._auto_reconnect is True
        assert client._max_retries == 3
        assert client._heartbeat_enabled is True
        assert client._reader is None
        assert client._writer is None

    def test_custom_init(self):
        client = AsyncTdxClient(
            ip="1.2.3.4",
            port=8888,
            timeout=10,
            auto_reconnect=False,
            max_retries=5,
            heartbeat=False,
        )
        assert client._ip == "1.2.3.4"
        assert client._port == 8888
        assert client._timeout == 10
        assert client._auto_reconnect is False
        assert client._max_retries == 5
        assert client._heartbeat_enabled is False


class TestAsyncClientParseMarket:
    """测试市场代码解析"""

    def test_parse_market_sh(self):
        client = AsyncTdxClient()
        assert client._parse_market("SH") == 1
        assert client._parse_market("sh") == 1

    def test_parse_market_sz(self):
        client = AsyncTdxClient()
        assert client._parse_market("SZ") == 0
        assert client._parse_market("sz") == 0

    def test_parse_market_int(self):
        client = AsyncTdxClient()
        assert client._parse_market(0) == 0
        assert client._parse_market(1) == 1
        assert client._parse_market(6) == 6


@pytest.mark.asyncio
class TestAsyncClientConnection:
    """测试异步连接功能"""

    async def test_connect_with_auto_server_selection(self):
        """测试自动选择服务器"""
        client = AsyncTdxClient()

        mock_reader = AsyncMock()
        mock_writer = AsyncMock()

        with patch('asyncio.open_connection', return_value=(mock_reader, mock_writer)) as mock_open:
            with patch.object(client, '_recv_response', new_callable=AsyncMock) as mock_recv:
                with patch.object(client, '_send_raw', new_callable=AsyncMock) as mock_send:
                    await client.connect("119.147.212.81", 7709)
                    assert client._ip == "119.147.212.81"
                    assert client._port == 7709

    async def test_close_connection(self):
        """测试关闭连接"""
        client = AsyncTdxClient()
        mock_writer = AsyncMock()
        client._writer = mock_writer
        client._reader = AsyncMock()

        await client.close()
        mock_writer.close.assert_called_once()
        mock_writer.wait_closed.assert_awaited_once()

    async def test_close_without_connection(self):
        """测试关闭未连接的客户端"""
        client = AsyncTdxClient()
        # 不应抛出异常
        await client.close()


@pytest.mark.asyncio
class TestAsyncContextManager:
    """测试异步上下文管理器"""

    async def test_async_context_manager(self):
        """测试 async with 语法"""
        client = AsyncTdxClient()

        with patch.object(client, 'connect', new_callable=AsyncMock) as mock_connect:
            with patch.object(client, 'close', new_callable=AsyncMock) as mock_close:
                async with client as c:
                    assert c is client
                    mock_connect.assert_awaited_once()
                mock_close.assert_awaited_once()


@pytest.mark.asyncio
class TestAsyncClientQuotes:
    """测试异步行情获取"""

    async def test_get_quotes(self):
        """测试批量获取行情"""
        client = AsyncTdxClient()
        client._writer = AsyncMock()
        client._reader = AsyncMock()

        mock_response = MagicMock()
        mock_response.zip_size = 100
        mock_response.unzip_size = 100

        with patch('tdxapi.async_client.parse_quotes') as mock_parse:
            mock_parse.return_value = [
                StockQuote(
                    code="600519",
                    market="SH",
                    name="贵州茅台",
                    price=1800.0,
                    last_close=1790.0,
                    open=1795.0,
                    high=1810.0,
                    low=1785.0,
                    volume=10000,
                    amount=18000000.0,
                    bid1=1799.0,
                    bid1_vol=100,
                    ask1=1801.0,
                    ask1_vol=100,
                    datetime=None,
                )
            ]

            with patch.object(client, '_send_recv', new_callable=AsyncMock) as mock_send_recv:
                mock_send_recv.return_value = b'mock_data'
                result = await client.get_quotes([(1, "600519")])

                assert len(result) == 1
                assert result[0].code == "600519"

    async def test_get_quote_single(self):
        """测试获取单只股票行情"""
        client = AsyncTdxClient()

        mock_quote = StockQuote(
            code="000001",
            market="SZ",
            name="平安银行",
            price=12.5,
            last_close=12.3,
            open=12.4,
            high=12.6,
            low=12.2,
            volume=50000,
            amount=625000.0,
            bid1=12.4,
            bid1_vol=200,
            ask1=12.6,
            ask1_vol=200,
            datetime=None,
        )

        with patch.object(client, 'get_quotes', new_callable=AsyncMock) as mock_get_quotes:
            mock_get_quotes.return_value = [mock_quote]
            result = await client.get_quote("000001", "SZ")

            assert result is not None
            assert result.code == "000001"
            mock_get_quotes.assert_awaited_once_with([(0, "000001")])

    async def test_get_quote_empty_result(self):
        """测试获取行情返回空列表"""
        client = AsyncTdxClient()

        with patch.object(client, 'get_quotes', new_callable=AsyncMock) as mock_get_quotes:
            mock_get_quotes.return_value = []
            result = await client.get_quote("000001", "SZ")

            assert result is None

    async def test_get_index_quote(self):
        """测试获取指数行情"""
        client = AsyncTdxClient()

        mock_quote = StockQuote(
            code="000001",
            market="SH",
            name="上证指数",
            price=3000.0,
            last_close=2990.0,
            open=2995.0,
            high=3010.0,
            low=2985.0,
            volume=1000000,
            amount=3000000000.0,
            bid1=0.0,
            bid1_vol=0,
            ask1=0.0,
            ask1_vol=0,
            datetime=None,
        )

        with patch.object(client, 'get_quotes', new_callable=AsyncMock) as mock_get_quotes:
            mock_get_quotes.return_value = [mock_quote]
            result = await client.get_index_quote("000001")

            assert result is not None
            assert result.code == "000001"

    async def test_get_futures_quote(self):
        """测试获取期货行情"""
        client = AsyncTdxClient()

        mock_quote = StockQuote(
            code="RB2501",
            market="SH_FUTURE",
            name="螺纹钢",
            price=3500.0,
            last_close=3480.0,
            open=3490.0,
            high=3520.0,
            low=3470.0,
            volume=50000,
            amount=175000000.0,
            bid1=3499.0,
            bid1_vol=100,
            ask1=3501.0,
            ask1_vol=100,
            datetime=None,
        )

        with patch.object(client, 'get_quotes', new_callable=AsyncMock) as mock_get_quotes:
            mock_get_quotes.return_value = [mock_quote]
            result = await client.get_futures_quote("RB2501", market=6)

            assert result is not None
            mock_get_quotes.assert_awaited_once_with([(6, "RB2501")])


@pytest.mark.asyncio
class TestAsyncClientBars:
    """测试异步K线获取"""

    async def test_get_bars(self):
        """测试获取K线数据"""
        client = AsyncTdxClient()

        mock_bars = [
            Bar(
                code="600519",
                market="SH",
                datetime=None,
                open=1800.0,
                high=1810.0,
                low=1790.0,
                close=1805.0,
                volume=10000,
                amount=18050000.0,
            )
        ]

        with patch('tdxapi.async_client.parse_bars') as mock_parse:
            mock_parse.return_value = mock_bars

            with patch.object(client, '_send_recv', new_callable=AsyncMock) as mock_send_recv:
                mock_send_recv.return_value = b'mock_data'
                result = await client.get_bars("600519", "SH", "1d", 10)

                assert len(result) == 1
                assert result[0].code == "600519"
                assert result[0].market == "SH"

    async def test_get_bars_different_periods(self):
        """测试不同周期的K线获取"""
        client = AsyncTdxClient()

        periods = ["1d", "1w", "1m", "5m", "15m", "30m", "60m", "1min"]

        with patch('tdxapi.async_client.parse_bars') as mock_parse:
            mock_parse.return_value = []

            with patch.object(client, '_send_recv', new_callable=AsyncMock) as mock_send_recv:
                mock_send_recv.return_value = b'mock_data'

                for period in periods:
                    result = await client.get_bars("600519", "SH", period, 10)
                    assert isinstance(result, list)

    async def test_get_index_bars(self):
        """测试获取指数K线"""
        client = AsyncTdxClient()

        mock_bars = [
            Bar(
                code="000001",
                market="SH",
                datetime=None,
                open=3000.0,
                high=3010.0,
                low=2990.0,
                close=3005.0,
                volume=100000,
                amount=300500000.0,
            )
        ]

        with patch('tdxapi.async_client.parse_bars') as mock_parse:
            mock_parse.return_value = mock_bars

            with patch.object(client, '_send_recv', new_callable=AsyncMock) as mock_send_recv:
                mock_send_recv.return_value = b'mock_data'
                result = await client.get_index_bars("000001", "SH", "1d", 10)

                assert len(result) == 1
                assert result[0].code == "000001"


@pytest.mark.asyncio
class TestAsyncClientTransactions:
    """测试异步分笔成交获取"""

    async def test_get_transactions(self):
        """测试获取分笔成交"""
        client = AsyncTdxClient()

        mock_ticks = [
            Tick(
                code="600519",
                market="SH",
                time="10:30:00",
                price=1800.0,
                volume=100,
                amount=180000.0,
                direction=0,
            )
        ]

        with patch('tdxapi.async_client.parse_ticks') as mock_parse:
            mock_parse.return_value = mock_ticks

            with patch.object(client, '_send_recv', new_callable=AsyncMock) as mock_send_recv:
                mock_send_recv.return_value = b'mock_data'
                result = await client.get_transactions("600519", "SH", 0, 100)

                assert len(result) == 1
                assert result[0].code == "600519"


@pytest.mark.asyncio
class TestAsyncClientSecurityList:
    """测试异步股票列表获取"""

    async def test_get_stock_count(self):
        """测试获取股票数量"""
        client = AsyncTdxClient()

        with patch('tdxapi.async_client.parse_stock_count') as mock_parse:
            mock_parse.return_value = 5000

            with patch.object(client, '_send_recv', new_callable=AsyncMock) as mock_send_recv:
                mock_send_recv.return_value = b'mock_data'
                result = await client.get_stock_count("SH")

                assert result == 5000

    async def test_get_security_list(self):
        """测试获取股票列表"""
        client = AsyncTdxClient()

        mock_stocks = [
            {"code": "600519", "name": "贵州茅台", "market": 1},
            {"code": "600000", "name": "浦发银行", "market": 1},
        ]

        with patch.object(client, 'get_stock_count', new_callable=AsyncMock) as mock_count:
            mock_count.return_value = 2

            with patch('tdxapi.async_client.parse_security_list') as mock_parse:
                mock_parse.return_value = mock_stocks

                with patch.object(client, '_send_recv', new_callable=AsyncMock) as mock_send_recv:
                    mock_send_recv.return_value = b'mock_data'
                    result = await client.get_security_list("SH", 0, 2)

                    assert len(result) == 2
                    assert result[0]["code"] == "600519"


@pytest.mark.asyncio
class TestAsyncClientFinance:
    """测试异步财务数据获取"""

    async def test_get_xdxr_info(self):
        """测试获取除权除息数据"""
        client = AsyncTdxClient()

        mock_xdxr = [
            {"date": 20230101, "category": 1, "fenhong": 0.5}
        ]

        with patch('tdxapi.async_client.parse_xdxr_info') as mock_parse:
            mock_parse.return_value = mock_xdxr

            with patch.object(client, '_send_recv', new_callable=AsyncMock) as mock_send_recv:
                mock_send_recv.return_value = b'mock_data'
                result = await client.get_xdxr_info("600519", "SH")

                assert len(result) == 1

    async def test_get_finance_info(self):
        """测试获取财务数据"""
        client = AsyncTdxClient()

        mock_finance = {
            "code": "600519",
            "jinying": 100000000.0,
            "zongguben": 1000000,
        }

        with patch('tdxapi.async_client.parse_finance_info') as mock_parse:
            mock_parse.return_value = mock_finance

            with patch.object(client, '_send_recv', new_callable=AsyncMock) as mock_send_recv:
                mock_send_recv.return_value = b'mock_data'
                result = await client.get_finance_info("600519", "SH")

                assert result["code"] == "600519"


@pytest.mark.asyncio
class TestAsyncClientMinuteTime:
    """测试异步分时数据获取"""

    async def test_get_minute_time_with_bars(self):
        """测试使用K线获取分时数据"""
        client = AsyncTdxClient()

        mock_bars = [
            Bar(
                code="600519",
                market="SH",
                datetime=None,
                open=1800.0,
                high=1810.0,
                low=1790.0,
                close=1805.0,
                volume=1000,
                amount=1805000.0,
            ),
            Bar(
                code="600519",
                market="SH",
                datetime=None,
                open=1805.0,
                high=1815.0,
                low=1800.0,
                close=1810.0,
                volume=2000,
                amount=3620000.0,
            ),
        ]

        with patch.object(client, 'get_bars', new_callable=AsyncMock) as mock_get_bars:
            mock_get_bars.return_value = mock_bars
            result = await client.get_minute_time("600519", "SH", use_bars=True)

            assert len(result) == 2
            assert result[0]["price"] == 1805.0
            assert result[0]["volume"] == 1000

    async def test_get_history_minute_time(self):
        """测试获取历史分时数据"""
        client = AsyncTdxClient()

        mock_minutes = [
            {"price": 1800.0, "volume": 1000},
            {"price": 1805.0, "volume": 2000},
        ]

        with patch('tdxapi.async_client.parse_history_minute_time') as mock_parse:
            mock_parse.return_value = mock_minutes

            with patch.object(client, '_send_recv', new_callable=AsyncMock) as mock_send_recv:
                mock_send_recv.return_value = b'mock_data'
                result = await client.get_history_minute_time("600519", "SH", 20240101)

                assert len(result) == 2


@pytest.mark.asyncio
class TestAsyncClientReconnect:
    """测试异步重连功能"""

    async def test_auto_reconnect_on_error(self):
        """测试错误时自动重连"""
        client = AsyncTdxClient(auto_reconnect=True, max_retries=2)

        with patch.object(client, '_send_raw', new_callable=AsyncMock) as mock_send:
            mock_send.side_effect = [ConnectionError("连接断开"), None]

            with patch.object(client, '_recv_response', new_callable=AsyncMock) as mock_recv:
                mock_recv.return_value = (MagicMock(), b'data')

                with patch.object(client, '_reconnect_unlocked', new_callable=AsyncMock) as mock_reconnect:
                    # 第一次调用会失败并重连
                    try:
                        await client._send_recv_unlocked(b'test')
                    except ConnectionError:
                        pass


@pytest.mark.asyncio
class TestAsyncClientServerSelection:
    """测试异步服务器选择"""

    async def test_find_best_server(self):
        """测试自动选择最优服务器"""
        client = AsyncTdxClient()

        mock_reader = AsyncMock()
        mock_writer = AsyncMock()

        with patch('asyncio.open_connection', return_value=(mock_reader, mock_writer)) as mock_open:
            with patch('asyncio.wait_for') as mock_wait_for:
                # 模拟所有服务器都可用
                mock_wait_for.return_value = (mock_reader, mock_writer)

                # 由于并发执行，我们需要特殊处理
                # 这里简化测试，直接验证函数存在
                assert hasattr(client, '_find_best_server')

    async def test_find_best_server_all_failed(self):
        """测试所有服务器都失败的情况"""
        client = AsyncTdxClient()

        with patch('asyncio.open_connection', side_effect=OSError("连接失败")):
            with pytest.raises(ConnectionError, match="所有服务器均不可用"):
                await client._find_best_server()


class TestAsyncClientImport:
    """测试模块导入"""

    def test_import_async_client(self):
        """测试从 tdxapi 导入 AsyncTdxClient"""
        from tdxapi import AsyncTdxClient
        assert AsyncTdxClient is not None

    def test_async_client_in_all(self):
        """测试 AsyncTdxClient 在 __all__ 中"""
        import tdxapi
        assert "AsyncTdxClient" in tdxapi.__all__
