"""
QuoteSubscription 单元测试
"""

import asyncio
import pytest
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, patch

from tdxapi.subscription import (
    QuoteSubscription,
    MultiQuoteSubscription,
    SubscriptionConfig,
    SubscriptionStats,
)
from tdxapi.async_client import AsyncTdxClient
from tdxapi.models import StockQuote


class TestSubscriptionConfig:
    """测试订阅配置"""

    def test_default_config(self):
        """测试默认配置"""
        config = SubscriptionConfig()
        assert config.interval == 1.0
        assert config.max_retries == 3
        assert config.retry_delay == 0.5
        assert config.batch_size == 50
        assert config.continue_on_error is True

    def test_custom_config(self):
        """测试自定义配置"""
        config = SubscriptionConfig(
            interval=2.5,
            max_retries=5,
            retry_delay=1.0,
            batch_size=100,
            continue_on_error=False,
        )
        assert config.interval == 2.5
        assert config.max_retries == 5
        assert config.retry_delay == 1.0
        assert config.batch_size == 100
        assert config.continue_on_error is False


class TestSubscriptionStats:
    """测试订阅统计"""

    def test_default_stats(self):
        """测试默认统计"""
        stats = SubscriptionStats()
        assert stats.start_time is None
        assert stats.stop_time is None
        assert stats.total_updates == 0
        assert stats.success_count == 0
        assert stats.error_count == 0
        assert stats.last_error is None
        assert stats.last_update_time is None

    def test_is_running(self):
        """测试运行状态"""
        stats = SubscriptionStats()
        assert stats.is_running is False

        stats.start_time = datetime.now()
        assert stats.is_running is True

        stats.stop_time = datetime.now()
        assert stats.is_running is False

    def test_elapsed_seconds(self):
        """测试运行时间计算"""
        stats = SubscriptionStats()
        assert stats.elapsed_seconds == 0.0

        stats.start_time = datetime.now()
        assert stats.elapsed_seconds >= 0.0


@pytest.mark.asyncio
class TestQuoteSubscriptionInit:
    """测试 QuoteSubscription 初始化"""

    async def test_default_init(self):
        """测试默认初始化"""
        mock_client = AsyncMock(spec=AsyncTdxClient)
        sub = QuoteSubscription(mock_client)

        assert sub.is_running is False
        assert len(sub.subscribed_codes) == 0
        assert sub.config.interval == 1.0

    async def test_custom_config_init(self):
        """测试自定义配置初始化"""
        mock_client = AsyncMock(spec=AsyncTdxClient)
        config = SubscriptionConfig(interval=2.0, max_retries=5)
        sub = QuoteSubscription(mock_client, config)

        assert sub.config.interval == 2.0
        assert sub.config.max_retries == 5


@pytest.mark.asyncio
class TestQuoteSubscriptionCallbacks:
    """测试回调函数管理"""

    async def test_register_quote_callback(self):
        """测试注册单条回调"""
        mock_client = AsyncMock(spec=AsyncTdxClient)
        sub = QuoteSubscription(mock_client)

        def callback(quote: StockQuote):
            pass

        sub.register_callback(callback)
        assert callback in sub._quote_callbacks

    async def test_register_quotes_callback(self):
        """测试注册批量回调"""
        mock_client = AsyncMock(spec=AsyncTdxClient)
        sub = QuoteSubscription(mock_client)

        def callback(quotes: list):
            pass

        sub.register_quotes_callback(callback)
        assert callback in sub._quotes_callbacks

    async def test_register_error_callback(self):
        """测试注册错误回调"""
        mock_client = AsyncMock(spec=AsyncTdxClient)
        sub = QuoteSubscription(mock_client)

        def callback(error: Exception, code: str):
            pass

        sub.register_error_callback(callback)
        assert callback in sub._error_callbacks

    async def test_unregister_callback(self):
        """测试注销回调"""
        mock_client = AsyncMock(spec=AsyncTdxClient)
        sub = QuoteSubscription(mock_client)

        def callback(quote: StockQuote):
            pass

        sub.register_callback(callback)
        assert sub.unregister_callback(callback) is True
        assert callback not in sub._quote_callbacks

    async def test_unregister_nonexistent_callback(self):
        """测试注销不存在的回调"""
        mock_client = AsyncMock(spec=AsyncTdxClient)
        sub = QuoteSubscription(mock_client)

        def callback(quote: StockQuote):
            pass

        assert sub.unregister_callback(callback) is False

    async def test_clear_callbacks(self):
        """测试清除所有回调"""
        mock_client = AsyncMock(spec=AsyncTdxClient)
        sub = QuoteSubscription(mock_client)

        def callback1(quote: StockQuote):
            pass

        def callback2(quotes: list):
            pass

        sub.register_callback(callback1)
        sub.register_quotes_callback(callback2)

        sub.clear_callbacks()
        assert len(sub._quote_callbacks) == 0
        assert len(sub._quotes_callbacks) == 0


@pytest.mark.asyncio
class TestQuoteSubscriptionCodeParsing:
    """测试代码解析"""

    async def test_parse_code_sh(self):
        """测试解析上海股票代码"""
        mock_client = AsyncMock(spec=AsyncTdxClient)
        sub = QuoteSubscription(mock_client)

        assert sub._parse_code("600519") == (1, "600519")
        assert sub._parse_code("SH:600519") == (1, "600519")
        assert sub._parse_code("sh:600519") == (1, "600519")

    async def test_parse_code_sz(self):
        """测试解析深圳股票代码"""
        mock_client = AsyncMock(spec=AsyncTdxClient)
        sub = QuoteSubscription(mock_client)

        assert sub._parse_code("000001") == (0, "000001")
        assert sub._parse_code("300001") == (0, "300001")
        assert sub._parse_code("SZ:000001") == (0, "000001")
        assert sub._parse_code("sz:000001") == (0, "000001")

    async def test_code_key(self):
        """测试代码键生成"""
        mock_client = AsyncMock(spec=AsyncTdxClient)
        sub = QuoteSubscription(mock_client)

        assert sub._code_key(1, "600519") == "SH:600519"
        assert sub._code_key(0, "000001") == "SZ:000001"


@pytest.mark.asyncio
class TestQuoteSubscriptionSubscribe:
    """测试订阅功能"""

    async def test_subscribe_single_code(self):
        """测试订阅单只股票"""
        mock_client = AsyncMock(spec=AsyncTdxClient)
        sub = QuoteSubscription(mock_client)

        await sub.subscribe_quotes(["600519"], auto_start=False)

        assert "SH:600519" in sub.subscribed_codes
        assert sub.config.interval == 1.0

    async def test_subscribe_multiple_codes(self):
        """测试订阅多只股票"""
        mock_client = AsyncMock(spec=AsyncTdxClient)
        sub = QuoteSubscription(mock_client)

        await sub.subscribe_quotes(["600519", "000001", "300001"], auto_start=False)

        assert len(sub.subscribed_codes) == 3
        assert "SH:600519" in sub.subscribed_codes
        assert "SZ:000001" in sub.subscribed_codes
        assert "SZ:300001" in sub.subscribed_codes

    async def test_subscribe_with_market_prefix(self):
        """测试带市场前缀的订阅"""
        mock_client = AsyncMock(spec=AsyncTdxClient)
        sub = QuoteSubscription(mock_client)

        await sub.subscribe_quotes(["SH:600519", "SZ:000001"], auto_start=False)

        assert "SH:600519" in sub.subscribed_codes
        assert "SZ:000001" in sub.subscribed_codes

    async def test_subscribe_with_custom_interval(self):
        """测试自定义轮询间隔"""
        mock_client = AsyncMock(spec=AsyncTdxClient)
        sub = QuoteSubscription(mock_client)

        await sub.subscribe_quotes(["600519"], interval=2.5, auto_start=False)

        assert sub.config.interval == 2.5

    async def test_unsubscribe_codes(self):
        """测试取消订阅"""
        mock_client = AsyncMock(spec=AsyncTdxClient)
        sub = QuoteSubscription(mock_client)

        await sub.subscribe_quotes(["600519", "000001", "300001"], auto_start=False)
        await sub.unsubscribe_quotes(["600519"])

        assert "SH:600519" not in sub.subscribed_codes
        assert "SZ:000001" in sub.subscribed_codes

    async def test_unsubscribe_all_stops_subscription(self):
        """测试取消所有订阅后自动停止"""
        mock_client = AsyncMock(spec=AsyncTdxClient)
        sub = QuoteSubscription(mock_client)

        await sub.subscribe_quotes(["600519"], auto_start=False)
        sub._running = True  # 模拟运行状态

        await sub.unsubscribe_quotes(["600519"])

        assert sub.is_running is False


@pytest.mark.asyncio
class TestQuoteSubscriptionStartStop:
    """测试启动和停止"""

    async def test_start_without_codes_raises(self):
        """测试没有订阅代码时启动报错"""
        mock_client = AsyncMock(spec=AsyncTdxClient)
        sub = QuoteSubscription(mock_client)

        with pytest.raises(ValueError, match="没有订阅任何股票"):
            await sub.start()

    async def test_start_already_running(self):
        """测试重复启动"""
        mock_client = AsyncMock(spec=AsyncTdxClient)
        sub = QuoteSubscription(mock_client)

        await sub.subscribe_quotes(["600519"], auto_start=False)
        sub._running = True

        # 不应抛出异常
        await sub.start()

    async def test_stop_when_not_running(self):
        """测试停止未运行的订阅"""
        mock_client = AsyncMock(spec=AsyncTdxClient)
        sub = QuoteSubscription(mock_client)

        # 不应抛出异常
        await sub.stop()

    async def test_stop_cancels_task(self):
        """测试停止取消任务"""
        mock_client = AsyncMock(spec=AsyncTdxClient)
        sub = QuoteSubscription(mock_client)

        await sub.subscribe_quotes(["600519"], auto_start=False)

        # 模拟运行状态
        sub._running = True
        sub._stop_event = asyncio.Event()
        sub._task = asyncio.create_task(asyncio.sleep(10))

        await sub.stop()

        assert sub.is_running is False
        assert sub._task is None

    async def test_async_context_manager(self):
        """测试异步上下文管理器"""
        mock_client = AsyncMock(spec=AsyncTdxClient)

        async with QuoteSubscription(mock_client) as sub:
            assert isinstance(sub, QuoteSubscription)


@pytest.mark.asyncio
class TestQuoteSubscriptionPolling:
    """测试轮询功能"""

    async def test_poll_loop_fetches_data(self):
        """测试轮询循环获取数据"""
        mock_client = AsyncMock(spec=AsyncTdxClient)

        mock_quote = StockQuote(
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
            datetime=datetime.now(),
        )

        mock_client.get_quotes.return_value = [mock_quote]

        sub = QuoteSubscription(mock_client)
        await sub.subscribe_quotes(["600519"], interval=0.1, auto_start=False)

        quotes_received = []

        def callback(quote: StockQuote):
            quotes_received.append(quote)

        sub.register_callback(callback)

        # 启动并运行一小段时间
        await sub.start()
        await asyncio.sleep(0.25)
        await sub.stop()

        assert len(quotes_received) >= 1
        assert quotes_received[0].code == "600519"

    async def test_poll_loop_batch_callback(self):
        """测试批量回调"""
        mock_client = AsyncMock(spec=AsyncTdxClient)

        mock_quotes = [
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
                datetime=datetime.now(),
            ),
            StockQuote(
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
                datetime=datetime.now(),
            ),
        ]

        mock_client.get_quotes.return_value = mock_quotes

        sub = QuoteSubscription(mock_client)
        await sub.subscribe_quotes(["600519", "000001"], interval=0.1, auto_start=False)

        batches_received = []

        def callback(quotes: list):
            batches_received.append(quotes)

        sub.register_quotes_callback(callback)

        await sub.start()
        await asyncio.sleep(0.25)
        await sub.stop()

        assert len(batches_received) >= 1
        assert len(batches_received[0]) == 2

    async def test_poll_loop_error_callback(self):
        """测试错误回调"""
        mock_client = AsyncMock(spec=AsyncTdxClient)
        mock_client.get_quotes.side_effect = ConnectionError("连接失败")

        sub = QuoteSubscription(mock_client)
        sub._config.continue_on_error = True
        await sub.subscribe_quotes(["600519"], interval=0.1, auto_start=False)

        errors_received = []

        def callback(error: Exception, code: str):
            errors_received.append((error, code))

        sub.register_error_callback(callback)

        await sub.start()
        await asyncio.sleep(0.25)
        await sub.stop()

        assert len(errors_received) >= 1
        assert isinstance(errors_received[0][0], ConnectionError)

    async def test_price_changed_only(self):
        """测试只在价格变化时触发回调"""
        mock_client = AsyncMock(spec=AsyncTdxClient)

        # 第一次返回价格 1800
        mock_quote1 = StockQuote(
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
            datetime=datetime.now(),
        )

        # 第二次返回相同价格
        mock_quote2 = StockQuote(
            code="600519",
            market="SH",
            name="贵州茅台",
            price=1800.0,  # 价格相同
            last_close=1790.0,
            open=1795.0,
            high=1810.0,
            low=1785.0,
            volume=10001,  # 成交量变化
            amount=18000000.0,
            bid1=1799.0,
            bid1_vol=100,
            ask1=1801.0,
            ask1_vol=100,
            datetime=datetime.now(),
        )

        mock_client.get_quotes.side_effect = [[mock_quote1], [mock_quote2]]

        sub = QuoteSubscription(mock_client)
        sub.set_price_changed_only(True)
        await sub.subscribe_quotes(["600519"], interval=0.1, auto_start=False)

        quotes_received = []

        def callback(quote: StockQuote):
            quotes_received.append(quote)

        sub.register_callback(callback)

        await sub.start()
        await asyncio.sleep(0.25)
        await sub.stop()

        # 价格相同，应该只触发一次回调
        assert len(quotes_received) == 1


@pytest.mark.asyncio
class TestQuoteSubscriptionRetry:
    """测试重试机制"""

    async def test_retry_on_error(self):
        """测试错误时重试"""
        mock_client = AsyncMock(spec=AsyncTdxClient)

        # 第一次失败，第二次成功
        mock_quote = StockQuote(
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
            datetime=datetime.now(),
        )

        mock_client.get_quotes.side_effect = [ConnectionError("连接失败"), [mock_quote]]

        sub = QuoteSubscription(mock_client)
        sub._config.max_retries = 2
        sub._config.retry_delay = 0.01

        await sub.subscribe_quotes(["600519"], interval=0.1, auto_start=False)

        quotes_received = []

        def callback(quote: StockQuote):
            quotes_received.append(quote)

        sub.register_callback(callback)

        await sub.start()
        await asyncio.sleep(0.15)
        await sub.stop()

        # 应该收到数据（重试后成功）
        assert len(quotes_received) >= 1

    async def test_retry_exhausted(self):
        """测试重试次数耗尽"""
        mock_client = AsyncMock(spec=AsyncTdxClient)
        mock_client.get_quotes.side_effect = ConnectionError("连接失败")

        sub = QuoteSubscription(mock_client)
        sub._config.max_retries = 2
        sub._config.retry_delay = 0.01
        sub._config.continue_on_error = True

        await sub.subscribe_quotes(["600519"], interval=0.1, auto_start=False)

        errors_received = []

        def callback(error: Exception, code: str):
            errors_received.append(error)

        sub.register_error_callback(callback)

        await sub.start()
        await asyncio.sleep(0.15)
        await sub.stop()

        # 应该收到错误通知
        assert len(errors_received) >= 1
        assert sub.stats.error_count >= 1


@pytest.mark.asyncio
class TestQuoteSubscriptionStats:
    """测试统计信息"""

    async def test_stats_updated(self):
        """测试统计信息更新"""
        mock_client = AsyncMock(spec=AsyncTdxClient)

        mock_quote = StockQuote(
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
            datetime=datetime.now(),
        )

        mock_client.get_quotes.return_value = [mock_quote]

        sub = QuoteSubscription(mock_client)
        await sub.subscribe_quotes(["600519"], interval=0.1, auto_start=False)

        await sub.start()
        await asyncio.sleep(0.25)
        await sub.stop()

        assert sub.stats.total_updates >= 1
        assert sub.stats.success_count >= 1
        assert sub.stats.start_time is not None
        assert sub.stats.stop_time is not None
        assert sub.stats.elapsed_seconds > 0


@pytest.mark.asyncio
class TestMultiQuoteSubscription:
    """测试多客户端订阅"""

    async def test_init_with_multiple_clients(self):
        """测试多客户端初始化"""
        mock_client1 = AsyncMock(spec=AsyncTdxClient)
        mock_client2 = AsyncMock(spec=AsyncTdxClient)

        multi_sub = MultiQuoteSubscription([mock_client1, mock_client2])

        assert len(multi_sub._clients) == 2

    async def test_subscribe_distributes_codes(self):
        """测试订阅时分配代码"""
        mock_client1 = AsyncMock(spec=AsyncTdxClient)
        mock_client2 = AsyncMock(spec=AsyncTdxClient)

        multi_sub = MultiQuoteSubscription([mock_client1, mock_client2])

        await multi_sub.subscribe_quotes(
            ["600519", "000001", "300001", "600000"],
            auto_start=False
        )

        # 应该创建多个订阅
        assert len(multi_sub._subscriptions) == 2

    async def test_start_stop_all(self):
        """测试启动和停止所有订阅"""
        mock_client1 = AsyncMock(spec=AsyncTdxClient)
        mock_client2 = AsyncMock(spec=AsyncTdxClient)

        multi_sub = MultiQuoteSubscription([mock_client1, mock_client2])

        await multi_sub.subscribe_quotes(["600519", "000001"], auto_start=False)

        await multi_sub.start()
        assert multi_sub.is_running is True

        await multi_sub.stop()
        assert multi_sub.is_running is False

    async def test_register_callbacks_to_all(self):
        """测试回调注册到所有订阅"""
        mock_client1 = AsyncMock(spec=AsyncTdxClient)
        mock_client2 = AsyncMock(spec=AsyncTdxClient)

        multi_sub = MultiQuoteSubscription([mock_client1, mock_client2])

        def callback(quote: StockQuote):
            pass

        multi_sub.register_callback(callback)

        await multi_sub.subscribe_quotes(["600519", "000001"], auto_start=False)

        # 回调应该被复制到所有子订阅
        for sub in multi_sub._subscriptions:
            assert callback in sub._quote_callbacks

    async def test_async_context_manager(self):
        """测试异步上下文管理器"""
        mock_client1 = AsyncMock(spec=AsyncTdxClient)
        mock_client2 = AsyncMock(spec=AsyncTdxClient)

        async with MultiQuoteSubscription([mock_client1, mock_client2]) as multi_sub:
            assert isinstance(multi_sub, MultiQuoteSubscription)

    async def test_no_clients_raises(self):
        """测试没有客户端时报错"""
        multi_sub = MultiQuoteSubscription([])

        with pytest.raises(ValueError, match="没有可用的客户端"):
            await multi_sub.subscribe_quotes(["600519"])

    async def test_get_stats(self):
        """测试获取统计信息"""
        mock_client1 = AsyncMock(spec=AsyncTdxClient)
        mock_client2 = AsyncMock(spec=AsyncTdxClient)

        multi_sub = MultiQuoteSubscription([mock_client1, mock_client2])

        await multi_sub.subscribe_quotes(["600519", "000001"], auto_start=False)

        stats = multi_sub.get_stats()
        assert len(stats) == 2
