"""
连接池模块单元测试
"""

import asyncio
import pytest
from unittest.mock import Mock, AsyncMock, patch, MagicMock

from tdxapi.connection_pool import (
    TdxConnectionPool,
    PoolConnection,
    PoolStats,
    PooledClient,
    get_default_pool,
    close_default_pool,
)
from tdxapi.async_client import AsyncTdxClient


class TestPoolStats:
    """测试 PoolStats 统计类"""

    def test_initial_stats(self):
        """测试初始统计值"""
        stats = PoolStats()
        assert stats.total_connections == 0
        assert stats.available_connections == 0
        assert stats.in_use_connections == 0
        assert stats.hit_rate == 0.0

    def test_hit_rate_calculation(self):
        """测试命中率计算"""
        stats = PoolStats(total_hits=80, total_misses=20)
        assert stats.hit_rate == 0.8

    def test_hit_rate_zero_division(self):
        """测试命中率为0的情况"""
        stats = PoolStats()
        assert stats.hit_rate == 0.0


class TestPoolConnection:
    """测试 PoolConnection 连接包装类"""

    @pytest.fixture
    def mock_client(self):
        """创建模拟客户端"""
        client = Mock(spec=AsyncTdxClient)
        client._ip = "127.0.0.1"
        client._port = 7709
        return client

    @pytest.fixture
    def mock_pool(self):
        """创建模拟连接池"""
        return Mock(spec=TdxConnectionPool)

    @pytest.mark.asyncio
    async def test_acquire_release(self, mock_client, mock_pool):
        """测试连接获取和释放"""
        conn = PoolConnection(client=mock_client, pool=mock_pool)

        # 初始状态
        assert not conn.in_use
        assert conn.use_count == 0

        # 获取连接
        result = await conn.acquire()
        assert result is True
        assert conn.in_use
        assert conn.use_count == 1

        # 再次获取应该失败
        result = await conn.acquire()
        assert result is False

        # 释放连接
        await conn.release()
        assert not conn.in_use

    @pytest.mark.asyncio
    async def test_health_check_success(self, mock_client, mock_pool):
        """测试健康检查成功"""
        mock_client.get_stock_count = AsyncMock(return_value=4000)
        conn = PoolConnection(client=mock_client, pool=mock_pool)

        is_healthy = await conn.is_healthy()
        assert is_healthy is True
        mock_client.get_stock_count.assert_called_once_with("SH")

    @pytest.mark.asyncio
    async def test_health_check_failure(self, mock_client, mock_pool):
        """测试健康检查失败"""
        mock_client.get_stock_count = AsyncMock(side_effect=ConnectionError("Failed"))
        conn = PoolConnection(client=mock_client, pool=mock_pool)

        is_healthy = await conn.is_healthy()
        assert is_healthy is False

    @pytest.mark.asyncio
    async def test_health_check_timeout(self, mock_client, mock_pool):
        """测试健康检查超时"""
        async def slow_check(*args, **kwargs):
            await asyncio.sleep(10)
            return 4000

        mock_client.get_stock_count = slow_check
        conn = PoolConnection(client=mock_client, pool=mock_pool)

        is_healthy = await conn.is_healthy(timeout=0.01)
        assert is_healthy is False

    def test_idle_time(self, mock_client, mock_pool):
        """测试空闲时间计算"""
        conn = PoolConnection(client=mock_client, pool=mock_pool)
        assert conn.idle_time >= 0

    def test_age(self, mock_client, mock_pool):
        """测试连接年龄计算"""
        conn = PoolConnection(client=mock_client, pool=mock_pool)
        assert conn.age >= 0


class TestTdxConnectionPool:
    """测试 TdxConnectionPool 连接池类"""

    @pytest.fixture
    def pool(self):
        """创建测试连接池"""
        return TdxConnectionPool(
            max_size=5,
            min_size=1,
            max_idle_time=60.0,
            max_connection_age=300.0,
            acquire_timeout=5.0,
            health_check_interval=10.0,
        )

    @pytest.mark.asyncio
    async def test_pool_initialization(self, pool):
        """测试连接池初始化"""
        with patch.object(pool, '_create_connection', new_callable=AsyncMock) as mock_create:
            mock_conn = Mock(spec=PoolConnection)
            mock_create.return_value = mock_conn

            await pool.initialize()

            assert not pool._closed
            assert pool._maintenance_task is not None
            mock_create.assert_called()

    @pytest.mark.asyncio
    async def test_pool_close(self, pool):
        """测试连接池关闭"""
        with patch.object(pool, '_create_connection', new_callable=AsyncMock) as mock_create:
            mock_conn = Mock(spec=PoolConnection)
            mock_conn.client = Mock()
            mock_conn.client.close = AsyncMock()
            mock_create.return_value = mock_conn

            await pool.initialize()
            await pool.close()

            assert pool._closed

    @pytest.mark.asyncio
    async def test_server_selection_round_robin(self, pool):
        """测试轮询负载均衡"""
        pool._load_balance_strategy = "round_robin"
        pool._servers = [("192.168.1.1", 7709), ("192.168.1.2", 7709)]

        server1 = pool._select_server()
        server2 = pool._select_server()
        server3 = pool._select_server()

        assert server1 == ("192.168.1.1", 7709)
        assert server2 == ("192.168.1.2", 7709)
        assert server3 == ("192.168.1.1", 7709)  # 循环

    @pytest.mark.asyncio
    async def test_server_selection_random(self, pool):
        """测试随机负载均衡"""
        pool._load_balance_strategy = "random"
        pool._servers = [("192.168.1.1", 7709), ("192.168.1.2", 7709)]

        server = pool._select_server()
        assert server in pool._servers

    @pytest.mark.asyncio
    async def test_server_selection_least_connections(self, pool):
        """测试最少连接负载均衡"""
        pool._load_balance_strategy = "least_connections"
        pool._servers = [("192.168.1.1", 7709), ("192.168.1.2", 7709)]
        pool._server_connection_counts = {
            ("192.168.1.1", 7709): 5,
            ("192.168.1.2", 7709): 2,
        }

        server = pool._select_server()
        assert server == ("192.168.1.2", 7709)

    @pytest.mark.asyncio
    async def test_should_recycle_age(self, pool):
        """测试基于年龄的连接回收"""
        mock_client = Mock(spec=AsyncTdxClient)
        conn = PoolConnection(client=mock_client, pool=pool)

        # 模拟连接已存在很久
        conn.created_at = 0  # 很久之前创建

        assert pool._should_recycle(conn) is True

    @pytest.mark.asyncio
    async def test_should_recycle_idle_time(self, pool):
        """测试基于空闲时间的连接回收"""
        mock_client = Mock(spec=AsyncTdxClient)
        conn = PoolConnection(client=mock_client, pool=pool)

        # 模拟连接已空闲很久
        conn.last_used_at = 0  # 很久之前使用

        assert pool._should_recycle(conn) is True

    @pytest.mark.asyncio
    async def test_should_not_recycle(self, pool):
        """测试不需要回收的连接"""
        mock_client = Mock(spec=AsyncTdxClient)
        conn = PoolConnection(client=mock_client, pool=pool)

        assert pool._should_recycle(conn) is False

    @pytest.mark.asyncio
    async def test_get_stats_dict(self, pool):
        """测试获取统计信息字典"""
        stats = pool.get_stats_dict()

        assert "total_connections" in stats
        assert "available_connections" in stats
        assert "in_use_connections" in stats
        assert "hit_rate" in stats

    @pytest.mark.asyncio
    async def test_acquire_timeout(self, pool):
        """测试获取连接超时"""
        pool._closed = True  # 关闭池，使其无法创建连接
        pool._max_size = 1

        with pytest.raises((asyncio.TimeoutError, ConnectionError)):
            await pool.acquire()


class TestPooledClient:
    """测试 PooledClient 客户端包装器"""

    @pytest.fixture
    def mock_conn(self):
        """创建模拟连接"""
        conn = Mock(spec=PoolConnection)
        conn.client = Mock(spec=AsyncTdxClient)
        conn.client.get_quote = AsyncMock(return_value={"code": "000001"})
        return conn

    @pytest.fixture
    def mock_pool(self):
        """创建模拟连接池"""
        pool = Mock(spec=TdxConnectionPool)
        pool.release = AsyncMock()
        return pool

    @pytest.mark.asyncio
    async def test_pooled_client_proxy(self, mock_conn, mock_pool):
        """测试客户端代理"""
        client = PooledClient(mock_conn, mock_pool)

        # 测试方法代理
        result = await client.get_quote("000001", "SZ")
        mock_conn.client.get_quote.assert_called_once_with("000001", "SZ")

    @pytest.mark.asyncio
    async def test_pooled_client_context_manager(self, mock_conn, mock_pool):
        """测试异步上下文管理器"""
        async with PooledClient(mock_conn, mock_pool) as client:
            assert isinstance(client, PooledClient)

        # 验证连接已释放
        mock_pool.release.assert_called_once_with(mock_conn)

    @pytest.mark.asyncio
    async def test_pooled_client_close(self, mock_conn, mock_pool):
        """测试客户端关闭"""
        client = PooledClient(mock_conn, mock_pool)
        await client.close()

        # 验证连接已释放
        mock_pool.release.assert_called_once_with(mock_conn)

        # 再次关闭应该无操作
        await client.close()
        assert mock_pool.release.call_count == 1


class TestDefaultPool:
    """测试默认连接池单例"""

    @pytest.mark.asyncio
    async def test_get_default_pool(self):
        """测试获取默认连接池"""
        await close_default_pool()  # 先清理

        with patch('tdxapi.connection_pool.TdxConnectionPool.initialize', new_callable=AsyncMock) as mock_init:
            pool = await get_default_pool(max_size=5, min_size=1)
            assert pool is not None
            assert pool._max_size == 5
            assert pool._min_size == 1

        await close_default_pool()

    @pytest.mark.asyncio
    async def test_default_pool_singleton(self):
        """测试默认连接池单例模式"""
        # 重置全局状态
        await close_default_pool()

        async def mock_initialize(self):
            """模拟初始化"""
            self._closed = False
            self._maintenance_task = None

        with patch.object(TdxConnectionPool, 'initialize', mock_initialize):
            # 第一次调用应该创建新实例
            pool1 = await get_default_pool()

            # 第二次调用应该返回相同实例
            pool2 = await get_default_pool()
            assert pool1 is pool2

        await close_default_pool()

    @pytest.mark.asyncio
    async def test_close_default_pool(self):
        """测试关闭默认连接池"""
        await close_default_pool()

        with patch('tdxapi.connection_pool.TdxConnectionPool.initialize', new_callable=AsyncMock):
            pool = await get_default_pool()
            await close_default_pool()

            from tdxapi.connection_pool import _default_pool
            assert _default_pool is None


class TestIntegration:
    """集成测试（需要实际服务器）"""

    @pytest.mark.asyncio
    @pytest.mark.skip("需要实际服务器")
    async def test_pool_with_real_connection(self):
        """测试使用真实连接的连接池"""
        pool = TdxConnectionPool(
            max_size=2,
            min_size=1,
            acquire_timeout=10.0,
        )

        await pool.initialize()

        try:
            # 测试获取连接
            async with pool.acquire_ctx() as client:
                quote = await client.get_quote("000001", "SH")
                assert quote is not None

            # 测试创建客户端
            pooled_client = await pool.create_client()
            async with pooled_client as client:
                quote = await client.get_quote("000001", "SH")
                assert quote is not None

            # 检查统计信息
            stats = pool.get_stats_dict()
            assert stats["total_requests"] >= 2

        finally:
            await pool.close()

    @pytest.mark.asyncio
    @pytest.mark.skip("需要实际服务器")
    async def test_concurrent_requests(self):
        """测试并发请求"""
        pool = TdxConnectionPool(
            max_size=3,
            min_size=1,
            acquire_timeout=10.0,
        )

        await pool.initialize()

        try:
            async def fetch_quote(code):
                async with pool.acquire_ctx() as client:
                    return await client.get_quote(code, "SH")

            # 并发获取多个股票
            codes = ["000001", "000002", "000063", "000333", "000858"]
            results = await asyncio.gather(*[fetch_quote(code) for code in codes])

            assert all(r is not None for r in results)

        finally:
            await pool.close()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
