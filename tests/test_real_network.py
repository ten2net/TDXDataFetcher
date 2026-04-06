"""
真实网络测试 - 历史分时/分笔数据
"""

import pytest
import datetime


class TestRealMinuteTime:
    """当日分时数据测试"""

    def test_get_minute_time_sh(self):
        """测试上海股票分时数据"""
        from tdxapi import TdxClient

        with TdxClient(heartbeat=False) as client:
            data = client.get_minute_time("600519", "SH")
            assert isinstance(data, list)
            if data:
                assert "price" in data[0]
                assert "volume" in data[0]
                print(f"分时数据条数: {len(data)}")
                print(f"前3条: {data[:3]}")
                print(f"后3条: {data[-3:]}")

    def test_get_minute_time_sz(self):
        """测试深圳股票分时数据"""
        from tdxapi import TdxClient

        with TdxClient(heartbeat=False) as client:
            data = client.get_minute_time("000001", "SZ")
            assert isinstance(data, list)
            if data:
                print(f"深圳分时数据条数: {len(data)}")

    def test_get_minute_time_index(self):
        """测试指数分时数据"""
        from tdxapi import TdxClient

        with TdxClient(heartbeat=False) as client:
            data = client.get_minute_time("000001", "SH")
            assert isinstance(data, list)
            if data:
                print(f"指数分时数据条数: {len(data)}")


class TestRealHistoryMinute:
    """历史分时数据测试"""

    def test_get_history_minute_sh(self):
        """测试上海股票历史分时"""
        from tdxapi import TdxClient

        with TdxClient(heartbeat=False) as client:
            yesterday = int(
                (datetime.datetime.now() - datetime.timedelta(days=1)).strftime(
                    "%Y%m%d"
                )
            )
            data = client.get_history_minute_time("600519", "SH", date=yesterday)
            assert isinstance(data, list)
            if data:
                assert "hour" in data[0]
                assert "minute" in data[0]
                assert "price" in data[0]
                print(f"历史分时数据条数: {len(data)}")
                print(f"前3条: {data[:3]}")

    def test_get_history_minute_sz(self):
        """测试深圳股票历史分时"""
        from tdxapi import TdxClient

        with TdxClient(heartbeat=False) as client:
            last_week = int(
                (datetime.datetime.now() - datetime.timedelta(days=7)).strftime(
                    "%Y%m%d"
                )
            )
            data = client.get_history_minute_time("000001", "SZ", date=last_week)
            assert isinstance(data, list)
            if data:
                print(f"深圳历史分时数据条数: {len(data)}")


class TestRealTransactions:
    """分笔成交数据测试"""

    def test_get_transactions_sh(self):
        """测试上海分笔成交"""
        from tdxapi import TdxClient

        with TdxClient(heartbeat=False) as client:
            ticks = client.get_transactions("600519", "SH", count=50)
            assert isinstance(ticks, list)
            if ticks:
                assert hasattr(ticks[0], "code")
                assert hasattr(ticks[0], "price")
                assert hasattr(ticks[0], "time")
                print(f"分笔成交数据条数: {len(ticks)}")
                print(f"前3条: {[(t.time, t.price, t.volume) for t in ticks[:3]]}")

    def test_get_transactions_sz(self):
        """测试深圳分笔成交"""
        from tdxapi import TdxClient

        with TdxClient(heartbeat=False) as client:
            ticks = client.get_transactions("000001", "SZ", count=50)
            assert isinstance(ticks, list)
            if ticks:
                print(f"深圳分笔成交数据条数: {len(ticks)}")

    def test_get_history_transactions(self):
        """测试历史分笔成交"""
        from tdxapi import TdxClient

        with TdxClient(heartbeat=False) as client:
            last_week = int(
                (datetime.datetime.now() - datetime.timedelta(days=5)).strftime(
                    "%Y%m%d"
                )
            )
            ticks = client.get_history_transactions(
                "600519", "SH", date=last_week, count=50
            )
            assert isinstance(ticks, list)
            if ticks:
                print(f"历史分笔数据条数: {len(ticks)}")


class TestRealIndexFutures:
    """指数/期货数据测试"""

    def test_get_index_quote(self):
        """测试获取指数行情"""
        from tdxapi import TdxClient

        with TdxClient(heartbeat=False) as client:
            quote = client.get_index_quote("000001")
            if quote:
                print(f"上证指数: {quote.price}")

    def test_get_futures_quote(self):
        """测试期货行情"""
        from tdxapi import TdxClient

        with TdxClient(heartbeat=False) as client:
            quote = client.get_futures_quote("IF2504", 6)
            if quote:
                print(f"期货行情: {quote.price}")


class TestRealXDXRFinance:
    """除权除息/财务数据测试"""

    def test_get_xdxr_info(self):
        """测试除权除息数据"""
        from tdxapi import TdxClient

        with TdxClient(heartbeat=False) as client:
            data = client.get_xdxr_info("600519", "SH")
            assert isinstance(data, list)
            if data:
                print(f"除权除息数据条数: {len(data)}")
                print(f"第一条: {data[0] if data else 'N/A'}")

    def test_get_finance_info(self):
        """测试财务数据"""
        from tdxapi import TdxClient

        with TdxClient(heartbeat=False) as client:
            data = client.get_finance_info("600519", "SH")
            assert isinstance(data, dict)
            if data:
                print(f"财务数据: {data}")


class TestThreadSafety:
    """多线程安全测试"""

    def test_concurrent_requests(self):
        """测试并发请求"""
        import threading
        from tdxapi import TdxClient

        results = []
        errors = []

        def fetch_quote(code, market):
            try:
                with TdxClient(heartbeat=False) as client:
                    quote = client.get_quote(code, market)
                    results.append((code, market, quote.price if quote else None))
            except Exception as e:
                errors.append((code, market, str(e)))

        threads = [
            threading.Thread(target=fetch_quote, args=("600519", "SH")),
            threading.Thread(target=fetch_quote, args=("000001", "SZ")),
            threading.Thread(target=fetch_quote, args=("601888", "SH")),
        ]

        for t in threads:
            t.start()
        for t in threads:
            t.join()

        print(f"成功: {len(results)}, 失败: {len(errors)}")
        if errors:
            print(f"错误: {errors}")
        assert len(errors) == 0, f"并发请求出现错误: {errors}"
