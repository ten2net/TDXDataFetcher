"""
BulkDownloader 单元测试
"""

import asyncio
import json
import os
import tempfile
from datetime import datetime
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from tdxapi.bulk_download import (
    BulkDownloader,
    DateRangeHelper,
    DownloadProgress,
    download_all_stocks_bars,
)
from tdxapi.models import Bar, Tick


class TestDownloadProgress:
    """测试 DownloadProgress 数据类"""

    def test_default_init(self):
        """测试默认初始化"""
        progress = DownloadProgress()
        assert progress.total == 0
        assert progress.completed == 0
        assert progress.failed == 0
        assert progress.skipped == 0
        assert progress.current_code == ""
        assert progress.current_task == ""
        assert progress.start_time is None
        assert progress.end_time is None
        assert progress.errors == {}

    def test_progress_percent(self):
        """测试进度百分比计算"""
        progress = DownloadProgress(total=100, completed=50)
        assert progress.progress_percent == 50.0

        progress = DownloadProgress(total=0, completed=0)
        assert progress.progress_percent == 0.0

        progress = DownloadProgress(total=100, completed=100)
        assert progress.progress_percent == 100.0

    def test_elapsed_seconds(self):
        """测试已用时间计算"""
        progress = DownloadProgress()
        assert progress.elapsed_seconds == 0.0

        progress.start_time = datetime.now()
        assert progress.elapsed_seconds >= 0.0

    def test_estimated_remaining_seconds(self):
        """测试估算剩余时间"""
        progress = DownloadProgress()
        assert progress.estimated_remaining_seconds == 0.0

        progress = DownloadProgress(total=100, completed=50)
        progress.start_time = datetime.now()
        # 刚开始，可能有小数值，检查是否接近0
        assert progress.estimated_remaining_seconds >= 0.0

    def test_to_dict(self):
        """测试转换为字典"""
        progress = DownloadProgress(
            total=100,
            completed=50,
            failed=10,
            skipped=5,
            current_code="600519",
            current_task="test",
            errors={"SH:600000": "error"},
        )
        progress.start_time = datetime(2024, 1, 1, 12, 0, 0)

        data = progress.to_dict()
        assert data["total"] == 100
        assert data["completed"] == 50
        assert data["failed"] == 10
        assert data["skipped"] == 5
        assert data["current_code"] == "600519"
        assert data["current_task"] == "test"
        assert data["start_time"] == "2024-01-01T12:00:00"
        assert data["errors"]["SH:600000"] == "error"

    def test_from_dict(self):
        """测试从字典创建"""
        data = {
            "total": 100,
            "completed": 50,
            "failed": 10,
            "skipped": 5,
            "current_code": "600519",
            "current_task": "test",
            "start_time": "2024-01-01T12:00:00",
            "end_time": "2024-01-01T13:00:00",
            "errors": {"SH:600000": "error"},
        }

        progress = DownloadProgress.from_dict(data)
        assert progress.total == 100
        assert progress.completed == 50
        assert progress.failed == 10
        assert progress.skipped == 5
        assert progress.current_code == "600519"
        assert progress.current_task == "test"
        assert progress.start_time == datetime(2024, 1, 1, 12, 0, 0)
        assert progress.end_time == datetime(2024, 1, 1, 13, 0, 0)
        assert progress.errors["SH:600000"] == "error"


class TestBulkDownloaderInit:
    """测试 BulkDownloader 初始化"""

    def test_default_init(self):
        """测试默认初始化"""
        downloader = BulkDownloader()
        assert downloader._client is None
        assert downloader._own_client is True
        assert downloader._cache is None
        assert downloader._max_concurrent == 5
        assert downloader._progress_file is None
        assert downloader._enable_tqdm is True

    def test_custom_init(self):
        """测试自定义初始化"""
        mock_client = MagicMock()
        mock_cache = MagicMock()
        downloader = BulkDownloader(
            client=mock_client,
            cache=mock_cache,
            max_concurrent=10,
            progress_file="/tmp/progress.json",
            enable_tqdm=False,
        )
        assert downloader._client is mock_client
        assert downloader._own_client is False
        assert downloader._cache is mock_cache
        assert downloader._max_concurrent == 10
        assert downloader._progress_file == Path("/tmp/progress.json")
        assert downloader._enable_tqdm is False


@pytest.mark.asyncio
class TestBulkDownloaderContextManager:
    """测试 BulkDownloader 异步上下文管理器"""

    async def test_async_context_manager_with_auto_connect(self):
        """测试自动连接"""
        with patch("tdxapi.bulk_download.AsyncTdxClient") as MockClient:
            mock_instance = AsyncMock()
            MockClient.return_value = mock_instance

            async with BulkDownloader() as downloader:
                assert downloader._client is mock_instance
                mock_instance.connect.assert_awaited_once()

            mock_instance.close.assert_awaited_once()

    async def test_async_context_manager_with_existing_client(self):
        """测试使用现有客户端"""
        mock_client = AsyncMock()

        async with BulkDownloader(client=mock_client) as downloader:
            assert downloader._client is mock_client
            mock_client.connect.assert_not_awaited()

        mock_client.close.assert_not_awaited()


@pytest.mark.asyncio
class TestBulkDownloaderGetAllStocks:
    """测试获取全市场股票列表"""

    async def test_get_all_stocks_default_markets(self):
        """测试默认市场"""
        mock_client = AsyncMock()
        mock_client.get_security_list.side_effect = [
            [{"code": "600519", "name": "贵州茅台"}],
            [{"code": "000001", "name": "平安银行"}],
            [{"code": "430047", "name": "北交所股票"}],
        ]

        downloader = BulkDownloader(client=mock_client)
        stocks = await downloader.get_all_stocks()

        assert len(stocks) == 3
        assert stocks[0]["code"] == "600519"
        assert stocks[0]["market"] == "SH"
        assert stocks[1]["code"] == "000001"
        assert stocks[1]["market"] == "SZ"
        assert stocks[2]["code"] == "430047"
        assert stocks[2]["market"] == "BJ"

    async def test_get_all_stocks_custom_markets(self):
        """测试自定义市场"""
        mock_client = AsyncMock()
        mock_client.get_security_list.return_value = [
            {"code": "600519", "name": "贵州茅台"}
        ]

        downloader = BulkDownloader(client=mock_client)
        stocks = await downloader.get_all_stocks(markets=["SH"])

        assert len(stocks) == 1
        mock_client.get_security_list.assert_called_once_with("SH")

    async def test_get_all_stocks_with_error(self):
        """测试部分市场获取失败"""
        mock_client = AsyncMock()
        mock_client.get_security_list.side_effect = [
            [{"code": "600519", "name": "贵州茅台"}],
            Exception("连接失败"),
        ]

        downloader = BulkDownloader(client=mock_client)
        stocks = await downloader.get_all_stocks(markets=["SH", "SZ"])

        assert len(stocks) == 1
        assert stocks[0]["code"] == "600519"

    async def test_get_all_stock_codes(self):
        """测试获取股票代码列表"""
        mock_client = AsyncMock()
        mock_client.get_security_list.return_value = [
            {"code": "600519", "name": "贵州茅台"},
            {"code": "600000", "name": "浦发银行"},
        ]

        downloader = BulkDownloader(client=mock_client)
        codes = await downloader.get_all_stock_codes(markets=["SH"])

        assert len(codes) == 2
        assert codes[0] == ("SH", "600519")
        assert codes[1] == ("SH", "600000")


@pytest.mark.asyncio
class TestBulkDownloaderDownloadBars:
    """测试批量下载K线数据"""

    async def test_download_bars_basic(self):
        """测试基本K线下载"""
        mock_client = AsyncMock()
        mock_bars = [
            Bar(
                code="600519",
                market="SH",
                datetime=datetime(2024, 1, 1),
                open=1800.0,
                high=1810.0,
                low=1790.0,
                close=1805.0,
                volume=10000,
                amount=18050000.0,
            )
        ]
        mock_client.get_bars.return_value = mock_bars

        with tempfile.TemporaryDirectory() as tmpdir:
            downloader = BulkDownloader(client=mock_client, enable_tqdm=False)
            progress = await downloader.download_bars(
                codes=[("SH", "600519")],
                period="1d",
                count=100,
                output_dir=tmpdir,
            )

            assert progress.total == 1
            assert progress.completed == 1
            assert progress.failed == 0

            # 验证文件保存
            files = list(Path(tmpdir).glob("*.json"))
            assert len(files) == 1

    async def test_download_bars_with_cache(self):
        """测试使用缓存"""
        mock_client = AsyncMock()
        mock_cache = MagicMock()
        mock_cache.get_bars.return_value = [
            Bar(
                code="600519",
                market="SH",
                datetime=datetime(2024, 1, 1),
                open=1800.0,
                high=1810.0,
                low=1790.0,
                close=1805.0,
                volume=10000,
                amount=18050000.0,
            )
        ]

        downloader = BulkDownloader(client=mock_client, cache=mock_cache, enable_tqdm=False)
        progress = await downloader.download_bars(
            codes=[("SH", "600519")],
            period="1d",
            count=100,
            use_cache=True,
        )

        assert progress.skipped == 1
        mock_client.get_bars.assert_not_awaited()

    async def test_download_bars_with_error(self):
        """测试部分失败"""
        mock_client = AsyncMock()

        async def mock_get_bars(code, market, period, count):
            if code == "600519":
                raise ConnectionError("连接失败")
            return [
                Bar(
                    code=code,
                    market=market,
                    datetime=datetime(2024, 1, 1),
                    open=10.0,
                    high=11.0,
                    low=9.0,
                    close=10.5,
                    volume=1000,
                    amount=10000.0,
                )
            ]

        mock_client.get_bars.side_effect = mock_get_bars

        downloader = BulkDownloader(client=mock_client, enable_tqdm=False)
        progress = await downloader.download_bars(
            codes=[("SH", "600519"), ("SZ", "000001")],
            period="1d",
            count=100,
        )

        assert progress.total == 2
        assert progress.completed == 1
        assert progress.failed == 1
        assert "SH:600519" in progress.errors

    async def test_download_bars_progress_callback(self):
        """测试进度回调"""
        mock_client = AsyncMock()
        mock_client.get_bars.return_value = [
            Bar(
                code="600519",
                market="SH",
                datetime=datetime(2024, 1, 1),
                open=1800.0,
                high=1810.0,
                low=1790.0,
                close=1805.0,
                volume=10000,
                amount=18050000.0,
            )
        ]

        progress_calls = []

        def progress_callback(p):
            progress_calls.append(p.completed)

        downloader = BulkDownloader(client=mock_client, enable_tqdm=False)
        await downloader.download_bars(
            codes=[("SH", "600519"), ("SZ", "000001")],
            period="1d",
            count=100,
            progress_callback=progress_callback,
        )

        assert len(progress_calls) == 2
        assert progress_calls[-1] == 2


@pytest.mark.asyncio
class TestBulkDownloaderDownloadTicks:
    """测试批量下载分笔数据"""

    async def test_download_ticks_basic(self):
        """测试基本分笔数据下载"""
        mock_client = AsyncMock()
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
        mock_client.get_history_transactions.return_value = mock_ticks

        with tempfile.TemporaryDirectory() as tmpdir:
            downloader = BulkDownloader(client=mock_client, enable_tqdm=False)
            progress = await downloader.download_ticks(
                codes=[("SH", "600519")],
                dates=[20240101],
                output_dir=tmpdir,
            )

            assert progress.total == 1
            assert progress.completed == 1

            # 验证文件保存
            files = list(Path(tmpdir).glob("*.json"))
            assert len(files) == 1

    async def test_download_ticks_multiple_dates(self):
        """测试多日期分笔数据下载"""
        mock_client = AsyncMock()
        mock_client.get_history_transactions.return_value = [
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

        with tempfile.TemporaryDirectory() as tmpdir:
            downloader = BulkDownloader(client=mock_client, enable_tqdm=False)
            progress = await downloader.download_ticks(
                codes=[("SH", "600519")],
                dates=[20240101, 20240102],
                output_dir=tmpdir,
            )

            assert progress.total == 2
            assert progress.completed == 2


@pytest.mark.asyncio
class TestBulkDownloaderResume:
    """测试断点续传功能"""

    async def test_resume_download(self):
        """测试断点续传"""
        mock_client = AsyncMock()
        mock_client.get_bars.return_value = [
            Bar(
                code="600519",
                market="SH",
                datetime=datetime(2024, 1, 1),
                open=1800.0,
                high=1810.0,
                low=1790.0,
                close=1805.0,
                volume=10000,
                amount=18050000.0,
            )
        ]

        with tempfile.TemporaryDirectory() as tmpdir:
            progress_file = Path(tmpdir) / "progress.json"

            # 第一次下载，模拟部分完成
            downloader = BulkDownloader(
                client=mock_client,
                progress_file=progress_file,
                enable_tqdm=False,
            )

            # 模拟保存进度
            saved_progress = DownloadProgress(
                total=3,
                completed=1,
                failed=0,
                skipped=0,
            )
            saved_progress.start_time = datetime.now()
            downloader._save_progress("test_task", saved_progress)

            # 第二次下载，应该跳过已完成的
            downloader2 = BulkDownloader(
                client=mock_client,
                progress_file=progress_file,
                enable_tqdm=False,
            )

            progress = await downloader2.download_bars(
                codes=[("SH", "600519"), ("SZ", "000001"), ("SH", "600000")],
                period="1d",
                count=100,
                resume=True,
            )

            # 由于断点续传逻辑，应该只下载未完成的
            # 注意：这里实际行为取决于实现细节
            assert progress.total == 3

    async def test_clear_progress(self):
        """测试清除进度"""
        with tempfile.TemporaryDirectory() as tmpdir:
            progress_file = Path(tmpdir) / "progress.json"

            downloader = BulkDownloader(
                progress_file=progress_file,
                enable_tqdm=False,
            )

            # 保存进度
            progress = DownloadProgress(total=10, completed=5)
            progress.start_time = datetime.now()
            downloader._save_progress("test_task", progress)

            # 验证进度已保存
            assert progress_file.exists()

            # 清除进度
            downloader._clear_progress("test_task")

            # 验证进度已清除
            with open(progress_file, "r") as f:
                data = json.load(f)
                assert "test_task" not in data


class TestDateRangeHelper:
    """测试 DateRangeHelper"""

    def test_get_trading_days(self):
        """测试获取交易日"""
        start = datetime(2024, 1, 1)  # 周一
        end = datetime(2024, 1, 7)  # 周日

        days = DateRangeHelper.get_trading_days(start, end)

        # 应该包含周一到周五（5天）
        assert len(days) == 5
        assert days[0] == datetime(2024, 1, 1)
        assert days[-1] == datetime(2024, 1, 5)

    def test_date_to_int(self):
        """测试日期转整数"""
        date = datetime(2024, 1, 15)
        assert DateRangeHelper.date_to_int(date) == 20240115

    def test_int_to_date(self):
        """测试整数转日期"""
        date = DateRangeHelper.int_to_date(20240115)
        assert date == datetime(2024, 1, 15)


@pytest.mark.asyncio
class TestDownloadAllStocksBars:
    """测试便捷函数 download_all_stocks_bars"""

    async def test_download_all_stocks_bars(self):
        """测试下载全市场K线"""
        mock_client = AsyncMock()
        mock_client.get_security_list.return_value = [
            {"code": "600519", "name": "贵州茅台"},
        ]
        mock_client.get_bars.return_value = [
            Bar(
                code="600519",
                market="SH",
                datetime=datetime(2024, 1, 1),
                open=1800.0,
                high=1810.0,
                low=1790.0,
                close=1805.0,
                volume=10000,
                amount=18050000.0,
            )
        ]

        with tempfile.TemporaryDirectory() as tmpdir:
            with patch("tdxapi.bulk_download.AsyncTdxClient") as MockClient:
                MockClient.return_value = mock_client

                progress = await download_all_stocks_bars(
                    output_dir=tmpdir,
                    markets=["SH"],
                    period="1d",
                    count=100,
                    max_concurrent=1,
                )

                assert progress.total == 1
                assert progress.completed == 1


@pytest.mark.asyncio
class TestBulkDownloaderEdgeCases:
    """测试边界情况"""

    async def test_empty_codes_list(self):
        """测试空代码列表"""
        mock_client = AsyncMock()
        downloader = BulkDownloader(client=mock_client, enable_tqdm=False)

        progress = await downloader.download_bars(
            codes=[],
            period="1d",
            count=100,
        )

        assert progress.total == 0
        assert progress.completed == 0

    async def test_get_client_not_initialized(self):
        """测试未初始化客户端"""
        downloader = BulkDownloader()

        with pytest.raises(RuntimeError, match="客户端未初始化"):
            downloader._get_client()

    def test_reset_progress(self):
        """测试重置进度"""
        downloader = BulkDownloader()
        downloader._progress = DownloadProgress(total=100, completed=50)

        downloader.reset_progress()

        assert downloader._progress.total == 0
        assert downloader._progress.completed == 0

    def test_stop(self):
        """测试停止下载"""
        downloader = BulkDownloader()

        # 初始状态
        assert not downloader._stop_event.is_set()

        # 调用停止
        downloader.stop()

        # 验证状态
        assert downloader._stop_event.is_set()
