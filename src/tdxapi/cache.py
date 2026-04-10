"""
SQLite本地缓存模块

提供K线数据和分笔数据的本地SQLite存储和查询功能。
"""

import sqlite3
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Union

from tdxapi.models import Bar, Tick


class TdxCache:
    """通达信数据本地SQLite缓存类

    支持K线数据(bars)和分笔数据(ticks)的存储和查询，
    可按股票代码、日期范围进行查询。

    Attributes:
        db_path: SQLite数据库文件路径
    """

    def __init__(self, db_path: Union[str, Path] = "tdx_cache.db"):
        """初始化缓存实例

        Args:
            db_path: SQLite数据库文件路径，默认为当前目录下的tdx_cache.db
        """
        self.db_path = Path(db_path)
        self._init_db()

    @contextmanager
    def _get_connection(self):
        """获取数据库连接的上下文管理器

        Yields:
            sqlite3.Connection: 数据库连接对象
        """
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def _init_db(self) -> None:
        """初始化数据库表结构"""
        with self._get_connection() as conn:
            cursor = conn.cursor()

            # K线数据表
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS bars (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    code TEXT NOT NULL,
                    market TEXT NOT NULL,
                    datetime TEXT NOT NULL,
                    open REAL NOT NULL,
                    high REAL NOT NULL,
                    low REAL NOT NULL,
                    close REAL NOT NULL,
                    volume INTEGER NOT NULL,
                    amount REAL NOT NULL,
                    UNIQUE(code, market, datetime)
                )
                """)

            # 分笔数据表
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS ticks (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    code TEXT NOT NULL,
                    market TEXT NOT NULL,
                    time TEXT NOT NULL,
                    price REAL NOT NULL,
                    volume INTEGER NOT NULL,
                    amount REAL NOT NULL,
                    direction INTEGER NOT NULL,
                    UNIQUE(code, market, time, price, volume)
                )
                """)

            # 创建索引优化查询性能
            cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_bars_code_market ON bars(code, market)"
            )
            cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_bars_datetime ON bars(datetime)"
            )
            cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_ticks_code_market ON ticks(code, market)"
            )
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_ticks_time ON ticks(time)")

    def save_bars(self, bars: List[Bar]) -> int:
        """保存K线数据到本地缓存

        Args:
            bars: K线数据列表

        Returns:
            实际保存的数据条数（已存在的数据会被跳过）

        Raises:
            ValueError: 输入数据为空列表
        """
        if not bars:
            raise ValueError("bars列表不能为空")

        saved_count = 0
        with self._get_connection() as conn:
            cursor = conn.cursor()
            for bar in bars:
                try:
                    cursor.execute(
                        """
                        INSERT OR IGNORE INTO bars
                        (code, market, datetime, open, high, low, close, volume, amount)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            bar.code,
                            bar.market,
                            bar.datetime.isoformat(),
                            bar.open,
                            bar.high,
                            bar.low,
                            bar.close,
                            bar.volume,
                            bar.amount,
                        ),
                    )
                    if cursor.rowcount > 0:
                        saved_count += 1
                except sqlite3.Error:
                    # 单条数据失败继续处理其他数据
                    continue

        return saved_count

    def get_bars(
        self,
        code: str,
        market: str,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
    ) -> List[Bar]:
        """从本地缓存查询K线数据

        Args:
            code: 股票代码
            market: 市场代码(SH/SZ/BJ)
            start_date: 开始日期(包含)，为None则不限制
            end_date: 结束日期(包含)，为None则不限制

        Returns:
            K线数据列表，按时间升序排列
        """
        with self._get_connection() as conn:
            cursor = conn.cursor()

            query = "SELECT * FROM bars WHERE code = ? AND market = ?"
            params: List[Union[str, int]] = [code, market]

            if start_date is not None:
                query += " AND datetime >= ?"
                params.append(start_date.isoformat())

            if end_date is not None:
                query += " AND datetime <= ?"
                params.append(end_date.isoformat())

            query += " ORDER BY datetime ASC"

            cursor.execute(query, params)
            rows = cursor.fetchall()

            return [
                Bar(
                    code=row["code"],
                    market=row["market"],
                    datetime=datetime.fromisoformat(row["datetime"]),
                    open=row["open"],
                    high=row["high"],
                    low=row["low"],
                    close=row["close"],
                    volume=row["volume"],
                    amount=row["amount"],
                )
                for row in rows
            ]

    def save_ticks(self, ticks: List[Tick]) -> int:
        """保存分笔数据到本地缓存

        Args:
            ticks: 分笔数据列表

        Returns:
            实际保存的数据条数（已存在的数据会被跳过）

        Raises:
            ValueError: 输入数据为空列表
        """
        if not ticks:
            raise ValueError("ticks列表不能为空")

        saved_count = 0
        with self._get_connection() as conn:
            cursor = conn.cursor()
            for tick in ticks:
                try:
                    cursor.execute(
                        """
                        INSERT OR IGNORE INTO ticks
                        (code, market, time, price, volume, amount, direction)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            tick.code,
                            tick.market,
                            tick.time,
                            tick.price,
                            tick.volume,
                            tick.amount,
                            tick.direction,
                        ),
                    )
                    if cursor.rowcount > 0:
                        saved_count += 1
                except sqlite3.Error:
                    # 单条数据失败继续处理其他数据
                    continue

        return saved_count

    def get_ticks(
        self,
        code: str,
        market: str,
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
    ) -> List[Tick]:
        """从本地缓存查询分笔数据

        Args:
            code: 股票代码
            market: 市场代码(SH/SZ/BJ)
            start_time: 开始时间(包含)，格式"HH:MM:SS"，为None则不限制
            end_time: 结束时间(包含)，格式"HH:MM:SS"，为None则不限制

        Returns:
            分笔数据列表，按时间升序排列
        """
        with self._get_connection() as conn:
            cursor = conn.cursor()

            query = "SELECT * FROM ticks WHERE code = ? AND market = ?"
            params: List[str] = [code, market]

            if start_time is not None:
                query += " AND time >= ?"
                params.append(start_time)

            if end_time is not None:
                query += " AND time <= ?"
                params.append(end_time)

            query += " ORDER BY time ASC"

            cursor.execute(query, params)
            rows = cursor.fetchall()

            return [
                Tick(
                    code=row["code"],
                    market=row["market"],
                    time=row["time"],
                    price=row["price"],
                    volume=row["volume"],
                    amount=row["amount"],
                    direction=row["direction"],
                )
                for row in rows
            ]

    def clear_cache(
        self, code: Optional[str] = None, market: Optional[str] = None
    ) -> int:
        """清除本地缓存数据

        Args:
            code: 股票代码，为None则清除所有股票
            market: 市场代码，为None则清除所有市场

        Returns:
            删除的数据条数
        """
        with self._get_connection() as conn:
            cursor = conn.cursor()
            total_deleted = 0

            # 清除K线数据
            if code and market:
                cursor.execute(
                    "DELETE FROM bars WHERE code = ? AND market = ?",
                    (code, market),
                )
            elif code:
                cursor.execute("DELETE FROM bars WHERE code = ?", (code,))
            elif market:
                cursor.execute("DELETE FROM bars WHERE market = ?", (market,))
            else:
                cursor.execute("DELETE FROM bars")
            total_deleted += cursor.rowcount

            # 清除分笔数据
            if code and market:
                cursor.execute(
                    "DELETE FROM ticks WHERE code = ? AND market = ?",
                    (code, market),
                )
            elif code:
                cursor.execute("DELETE FROM ticks WHERE code = ?", (code,))
            elif market:
                cursor.execute("DELETE FROM ticks WHERE market = ?", (market,))
            else:
                cursor.execute("DELETE FROM ticks")
            total_deleted += cursor.rowcount

            return total_deleted

    def get_cache_info(self) -> dict:
        """获取缓存统计信息

        Returns:
            包含缓存统计信息的字典
        """
        with self._get_connection() as conn:
            cursor = conn.cursor()

            cursor.execute("SELECT COUNT(*) FROM bars")
            bars_count = cursor.fetchone()[0]

            cursor.execute("SELECT COUNT(*) FROM ticks")
            ticks_count = cursor.fetchone()[0]

            cursor.execute("SELECT COUNT(DISTINCT code || '_' || market) FROM bars")
            bars_stocks = cursor.fetchone()[0]

            cursor.execute("SELECT COUNT(DISTINCT code || '_' || market) FROM ticks")
            ticks_stocks = cursor.fetchone()[0]

            return {
                "db_path": str(self.db_path),
                "bars_count": bars_count,
                "ticks_count": ticks_count,
                "bars_stocks": bars_stocks,
                "ticks_stocks": ticks_stocks,
            }
