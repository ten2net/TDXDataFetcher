"""
SQLite本地缓存模块

提供K线数据和分笔数据的本地SQLite存储和查询功能。
支持zlib和lz4数据压缩，压缩/解压对用户透明。
"""

import json
import sqlite3
import zlib
from contextlib import contextmanager
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import List, Optional, Union

from tdxapi.models import Bar, Tick


class CompressionType(Enum):
    """压缩类型枚举"""
    NONE = "none"
    ZLIB = "zlib"
    LZ4 = "lz4"


class TdxCache:
    """通达信数据本地SQLite缓存类

    支持K线数据(bars)和分笔数据(ticks)的存储和查询，
    可按股票代码、日期范围进行查询。
    支持数据压缩存储（zlib/lz4），压缩/解压对用户透明。

    Attributes:
        db_path: SQLite数据库文件路径
        compression: 压缩类型
        compression_level: 压缩级别（zlib: 0-9, lz4: 1-12）
    """

    # 数据库版本，用于迁移
    DB_VERSION = 2

    def __init__(
        self,
        db_path: Union[str, Path] = "tdx_cache.db",
        compression: Union[str, CompressionType] = CompressionType.ZLIB,
        compression_level: Optional[int] = None,
    ):
        """初始化缓存实例

        Args:
            db_path: SQLite数据库文件路径，默认为当前目录下的tdx_cache.db
            compression: 压缩类型，可选 'none', 'zlib', 'lz4'，默认为 'zlib'
            compression_level: 压缩级别，None则使用默认值（zlib: 6, lz4: 5）
        """
        self.db_path = Path(db_path)

        # 处理压缩类型
        if isinstance(compression, str):
            compression = CompressionType(compression.lower())
        self.compression = compression

        # 设置压缩级别
        if compression_level is None:
            if compression == CompressionType.ZLIB:
                self.compression_level = 6
            elif compression == CompressionType.LZ4:
                self.compression_level = 5
            else:
                self.compression_level = 0
        else:
            self.compression_level = compression_level

        # 检查lz4可用性
        self._lz4_available = self._check_lz4_available()
        if self.compression == CompressionType.LZ4 and not self._lz4_available:
            raise ImportError(
                "lz4 is not installed. Install it with: pip install tdxapi[compression]"
            )

        self._init_db()

    def _check_lz4_available(self) -> bool:
        """检查lz4是否可用"""
        try:
            import lz4.frame  # noqa: F401
            return True
        except ImportError:
            return False

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

            # 元数据表（存储版本和配置）
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS metadata (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                )
                """)

            # K线数据表（使用BLOB存储压缩数据）
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS bars (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    code TEXT NOT NULL,
                    market TEXT NOT NULL,
                    datetime TEXT NOT NULL,
                    data BLOB NOT NULL,
                    compression TEXT NOT NULL DEFAULT 'none',
                    UNIQUE(code, market, datetime)
                )
                """)

            # 分笔数据表（使用BLOB存储压缩数据）
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS ticks (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    code TEXT NOT NULL,
                    market TEXT NOT NULL,
                    time TEXT NOT NULL,
                    data BLOB NOT NULL,
                    compression TEXT NOT NULL DEFAULT 'none',
                    UNIQUE(code, market, time)
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

            # 检查并更新数据库版本
            cursor.execute(
                "SELECT value FROM metadata WHERE key = 'version'"
            )
            row = cursor.fetchone()
            current_version = int(row["value"]) if row else 1

            if current_version < self.DB_VERSION:
                self._migrate_db(cursor, current_version)

            # 保存当前版本
            cursor.execute(
                """
                INSERT OR REPLACE INTO metadata (key, value)
                VALUES ('version', ?)
                """,
                (str(self.DB_VERSION),),
            )

    def _migrate_db(self, cursor, from_version: int) -> None:
        """数据库迁移

        Args:
            cursor: 数据库游标
            from_version: 当前数据库版本
        """
        if from_version < 2:
            # 迁移到版本2：添加压缩支持
            # 旧表结构没有 compression 列，需要迁移数据
            # 这里我们只添加列，旧数据保持为 'none' 压缩类型
            try:
                cursor.execute(
                    "ALTER TABLE bars ADD COLUMN compression TEXT NOT NULL DEFAULT 'none'"
                )
            except sqlite3.OperationalError:
                pass  # 列已存在

            try:
                cursor.execute(
                    "ALTER TABLE ticks ADD COLUMN compression TEXT NOT NULL DEFAULT 'none'"
                )
            except sqlite3.OperationalError:
                pass  # 列已存在

    def _compress(self, data: bytes) -> tuple[bytes, CompressionType]:
        """压缩数据

        Args:
            data: 原始数据

        Returns:
            (压缩后的数据, 使用的压缩类型)
        """
        if self.compression == CompressionType.NONE:
            return data, CompressionType.NONE

        if self.compression == CompressionType.ZLIB:
            compressed = zlib.compress(data, level=self.compression_level)
            return compressed, CompressionType.ZLIB

        if self.compression == CompressionType.LZ4:
            import lz4.frame
            compressed = lz4.frame.compress(
                data, compression_level=self.compression_level
            )
            return compressed, CompressionType.LZ4

        return data, CompressionType.NONE

    def _decompress(self, data: bytes, compression: str) -> bytes:
        """解压数据

        Args:
            data: 压缩数据
            compression: 压缩类型字符串

        Returns:
            解压后的原始数据
        """
        comp_type = CompressionType(compression)

        if comp_type == CompressionType.NONE:
            return data

        if comp_type == CompressionType.ZLIB:
            return zlib.decompress(data)

        if comp_type == CompressionType.LZ4:
            import lz4.frame
            return lz4.frame.decompress(data)

        return data

    def _bar_to_dict(self, bar: Bar) -> dict:
        """将Bar对象转换为字典"""
        return {
            "code": bar.code,
            "market": bar.market,
            "datetime": bar.datetime.isoformat(),
            "open": bar.open,
            "high": bar.high,
            "low": bar.low,
            "close": bar.close,
            "volume": bar.volume,
            "amount": bar.amount,
        }

    def _dict_to_bar(self, data: dict) -> Bar:
        """将字典转换为Bar对象"""
        return Bar(
            code=data["code"],
            market=data["market"],
            datetime=datetime.fromisoformat(data["datetime"]),
            open=data["open"],
            high=data["high"],
            low=data["low"],
            close=data["close"],
            volume=data["volume"],
            amount=data["amount"],
        )

    def _tick_to_dict(self, tick: Tick) -> dict:
        """将Tick对象转换为字典"""
        return {
            "code": tick.code,
            "market": tick.market,
            "time": tick.time,
            "price": tick.price,
            "volume": tick.volume,
            "amount": tick.amount,
            "direction": tick.direction,
        }

    def _dict_to_tick(self, data: dict) -> Tick:
        """将字典转换为Tick对象"""
        return Tick(
            code=data["code"],
            market=data["market"],
            time=data["time"],
            price=data["price"],
            volume=data["volume"],
            amount=data["amount"],
            direction=data["direction"],
        )

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
                    # 序列化为JSON并压缩
                    json_data = json.dumps(self._bar_to_dict(bar), ensure_ascii=False)
                    compressed_data, compression_used = self._compress(
                        json_data.encode("utf-8")
                    )

                    cursor.execute(
                        """
                        INSERT OR IGNORE INTO bars
                        (code, market, datetime, data, compression)
                        VALUES (?, ?, ?, ?, ?)
                        """,
                        (
                            bar.code,
                            bar.market,
                            bar.datetime.isoformat(),
                            compressed_data,
                            compression_used.value,
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

            bars = []
            for row in rows:
                try:
                    # 解压并反序列化
                    compressed_data = row["data"]
                    compression = row["compression"]
                    json_data = self._decompress(compressed_data, compression)
                    data = json.loads(json_data.decode("utf-8"))
                    bars.append(self._dict_to_bar(data))
                except (json.JSONDecodeError, zlib.error, Exception):
                    # 数据损坏，跳过
                    continue

            return bars

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
                    # 序列化为JSON并压缩
                    json_data = json.dumps(self._tick_to_dict(tick), ensure_ascii=False)
                    compressed_data, compression_used = self._compress(
                        json_data.encode("utf-8")
                    )

                    cursor.execute(
                        """
                        INSERT OR IGNORE INTO ticks
                        (code, market, time, data, compression)
                        VALUES (?, ?, ?, ?, ?)
                        """,
                        (
                            tick.code,
                            tick.market,
                            tick.time,
                            compressed_data,
                            compression_used.value,
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

            ticks = []
            for row in rows:
                try:
                    # 解压并反序列化
                    compressed_data = row["data"]
                    compression = row["compression"]
                    json_data = self._decompress(compressed_data, compression)
                    data = json.loads(json_data.decode("utf-8"))
                    ticks.append(self._dict_to_tick(data))
                except (json.JSONDecodeError, zlib.error, Exception):
                    # 数据损坏，跳过
                    continue

            return ticks

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

            # 获取压缩统计
            cursor.execute(
                "SELECT compression, COUNT(*) FROM bars GROUP BY compression"
            )
            bars_compression = {row[0]: row[1] for row in cursor.fetchall()}

            cursor.execute(
                "SELECT compression, COUNT(*) FROM ticks GROUP BY compression"
            )
            ticks_compression = {row[0]: row[1] for row in cursor.fetchall()}

            return {
                "db_path": str(self.db_path),
                "bars_count": bars_count,
                "ticks_count": ticks_count,
                "bars_stocks": bars_stocks,
                "ticks_stocks": ticks_stocks,
                "bars_compression": bars_compression,
                "ticks_compression": ticks_compression,
                "default_compression": self.compression.value,
            }

    def get_compression_stats(self) -> dict:
        """获取压缩统计信息

        Returns:
            包含压缩前后数据大小对比的字典
        """
        with self._get_connection() as conn:
            cursor = conn.cursor()

            # 计算bars的总数据大小（近似）
            cursor.execute("SELECT SUM(LENGTH(data)) FROM bars")
            bars_compressed_size = cursor.fetchone()[0] or 0

            cursor.execute(
                """
                SELECT SUM(LENGTH(data)) FROM bars WHERE compression = 'none'
                """
            )
            bars_uncompressed_size = cursor.fetchone()[0] or 0

            # 计算ticks的总数据大小（近似）
            cursor.execute("SELECT SUM(LENGTH(data)) FROM ticks")
            ticks_compressed_size = cursor.fetchone()[0] or 0

            cursor.execute(
                """
                SELECT SUM(LENGTH(data)) FROM ticks WHERE compression = 'none'
                """
            )
            ticks_uncompressed_size = cursor.fetchone()[0] or 0

            total_compressed = bars_compressed_size + ticks_compressed_size
            total_uncompressed = bars_uncompressed_size + ticks_uncompressed_size

            compression_ratio = (
                (total_compressed / total_uncompressed * 100)
                if total_uncompressed > 0
                else 0
            )

            return {
                "bars_compressed_bytes": bars_compressed_size,
                "bars_uncompressed_bytes": bars_uncompressed_size,
                "ticks_compressed_bytes": ticks_compressed_size,
                "ticks_uncompressed_bytes": ticks_uncompressed_size,
                "total_compressed_bytes": total_compressed,
                "total_uncompressed_bytes": total_uncompressed,
                "compression_ratio_percent": round(compression_ratio, 2),
            }
