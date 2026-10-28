"""适配器生命周期与跨插件入口载荷编解码。

入口层（``__init__.py`` 里由脚手架生成的插件类）只做薄封装：
参数进来 -> ``decode_params`` -> 适配器执行 -> ``encode_rows`` 出去。
这样跨插件 IPC 的 JSON 序列化不会丢字节（embedding BLOB 用 base64 包装）。
"""

from __future__ import annotations

import base64
import sys
from pathlib import Path
from typing import Any

from . import hot_schema
from .adapters import ADAPTERS, AdapterError, DatabaseAdapter

_BYTES_KEY = "__bytes_b64__"


def ensure_vendor_path(plugin_dir: str | Path) -> bool:
    """把 ``<plugin_dir>/vendor`` 加进 sys.path（幂等）。返回是否存在 vendor 目录。"""
    vendor = Path(plugin_dir) / "vendor"
    if not vendor.is_dir():
        return False
    s = str(vendor)
    if s not in sys.path:
        sys.path.insert(0, s)
    return True


def ensure_extra_paths(plugin_dir: str | Path, paths: Any) -> list[str]:
    """``[database] extra_paths``：装在插件目录外的可选驱动（duckdb、oracledb、方言包...）。

    按配置顺序排在 ``vendor/`` 之后（vendor 里的同名包优先，避免外部旧版本顶掉已验证的依赖）；
    相对路径按插件目录解析，不存在的目录跳过。返回实际加入的目录。
    """
    vendor = str(Path(plugin_dir) / "vendor")
    pos = sys.path.index(vendor) + 1 if vendor in sys.path else 0
    added: list[str] = []
    for raw in paths or []:
        p = Path(str(raw)).expanduser()
        if not p.is_absolute():
            p = Path(plugin_dir) / p
        s = str(p)
        if p.is_dir() and s not in sys.path:
            sys.path.insert(pos + len(added), s)
            added.append(s)
    return added


#: 各驱动需要能 import 的 Python 模块（http 只用标准库）
DRIVER_MODULES: dict[str, list[str]] = {
    "sqlite": ["aiosqlite"],
    "postgres": ["asyncpg"],
    "mysql": ["aiomysql", "pymysql"],
    "mongo": ["motor", "pymongo", "dns"],
    "neo4j": ["neo4j"],
    "redis": ["redis"],
    "sqlalchemy": ["sqlalchemy"],
    "odbc": ["pyodbc"],
    "http": [],
}

#: driver = "sqlalchemy" 时常用方言：(数据库, URL 示例, 需要的模块, pip 包)。async 方言另需 greenlet。
SQLALCHEMY_DIALECTS: list[tuple[str, str, list[str], str]] = [
    ("SQLite", "sqlite:///D:/data/x.db", ["sqlite3"], ""),
    ("SQLite (async)", "sqlite+aiosqlite:///D:/data/x.db", ["aiosqlite", "greenlet"], "greenlet"),
    ("PostgreSQL", "postgresql+psycopg://user:pass@host:5432/db", ["psycopg"], "psycopg[binary]"),
    ("PostgreSQL (async)", "postgresql+asyncpg://user:pass@host:5432/db", ["asyncpg", "greenlet"], "greenlet"),
    ("MySQL / MariaDB / TiDB", "mysql+pymysql://user:pass@host:3306/db", ["pymysql"], ""),
    ("SQL Server", "mssql+pyodbc://user:pass@host/db?driver=ODBC+Driver+17+for+SQL+Server", ["pyodbc"], ""),
    ("SQL Server (pymssql)", "mssql+pymssql://user:pass@host/db", ["pymssql"], "pymssql"),
    ("Oracle", "oracle+oracledb://user:pass@host:1521/?service_name=XE", ["oracledb"], "oracledb"),
    ("DuckDB", "duckdb:///D:/data/x.duckdb", ["duckdb", "duckdb_engine"], "duckdb duckdb-engine"),
    ("ClickHouse", "clickhouse+http://user:pass@host:8123/db", ["clickhouse_sqlalchemy"], "clickhouse-sqlalchemy"),
    ("Snowflake", "snowflake://user:pass@account/db/schema?warehouse=wh", ["snowflake.sqlalchemy"],
     "snowflake-sqlalchemy"),
    ("CockroachDB", "cockroachdb://user:pass@host:26257/db", ["sqlalchemy_cockroachdb", "psycopg2"],
     "sqlalchemy-cockroachdb psycopg2-binary"),
    ("达梦 DM", "dm+dmPython://SYSDBA:pass@host:5236", ["dmPython", "sqlalchemy_dm"], "dmPython sqlalchemy_dm（达梦安装包/官方提供）"),
]


def _importable(modules: list[str]) -> tuple[bool, str | None]:
    import importlib

    for mod in modules:
        try:
            importlib.import_module(mod)
        except Exception as exc:  # ImportError 之外，ABI 不匹配等也算不可用
            return False, f"{mod}: {exc}"
    return True, None


def driver_report(extra_paths: list[str]) -> dict:
    """``db_drivers`` 入口的数据：每个驱动的依赖是否可 import、SQLAlchemy 方言、ODBC 驱动、安装提示。

    同步函数（import 会阻塞），入口层放进线程执行。
    """
    target = extra_paths[0] if extra_paths else "<extra_path>"
    hint = f'uv pip install --python 3.11 --target "{target}" '
    drivers = []
    for name in ADAPTERS:
        ok, err = _importable(DRIVER_MODULES.get(name, []))
        drivers.append({"driver": name, "available": ok, "error": err})
    dialects = []
    for label, url, modules, pip in SQLALCHEMY_DIALECTS:
        ok, err = _importable(modules)
        dialects.append({"database": label, "url": url, "available": ok, "error": err,
                         "install": None if ok or not pip else hint + pip})
    try:
        import pyodbc

        odbc_drivers: list[str] | str = list(pyodbc.drivers())
    except Exception as exc:
        odbc_drivers = f"pyodbc unavailable: {exc}"
    return {
        "drivers": drivers,
        "sqlalchemy_dialects": dialects,
        "odbc_drivers": odbc_drivers,
        "extra_paths": extra_paths,
        "install_hint": hint + "<package>   # 然后把该目录写进 [database] extra_paths 并重启插件",
    }


def encode_value(value: Any) -> Any:
    """bytes -> {"__bytes_b64__": ...}，其余透传；递归处理容器。"""
    if isinstance(value, (bytes, bytearray)):
        return {_BYTES_KEY: base64.b64encode(bytes(value)).decode("ascii")}
    if isinstance(value, tuple):
        return [encode_value(v) for v in value]
    if isinstance(value, list):
        return [encode_value(v) for v in value]
    if isinstance(value, dict):
        return {k: encode_value(v) for k, v in value.items()}
    return value


def decode_value(value: Any) -> Any:
    """{"__bytes_b64__": ...} -> bytes；其余透传；递归处理容器。"""
    if isinstance(value, dict):
        if set(value.keys()) == {_BYTES_KEY} and isinstance(value[_BYTES_KEY], str):
            return base64.b64decode(value[_BYTES_KEY])
        return {k: decode_value(v) for k, v in value.items()}
    if isinstance(value, list):
        return [decode_value(v) for v in value]
    return value


def decode_params(params: Any) -> tuple:
    """入口层收到 JSON 数组形态的参数 -> 适配器用的 tuple。"""
    if params is None:
        return ()
    if isinstance(params, tuple):
        return tuple(decode_value(p) for p in params)
    if isinstance(params, list):
        return tuple(decode_value(p) for p in params)
    return (decode_value(params),)


class DatabaseService:
    """持有当前适配器，负责 driver 选择、连接与关闭。"""

    def __init__(self, data_dir=None) -> None:
        self._adapter: DatabaseAdapter | None = None
        self._driver: str = ""
        self._data_dir = data_dir

    @property
    def adapter(self) -> DatabaseAdapter:
        if self._adapter is None:
            raise AdapterError("database adapter not started")
        return self._adapter

    @property
    def driver(self) -> str:
        return self._driver

    # ------------------------------------------------------------------

    def _sqlite_default_path(self, config: dict) -> str:
        if self._data_dir is not None:
            return str(self._data_dir / "plugin_database.db")
        return str(config.get("path") or "plugin_database.db")

    async def start(self, database_cfg: dict) -> dict:
        """按 [database] 配置段选择并连接驱动。返回连接描述。

        先连接新适配器，成功后才关掉旧适配器：切换失败（DSN 不可达、依赖缺失）
        时旧连接原样保留，``db_status`` 仍与可用状态一致。
        """
        cfg = dict(database_cfg or {})
        driver = str(cfg.get("driver", "sqlite")).strip().lower()
        factory = ADAPTERS.get(driver)
        if factory is None:
            raise AdapterError(
                f"unknown database driver {driver!r}; expected one of {sorted(ADAPTERS)}"
            )
        sub = dict(cfg.get(driver) or {})
        if driver == "sqlite" and not sub.get("path") and self._data_dir is not None:
            sub["path"] = self._sqlite_default_path(sub)
        adapter = factory()
        try:
            await adapter.connect(sub)
        except BaseException:
            # 连接失败：新适配器没建起来，旧适配器必须原样可用。
            await self._quiet_close(adapter)
            raise
        previous = self._adapter
        self._adapter = adapter
        self._driver = driver
        if previous is not None:
            await self._quiet_close(previous)
        return {"driver": driver}

    @staticmethod
    async def _quiet_close(adapter: DatabaseAdapter) -> None:
        """关闭适配器并吞掉错误（切换路径不能在旧连接上二次失败）。"""
        try:
            await adapter.close()
        except Exception:
            pass

    async def stop(self) -> None:
        adapter, self._adapter, self._driver = self._adapter, None, ""
        if adapter is not None:
            await adapter.close()

    # ------------------------------------------------------------------

    async def execute(self, sql: str, params: Any) -> int:
        return await self.adapter.execute(sql, decode_params(params))

    async def fetch_all(self, sql: str, params: Any) -> list[dict]:
        return [encode_value(r) for r in await self.adapter.fetch_all(sql, decode_params(params))]

    async def fetch_one(self, sql: str, params: Any) -> dict | None:
        row = await self.adapter.fetch_one(sql, decode_params(params))
        return encode_value(row) if row is not None else None

    # 事务说明：transaction() 的异步上下文管理器只能在持有适配器对象的进程内
    # 使用（跨插件 IPC 无法传递上下文管理器）。跨插件消费方使用 execute/fetch
    # 入口；需要事务的知识库写入走 INSERT OR REPLACE 单语句幂等写。


__all__ = [
    "DatabaseService",
    "decode_params",
    "decode_value",
    "encode_value",
    "hot_schema",
]
