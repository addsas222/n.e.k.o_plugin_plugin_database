"""ODBC 通用适配器（pyodbc）：本机装了 ODBC 驱动的任何库都能连。

SQL Server、Access/Excel/CSV（Windows 自带 Access 驱动）、达梦 / 人大金仓 / Oracle / DB2 /
Cassandra（Simba/DataStax ODBC）等厂商 ODBC 驱动。``sql`` 是目标库原生 SQL，``?`` 占位符
是 ODBC 原生语法，不转译。

阻塞调用全部放进 ``asyncio.to_thread``。事务外每次调用开一条连接（ODBC 驱动管理器自带连接池），
执行后提交并关闭；``transaction()`` 钉住一条连接，期间 execute/fetch 经 contextvar 路由到它。
连接默认 autocommit 关闭；驱动不支持关闭（如 Access Text 驱动报 HYC00）时自动退回 autocommit，
此时 ``transaction()`` 返回结构化错误而不是假装有事务。
"""

from __future__ import annotations

import asyncio
import contextvars
from typing import Any

from .base import AdapterError, DatabaseAdapter, TransactionHandle

#: SQL_DBMS_NAME 关键字 -> hot_knowledge / skill_stats 使用的 SQL 方言族
_HOT_DIALECT = (("sqlite", "sqlite"), ("postgres", "postgres"), ("mysql", "mysql"), ("mariadb", "mysql"))
#: 连接串分段配置键（connection_string 为空时拼接）
_PARTS = (("dsn", "DSN"), ("driver", "DRIVER"), ("server", "SERVER"), ("database", "DATABASE"),
          ("uid", "UID"), ("pwd", "PWD"))


def _odbc_value(key: str, value: str) -> str:
    """ODBC 连接串取值：DRIVER 总是加花括号；含 ``;{}`` 或首尾空格的值用 ``{...}`` 包住（``}`` 写成 ``}}``）。"""
    if key == "DRIVER" and value.startswith("{"):
        return value
    if key == "DRIVER" or any(ch in value for ch in ";{}") or value != value.strip():
        return "{" + value.replace("}", "}}") + "}"
    return value


def build_connection_string(config: dict[str, Any]) -> str:
    cs = str(config.get("connection_string") or "").strip()
    if cs:
        return cs
    return ";".join(
        f"{key}={_odbc_value(key, str(config[name]))}" for name, key in _PARTS if str(config.get(name) or "").strip()
    )


def _rows(cur, one: bool) -> Any:
    if cur.description is None:  # 非查询语句
        return None if one else []
    cols = [d[0] for d in cur.description]
    if one:
        row = cur.fetchone()
        return dict(zip(cols, row)) if row is not None else None
    return [dict(zip(cols, r)) for r in cur.fetchall()]


class _OdbcTransaction(TransactionHandle):
    def __init__(self, adapter: "ODBCAdapter") -> None:
        self._adapter = adapter
        self._conn: Any = None
        self._token: contextvars.Token | None = None

    async def _begin(self) -> None:
        try:
            self._conn = await asyncio.to_thread(self._adapter._connect)
        except Exception as exc:
            raise AdapterError(f"odbc transaction start failed: {exc}") from exc
        self._token = self._adapter._txn_conn.set(self._conn)

    async def _finish(self, commit: bool) -> None:
        conn, token = self._conn, self._token
        self._conn = self._token = None
        if token is not None:
            self._adapter._txn_conn.reset(token)
        if conn is None:
            return

        def end():
            try:
                if commit:
                    conn.commit()
                else:
                    conn.rollback()
            finally:
                conn.close()

        try:
            await asyncio.to_thread(end)
        except Exception as exc:
            raise AdapterError(f"odbc {'commit' if commit else 'rollback'} failed: {exc}") from exc

    async def _commit(self) -> None:
        await self._finish(commit=True)

    async def _rollback(self) -> None:
        await self._finish(commit=False)


class ODBCAdapter(DatabaseAdapter):
    """config 键：
        connection_string: 完整 ODBC 连接串（优先）
        dsn / driver / server / database / uid / pwd: connection_string 为空时拼接
        timeout: 登录超时秒数（默认 10）
    """

    driver = "odbc"

    _txn_conn: contextvars.ContextVar = contextvars.ContextVar("odbc_txn_conn", default=None)

    def __init__(self) -> None:
        self._cs = ""
        self._timeout = 10
        self._autocommit = False
        self._connected = False
        self.hot_dialect: str | None = None

    def _connect(self):
        import pyodbc

        return pyodbc.connect(self._cs, autocommit=self._autocommit, timeout=self._timeout)

    async def connect(self, config: dict[str, Any]) -> None:
        self._cs = build_connection_string(config)
        if not self._cs:
            raise AdapterError(
                "odbc adapter requires [database.odbc] connection_string (or dsn / driver+server+database)"
            )
        self._timeout = int(config.get("timeout", 10))
        self._autocommit = False

        def probe() -> str:
            import pyodbc

            try:
                cn = self._connect()
            except pyodbc.Error as exc:
                if not (exc.args and exc.args[0] == "HYC00"):
                    raise
                self._autocommit = True  # 驱动不支持关闭 autocommit（只读/文本类驱动）
                cn = self._connect()
            try:
                return str(cn.getinfo(pyodbc.SQL_DBMS_NAME) or "")
            finally:
                cn.close()

        try:
            dbms = (await asyncio.to_thread(probe)).lower()
        except Exception as exc:
            raise AdapterError(f"odbc connect failed: {exc}") from exc
        self.hot_dialect = next((d for key, d in _HOT_DIALECT if key in dbms), None)
        self._connected = True

    def _require(self) -> None:
        if not self._connected:
            raise AdapterError("odbc adapter not connected")

    async def _call(self, sql: str, params: tuple, fn) -> Any:
        """执行一条语句后把游标交给 ``fn(cursor)`` 取结果。"""
        self._require()
        params = tuple(params)

        def run(cn):
            cur = cn.cursor()
            try:
                if params:
                    cur.execute(sql, params)
                else:
                    cur.execute(sql)
                return fn(cur)
            finally:
                cur.close()

        conn = self._txn_conn.get()
        if conn is not None:
            return await asyncio.to_thread(run, conn)

        def job():
            cn = self._connect()
            try:
                result = run(cn)
                if not self._autocommit:
                    cn.commit()
                return result
            finally:
                cn.close()  # 未提交的写在 close 时由驱动回滚

        return await asyncio.to_thread(job)

    async def execute(self, sql: str, params: tuple = ()) -> int:
        try:
            return await self._call(sql, params, lambda cur: max(cur.rowcount, 0))
        except AdapterError:
            raise
        except Exception as exc:
            raise AdapterError(f"odbc execute failed: {exc}") from exc

    async def fetch_all(self, sql: str, params: tuple = ()) -> list[dict]:
        try:
            return await self._call(sql, params, lambda cur: _rows(cur, one=False))
        except AdapterError:
            raise
        except Exception as exc:
            raise AdapterError(f"odbc fetch failed: {exc}") from exc

    async def fetch_one(self, sql: str, params: tuple = ()) -> dict | None:
        try:
            return await self._call(sql, params, lambda cur: _rows(cur, one=True))
        except AdapterError:
            raise
        except Exception as exc:
            raise AdapterError(f"odbc fetch failed: {exc}") from exc

    async def transaction(self) -> TransactionHandle:
        self._require()
        if self._autocommit:
            raise AdapterError(
                "this ODBC driver cannot turn autocommit off, so transactions are unavailable"
            )
        return _OdbcTransaction(self)

    async def close(self) -> None:
        self._connected = False
