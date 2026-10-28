"""SQLAlchemy 通用适配器：任意 SQLAlchemy URL（同步或异步方言）都能连。

``sql`` 是目标库的原生 SQL，占位符统一写 ``?``，按方言的 DBAPI paramstyle 转译后走
``exec_driver_sql``（qmark 原样；format/pyformat -> ``%s``；numeric -> ``:1``；
numeric_dollar -> ``$1``；named -> ``:p1``）。跳过引号/注释里的 ``?``，规则同 postgres 适配器。
不带 params 时 SQL 原样下发（不转译），PG 的 jsonb ``?`` 运算符此时可直接写。

同步方言（sqlite、mssql+pyodbc、oracle+oracledb、duckdb ...）整段放进 ``asyncio.to_thread``；
异步方言（sqlite+aiosqlite、postgresql+asyncpg ...）用 ``create_async_engine``，需要 greenlet。
方言包与 DBAPI 不在 vendor 里时装进 ``[database] extra_paths``（``db_drivers`` 入口列出缺什么）。

事务：``transaction()`` 钉住一条连接，期间本适配器的 execute/fetch 经 contextvar 路由到它
（同 postgres 适配器）；事务外每次调用借一条池连接，执行后立即提交归还。
内存 SQLite（``sqlite://``）只有一条共享连接：事务外调用与整个事务由一把 asyncio 锁串行化。
"""

from __future__ import annotations

import asyncio
import contextlib
import contextvars
import importlib
from typing import Any

from .base import AdapterError, DatabaseAdapter, TransactionHandle, _scan_sql

#: SQLAlchemy 方言名 -> hot_knowledge / skill_stats 使用的 SQL 方言族（其余方言不支持 hot_*）
_HOT_DIALECT = {"sqlite": "sqlite", "postgresql": "postgres", "mysql": "mysql", "mariadb": "mysql"}
#: 原样传给 create_engine 的连接池参数（只传配置里写了的）
_POOL_KEYS = ("pool_size", "max_overflow", "pool_timeout", "pool_recycle", "pool_pre_ping")


def convert_qmark(sql: str, params: tuple, paramstyle: str, *, hash_comments: bool = False) -> tuple[str, Any]:
    """把 ``?`` 占位符 + 位置参数转成 DBAPI ``paramstyle`` 需要的 (sql, params)。"""
    params = tuple(params)
    if not params or paramstyle == "qmark":
        return sql, params
    if paramstyle in ("format", "pyformat"):
        return _scan_sql(sql, lambda i: "%s", hash_comments=hash_comments), params
    if paramstyle == "numeric":
        return _scan_sql(sql, lambda i: f":{i}", hash_comments=hash_comments), params
    if paramstyle == "numeric_dollar":
        return _scan_sql(sql, lambda i: f"${i}", hash_comments=hash_comments), params
    if paramstyle == "named":
        return _scan_sql(sql, lambda i: f":p{i}", hash_comments=hash_comments), {
            f"p{i}": v for i, v in enumerate(params, 1)
        }
    raise AdapterError(f"unsupported DBAPI paramstyle {paramstyle!r}")


class _SATransaction(TransactionHandle):
    def __init__(self, adapter: "SQLAlchemyAdapter") -> None:
        self._adapter = adapter
        self._conn: Any = None
        self._token: contextvars.Token | None = None

    async def _begin(self) -> None:
        a = self._adapter
        if a._mem_lock is not None:
            await a._mem_lock.acquire()  # 整个事务期间持有，_finish 释放
        try:
            self._conn = await (a._engine.connect() if a._async else asyncio.to_thread(a._engine.connect))
        except Exception as exc:
            if a._mem_lock is not None:
                a._mem_lock.release()
            raise AdapterError(f"sqlalchemy transaction start failed: {exc}") from exc
        self._token = a._txn_conn.set(self._conn)

    async def _finish(self, commit: bool) -> None:
        a, conn, token = self._adapter, self._conn, self._token
        self._conn = self._token = None
        if token is not None:
            a._txn_conn.reset(token)
        if conn is None:
            return
        try:
            await a._run(conn, (lambda c: c.commit()) if commit else (lambda c: c.rollback()))
        except Exception as exc:
            raise AdapterError(f"sqlalchemy {'commit' if commit else 'rollback'} failed: {exc}") from exc
        finally:
            try:
                await (conn.close() if a._async else asyncio.to_thread(conn.close))
            finally:
                if a._mem_lock is not None:
                    a._mem_lock.release()

    async def _commit(self) -> None:
        await self._finish(commit=True)

    async def _rollback(self) -> None:
        await self._finish(commit=False)


class SQLAlchemyAdapter(DatabaseAdapter):
    """config 键：
        url: 任意 SQLAlchemy URL，如 ``mssql+pyodbc://...``、``oracle+oracledb://...``、
             ``duckdb:///D:/x.duckdb``、``sqlite+aiosqlite:///D:/x.db``（必填）
        connect_args: 传给 DBAPI connect() 的字典（可选）
        pool_size / max_overflow / pool_timeout / pool_recycle / pool_pre_ping: 连接池参数（可选）
    """

    driver = "sqlalchemy"

    _txn_conn: contextvars.ContextVar = contextvars.ContextVar("sa_txn_conn", default=None)

    def __init__(self) -> None:
        self._engine: Any = None
        self._async = False
        self._paramstyle = "qmark"
        self._hash_comments = False
        #: 内存 SQLite 共享单连接时串行化调用的锁；其余库为 None
        self._mem_lock: asyncio.Lock | None = None
        #: 后端库对应的 hot 方言族（见 hot_schema.dialect_of）；None = hot_* 不可用
        self.hot_dialect: str | None = None

    async def connect(self, config: dict[str, Any]) -> None:
        url = str(config.get("url") or "").strip()
        if not url:
            raise AdapterError("sqlalchemy adapter requires [database.sqlalchemy] url")
        try:
            import sqlalchemy as sa

            u = sa.make_url(url)
            self._async = bool(getattr(u.get_dialect(), "is_async", False))
            kw: dict[str, Any] = {k: config[k] for k in _POOL_KEYS if config.get(k) not in (None, "")}
            if config.get("connect_args"):
                kw["connect_args"] = dict(config["connect_args"])
            memory = u.get_backend_name() == "sqlite" and u.database in (None, "", ":memory:")
            self._mem_lock = asyncio.Lock() if memory else None
            if self._async:
                try:
                    importlib.import_module("greenlet")
                except ImportError as exc:
                    raise AdapterError(
                        f"async SQLAlchemy URL {u.drivername!r} needs greenlet (not vendored): "
                        "install it into [database] extra_paths, or use the sync driver URL"
                    ) from exc
                from sqlalchemy.ext.asyncio import create_async_engine

                self._engine = create_async_engine(u, **kw)
            else:
                if memory:
                    # 内存库默认按线程隔离（SingletonThreadPool），而每次调用落在不同 to_thread 线程：共享一条连接
                    kw.setdefault("poolclass", sa.pool.StaticPool)
                    kw["connect_args"] = {"check_same_thread": False, **kw.get("connect_args", {})}
                self._engine = sa.create_engine(u, **kw)
            dialect = self._engine.dialect
            self._paramstyle = dialect.paramstyle
            self._hash_comments = dialect.name in ("mysql", "mariadb")
            self.hot_dialect = _HOT_DIALECT.get(dialect.name)
            await self._call(lambda c: None)  # 借出连接即真正建连：不可达/认证失败在这里暴露
        except Exception as exc:
            await self._dispose()
            if isinstance(exc, AdapterError):
                raise
            raise AdapterError(f"sqlalchemy connect failed: {exc}") from exc

    async def _run(self, conn: Any, fn) -> Any:
        """在连接上执行同步函数 ``fn(sync_connection)``：异步引擎走 run_sync，同步引擎走线程。"""
        if self._async:
            return await conn.run_sync(fn)
        return await asyncio.to_thread(fn, conn)

    async def _call(self, fn) -> Any:
        if self._engine is None:
            raise AdapterError("sqlalchemy adapter not connected")
        conn = self._txn_conn.get()
        if conn is not None:
            return await self._run(conn, fn)
        async with self._mem_lock or contextlib.nullcontext():
            if self._async:
                async with self._engine.connect() as c:
                    result = await c.run_sync(fn)
                    await c.commit()
                    return result

            def job():
                with self._engine.connect() as c:
                    result = fn(c)
                    c.commit()
                    return result

            return await asyncio.to_thread(job)

    def _stmt(self, sql: str, params: tuple):
        sql, p = convert_qmark(sql, params, self._paramstyle, hash_comments=self._hash_comments)
        return lambda c: c.exec_driver_sql(sql, p) if p else c.exec_driver_sql(sql)

    async def execute(self, sql: str, params: tuple = ()) -> int:
        stmt = self._stmt(sql, params)
        try:
            return await self._call(lambda c: max(stmt(c).rowcount, 0))
        except AdapterError:
            raise
        except Exception as exc:
            raise AdapterError(f"sqlalchemy execute failed: {exc}") from exc

    async def _fetch(self, sql: str, params: tuple, one: bool) -> Any:
        stmt = self._stmt(sql, params)

        def rows(c):
            res = stmt(c)
            if not res.returns_rows:
                return None if one else []
            if one:
                row = res.mappings().first()
                return dict(row) if row is not None else None
            return [dict(r) for r in res.mappings()]

        try:
            return await self._call(rows)
        except AdapterError:
            raise
        except Exception as exc:
            raise AdapterError(f"sqlalchemy fetch failed: {exc}") from exc

    async def fetch_all(self, sql: str, params: tuple = ()) -> list[dict]:
        return await self._fetch(sql, params, one=False)

    async def fetch_one(self, sql: str, params: tuple = ()) -> dict | None:
        return await self._fetch(sql, params, one=True)

    async def transaction(self) -> TransactionHandle:
        if self._engine is None:
            raise AdapterError("sqlalchemy adapter not connected")
        return _SATransaction(self)

    async def _dispose(self) -> None:
        engine, self._engine = self._engine, None
        if engine is not None:
            await (engine.dispose() if self._async else asyncio.to_thread(engine.dispose))

    async def close(self) -> None:
        try:
            await self._dispose()
        except Exception as exc:
            raise AdapterError(f"sqlalchemy close failed: {exc}") from exc

