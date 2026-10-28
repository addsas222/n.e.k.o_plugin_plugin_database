"""Neo4j 适配器（官方 neo4j 异步驱动，bolt:// / neo4j://）。

``sql`` 参数传 **Cypher**。参数两种写法：

* 命名参数：``params=[{"name": "x"}]``（单个 dict）原样作为 Cypher 参数；
* 位置参数：``params=["x", 1]`` 映射为 ``$p1``、``$p2`` ……

``?`` **不**转译：通用 SQL 扫描器把 ``--`` 当行注释，而 Cypher 里
``(a)--(b)`` 是关系模式。

``execute`` 返回写操作计数之和（节点/关系增删、属性设置、标签增删）；
``fetch_*`` 返回 ``record.data()``，时间类型转 ISO 字符串、空间点转列表。

事务语义：``transaction()`` 开一个 session + 显式事务；事务期间本适配器的
``execute`` / ``fetch_*`` 经 contextvar 自动路由到该事务（同 postgres 适配器）。
"""

from __future__ import annotations

import contextvars
from typing import Any

from .base import AdapterError, DatabaseAdapter, TransactionHandle

_COUNTERS = (
    "nodes_created",
    "nodes_deleted",
    "relationships_created",
    "relationships_deleted",
    "properties_set",
    "labels_added",
    "labels_removed",
)


def _cypher_params(params: tuple) -> dict:
    if len(params) == 1 and isinstance(params[0], dict):
        return dict(params[0])
    return {f"p{i}": v for i, v in enumerate(params, 1)}


def _jsonable(value: Any) -> Any:
    """neo4j.time 类型 -> ISO 字符串；Point（tuple 子类）/列表 -> list；递归容器。"""
    if hasattr(value, "iso_format"):
        return value.iso_format()
    if isinstance(value, dict):
        return {k: _jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonable(v) for v in value]
    return value


def _count(summary: Any) -> int:
    c = summary.counters
    return sum(int(getattr(c, k, 0)) for k in _COUNTERS)


class _Neo4jTransaction(TransactionHandle):
    def __init__(self, adapter: "Neo4jAdapter") -> None:
        self._adapter = adapter
        self._session: Any = None
        self._tx: Any = None
        self._token: contextvars.Token | None = None

    async def _begin(self) -> None:
        try:
            self._session = self._adapter._require_driver().session(
                database=self._adapter._database
            )
            self._tx = await self._session.begin_transaction()
        except Exception as exc:
            await self._close_session()
            raise AdapterError(f"neo4j begin transaction failed: {exc}") from exc
        self._token = self._adapter._txn.set(self._tx)

    async def _close_session(self) -> None:
        session, self._session = self._session, None
        if session is not None:
            try:
                await session.close()
            except Exception:
                pass

    async def _finish(self, commit: bool) -> None:
        tx, token = self._tx, self._token
        self._tx = self._token = None
        if token is not None:
            self._adapter._txn.reset(token)
        try:
            if tx is not None:
                await (tx.commit() if commit else tx.rollback())
        except Exception as exc:
            raise AdapterError(f"neo4j {'commit' if commit else 'rollback'} failed: {exc}") from exc
        finally:
            await self._close_session()

    async def _commit(self) -> None:
        await self._finish(commit=True)

    async def _rollback(self) -> None:
        await self._finish(commit=False)


class Neo4jAdapter(DatabaseAdapter):
    """neo4j 异步驱动（自带连接池）。

    config 键：
        uri: ``bolt://127.0.0.1:7687`` / ``neo4j://...``（必填）
        user / password: 认证（user 留空 = 不认证）
        database: 库名（留空 = 服务端默认库）
        connect_timeout: 建连超时秒数（默认 10）
        max_pool_size: 连接池上限（默认 10）
    """

    driver = "neo4j"

    #: 事务期间的显式事务对象（按任务上下文隔离）
    _txn: contextvars.ContextVar = contextvars.ContextVar("neo4j_txn", default=None)

    def __init__(self) -> None:
        self._driver: Any = None
        self._database: str | None = None

    async def connect(self, config: dict[str, Any]) -> None:
        uri = str(config.get("uri") or "").strip()
        if not uri:
            raise AdapterError("neo4j adapter requires [database.neo4j] uri")
        user = str(config.get("user") or "")
        self._database = str(config.get("database") or "").strip() or None
        try:
            # 导入放在保护块内：vendor 缺失时返回结构化错误（同 postgres 适配器）
            import neo4j

            self._driver = neo4j.AsyncGraphDatabase.driver(
                uri,
                auth=(user, str(config.get("password") or "")) if user else None,
                connection_timeout=float(config.get("connect_timeout", 10)),
                max_connection_pool_size=int(config.get("max_pool_size", 10)),
            )
            await self._driver.verify_connectivity()
        except Exception as exc:
            await self._discard_driver()
            raise AdapterError(f"neo4j connect failed: {exc}") from exc

    async def _discard_driver(self) -> None:
        drv, self._driver = self._driver, None
        if drv is not None:
            try:
                await drv.close()
            except Exception:
                pass

    def _require_driver(self) -> Any:
        if self._driver is None:
            raise AdapterError("neo4j adapter not connected")
        return self._driver

    async def _run(self, query: str, params: tuple) -> tuple[list[dict], Any]:
        """返回 (records 的 data() 列表, summary)。事务期间走事务对象。"""
        kw = _cypher_params(params)
        tx = self._txn.get()
        try:
            if tx is not None:
                result = await tx.run(query, kw)
                rows = await result.data()
                summary = await result.consume()
            else:
                res = await self._require_driver().execute_query(
                    query, kw, database_=self._database
                )
                rows, summary = [r.data() for r in res.records], res.summary
        except AdapterError:
            raise
        except Exception as exc:
            raise AdapterError(f"neo4j query failed: {exc}") from exc
        return [_jsonable(r) for r in rows], summary

    async def execute(self, sql: str, params: tuple = ()) -> int:
        _, summary = await self._run(sql, params)
        return _count(summary)

    async def fetch_all(self, sql: str, params: tuple = ()) -> list[dict]:
        rows, _ = await self._run(sql, params)
        return rows

    async def fetch_one(self, sql: str, params: tuple = ()) -> dict | None:
        # 不用 result.single(strict=False)：多条记录时它会发警告
        rows, _ = await self._run(sql, params)
        return rows[0] if rows else None

    async def transaction(self) -> TransactionHandle:
        self._require_driver()
        return _Neo4jTransaction(self)

    async def close(self) -> None:
        drv, self._driver = self._driver, None
        if drv is not None:
            try:
                await drv.close()
            except Exception as exc:
                raise AdapterError(f"neo4j close failed: {exc}") from exc
