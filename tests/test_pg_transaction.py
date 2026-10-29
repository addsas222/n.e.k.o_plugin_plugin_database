"""PostgreSQL 事务句柄的归还行为（不需要真实服务）。

回归背景：``_PgTransaction._finish`` 曾经写成 ``pool.release(conn)``——asyncpg 的
``Pool.release`` 是协程，漏 ``await`` 时协程对象被丢弃、连接永远留在「已借出」状态。
后果不是立刻报错，而是：连续事务把池（默认 max_size=5）耗尽后 acquire 永久阻塞，
最后 ``pool.close()`` 等待那条回不来的连接而挂死整个插件进程。

这条用例用一个假的 asyncpg 池把「release 必须被 await」钉住：只要漏掉 await，
``released`` 列表就是空的。
"""

from __future__ import annotations

import asyncio
import contextvars

import pytest
from plugin.plugins.plugin_database.adapters.postgres_adapter import _PgTransaction


class _FakeTr:
    def __init__(self) -> None:
        self.started = 0
        self.committed = 0
        self.rolled_back = 0

    async def start(self) -> None:
        self.started += 1

    async def commit(self) -> None:
        self.committed += 1

    async def rollback(self) -> None:
        self.rolled_back += 1


class _FakeConn:
    def __init__(self) -> None:
        self.tr = _FakeTr()

    def transaction(self) -> _FakeTr:
        return self.tr


class _FakePool:
    def __init__(self) -> None:
        self.conn = _FakeConn()
        self.released: list[_FakeConn] = []
        self.in_use = 0

    async def acquire(self) -> _FakeConn:
        self.in_use += 1
        return self.conn

    async def release(self, conn: _FakeConn) -> None:
        # asyncpg 的 release 是协程：这里也写成协程，漏 await 时
        # Python 只会打印 RuntimeWarning，released 不会增加。
        self.released.append(conn)
        self.in_use -= 1


class _FakeAdapter:
    def __init__(self) -> None:
        self.pool = _FakePool()
        self._acquire_timeout = 5.0
        self._txn_conn: contextvars.ContextVar = contextvars.ContextVar("fake_pg_txn", default=None)

    def _require_pool(self) -> _FakePool:
        return self.pool


async def test_commit_returns_connection_to_pool():
    adapter = _FakeAdapter()
    async with _PgTransaction(adapter):
        pass
    assert adapter.pool.conn.tr.committed == 1
    assert adapter.pool.released == [adapter.pool.conn]
    assert adapter.pool.in_use == 0


async def test_rollback_returns_connection_to_pool():
    adapter = _FakeAdapter()
    with pytest.raises(RuntimeError):
        async with _PgTransaction(adapter):
            raise RuntimeError("boom")
    assert adapter.pool.conn.tr.rolled_back == 1
    assert adapter.pool.released == [adapter.pool.conn]
    assert adapter.pool.in_use == 0


async def test_release_happens_even_if_commit_fails():
    adapter = _FakeAdapter()

    async def boom() -> None:
        raise RuntimeError("commit failed")

    adapter.pool.conn.tr.commit = boom  # type: ignore[method-assign]
    with pytest.raises(RuntimeError):
        async with _PgTransaction(adapter):
            pass
    assert adapter.pool.released == [adapter.pool.conn]
    assert adapter.pool.in_use == 0


async def test_contextvar_reset_after_finish():
    adapter = _FakeAdapter()
    async with _PgTransaction(adapter):
        assert adapter._txn_conn.get() is adapter.pool.conn
    assert adapter._txn_conn.get() is None


async def test_pool_not_exhausted_by_repeated_transactions():
    """连续事务不得让池越借越少——这正是漏 await 时的症状。"""
    adapter = _FakeAdapter()
    for _ in range(8):
        async with _PgTransaction(adapter):
            pass
    assert adapter.pool.in_use == 0
    assert len(adapter.pool.released) == 8
    # 池只有一条连接，还能借到就说明每次都归还了
    assert await asyncio.wait_for(adapter.pool.acquire(), timeout=1) is adapter.pool.conn
