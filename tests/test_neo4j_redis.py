"""Neo4j / Redis 适配器：纯单元测试 + 环境变量门控的真实服务测试。

真实服务测试默认跳过，设置后启用：

    NEKO_TEST_NEO4J_URI=bolt://127.0.0.1:7687 NEKO_TEST_NEO4J_USER=... NEKO_TEST_NEO4J_PASSWORD=...
    NEKO_TEST_REDIS_URL=redis://127.0.0.1:6379/15

本机实例可能被别的应用共享：Neo4j 只碰 ``:NekoTest`` 标签，Redis 只写
``neko_test:*`` 键并逐个 DEL，绝不 FLUSH。
"""

from __future__ import annotations

import os

import pytest
from plugin.plugins.plugin_database import _no_hot, hot_schema
from plugin.plugins.plugin_database.adapters import (
    ADAPTERS,
    AdapterError,
    Neo4jAdapter,
    RedisAdapter,
    neo4j_adapter,
    redis_adapter,
)
from plugin.plugins.plugin_database.service import DatabaseService

# ---------------------------------------------------------------------------
# 注册与配置校验
# ---------------------------------------------------------------------------


def test_adapters_registered():
    assert ADAPTERS["neo4j"] is Neo4jAdapter
    assert ADAPTERS["redis"] is RedisAdapter


async def test_missing_uri_and_url():
    with pytest.raises(AdapterError, match="uri"):
        await Neo4jAdapter().connect({})
    with pytest.raises(AdapterError, match="url"):
        await RedisAdapter().connect({"url": "  "})


async def test_unreachable_servers_give_structured_error():
    with pytest.raises(AdapterError, match="neo4j connect failed"):
        await Neo4jAdapter().connect({"uri": "bolt://127.0.0.1:1", "connect_timeout": 2})
    with pytest.raises(AdapterError, match="redis connect failed"):
        await RedisAdapter().connect({"url": "redis://127.0.0.1:1/0", "connect_timeout": 2})


async def test_unknown_driver_lists_new_drivers(tmp_path):
    with pytest.raises(AdapterError, match=r"'neo4j'.*'redis'"):
        await DatabaseService(tmp_path).start({"driver": "oracle"})


# ---------------------------------------------------------------------------
# Neo4j：参数映射与结果转换
# ---------------------------------------------------------------------------


def test_neo4j_param_mapping():
    assert neo4j_adapter._cypher_params(({"name": "x"},)) == {"name": "x"}
    assert neo4j_adapter._cypher_params(("a", 2)) == {"p1": "a", "p2": 2}
    assert neo4j_adapter._cypher_params(()) == {}


def test_neo4j_jsonable_time_and_point():
    from neo4j.spatial import CartesianPoint
    from neo4j.time import Date, DateTime

    row = {"d": Date(2026, 10, 22), "t": DateTime(2026, 10, 22, 8, 30, 0), "p": CartesianPoint((1.0, 2.0))}
    out = neo4j_adapter._jsonable({"row": row, "xs": [Date(2026, 1, 2)]})
    assert out["row"]["d"] == "2026-10-22"
    assert out["row"]["t"].startswith("2026-10-22T08:30:00")
    assert out["row"]["p"] == [1.0, 2.0] and type(out["row"]["p"]) is list
    assert out["xs"] == ["2026-01-02"]


# ---------------------------------------------------------------------------
# Redis：分词、黑名单、结果归一
# ---------------------------------------------------------------------------


def test_redis_tokenise_with_params():
    a = RedisAdapter()
    assert a._args('SET "neko test:k"', ("hello world",)) == ["SET", "neko test:k", "hello world"]
    with pytest.raises(AdapterError, match="empty"):
        a._args("   ", ())
    with pytest.raises(AdapterError, match="parse"):
        a._args('GET "unterminated', ())


@pytest.mark.parametrize("cmd", sorted(redis_adapter.DEFAULT_DENY))
def test_redis_default_deny(cmd):
    with pytest.raises(AdapterError, match="denied"):
        RedisAdapter()._args(f"{cmd.lower()} x", ())


async def test_redis_deny_commands_config_replaces_default():
    a = RedisAdapter()
    # 连接必然失败（端口 1），但 deny_commands 已在建连前生效
    with pytest.raises(AdapterError):
        await a.connect({"url": "redis://127.0.0.1:1/0", "connect_timeout": 1, "deny_commands": ["get"]})
    with pytest.raises(AdapterError, match="denied"):
        a._args("GET k", ())
    assert a._args("FLUSHDB", ()) == ["FLUSHDB"]


def test_redis_result_normalisation():
    raw = {b"f": b"v", b"bin": b"\xff\xfe"}
    dec = redis_adapter._decode(raw)
    assert dec == {"f": "v", "bin": b"\xff\xfe"}
    assert redis_adapter._decode([b"a", (b"b",), {b"c"}]) == ["a", ["b"], ["c"]]
    assert [redis_adapter._rowcount(v) for v in (True, False, None, 3, "OK", ["a", "b"])] == [1, 0, 0, 3, 1, 2]
    assert redis_adapter._rows(None) == []
    assert redis_adapter._rows({"f": "v"}) == [{"f": "v"}]
    assert redis_adapter._rows(["a", "b"]) == [{"index": 0, "value": "a"}, {"index": 1, "value": "b"}]
    assert redis_adapter._rows("x") == [{"value": "x"}]


# ---------------------------------------------------------------------------
# 热数据表 / Skill 统计在图库与 KV 上的处理
# ---------------------------------------------------------------------------


class _FakeAdapter:
    def __init__(self, driver):
        self.driver = driver

    async def execute(self, *_a):  # pragma: no cover - 不应被调用
        raise AssertionError("hot schema must not send DDL to neo4j/redis")


@pytest.mark.parametrize("driver", ["neo4j", "redis"])
async def test_hot_schema_skipped_and_entries_guarded(driver):
    info = await hot_schema.register_hot_schema(_FakeAdapter(driver))
    assert info["driver"] == driver and "skipped" in info
    err = _no_hot(_FakeAdapter(driver))
    assert err is not None and "does not support" in str(err.error)
    assert _no_hot(_FakeAdapter("sqlite")) is None and _no_hot(_FakeAdapter("mongo")) is None


# ---------------------------------------------------------------------------
# 真实服务（环境变量存在时才运行）
# ---------------------------------------------------------------------------


async def test_neo4j_crud_when_server_available():
    uri = os.environ.get("NEKO_TEST_NEO4J_URI")
    if not uri:
        pytest.skip("NEKO_TEST_NEO4J_URI not set")
    a = Neo4jAdapter()
    await a.connect(
        {
            "uri": uri,
            "user": os.environ.get("NEKO_TEST_NEO4J_USER", ""),
            "password": os.environ.get("NEKO_TEST_NEO4J_PASSWORD", ""),
        }
    )
    try:
        await a.execute("MATCH (n:NekoTest) DETACH DELETE n", ())
        assert await a.execute("CREATE (n:NekoTest {v: $p1, at: date($p2)})", ("a", "2026-10-22")) >= 2
        row = await a.fetch_one("MATCH (n:NekoTest) RETURN n.v AS v, n.at AS at", ())
        assert row == {"v": "a", "at": "2026-10-22"}
        async with await a.transaction():
            await a.execute("CREATE (n:NekoTest {v: $v})", ({"v": "b"},))
        with pytest.raises(RuntimeError):
            async with await a.transaction():
                await a.execute("CREATE (n:NekoTest {v: 'ghost'})", ())
                raise RuntimeError("boom")
        rows = await a.fetch_all("MATCH (n:NekoTest) RETURN n.v AS v ORDER BY v", ())
        assert [r["v"] for r in rows] == ["a", "b"]
    finally:
        await a.execute("MATCH (n:NekoTest) DETACH DELETE n", ())
        await a.close()


async def test_redis_crud_when_server_available():
    url = os.environ.get("NEKO_TEST_REDIS_URL")
    if not url:
        pytest.skip("NEKO_TEST_REDIS_URL not set")
    keys = ("neko_test:s", "neko_test:h", "neko_test:n")
    a = RedisAdapter()
    await a.connect({"url": url})
    try:
        assert await a.execute("SET neko_test:s", ("hello world",)) == 1
        assert await a.fetch_one("GET neko_test:s", ()) == {"value": "hello world"}
        assert await a.execute("HSET neko_test:h f1 v1 f2", ("v2",)) == 2
        assert await a.fetch_all("HGETALL neko_test:h", ()) == [{"f1": "v1", "f2": "v2"}]
        async with await a.transaction():
            assert await a.execute("INCRBY neko_test:n 5", ()) == 0  # 只入队
            await a.execute("INCRBY neko_test:n 2", ())
        assert await a.fetch_one("GET neko_test:n", ()) == {"value": "7"}
        with pytest.raises(RuntimeError):
            async with await a.transaction():
                await a.execute("INCRBY neko_test:n 100", ())
                raise RuntimeError("boom")
        assert await a.fetch_one("GET neko_test:n", ()) == {"value": "7"}
        with pytest.raises(AdapterError, match="denied"):
            await a.execute("FLUSHDB", ())
    finally:
        await a.execute("DEL", keys)
        await a.close()
