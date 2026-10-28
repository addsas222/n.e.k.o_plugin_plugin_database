"""MongoDB 适配器：纯单元测试 + 环境变量门控的真实服务全量测试。

真实服务测试默认跳过，设置后启用（事务部分要求副本集）：

    NEKO_TEST_MONGO_URI="mongodb://127.0.0.1:27018/?directConnection=true" NEKO_TEST_MONGO_DB=neko_test

只碰 ``neko_t_*`` 集合，结束时 drop 掉。
"""

from __future__ import annotations

import json
import os

import pytest
from plugin.plugins.plugin_database.adapters import ADAPTERS, AdapterError, mongo_adapter

URI = os.environ.get("NEKO_TEST_MONGO_URI")
DB = os.environ.get("NEKO_TEST_MONGO_DB")
live = pytest.mark.skipif(not (URI and DB), reason="NEKO_TEST_MONGO_URI / NEKO_TEST_MONGO_DB not set")


def test_objectid_roundtrip_in_nested_filters_and_pipeline():
    from bson import ObjectId

    hex_id = "0123456789abcdef01234567"
    op = mongo_adapter._decode_op(json.dumps({
        "op": "bulk_write", "collection": "c",
        "filter": {"$or": [{"_id": hex_id}, {"x": 1}]},
        "pipeline": [{"$match": {"_id": {"$in": [hex_id, "nothex"]}}}],
        "requests": [{"op": "delete_one", "filter": {"_id": {"$eq": hex_id}}}],
    }))
    assert op["filter"]["$or"][0]["_id"] == ObjectId(hex_id)
    assert op["pipeline"][0]["$match"]["_id"]["$in"] == [ObjectId(hex_id), "nothex"]
    assert op["requests"][0]["filter"]["_id"]["$eq"] == ObjectId(hex_id)
    # 出站：任意位置的 ObjectId（含 distinct 返回的裸值、$group 的 dict _id）都可 JSON 化
    rows = mongo_adapter._rows([ObjectId(hex_id), {"_id": {"k": ObjectId(hex_id)}, "n": 1}])
    assert rows == [{"value": hex_id}, {"_id": {"k": hex_id}, "n": 1}]
    json.dumps(rows)


def test_bulk_request_validation():
    from pymongo import InsertOne, UpdateMany

    assert isinstance(mongo_adapter._bulk_request({"op": "insert_one", "doc": {"a": 1}}), InsertOne)
    assert isinstance(
        mongo_adapter._bulk_request({"op": "update_many", "filter": {}, "update": {"$set": {"a": 1}}}), UpdateMany
    )
    with pytest.raises(AdapterError, match="missing keys"):
        mongo_adapter._bulk_request({"op": "replace_one", "filter": {}})
    with pytest.raises(AdapterError, match="must be one of"):
        mongo_adapter._bulk_request({"op": "drop"})


async def test_bad_json_and_not_connected():
    a = ADAPTERS["mongo"]()
    with pytest.raises(AdapterError, match="JSON-encoded"):
        await a.execute("SELECT 1")
    with pytest.raises(AdapterError, match="not connected"):
        await a.fetch_all(json.dumps({"op": "list_collections"}))


# ---------------------------------------------------------------------------
# 真实服务
# ---------------------------------------------------------------------------


def _op(**kw) -> str:
    return json.dumps(kw)


@live
async def test_mongo_all_ops_live():
    a = ADAPTERS["mongo"]()
    await a.connect({"uri": URI, "database": DB})
    c = "neko_t_ops"
    try:
        await a._db.drop_collection(c)
        await a.execute(_op(op="create_collection", collection=c))
        assert await a.execute(_op(op="insert_many", collection=c, docs=[
            {"k": "a", "n": 1, "tag": "x"}, {"k": "b", "n": 2, "tag": "x"}, {"k": "c", "n": 3, "tag": "y"},
        ])) == 3
        assert await a.execute(_op(op="insert_one", collection=c, doc={"k": "d", "n": 4, "tag": "y"})) == 1

        # find / find_one + _id 往返：出站 hex 字符串可直接用于 update/delete
        row = await a.fetch_one(_op(op="find_one", collection=c, filter={"k": "a"}))
        assert isinstance(row["_id"], str) and len(row["_id"]) == 24
        assert await a.execute(_op(op="update_one", collection=c, filter={"_id": row["_id"]},
                                   update={"$set": {"n": 10}})) == 1
        rows = await a.fetch_all(_op(op="find", collection=c, filter={}, sort=[["n", -1]], limit=2,
                                     projection={"_id": 0, "k": 1, "n": 1}))
        assert rows == [{"k": "a", "n": 10}, {"k": "d", "n": 4}]
        assert await a.execute(_op(op="replace_one", collection=c, filter={"k": "zz"},
                                   replacement={"k": "zz", "n": 0}, upsert=True)) == 1

        # count / estimated_count / distinct / aggregate
        assert await a.fetch_one(_op(op="count", collection=c, filter={"tag": "x"})) == {"count": 2}
        assert (await a.fetch_all(_op(op="estimated_count", collection=c)))[0]["count"] == 5
        vals = sorted(r["value"] for r in await a.fetch_all(_op(op="distinct", collection=c, key="tag",
                                                                 filter={"tag": {"$ne": None}})))
        assert vals == ["x", "y"]
        agg = await a.fetch_all(_op(op="aggregate", collection=c, allowDiskUse=True, pipeline=[
            {"$match": {"tag": {"$in": ["x", "y"]}}},
            {"$group": {"_id": "$tag", "total": {"$sum": "$n"}}},
            {"$sort": {"_id": 1}},
        ]))
        assert agg == [{"_id": "x", "total": 12}, {"_id": "y", "total": 7}]
        ids = await a.fetch_all(_op(op="distinct", collection=c, key="_id"))
        json.dumps(ids)  # ObjectId 裸值也已转成字符串

        # find_one_and_*
        doc = await a.fetch_one(_op(op="find_one_and_update", collection=c, filter={"k": "b"},
                                    update={"$inc": {"n": 5}}, return_document="after"))
        assert doc["n"] == 7
        doc = await a.fetch_one(_op(op="find_one_and_update", collection=c, filter={"k": "b"},
                                    update={"$inc": {"n": 1}}))
        assert doc["n"] == 7  # 默认 before
        doc = await a.fetch_one(_op(op="find_one_and_replace", collection=c, filter={"k": "c"},
                                    replacement={"k": "c", "n": 30}, return_document="after",
                                    projection={"_id": 0}))
        assert doc == {"k": "c", "n": 30}
        doc = await a.fetch_one(_op(op="find_one_and_delete", collection=c, filter={"k": "zz"}))
        assert doc["k"] == "zz"
        assert await a.fetch_one(_op(op="find_one_and_delete", collection=c, filter={"k": "zz"})) is None
        with pytest.raises(AdapterError, match="return_document"):
            await a.fetch_one(_op(op="find_one_and_update", collection=c, filter={}, update={"$set": {"q": 1}},
                                  return_document="sideways"))

        # bulk_write：插 1 + 改 2 + upsert 1 + 删 1
        n = await a.execute(_op(op="bulk_write", collection=c, requests=[
            {"op": "insert_one", "doc": {"k": "e", "n": 5}},
            {"op": "update_many", "filter": {"tag": "x"}, "update": {"$set": {"seen": True}}},
            {"op": "update_one", "filter": {"k": "new"}, "update": {"$set": {"n": 9}}, "upsert": True},
            {"op": "replace_one", "filter": {"k": "e"}, "replacement": {"k": "e", "n": 50}},
            {"op": "delete_one", "filter": {"k": "d"}},
        ]))
        assert n == 1 + 2 + 1 + 1 + 1
        assert await a.execute(_op(op="bulk_write", collection=c, requests=[
            {"op": "delete_many", "filter": {"k": {"$in": ["new", "e"]}}}])) == 2

        # 索引
        assert await a.execute(_op(op="create_index", collection=c, keys={"k": 1}, unique=True, name="k_uniq")) == 1
        names = {r["name"] for r in await a.fetch_all(_op(op="list_indexes", collection=c))}
        assert names == {"_id_", "k_uniq"}
        assert await a.execute(_op(op="drop_index", collection=c, name="k_uniq")) == 1
        assert {r["name"] for r in await a.fetch_all(_op(op="list_indexes", collection=c))} == {"_id_"}
        assert c in {r["name"] for r in await a.fetch_all(_op(op="list_collections"))}

        # 删除
        assert await a.execute(_op(op="delete_many", collection=c, filter={"k": {"$in": ["a", "b", "c"]}})) == 3
        with pytest.raises(AdapterError, match="unsupported mongo execute op"):
            await a.execute(_op(op="drop_database", collection=c))
    finally:
        await a._db.drop_collection(c)
        await a.close()


@live
async def test_mongo_transaction_commit_and_rollback_live():
    a = ADAPTERS["mongo"]()
    await a.connect({"uri": URI, "database": DB})
    c = "neko_t_txn"
    try:
        await a._db.drop_collection(c)
        await a.execute(_op(op="create_collection", collection=c))  # 事务里不能隐式建集合（旧版本）
        async with await a.transaction():
            await a.execute(_op(op="insert_one", collection=c, doc={"v": 1}))
            await a.execute(_op(op="bulk_write", collection=c, requests=[{"op": "insert_one", "doc": {"v": 2}}]))
            # 事务内读到自己的未提交写
            assert await a.fetch_one(_op(op="count", collection=c, filter={})) == {"count": 2}
        assert await a.fetch_one(_op(op="count", collection=c, filter={})) == {"count": 2}

        with pytest.raises(RuntimeError):
            async with await a.transaction():
                await a.execute(_op(op="insert_one", collection=c, doc={"v": "ghost"}))
                await a.fetch_one(_op(op="find_one_and_update", collection=c, filter={"v": 1},
                                      update={"$set": {"v": 100}}))
                raise RuntimeError("boom")
        assert await a.fetch_one(_op(op="count", collection=c, filter={})) == {"count": 2}
        assert await a.fetch_one(_op(op="find_one", collection=c, filter={"v": 100})) is None
    finally:
        await a._db.drop_collection(c)
        await a.close()


@live
async def test_mongo_hot_schema_idempotent_live(tmp_path):
    """mongo 上 register_hot_schema 可重复调用（入口层全链路见 tests/stub_run.py 的 mongo 段）。"""
    from plugin.plugins.plugin_database import hot_schema
    from plugin.plugins.plugin_database.service import DatabaseService

    svc = DatabaseService(tmp_path)
    await svc.start({"driver": "mongo", "mongo": {"uri": URI, "database": DB}})
    db = svc.adapter._db
    try:
        for name in ("hot_knowledge", "skill_stats"):
            await db.drop_collection(name)
        info = await hot_schema.register_hot_schema(svc.adapter)
        assert info == {"driver": "mongo", "collection": "hot_knowledge", "indexes": 2}
        # 回归：集合已存在时重复注册不能报错（知识库每次初始化都会调）
        assert await hot_schema.register_hot_schema(svc.adapter) == info
        assert await svc.execute(hot_schema.mongo_demote_op("9999-12-31T00:00:00"), []) == 0
        assert hot_schema.dialect_of(svc.adapter) == "mongo"
    finally:
        for name in ("hot_knowledge", "skill_stats"):
            await db.drop_collection(name)
        await svc.stop()
