"""MongoDB 适配器（motor，文档模型，不映射关系表）。

MongoDB 没有 SQL。为保持 ``DatabaseAdapter`` 的统一签名，``sql`` 参数传
**JSON 字符串编码的文档操作**，``params`` 忽略。

execute 的操作（返回受影响文档数）：

    {"op": "insert_one",  "collection": "hot_knowledge", "doc": {...}}
    {"op": "insert_many", "collection": "...", "docs": [...]}
    {"op": "update_one",  "collection": "...", "filter": {...}, "update": {...},
                          "upsert": false}
    {"op": "update_many", ...}
    {"op": "replace_one", "collection": "...", "filter": {...}, "replacement": {...},
                          "upsert": false}
    {"op": "delete_one" | "delete_many", "collection": "...", "filter": {...}}
    {"op": "bulk_write",  "collection": "...", "ordered": true,
                          "requests": [{"op": "insert_one", "doc": {...}},
                                       {"op": "update_many", "filter": {...}, "update": {...}}, ...]}
                          （requests 里每项与上面同名单条操作同键：insert_one / update_one /
                          update_many / replace_one / delete_one / delete_many）
    {"op": "create_index", "collection": "...", "keys": {"field": 1},
                           "unique": false, "name": "可选"}
    {"op": "drop_index",   "collection": "...", "name": "idx_name"}   # 或 "keys": {"field": 1}
    {"op": "create_collection", "collection": "..."}            # 已存在则跳过

fetch_all / fetch_one 的操作（返回 dict 列表 / 第一个 dict）：

    {"op": "find",     "collection": "...", "filter": {...}, "sort": [["f", 1]],
                       "limit": 100, "skip": 0, "projection": {...}}
    {"op": "find_one", "collection": "...", "filter": {...}, "projection": {...}}
    {"op": "aggregate", "collection": "...", "pipeline": [...], "allowDiskUse": false,
                        "limit": 100}
    {"op": "distinct", "collection": "...", "key": "field", "filter": {...}}
                       -> [{"value": v}, ...]
    {"op": "count",    "collection": "...", "filter": {...}}   -> {"count": n}
    {"op": "estimated_count", "collection": "..."}             -> {"count": n}
    {"op": "list_collections"}                                 -> [{"name", "type", ...}]
    {"op": "list_indexes", "collection": "..."}                -> [{"name", "key", ...}]
    {"op": "find_one_and_update",  "collection": "...", "filter": {...}, "update": {...},
                                   "upsert": false, "sort": [...], "projection": {...},
                                   "return_document": "before" | "after"}
    {"op": "find_one_and_replace", ... "replacement": {...} ...}
    {"op": "find_one_and_delete",  "collection": "...", "filter": {...}, "sort": [...]}
                       （find_one_and_* 会写库，返回命中文档；没命中返回空）

``find`` / ``aggregate`` 不写 ``limit`` 时最多返回 100 条（跨插件 IPC 体积保护）。
出站把 ObjectId 转成 24 位 hex 字符串；入站 ``filter`` / ``update`` / ``replacement`` /
``pipeline``（含 bulk_write 每项）里 ``_id`` / ``$eq`` / ``$in`` 位置的 24 位 hex 自动转回 ObjectId。

事务：``transaction()`` 返回的 CM 在进入时开启 client session + transaction，期间本适配器
的所有操作经 contextvar 带上该 session（要求副本集 / 分片集群；单机 mongod 会得到结构化错误）。
"""

from __future__ import annotations

import contextvars
import json
from typing import Any

from .base import AdapterError, DatabaseAdapter, TransactionHandle


def _decode_op(sql: str) -> dict:
    try:
        op = json.loads(sql)
    except json.JSONDecodeError as exc:
        raise AdapterError(
            "mongo adapter expects a JSON-encoded operation as sql, e.g. "
            '{"op":"find","collection":"t","filter":{}}'
        ) from exc
    if not isinstance(op, dict) or "op" not in op:
        raise AdapterError("mongo operation JSON must be an object with an 'op' key")
    # 入站：把此前出站转成字符串的 _id 转回 ObjectId，避免 update/delete 静默匹配 0 条
    # （bson 缺失/ABI 不匹配也会在这里抛错，同样转成结构化错误）
    try:
        _encode_keys(op)
        for req in op.get("requests") or []:
            if isinstance(req, dict):
                _encode_keys(req)
    except AdapterError:
        raise
    except Exception as exc:
        raise AdapterError(f"mongo operation decode failed: {exc}") from exc
    return op


def _encode_keys(op: dict) -> None:
    for key in ("filter", "update", "replacement", "pipeline"):
        if isinstance(op.get(key), (dict, list)):
            op[key] = _encode_objectid(op[key])


def _is_objectid_hex(value: str) -> bool:
    import re

    return bool(re.fullmatch(r"[0-9a-f]{24}", str(value)))


def _encode_objectid(doc: Any) -> Any:
    """递归把 ``_id``/``$eq``/``$in`` 位置的 24 位 hex 字符串转回 ObjectId。"""
    if isinstance(doc, list):
        return [_encode_objectid(d) for d in doc]
    if not isinstance(doc, dict):
        return doc
    out = {}
    for k, v in doc.items():
        if k in ("_id", "$eq") and isinstance(v, str) and _is_objectid_hex(v):
            out[k] = _objectid(v)
        elif k == "$in" and isinstance(v, list):
            out[k] = [
                _objectid(x) if isinstance(x, str) and _is_objectid_hex(x) else x for x in v
            ]
        elif isinstance(v, (dict, list)):  # $or / $and 等列表里的子条件同样要转
            out[k] = _encode_objectid(v)
        else:
            out[k] = v
    return out


def _objectid(value: str) -> Any:
    from bson import ObjectId

    return ObjectId(value)


def _require(op: dict, *keys: str) -> None:
    missing = [k for k in keys if k not in op]
    if missing:
        raise AdapterError(f"mongo op {op.get('op')!r} missing keys: {', '.join(missing)}")


def _clean_doc(doc: Any) -> Any:
    """递归把 ObjectId / Decimal128 / Timestamp 等 BSON 专有类型转成字符串（可 JSON 化）。"""
    if isinstance(doc, list):
        return [_clean_doc(d) for d in doc]
    if isinstance(doc, dict):  # SON 也是 dict 子类
        return {k: _clean_doc(v) for k, v in doc.items()}
    if doc.__class__.__name__ in ("ObjectId", "Decimal128", "Timestamp", "Regex", "UUID"):
        return str(doc)
    return doc


def _rows(docs: list) -> list[dict]:
    """结果统一成 list[dict]：非 dict 值（distinct）包成 ``{"value": v}``。"""
    return [d if isinstance(d, dict) else {"value": d} for d in _clean_doc(docs)]


class _MongoTransaction(TransactionHandle):
    def __init__(self, adapter: "MongoDBAdapter") -> None:
        self._adapter = adapter
        self._session: Any = None
        self._token: contextvars.Token | None = None

    async def _begin(self) -> None:
        try:
            self._session = await self._adapter._client.start_session()
            self._session.start_transaction()
        except Exception as exc:
            if self._session is not None:
                await self._session.end_session()
            self._session = None
            raise AdapterError(f"mongo transaction start failed: {exc}") from exc
        self._token = self._adapter._txn_session.set(self._session)

    async def _finish(self, commit: bool) -> None:
        session, token = self._session, self._token
        self._session = self._token = None
        if token is not None:
            self._adapter._txn_session.reset(token)
        if session is None:
            return
        try:
            if commit:
                await session.commit_transaction()
            else:
                await session.abort_transaction()
        except Exception as exc:
            if commit:
                raise AdapterError(
                    "mongo transaction commit failed (multi-document transactions need a "
                    f"replica set / sharded cluster): {exc}"
                ) from exc
            # abort 失败不掩盖触发回滚的原始异常；服务端会在超时后自行中止事务
        finally:
            await session.end_session()

    async def _commit(self) -> None:
        await self._finish(commit=True)

    async def _rollback(self) -> None:
        await self._finish(commit=False)


#: 单条写操作；execute 统一走 bulk_write 构造请求，计数口径一致
_WRITE_OPS = frozenset({"insert_one", "update_one", "update_many", "replace_one", "delete_one", "delete_many"})


def _bulk_request(req: Any) -> Any:
    from pymongo import DeleteMany, DeleteOne, InsertOne, ReplaceOne, UpdateMany, UpdateOne

    if not isinstance(req, dict) or req.get("op") not in _WRITE_OPS:
        raise AdapterError(f"bulk_write request op must be one of {sorted(_WRITE_OPS)}: {req!r}")
    kind = req["op"]
    upsert = bool(req.get("upsert", False))
    if kind == "insert_one":
        _require(req, "doc")
        return InsertOne(req["doc"])
    if kind in ("update_one", "update_many"):
        _require(req, "filter", "update")
        return (UpdateOne if kind == "update_one" else UpdateMany)(req["filter"], req["update"], upsert=upsert)
    if kind == "replace_one":
        _require(req, "filter", "replacement")
        return ReplaceOne(req["filter"], req["replacement"], upsert=upsert)
    _require(req, "filter")
    return (DeleteOne if kind == "delete_one" else DeleteMany)(req["filter"])


class MongoDBAdapter(DatabaseAdapter):
    """motor AsyncIOMotorClient（内置连接池）。

    config 键：
        uri: ``mongodb://host:27017`` 或 ``mongodb+srv://...``（Atlas；dnspython 随 vendor 分发）
        database: 库名（必填）
        server_selection_ms: 选主超时毫秒（默认 5000）
    """

    driver = "mongo"

    #: 事务期间的 client session（按任务上下文隔离，同 postgres 适配器钉住连接的做法）
    _txn_session: contextvars.ContextVar = contextvars.ContextVar("mongo_txn_session", default=None)

    def __init__(self) -> None:
        self._client: Any = None
        self._db: Any = None

    async def connect(self, config: dict[str, Any]) -> None:
        uri = str(config.get("uri") or "").strip()
        dbname = str(config.get("database") or "").strip()
        if not uri or not dbname:
            raise AdapterError(
                "mongo adapter requires [database.mongo] uri and database"
            )
        try:
            # 依赖导入放在保护块内：motor/bson 缺失或 ABI 不匹配时也要结构化报错。
            import motor.motor_asyncio

            self._client = motor.motor_asyncio.AsyncIOMotorClient(
                uri, serverSelectionTimeoutMS=int(config.get("server_selection_ms", 5000))
            )
            self._db = self._client[dbname]
            # motor 是惰性连接：发一个轻量命令验证可达，避免把连接错误推迟到第一次操作。
            await self._client.admin.command("ping")
        except Exception as exc:
            if self._client is not None:
                self._client.close()
            self._client = None
            self._db = None
            raise AdapterError(f"mongo connect failed: {exc}") from exc

    def _require_db(self):
        if self._db is None:
            raise AdapterError("mongo adapter not connected")
        return self._db

    def _coll(self, op: dict):
        db = self._require_db()
        if not op.get("collection"):
            raise AdapterError(f"mongo op {op.get('op')!r} requires 'collection'")
        return db[op["collection"]]

    async def execute(self, sql: str, params: tuple = ()) -> int:
        op = _decode_op(sql)
        coll = self._coll(op)
        s = self._txn_session.get()
        try:
            kind = op["op"]
            if kind == "insert_many":
                _require(op, "docs")
                if not op["docs"]:
                    return 0
                res = await coll.insert_many(op["docs"], session=s)
                return len(res.inserted_ids)
            if kind in _WRITE_OPS or kind == "bulk_write":
                reqs = op.get("requests") if kind == "bulk_write" else [op]
                if not isinstance(reqs, list):
                    raise AdapterError("mongo op 'bulk_write' needs a 'requests' list")
                if not reqs:
                    return 0
                res = await coll.bulk_write(
                    [_bulk_request(r) for r in reqs], ordered=bool(op.get("ordered", True)), session=s
                )
                return res.inserted_count + res.modified_count + res.upserted_count + res.deleted_count
            if kind == "create_index":
                _require(op, "keys")
                extra = {"name": op["name"]} if op.get("name") else {}
                idx = await coll.create_index(
                    list(op["keys"].items()), unique=bool(op.get("unique", False)), session=s, **extra
                )
                return 1 if idx else 0
            if kind == "drop_index":
                if not op.get("name") and not op.get("keys"):
                    raise AdapterError("mongo op 'drop_index' needs 'name' or 'keys'")
                await coll.drop_index(op.get("name") or list(op["keys"].items()), session=s)
                return 1
            if kind == "create_collection":
                from pymongo.errors import CollectionInvalid

                try:  # 已存在则跳过（同 CREATE TABLE IF NOT EXISTS；register_hot_schema 每次初始化都会调）
                    await self._require_db().create_collection(op["collection"], session=s)
                except CollectionInvalid:
                    pass
                return 0
            raise AdapterError(f"unsupported mongo execute op: {kind!r}")
        except AdapterError:
            raise
        except Exception as exc:
            raise AdapterError(f"mongo execute failed: {exc}") from exc

    async def _fetch(self, sql: str) -> list[dict]:
        op = _decode_op(sql)
        s = self._txn_session.get()
        try:
            kind = op["op"]
            if kind == "list_collections":
                cursor = await self._require_db().list_collections(session=s)
                return _rows(await cursor.to_list(length=None))
            coll = self._coll(op)
            flt = op.get("filter") or {}
            if kind == "find_one":
                doc = await coll.find_one(flt, op.get("projection"), session=s)
                return _rows([doc] if doc is not None else [])
            if kind == "find":
                cursor = coll.find(flt, op.get("projection"), session=s)
                if op.get("sort"):
                    cursor = cursor.sort(op["sort"])
                if op.get("skip"):
                    cursor = cursor.skip(int(op["skip"]))
                return _rows(await cursor.to_list(length=int(op.get("limit", 100))))
            if kind == "aggregate":
                _require(op, "pipeline")
                cursor = coll.aggregate(op["pipeline"], allowDiskUse=bool(op.get("allowDiskUse", False)), session=s)
                return _rows(await cursor.to_list(length=int(op.get("limit", 100))))
            if kind == "distinct":
                _require(op, "key")
                return _rows(await coll.distinct(op["key"], flt, session=s))
            if kind == "count":
                return [{"count": await coll.count_documents(flt, session=s)}]
            if kind == "estimated_count":
                # 服务端不允许在事务内执行 count 命令，故不带 session
                return [{"count": await coll.estimated_document_count()}]
            if kind == "list_indexes":
                return _rows(await coll.list_indexes(session=s).to_list(length=None))
            if kind in ("find_one_and_update", "find_one_and_replace", "find_one_and_delete"):
                doc = await self._find_and_modify(coll, op, s)
                return _rows([doc] if doc is not None else [])
            raise AdapterError(f"unsupported mongo fetch op: {kind!r}")
        except AdapterError:
            raise
        except Exception as exc:
            raise AdapterError(f"mongo fetch failed: {exc}") from exc

    @staticmethod
    async def _find_and_modify(coll, op: dict, session) -> Any:
        from pymongo import ReturnDocument

        kind = op["op"]
        flt = op.get("filter") or {}
        kw: dict[str, Any] = {"projection": op.get("projection"), "sort": op.get("sort"), "session": session}
        if kind == "find_one_and_delete":
            return await coll.find_one_and_delete(flt, **kw)
        ret = str(op.get("return_document", "before")).lower()
        if ret not in ("before", "after"):
            raise AdapterError("return_document must be 'before' or 'after'")
        kw["upsert"] = bool(op.get("upsert", False))
        kw["return_document"] = ReturnDocument.AFTER if ret == "after" else ReturnDocument.BEFORE
        if kind == "find_one_and_update":
            _require(op, "update")
            return await coll.find_one_and_update(flt, op["update"], **kw)
        _require(op, "replacement")
        return await coll.find_one_and_replace(flt, op["replacement"], **kw)

    async def fetch_all(self, sql: str, params: tuple = ()) -> list[dict]:
        return await self._fetch(sql)

    async def fetch_one(self, sql: str, params: tuple = ()) -> dict | None:
        rows = await self._fetch(sql)
        return rows[0] if rows else None

    async def transaction(self) -> TransactionHandle:
        self._require_db()
        return _MongoTransaction(self)

    async def close(self) -> None:
        client, self._client, self._db = self._client, None, None
        if client is not None:
            try:
                client.close()
            except Exception as exc:
                raise AdapterError(f"mongo close failed: {exc}") from exc
