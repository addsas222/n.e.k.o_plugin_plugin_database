"""通用驱动 sqlalchemy / odbc / http：纯单元测试 + 本机可跑的实测。

* sqlalchemy：``sqlite:///<tmp>``（同步，线程）与 ``sqlite+aiosqlite``（异步，需 greenlet，缺则跳过）
* odbc：Windows 自带 "Microsoft Access Text Driver (*.txt, *.csv)" 读临时目录里的 CSV（缺驱动则跳过）
* http：线程里起一个 http.server 假服务，模拟 ES / CouchDB / 文本 / CSV 响应
"""

from __future__ import annotations

import asyncio
import base64
import importlib.util
import json
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest
from plugin.plugins.plugin_database import hot_schema
from plugin.plugins.plugin_database.adapters import ADAPTERS, AdapterError, http_adapter, odbc_adapter
from plugin.plugins.plugin_database.adapters.sqlalchemy_adapter import convert_qmark
from plugin.plugins.plugin_database.service import DatabaseService, driver_report, ensure_extra_paths

# ---------------------------------------------------------------------------
# 纯单元
# ---------------------------------------------------------------------------


def test_registered():
    for name in ("sqlalchemy", "odbc", "http"):
        assert ADAPTERS[name].driver == name


@pytest.mark.parametrize("driver", ["sqlalchemy", "odbc", "http"])
async def test_missing_config_is_structured_error(driver):
    with pytest.raises(AdapterError, match="requires"):
        await ADAPTERS[driver]().connect({})


def test_convert_qmark_per_paramstyle():
    sql = "SELECT * FROM t WHERE a = ? AND b = '?' AND c = ?"
    assert convert_qmark(sql, (1, 2), "qmark") == (sql, (1, 2))
    assert convert_qmark(sql, (1, 2), "format")[0] == "SELECT * FROM t WHERE a = %s AND b = '?' AND c = %s"
    assert convert_qmark(sql, (1, 2), "pyformat")[0].count("%s") == 2
    assert convert_qmark(sql, (1, 2), "numeric")[0] == "SELECT * FROM t WHERE a = :1 AND b = '?' AND c = :2"
    assert "$2" in convert_qmark(sql, (1, 2), "numeric_dollar")[0]
    named_sql, named = convert_qmark(sql, (1, 2), "named")
    assert ":p1" in named_sql and ":p2" in named_sql and named == {"p1": 1, "p2": 2}
    # 无参数时原样下发（PG jsonb 的 ? 运算符不被吃掉）
    assert convert_qmark("SELECT '{}'::jsonb ? 'k'", (), "format") == ("SELECT '{}'::jsonb ? 'k'", ())
    with pytest.raises(AdapterError, match="paramstyle"):
        convert_qmark(sql, (1,), "weird")


def test_odbc_connection_string_from_parts():
    assert odbc_adapter.build_connection_string({"connection_string": " DSN=x "}) == "DSN=x"
    cs = odbc_adapter.build_connection_string(
        {"driver": "ODBC Driver 17 for SQL Server", "server": "h,1433", "database": "db", "uid": "u", "pwd": "p;w}d"}
    )
    assert cs == "DRIVER={ODBC Driver 17 for SQL Server};SERVER=h,1433;DATABASE=db;UID=u;PWD={p;w}}d}"
    assert odbc_adapter.build_connection_string({"driver": "{X}", "dsn": ""}) == "DRIVER={X}"


def test_http_normalize_shapes():
    n = http_adapter.normalize_rows
    es = {"hits": {"hits": [{"_id": "1", "_score": 1.0, "_source": {"t": "a"}}]}}
    assert n(es) == [{"_id": "1", "_score": 1.0, "t": "a"}]
    assert n({"rows": [{"id": "x", "doc": {"_id": "x", "v": 1}}, {"id": "y", "key": "y", "value": 2}]}) == [
        {"_id": "x", "v": 1}, {"id": "y", "key": "y", "value": 2}]
    assert n({"docs": [{"a": 1}]}) == [{"a": 1}]
    assert n({"data": [{"x": 1}], "meta": []}) == [{"x": 1}]
    assert n({"response": {"numFound": 1, "docs": [{"id": "s"}]}}) == [{"id": "s"}]
    assert n({"result": [{"id": 1, "score": 0.9, "payload": {}}], "status": "ok"})[0]["id"] == 1
    assert n({"result": {"points": [{"id": 2}]}}) == [{"id": 2}]
    assert n({"result": {"status": "green"}}) == [{"status": "green"}]
    influx = {"results": [{"series": [{"name": "cpu", "tags": {"host": "a"}, "columns": ["time", "v"],
                                        "values": [[1, 0.5]]}]}]}
    assert n(influx) == [{"series": "cpu", "host": "a", "time": 1, "v": 0.5}]
    assert n([1, {"a": 1}]) == [{"value": 1}, {"a": 1}]
    assert n({"plain": 1}) == [{"plain": 1}]
    assert n("Ok.\n") == [{"text": "Ok.\n"}]
    assert n(None) == []
    assert http_adapter.affected_count({"deleted": 2, "updated": 1}) == 3
    assert http_adapter.affected_count({"items": [{}, {}]}) == 2
    assert http_adapter.affected_count([{}, {}, {}]) == 3
    assert http_adapter.affected_count({"acknowledged": True}) == 1


def test_http_rejects_absolute_url_paths():
    for path in ("http://evil/x", "//evil/x"):
        with pytest.raises(AdapterError, match="relative"):
            http_adapter._decode_request(json.dumps({"path": path}))
    with pytest.raises(AdapterError, match="JSON request"):
        http_adapter._decode_request("GET /")


def test_extra_paths_go_after_vendor(tmp_path, monkeypatch):
    plugin_dir = tmp_path / "plug"
    (plugin_dir / "vendor").mkdir(parents=True)
    (plugin_dir / "drivers").mkdir()
    outside = tmp_path / "outside"
    outside.mkdir()
    vendor = str(plugin_dir / "vendor")
    monkeypatch.setattr(sys, "path", ["a", vendor, "b"])
    added = ensure_extra_paths(plugin_dir, ["drivers", str(outside), str(tmp_path / "missing")])
    assert added == [str(plugin_dir / "drivers"), str(outside)]
    assert sys.path == ["a", vendor, *added, "b"]
    assert ensure_extra_paths(plugin_dir, ["drivers"]) == []  # 幂等


def test_driver_report_shape():
    rep = driver_report(["D:/x"])
    by = {d["driver"]: d for d in rep["drivers"]}
    assert set(by) == set(ADAPTERS)
    assert by["http"]["available"] and by["sqlalchemy"]["available"] and by["odbc"]["available"]
    sqlite = next(d for d in rep["sqlalchemy_dialects"] if d["database"] == "SQLite")
    assert sqlite["available"] and sqlite["install"] is None
    missing = [d for d in rep["sqlalchemy_dialects"] if not d["available"] and d["install"]]
    assert all('--target "D:/x"' in d["install"] for d in missing)
    assert isinstance(rep["odbc_drivers"], list)


# ---------------------------------------------------------------------------
# sqlalchemy 实测（sqlite 文件库）
# ---------------------------------------------------------------------------


async def _sa_roundtrip(url: str):
    a = ADAPTERS["sqlalchemy"]()
    await a.connect({"url": url, "pool_pre_ping": True})
    try:
        assert a.hot_dialect == "sqlite" and hot_schema.dialect_of(a) == "sqlite"
        await a.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT, b BLOB)")
        assert await a.execute("INSERT INTO t (v, b) VALUES (?, ?)", ("a?", b"\x00\x01")) == 1
        row = await a.fetch_one("SELECT v, b FROM t WHERE v = ?", ("a?",))
        assert row == {"v": "a?", "b": b"\x00\x01"}
        async with await a.transaction():
            await a.execute("INSERT INTO t (v) VALUES (?)", ("b",))
            assert len(await a.fetch_all("SELECT * FROM t")) == 2  # 事务内可见
        with pytest.raises(RuntimeError):
            async with await a.transaction():
                await a.execute("INSERT INTO t (v) VALUES (?)", ("ghost",))
                raise RuntimeError("boom")
        assert [r["v"] for r in await a.fetch_all("SELECT v FROM t ORDER BY id")] == ["a?", "b"]
        assert await a.fetch_one("SELECT v FROM t WHERE v = ?", ("none",)) is None
        assert await a.execute("UPDATE t SET v = ? WHERE v = ?", ("c", "b")) == 1
        assert await a.fetch_all("CREATE INDEX ix ON t (v)") == []  # 非查询语句 -> []
        with pytest.raises(AdapterError, match="sqlalchemy execute failed"):
            await a.execute("INSERT INTO nope VALUES (1)")
        # hot_knowledge 方言族 = sqlite：DDL 可用
        info = await hot_schema.register_hot_schema(a)
        assert info["table"] == "hot_knowledge"
    finally:
        await a.close()
    with pytest.raises(AdapterError, match="not connected"):
        await a.fetch_all("SELECT 1")


async def test_sqlalchemy_sqlite_sync_live(tmp_path):
    await _sa_roundtrip(f"sqlite:///{(tmp_path / 'sa.db').as_posix()}")


@pytest.mark.skipif(importlib.util.find_spec("greenlet") is None, reason="greenlet not installed")
async def test_sqlalchemy_sqlite_async_live(tmp_path):
    await _sa_roundtrip(f"sqlite+aiosqlite:///{(tmp_path / 'sa_async.db').as_posix()}")


async def test_sqlalchemy_sqlite_memory_shared_across_threads():
    # 回归：内存库默认按线程隔离，to_thread 换线程后会看不到上一次建的表
    a = ADAPTERS["sqlalchemy"]()
    await a.connect({"url": "sqlite://"})
    try:
        await a.execute("CREATE TABLE t (x)")
        for i in range(20):
            await a.execute("INSERT INTO t VALUES (?)", (i,))
        assert await a.fetch_one("SELECT count(*) AS n FROM t") == {"n": 20}
    finally:
        await a.close()


async def test_sqlalchemy_sqlite_memory_concurrent_and_rollback():
    # 回归：共享单连接无串行化时，并发写会互相提交/回滚，事务回滚不原子
    a = ADAPTERS["sqlalchemy"]()
    await a.connect({"url": "sqlite://"})
    try:
        await a.execute("CREATE TABLE t (x)")
        await asyncio.gather(*(a.execute("INSERT INTO t VALUES (?)", (i,)) for i in range(200)))
        assert await a.fetch_one("SELECT count(*) AS n FROM t") == {"n": 200}

        async def failing_txn():
            async with await a.transaction():
                await a.execute("INSERT INTO t VALUES (-1)")
                await asyncio.sleep(0.05)  # 让并发写在事务期间排队
                raise RuntimeError("boom")

        results = await asyncio.gather(failing_txn(), a.execute("INSERT INTO t VALUES (999)"),
                                       return_exceptions=True)
        assert isinstance(results[0], RuntimeError) and results[1] == 1
        assert await a.fetch_one("SELECT count(*) AS n FROM t WHERE x = -1") == {"n": 0}
        assert await a.fetch_one("SELECT count(*) AS n FROM t WHERE x = 999") == {"n": 1}
    finally:
        await a.close()


async def test_sqlalchemy_unreachable_and_unknown_dialect():
    with pytest.raises(AdapterError, match="sqlalchemy connect failed"):
        await ADAPTERS["sqlalchemy"]().connect({"url": "nosuchdialect://x"})
    with pytest.raises(AdapterError, match="sqlalchemy connect failed"):
        await ADAPTERS["sqlalchemy"]().connect({"url": "sqlite:///Z:/no/such/dir/x.db"})


async def test_service_sqlalchemy_switch(tmp_path):
    svc = DatabaseService(tmp_path)
    await svc.start({"driver": "sqlalchemy", "sqlalchemy": {"url": f"sqlite:///{(tmp_path / 's.db').as_posix()}"}})
    assert svc.driver == "sqlalchemy"
    assert (await svc.fetch_one("SELECT ? AS x", [b"\x01"]))["x"] == {"__bytes_b64__": "AQ=="}
    await svc.stop()


# ---------------------------------------------------------------------------
# odbc 实测（Access Text 驱动读 CSV）
# ---------------------------------------------------------------------------

_TEXT_DRIVER = "Microsoft Access Text Driver (*.txt, *.csv)"


def _has_text_driver() -> bool:
    try:
        import pyodbc

        return _TEXT_DRIVER in pyodbc.drivers()
    except Exception:
        return False


@pytest.mark.skipif(not _has_text_driver(), reason=f"ODBC driver {_TEXT_DRIVER!r} not installed")
async def test_odbc_access_text_driver_live(tmp_path):
    (tmp_path / "people.csv").write_text("id,name\n1,alice\n2,bob\n3,carol\n", encoding="utf-8")
    a = ADAPTERS["odbc"]()
    await a.connect({"driver": _TEXT_DRIVER, "connection_string": f"DRIVER={{{_TEXT_DRIVER}}};DBQ={tmp_path}"})
    try:
        rows = await a.fetch_all("SELECT id, name FROM [people.csv] WHERE id >= ? ORDER BY id", (2,))
        assert rows == [{"id": 2, "name": "bob"}, {"id": 3, "name": "carol"}]
        assert await a.fetch_one("SELECT name FROM [people.csv] WHERE id = ?", (1,)) == {"name": "alice"}
        assert await a.fetch_one("SELECT name FROM [people.csv] WHERE id = ?", (99,)) is None
        # 该驱动不支持关闭 autocommit：自动退回 autocommit，事务给出明确错误
        with pytest.raises(AdapterError, match="autocommit"):
            await a.transaction()
        assert a.hot_dialect is None and hot_schema.dialect_of(a) is None
        with pytest.raises(AdapterError, match="odbc fetch failed"):
            await a.fetch_all("SELECT * FROM [missing.csv]")
    finally:
        await a.close()


async def test_odbc_bad_driver_is_structured_error():
    with pytest.raises(AdapterError, match="odbc connect failed"):
        await ADAPTERS["odbc"]().connect({"driver": "No Such ODBC Driver 99", "server": "x", "timeout": 2})


# ---------------------------------------------------------------------------
# http 实测（本地假服务）
# ---------------------------------------------------------------------------

_AUTH = "Basic " + base64.b64encode(b"neko:secret").decode()


class _FakeDB(BaseHTTPRequestHandler):
    log_message = lambda *a, **k: None  # noqa: E731  静音

    def _reply(self, code: int, body, ctype="application/json"):
        raw = body if isinstance(body, bytes) else json.dumps(body).encode()
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)

    def _route(self):
        if self.headers.get("Authorization") != _AUTH:
            return self._reply(401, {"error": "unauthorized"})
        length = int(self.headers.get("Content-Length") or 0)
        body = self.rfile.read(length).decode() if length else ""
        if self.path == "/":
            return self._reply(200, {"version": {"number": "8.0.0"}})
        if self.path.startswith("/idx/_search?size=2"):
            q = json.loads(body)
            assert q == {"query": {"match": {"t": "neko"}}}
            return self._reply(200, {"hits": {"hits": [{"_id": "1", "_source": {"t": "neko"}}]}})
        if self.path == "/idx/_delete_by_query":
            return self._reply(200, {"deleted": 3})
        if self.path == "/db/_all_docs?include_docs=true":
            return self._reply(200, {"rows": [{"id": "a", "doc": {"_id": "a", "n": 1}}]})
        if self.path == "/sql":
            assert self.headers.get("Content-Type").startswith("text/plain") and body == "SELECT 1"
            return self._reply(200, b'{"x":1}\n{"x":2}\n', "application/x-ndjson")
        if self.path == "/csv":
            return self._reply(200, b"a,b\n1,2\n", "text/csv")
        if self.path in ("/moved", "/away"):  # 同源重定向跟随；跨主机重定向不跟随（凭据不外泄）
            port = self.server.server_address[1]
            self.send_response(302)
            self.send_header("Location", "/csv" if self.path == "/moved" else f"http://127.0.0.2:{port}/")
            self.send_header("Content-Length", "0")
            self.end_headers()
            return None
        return self._reply(404, {"error": "not found"})

    do_GET = do_POST = do_PUT = do_DELETE = _route


@pytest.fixture
def fake_http():
    server = ThreadingHTTPServer(("127.0.0.1", 0), _FakeDB)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    yield f"http://127.0.0.1:{server.server_address[1]}"
    server.shutdown()
    server.server_close()


async def test_http_adapter_live(fake_http):
    a = ADAPTERS["http"]()
    await a.connect({"base_url": fake_http + "/", "username": "neko", "password": "secret", "timeout": 5})
    try:
        rows = await a.fetch_all(json.dumps({"method": "POST", "path": "/idx/_search", "query": {"size": 2},
                                             "body": {"query": {"match": {"t": "neko"}}}}))
        assert rows == [{"_id": "1", "t": "neko"}]
        assert await a.execute(json.dumps({"method": "POST", "path": "idx/_delete_by_query", "body": {}})) == 3
        assert await a.fetch_one(json.dumps({"path": "/db/_all_docs", "query": {"include_docs": "true"}})) == {
            "_id": "a", "n": 1}
        assert await a.fetch_all(json.dumps({"method": "POST", "path": "/sql", "body": "SELECT 1"})) == [
            {"x": 1}, {"x": 2}]
        assert await a.fetch_all(json.dumps({"path": "/csv"})) == [{"a": "1", "b": "2"}]
        assert await a.fetch_all(json.dumps({"path": "/moved"})) == [{"a": "1", "b": "2"}]
        with pytest.raises(AdapterError, match="http 302"):
            await a.fetch_all(json.dumps({"path": "/away"}))
        raw = await a.fetch_all(json.dumps({"path": "/", "raw": True}))
        assert raw == [{"body": {"version": {"number": "8.0.0"}}}]
        with pytest.raises(AdapterError, match="http 404"):
            await a.fetch_all(json.dumps({"path": "/nope"}))
        with pytest.raises(AdapterError, match="does not support transactions"):
            await a.transaction()
    finally:
        await a.close()
    with pytest.raises(AdapterError, match="not connected"):
        await a.fetch_all(json.dumps({"path": "/"}))


async def test_http_connect_checks_auth(fake_http):
    with pytest.raises(AdapterError, match="http connect failed: http 401"):
        await ADAPTERS["http"]().connect({"base_url": fake_http, "token": "wrong"})
    # ping_path 留空：不做连通检查
    a = ADAPTERS["http"]()
    await a.connect({"base_url": fake_http, "ping_path": ""})
    await a.close()
