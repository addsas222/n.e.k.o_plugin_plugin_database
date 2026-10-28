"""HTTP/REST 通用适配器（标准库 urllib）：Elasticsearch / OpenSearch、CouchDB、InfluxDB、
Qdrant、Solr、ClickHouse HTTP 等走 HTTP 的数据库。

``sql`` 传 JSON 编码的请求，``params`` 忽略::

    {"method": "POST", "path": "/idx/_search", "query": {"size": 10},
     "body": {"query": {"match_all": {}}}, "headers": {...}, "raw": false}

* ``body`` 为 dict/list 时按 JSON 发送；为字符串时原样发送（ClickHouse SQL、ES ``_bulk``
  的 NDJSON、InfluxDB line protocol），Content-Type 默认 ``text/plain``，可用 ``headers`` 覆盖。
* ``path`` 必须是相对 ``base_url`` 的路径（拒绝绝对 URL）；重定向到别的 scheme/主机/端口时报错
  而不跟随，凭据不会被带去别的主机。
* ``fetch_*`` 把常见响应形状统一成 ``list[dict]``：ES/OpenSearch ``hits.hits[]``（``_source``
  合并 ``_id``/``_score``）、CouchDB ``rows[]``（有 ``doc`` 取 doc）与 ``docs[]``、Solr
  ``response.docs``、ClickHouse ``FORMAT JSON`` 的 ``data`` 与 ``JSONEachRow``、Qdrant ``result``
  （含 ``result.points``）、InfluxDB 1.x ``results[].series[]``、CSV（InfluxDB 2.x Flux）、
  JSON 列表与对象；非 JSON 文本返回 ``[{"text": ...}]``。``"raw": true`` 跳过归一化返回 ``[{"body": 响应体}]``。
* ``execute`` 返回影响数：响应里的 ``deleted``/``updated`` 之和、``items``（ES ``_bulk``）或
  列表长度（CouchDB ``_bulk_docs``），都没有时成功即 1。
* 不支持事务：``transaction()`` 返回结构化错误。
"""

from __future__ import annotations

import asyncio
import base64
import csv
import io
import json
import ssl
import urllib.error
import urllib.parse
import urllib.request
from typing import Any

from .base import AdapterError, DatabaseAdapter, TransactionHandle


def _decode_request(sql: str) -> dict:
    try:
        req = json.loads(sql)
    except json.JSONDecodeError as exc:
        raise AdapterError(
            'http adapter expects a JSON request as sql, e.g. {"method":"GET","path":"/_cat/indices"}'
        ) from exc
    if not isinstance(req, dict) or not isinstance(req.get("path", ""), str):
        raise AdapterError("http request JSON must be an object with a string 'path'")
    if "://" in req.get("path", "") or req.get("path", "").startswith("//"):
        raise AdapterError("http request path must be relative to base_url (absolute URLs are rejected)")
    return req


def _parse_body(raw: bytes, content_type: str) -> Any:
    text = raw.decode("utf-8", errors="replace")
    if not text.strip():
        return None
    if "csv" in content_type:
        return [row for row in csv.DictReader(io.StringIO(text)) if any(row.values())]
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass
    try:  # NDJSON（ClickHouse JSONEachRow 等）
        return [json.loads(line) for line in text.splitlines() if line.strip()]
    except json.JSONDecodeError:
        return text


def normalize_rows(body: Any) -> list[dict]:
    """把各家 REST 数据库的响应形状统一成 list[dict]。"""
    if body is None:
        return []
    if isinstance(body, str):
        return [{"text": body}]
    if isinstance(body, list):
        return [x if isinstance(x, dict) else {"value": x} for x in body]
    if not isinstance(body, dict):
        return [{"value": body}]
    hits = body.get("hits")
    if isinstance(hits, dict) and isinstance(hits.get("hits"), list):  # Elasticsearch / OpenSearch
        return [
            {**({"_id": h["_id"]} if "_id" in h else {}), **({"_score": h["_score"]} if "_score" in h else {}),
             **(h.get("_source") or h.get("fields") or {})}
            for h in hits["hits"]
        ]
    if isinstance(body.get("rows"), list):  # CouchDB views / _all_docs
        return [r["doc"] if isinstance(r, dict) and isinstance(r.get("doc"), dict) else r for r in body["rows"]]
    for key in ("docs", "data"):  # CouchDB _find / ClickHouse FORMAT JSON
        if isinstance(body.get(key), list):
            return normalize_rows(body[key])
    resp = body.get("response")
    if isinstance(resp, dict) and isinstance(resp.get("docs"), list):  # Solr
        return normalize_rows(resp["docs"])
    if "result" in body:  # Qdrant
        result = body["result"]
        if isinstance(result, dict) and isinstance(result.get("points"), list):
            return normalize_rows(result["points"])
        return normalize_rows(result)
    if isinstance(body.get("results"), list):  # InfluxDB 1.x /query
        out: list[dict] = []
        for res in body["results"]:
            for series in (res or {}).get("series") or []:
                cols = series.get("columns") or []
                for values in series.get("values") or []:
                    out.append({"series": series.get("name"), **(series.get("tags") or {}), **dict(zip(cols, values))})
        return out
    return [body]


def affected_count(body: Any) -> int:
    if isinstance(body, list):
        return len(body)
    if isinstance(body, dict):
        counts = [body[k] for k in ("deleted", "updated") if isinstance(body.get(k), int)]
        if counts:
            return sum(counts)
        if isinstance(body.get("items"), list):
            return len(body["items"])
    return 1


class _SameOriginRedirect(urllib.request.HTTPRedirectHandler):
    """只跟随同源重定向：urllib 默认会把 Authorization 头原样带去新主机。"""

    def __init__(self, origin: tuple[str, str]) -> None:
        self._origin = origin

    def redirect_request(self, req, fp, code, msg, headers, newurl):
        u = urllib.parse.urlsplit(newurl)
        if (u.scheme.lower(), u.netloc.lower()) != self._origin:
            return None  # 不跟随：原 3xx 以 HTTPError 抛出
        return super().redirect_request(req, fp, code, msg, headers, newurl)


class HTTPAdapter(DatabaseAdapter):
    """config 键：
        base_url: 如 ``http://127.0.0.1:9200``（必填）
        headers: 每个请求都带的头（dict，可选）
        username / password: Basic 认证；token: Bearer 认证（headers 里写了 Authorization 则以它为准）
        timeout: 秒（默认 30）
        verify_tls: false 时不校验 HTTPS 证书（自签名的 ES 8 等；默认 true）
        ping_path: connect 时 GET 的路径（默认 "/"；留空跳过连通性检查）
    """

    driver = "http"

    def __init__(self) -> None:
        self._base = ""
        self._headers: dict[str, str] = {}
        self._timeout = 30.0
        self._opener: urllib.request.OpenerDirector | None = None

    async def connect(self, config: dict[str, Any]) -> None:
        base = str(config.get("base_url") or "").strip().rstrip("/")
        if not base.lower().startswith(("http://", "https://")):
            raise AdapterError("http adapter requires [database.http] base_url (http:// or https://)")
        headers = {str(k): str(v) for k, v in dict(config.get("headers") or {}).items()}
        if not any(k.lower() == "authorization" for k in headers):
            if config.get("token"):
                headers["Authorization"] = f"Bearer {config['token']}"
            elif config.get("username"):
                pair = f"{config['username']}:{config.get('password') or ''}".encode()
                headers["Authorization"] = "Basic " + base64.b64encode(pair).decode("ascii")
        self._base, self._headers = base, headers
        self._timeout = float(config.get("timeout", 30))
        ctx = None if config.get("verify_tls", True) else ssl._create_unverified_context()
        u = urllib.parse.urlsplit(base)
        self._opener = urllib.request.build_opener(
            _SameOriginRedirect((u.scheme.lower(), u.netloc.lower())), urllib.request.HTTPSHandler(context=ctx)
        )
        ping = config.get("ping_path", "/")
        if ping:
            try:
                await self._request({"method": "GET", "path": str(ping)})
            except AdapterError as exc:
                self._base = ""
                raise AdapterError(f"http connect failed: {exc}") from exc

    def _send(self, req: dict) -> Any:
        method = str(req.get("method") or "GET").upper()
        url = f"{self._base}/{str(req.get('path') or '').lstrip('/')}"
        if req.get("query"):
            url += "?" + urllib.parse.urlencode(req["query"], doseq=True)
        headers = dict(self._headers)
        data = None
        body = req.get("body")
        if body is not None:
            if isinstance(body, str):
                data = body.encode("utf-8")
                headers.setdefault("Content-Type", "text/plain; charset=utf-8")
            else:
                data = json.dumps(body, ensure_ascii=False).encode("utf-8")
                headers.setdefault("Content-Type", "application/json")
        headers.update({str(k): str(v) for k, v in dict(req.get("headers") or {}).items()})
        request = urllib.request.Request(url, data=data, headers=headers, method=method)
        try:
            with self._opener.open(request, timeout=self._timeout) as resp:
                return _parse_body(resp.read(), resp.headers.get("Content-Type", ""))
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace")[:500]
            raise AdapterError(f"http {exc.code} {method} {req.get('path')}: {detail}") from exc
        except (urllib.error.URLError, OSError, ValueError) as exc:
            raise AdapterError(f"http {method} {req.get('path')} failed: {exc}") from exc

    async def _request(self, req: dict) -> Any:
        if not self._base:
            raise AdapterError("http adapter not connected")
        return await asyncio.to_thread(self._send, req)

    async def execute(self, sql: str, params: tuple = ()) -> int:
        return affected_count(await self._request(_decode_request(sql)))

    async def fetch_all(self, sql: str, params: tuple = ()) -> list[dict]:
        req = _decode_request(sql)
        body = await self._request(req)
        return [{"body": body}] if req.get("raw") else normalize_rows(body)

    async def fetch_one(self, sql: str, params: tuple = ()) -> dict | None:
        rows = await self.fetch_all(sql, params)
        return rows[0] if rows else None

    async def transaction(self) -> TransactionHandle:
        raise AdapterError("http driver does not support transactions (send one bulk request instead)")

    async def close(self) -> None:
        self._base = ""
