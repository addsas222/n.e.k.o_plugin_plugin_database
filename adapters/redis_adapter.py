"""Redis 适配器（redis-py asyncio；Valkey / KeyDB / Dragonfly 等兼容服务同样可用）。

``sql`` 参数传 **一条 Redis 命令行**，按 shell 规则分词后把 ``params`` 追加在末尾：

    db_execute("SET neko:greeting", ["hello world"])   # 含空格的值放 params
    db_fetch_one("GET neko:greeting")                  # -> {"value": "hello world"}
    db_fetch_all("HGETALL neko:h")                     # -> [{field: value, ...}]

返回值归一：
    execute   整数原样；OK/True 记 1、False/None 记 0、列表记长度、其余非空值记 1
    fetch_all dict -> [dict]；列表 -> [{"index": i, "value": v}]；标量 -> [{"value": v}]
    fetch_one dict 原样；其余 -> {"value": v}；None -> None
bytes 能按 UTF-8 解码就转 str，否则保留 bytes（入口层再 base64 包装）。

``deny_commands`` 里的命令直接拒绝（默认含清库、改配置、会占住连接的命令，能绕过黑名单的
脚本/函数（EVAL/FCALL/SCRIPT/FUNCTION）、SWAPDB/MODULE/ACL，以及
MULTI/EXEC/WATCH——事务请用 ``db_transaction``）：Redis 往往是多个应用共享的实例。

事务语义：``transaction()`` 是 MULTI/EXEC 管道。事务期间 ``execute`` 只入队并返回 0，
``fetch_*`` 拿不到结果、直接报错；正常退出时 EXEC，异常时丢弃队列。注意 Redis
没有回滚：EXEC 时单条命令出错，其余命令照样生效（错误会以 AdapterError 上报）。
"""

from __future__ import annotations

import contextvars
import shlex
from typing import Any

from .base import AdapterError, DatabaseAdapter, TransactionHandle

DEFAULT_DENY = frozenset(
    {
        "FLUSHALL", "FLUSHDB", "SHUTDOWN", "CONFIG", "DEBUG", "MONITOR",
        "SUBSCRIBE", "PSUBSCRIBE", "SSUBSCRIBE", "SYNC", "PSYNC", "REPLICAOF",
        "SLAVEOF", "MIGRATE", "MULTI", "EXEC", "DISCARD", "WATCH", "UNWATCH",
        # 脚本/函数能绕过黑名单调任意命令（EVAL "return redis.call('FLUSHALL')" 0）
        "EVAL", "EVALSHA", "EVAL_RO", "EVALSHA_RO", "FCALL", "FCALL_RO", "FUNCTION", "SCRIPT",
        "SWAPDB", "MODULE", "ACL",
    }
)


def _decode(value: Any) -> Any:
    if isinstance(value, (bytes, bytearray)):
        try:
            return bytes(value).decode("utf-8")
        except UnicodeDecodeError:
            return bytes(value)
    if isinstance(value, dict):
        return {_decode(k): _decode(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_decode(v) for v in value]
    return value


def _rowcount(value: Any) -> int:
    if isinstance(value, bool):  # bool 是 int 子类，先判
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, (list, tuple, set, dict)):
        return len(value)
    return 0 if value is None else 1


def _rows(value: Any) -> list[dict]:
    if value is None:
        return []
    if isinstance(value, dict):
        return [value]
    if isinstance(value, list):
        return [{"index": i, "value": v} for i, v in enumerate(value)]
    return [{"value": value}]


class _RedisTransaction(TransactionHandle):
    def __init__(self, adapter: "RedisAdapter") -> None:
        self._adapter = adapter
        self._pipe: Any = None
        self._token: contextvars.Token | None = None

    async def _begin(self) -> None:
        self._pipe = self._adapter._require_client().pipeline(transaction=True)
        self._token = self._adapter._txn.set(self._pipe)

    async def _finish(self, commit: bool) -> None:
        pipe, token = self._pipe, self._token
        self._pipe = self._token = None
        if token is not None:
            self._adapter._txn.reset(token)
        if pipe is None:
            return
        try:
            if commit:
                await pipe.execute()
        except Exception as exc:
            raise AdapterError(f"redis EXEC failed: {exc}") from exc
        finally:
            await pipe.reset()

    async def _commit(self) -> None:
        await self._finish(commit=True)

    async def _rollback(self) -> None:
        await self._finish(commit=False)


class RedisAdapter(DatabaseAdapter):
    """redis.asyncio 客户端（自带连接池）。

    config 键：
        url: ``redis://[:password@]127.0.0.1:6379/0``，也接受 ``rediss://``（必填）
        connect_timeout: 建连超时秒数（默认 10）
        deny_commands: 拒绝执行的命令列表（缺省 = ``DEFAULT_DENY``；显式给出则整体替换）
    """

    driver = "redis"

    #: 事务期间的 MULTI/EXEC 管道（按任务上下文隔离）
    _txn: contextvars.ContextVar = contextvars.ContextVar("redis_txn", default=None)

    def __init__(self) -> None:
        self._client: Any = None
        self._deny: frozenset[str] = DEFAULT_DENY

    async def connect(self, config: dict[str, Any]) -> None:
        url = str(config.get("url") or "").strip()
        if not url:
            raise AdapterError("redis adapter requires [database.redis] url")
        if config.get("deny_commands") is not None:
            self._deny = frozenset(str(c).upper() for c in config["deny_commands"])
        try:
            import redis.asyncio as aioredis

            self._client = aioredis.from_url(
                url, socket_connect_timeout=float(config.get("connect_timeout", 10))
            )
            await self._client.ping()
        except Exception as exc:
            client, self._client = self._client, None
            if client is not None:
                try:
                    await client.aclose()
                except Exception:
                    pass
            raise AdapterError(f"redis connect failed: {exc}") from exc

    def _require_client(self) -> Any:
        if self._client is None:
            raise AdapterError("redis adapter not connected")
        return self._client

    def _args(self, sql: str, params: tuple) -> list:
        try:
            args = shlex.split(sql) + list(params)
        except ValueError as exc:  # 引号不配对
            raise AdapterError(f"redis command parse failed: {exc}") from exc
        if not args:
            raise AdapterError("redis command is empty")
        if str(args[0]).upper() in self._deny:
            raise AdapterError(f"redis command {str(args[0]).upper()} is denied by deny_commands")
        return args

    async def _call(self, sql: str, params: tuple) -> Any:
        args = self._args(sql, params)
        client = self._require_client()
        try:
            return _decode(await client.execute_command(*args))
        except Exception as exc:
            raise AdapterError(f"redis command failed: {exc}") from exc

    def _no_fetch_in_txn(self) -> None:
        if self._txn.get() is not None:
            raise AdapterError("redis transaction only queues commands; fetch is unavailable inside it")

    async def execute(self, sql: str, params: tuple = ()) -> int:
        pipe = self._txn.get()
        if pipe is not None:
            pipe.execute_command(*self._args(sql, params))
            return 0
        return _rowcount(await self._call(sql, params))

    async def fetch_all(self, sql: str, params: tuple = ()) -> list[dict]:
        self._no_fetch_in_txn()
        return _rows(await self._call(sql, params))

    async def fetch_one(self, sql: str, params: tuple = ()) -> dict | None:
        self._no_fetch_in_txn()
        value = await self._call(sql, params)
        if value is None or isinstance(value, dict):
            return value
        return {"value": value}

    async def transaction(self) -> TransactionHandle:
        self._require_client()
        return _RedisTransaction(self)

    async def close(self) -> None:
        client, self._client = self._client, None
        if client is not None:
            try:
                await client.aclose()
            except Exception as exc:
                raise AdapterError(f"redis close failed: {exc}") from exc
