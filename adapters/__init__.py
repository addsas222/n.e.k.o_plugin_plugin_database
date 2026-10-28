"""统一导出：DatabaseAdapter 接口与九个驱动适配器（六个原生 + sqlalchemy / odbc / http 三个通用）。"""

from __future__ import annotations

from .base import AdapterError, DatabaseAdapter, TransactionHandle, rowcount_from_pg_status
from .http_adapter import HTTPAdapter
from .mongo_adapter import MongoDBAdapter
from .mysql_adapter import MySQLAdapter
from .neo4j_adapter import Neo4jAdapter
from .odbc_adapter import ODBCAdapter
from .postgres_adapter import PostgreSQLAdapter
from .redis_adapter import RedisAdapter
from .sqlalchemy_adapter import SQLAlchemyAdapter
from .sqlite_adapter import SQLiteAdapter

#: driver 名 -> 适配器工厂
ADAPTERS: dict[str, type[DatabaseAdapter]] = {
    SQLiteAdapter.driver: SQLiteAdapter,
    PostgreSQLAdapter.driver: PostgreSQLAdapter,
    MySQLAdapter.driver: MySQLAdapter,
    MongoDBAdapter.driver: MongoDBAdapter,
    Neo4jAdapter.driver: Neo4jAdapter,
    RedisAdapter.driver: RedisAdapter,
    SQLAlchemyAdapter.driver: SQLAlchemyAdapter,
    ODBCAdapter.driver: ODBCAdapter,
    HTTPAdapter.driver: HTTPAdapter,
}

__all__ = [
    "ADAPTERS",
    "AdapterError",
    "DatabaseAdapter",
    "HTTPAdapter",
    "MongoDBAdapter",
    "MySQLAdapter",
    "Neo4jAdapter",
    "ODBCAdapter",
    "PostgreSQLAdapter",
    "RedisAdapter",
    "SQLAlchemyAdapter",
    "SQLiteAdapter",
    "TransactionHandle",
    "rowcount_from_pg_status",
]
