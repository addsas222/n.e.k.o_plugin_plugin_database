# plugin_database

N.E.K.O 统一数据库插件（基础层）。提供 `DatabaseAdapter` 抽象、六个原生驱动
（SQLite / PostgreSQL / MySQL / MongoDB / Neo4j / Redis）和三个通用驱动
（`sqlalchemy` 任意 SQLAlchemy URL、`odbc` 任意 ODBC 驱动、`http` REST 数据库），
默认 SQLite 零配置运行；切换数据库只需改 `[database] driver` 与对应连接段。

## 架构

```
plugin_database（本插件，基础层）
├── adapters/base.py            DatabaseAdapter 抽象 + AdapterError + 事务骨架
├── adapters/sqlite_adapter.py  aiosqlite 单连接 + WAL（默认）
├── adapters/postgres_adapter.py asyncpg 连接池（? -> $n 转译）
├── adapters/mysql_adapter.py   aiomysql 连接池 + DictCursor（? -> %s 转译）
├── adapters/mongo_adapter.py   motor 文档模型（JSON 操作协议，不映射关系表）
├── adapters/neo4j_adapter.py   neo4j 异步驱动（sql 传 Cypher）
├── adapters/redis_adapter.py   redis.asyncio（sql 传命令行，带命令黑名单）
├── adapters/sqlalchemy_adapter.py 任意 SQLAlchemy URL（同步方言走线程，异步方言需 greenlet）
├── adapters/odbc_adapter.py    pyodbc（? 原生，阻塞调用走线程）
├── adapters/http_adapter.py    urllib（sql 传 JSON 请求，响应归一化成 list[dict]）
├── hot_schema.py               hot_knowledge 多方言 DDL + 降级 SQL + Mongo 索引
└── service.py                  驱动选择 / bytes<->base64 编解码 / vendor + extra_paths 注入 / 驱动清单
```

## 哪个数据库用哪个驱动

"vendor" = 插件自带；"extra_paths" = 自己装进 `[database] extra_paths` 目录
（`uv pip install --python 3.11 --target <目录> <包>`，把目录写进配置后重启插件）。
`db_drivers` 入口列出本机哪些可用，缺的给出安装命令。

| 数据库 | driver | 连接配置示例 | 依赖 |
|---|---|---|---|
| SQLite | `sqlite` | `path = ""`（默认库） | vendor |
| PostgreSQL / CockroachDB / TimescaleDB / Supabase | `postgres` | `dsn = "postgresql://user:pass@host:5432/db"`（Supabase 用其 Postgres 连接串） | vendor |
| MySQL / MariaDB / TiDB | `mysql` | `dsn = "mysql://user:pass@host:3306/db"`（TiDB 默认端口 4000） | vendor |
| MongoDB / Atlas / DocumentDB / Cosmos DB / FerretDB | `mongo` | `uri = "mongodb://..."` 或 `"mongodb+srv://..."`，见下方兼容服务说明 | vendor（含 dnspython） |
| Neo4j / Memgraph | `neo4j` | `uri = "bolt://host:7687"` | vendor |
| Redis / Valkey / KeyDB / Dragonfly | `redis` | `url = "redis://host:6379/0"` | vendor |
| SQL Server | `odbc` 或 `sqlalchemy` | `driver = "ODBC Driver 17 for SQL Server"` + server / database / uid / pwd；或 `url = "mssql+pyodbc://user:pass@host/db?driver=ODBC+Driver+17+for+SQL+Server"` | vendor（pyodbc）+ 微软 ODBC 驱动 |
| Oracle | `sqlalchemy` | `url = "oracle+oracledb://user:pass@host:1521/?service_name=XE"` | extra_paths：`oracledb` |
| DuckDB | `sqlalchemy` | `url = "duckdb:///D:/data/x.duckdb"` | extra_paths：`duckdb duckdb-engine`（约 40 MB，不随插件分发） |
| ClickHouse | `sqlalchemy` 或 `http` | `url = "clickhouse+http://user:pass@host:8123/db"`；或 http：`base_url = "http://host:8123"`，body 写 SQL（加 `FORMAT JSON`） | extra_paths：`clickhouse-sqlalchemy`；http 无依赖 |
| Snowflake | `sqlalchemy` | `url = "snowflake://user:pass@account/db/schema?warehouse=wh"` | extra_paths：`snowflake-sqlalchemy` |
| 达梦 DM | `sqlalchemy` 或 `odbc` | `url = "dm+dmPython://SYSDBA:pass@host:5236"`；或达梦 ODBC 驱动 | extra_paths：达梦安装包附带的 `dmPython` + `sqlalchemy_dm`（以官方发行为准，本机未实测） |
| 人大金仓 KingbaseES | `postgres` 或 `odbc` | PG 兼容模式下直接用 `postgres` 的 dsn；否则用金仓 ODBC 驱动 | vendor（PG 模式）/ 厂商 ODBC 驱动（本机未实测） |
| Access / Excel / CSV | `odbc` | `connection_string = "DRIVER={Microsoft Access Text Driver (*.txt, *.csv)};DBQ=D:/data/csv"`（Excel / Access 驱动同理） | Windows 自带 Access 驱动 |
| Elasticsearch / OpenSearch | `http` | `base_url = "https://host:9200"` + username / password；自签名证书 `verify_tls = false` | 无 |
| CouchDB | `http` | `base_url = "http://host:5984"` + username / password | 无 |
| InfluxDB | `http` | 1.x：`{"path": "/query", "query": {"db": "x", "q": "SELECT ..."}}`；2.x：`POST /api/v2/query`（Flux，CSV 响应自动解析），认证写 `headers = { Authorization = "Token xxx" }` | 无 |
| Qdrant | `http` | `base_url = "http://host:6333"`，API key 写 `headers = { "api-key" = "..." }` | 无 |
| Solr | `http` | `base_url = "http://host:8983/solr"` | 无 |
| Cassandra / ScyllaDB | `odbc` 或 `http` | **没有原生适配器**：用 DataStax / Simba 的 Cassandra ODBC 驱动，或经 HTTP 网关（Stargate REST、Scylla Alternator 等）走 `http`。`cassandra-driver` 不是 SQLAlchemy 方言，装进 extra_paths 也不能直接用 | 厂商 ODBC 驱动 / 网关 |

热数据表与 Skill 统计（`hot_*` / `skill_stat_*`）只在 sqlite / postgres / mysql 方言与 mongo 上可用：
`sqlalchemy` 连 SQLite / PostgreSQL / MySQL / MariaDB 时，或 `odbc` 连这三种库（按 `SQL_DBMS_NAME`
识别）时同样可用；其余驱动返回结构化 Err，`register_hot_schema` 返回 `skipped`。

## 接口（DatabaseAdapter）

```python
await adapter.connect(config)            # 建连接/池
n = await adapter.execute(sql, params)   # DML，返回受影响行数
rows = await adapter.fetch_all(sql, params)   # -> list[dict]
row = await adapter.fetch_one(sql, params)    # -> dict | None
async with await adapter.transaction():  # 异常回滚 / 正常提交
    ...
await adapter.close()
```

- 占位符统一写 `?`；postgres 适配器内部转 `$n`，mysql 转 `%s`，sqlite 原生支持。
- `transaction()` 是 async 方法，返回异步上下文管理器（`async with await ...`）。
- MongoDB 不支持 SQL：`sql` 传 JSON 字符串编码的文档操作，完整清单见
  `adapters/mongo_adapter.py` 模块 docstring：
  - `db_execute`：insert_one / insert_many / update_one / update_many / replace_one /
    delete_one / delete_many / bulk_write（requests 每项与单条操作同键）/ create_index /
    drop_index / create_collection（已存在则跳过），返回受影响文档数；
  - `db_fetch_all` / `db_fetch_one`：find / find_one / aggregate（pipeline、allowDiskUse）/
    distinct（`[{"value": v}]`）/ count、estimated_count（`{"count": n}`）/ list_collections /
    list_indexes / find_one_and_update / find_one_and_replace / find_one_and_delete
    （return_document = before | after）。find / aggregate 不写 limit 时最多返回 100 条；
  - 出站 ObjectId 转 24 位 hex；入站 filter / update / replacement / pipeline 里 `_id` / `$eq` / `$in`
    位置的 hex 自动转回 ObjectId；
  - 例：`db_fetch_all('{"op":"aggregate","collection":"t","pipeline":[{"$group":{"_id":"$tag","n":{"$sum":1}}}]}')`。
- MongoDB 兼容服务：Atlas 用 `mongodb+srv://`（dnspython 已在 vendor）；AWS DocumentDB 需
  `tls=true&tlsCAFile=...&retryWrites=false`；Azure Cosmos DB（Mongo API）直接用门户连接串
  （含 `retrywrites=false`）；FerretDB 用普通 `mongodb://`，事务与部分聚合算子视其版本而定。
  这几种只按文档说明，本机只实测了 MongoDB 7.0 副本集。
- SQLAlchemy：`sql` 写目标库原生 SQL，`?` 按方言 paramstyle 转译（qmark / format / numeric / named …），
  不带 params 时原样下发。异步 URL（`sqlite+aiosqlite`、`postgresql+asyncpg`）需要 greenlet（不随插件分发）。
- ODBC：`?` 是原生语法。驱动不支持关闭 autocommit（如 Access Text 驱动）时自动退回 autocommit，
  此时 `db_transaction` 返回明确错误。Access Text 驱动只支持读和 INSERT，不支持 UPDATE / DELETE。
- HTTP：`sql` 传 `{"method","path","query","body","headers","raw"}`；body 为字符串时原样发送
  （ClickHouse SQL、ES `_bulk` 的 NDJSON、InfluxDB line protocol）。`fetch_*` 把 ES `hits.hits`、
  CouchDB `rows` / `docs`、Solr `response.docs`、ClickHouse `data` / JSONEachRow、Qdrant `result`、
  InfluxDB 1.x `series`、CSV 统一成 `list[dict]`；`"raw": true` 返回 `[{"body": 原始响应}]`。
  `path` 只能是相对路径。不支持事务。
- Neo4j：`sql` 传 Cypher。`params=[{"name": "x"}]`（单个 dict）作命名参数；
  其余位置参数映射为 `$p1..$pn`（`?` 不转译：Cypher 的 `(a)--(b)` 会被当成注释）。
  `execute` 返回写计数之和（节点/关系增删、属性设置、标签增删），`fetch_*` 返回
  `record.data()`，时间类型转 ISO 字符串、空间点转列表。
- Redis：`sql` 传一条命令行（按 shell 规则分词），`params` 追加在末尾——含空格的
  值放 params：`db_execute("SET k", ["hello world"])`。`fetch_one("GET k")` 返回
  `{"value": ...}`，`fetch_all` 把 dict 返回成 `[dict]`、列表返回成
  `[{"index": i, "value": v}]`。`deny_commands` 默认拒绝清库/改配置/占连接的命令、
  可绕过黑名单的脚本/函数（EVAL/FCALL/SCRIPT/FUNCTION）、SWAPDB/MODULE/ACL，
  以及 MULTI/EXEC/WATCH（事务用 `db_transaction`）。Valkey / KeyDB / Dragonfly 等
  兼容服务同样可用；Memgraph 走 neo4j 驱动。

## 入口（跨插件调用）

| 入口 | 说明 |
|------|------|
| `db_execute` / `db_fetch_all` / `db_fetch_one` | 通用 SQL（params 中的 bytes 用 `{"__bytes_b64__": ...}` 包装） |
| `db_transaction` | 一组语句在单个事务里执行（Mongo 需副本集；Redis 为 MULTI/EXEC；HTTP 与不支持关闭 autocommit 的 ODBC 驱动返回 Err） |
| `db_status` / `db_capabilities` / `db_switch_driver` | 状态 / vec0+FTS5 能力探测 / 运行时切换驱动 |
| `db_drivers` | 全部驱动的依赖能否导入、SQLAlchemy 常用方言、本机 ODBC 驱动、安装命令 |
| `register_hot_schema` | 按当前驱动创建 `hot_knowledge` 表/集合（知识库初始化时调用） |
| `hot_promote` | 按 driver 语义 upsert 一条热知识（`content_hash` 去重） |
| `hot_demote` | 把 `last_hit_at` 早于 N 天的记录标记 `demoted` |
| `hot_query` | 按 `last_hit_at` 倒序查询（默认不含 demoted） |
| `skill_stat_record` / `skill_stat_query` | Skill 调用统计（次数/错误/耗时） |

所有入口返回 `Ok` / `Err(SdkError)`，任何驱动失败都是结构化错误，不会拖垮宿主。
Neo4j / Redis / HTTP，以及连到非 sqlite/postgres/mysql 库的 sqlalchemy / odbc 只支持 `db_*`
通用入口：`register_hot_schema` 返回 `skipped`，`hot_*` / `skill_stat_*` 返回 Err。

## 配置（config.example.toml）

```toml
[database]
driver = "sqlite"            # sqlite | postgres | mysql | mongo | neo4j | redis | sqlalchemy | odbc | http
extra_paths = []             # 插件外的驱动目录，排在 vendor/ 之后加入 sys.path（vendor 同名包优先）

[database.sqlite]
path = ""                    # 留空 = <插件数据>/data/plugin_database.db
vec0_path = ""               # sqlite-vec 扩展；留空=自动（vendor DLL），"none" 禁用
require_vec0 = false         # true：vec0 加载失败或无 DLL 可加载时阻止启动

[database.postgres]
dsn = "postgresql://user:pass@127.0.0.1:5432/neko"

[database.mysql]
dsn = "mysql://user:pass@127.0.0.1:3306/neko"

[database.mongo]
uri = "mongodb://127.0.0.1:27017"
database = "neko"

[database.neo4j]
uri = "bolt://127.0.0.1:7687"
user = "neo4j"               # 留空 = 不认证
password = "..."
database = ""                # 留空 = 服务端默认库

[database.redis]
url = "redis://127.0.0.1:6379/0"   # 密码：redis://:pass@host:6379/0
# deny_commands = [...]      # 缺省用内置黑名单；显式写出则整体替换

[database.sqlalchemy]
url = "mssql+pyodbc://user:pass@host/db?driver=ODBC+Driver+17+for+SQL+Server"
connect_args = {}            # 传给 DBAPI connect()
pool_pre_ping = true         # 另可写 pool_size / max_overflow / pool_timeout / pool_recycle

[database.odbc]
connection_string = ""       # 优先；留空则用 dsn / driver / server / database / uid / pwd 拼接
timeout = 10

[database.http]
base_url = "https://127.0.0.1:9200"
username = ""                # Basic；或 token = "..."（Bearer）；或 headers = { Authorization = "..." }
verify_tls = true
ping_path = "/"              # connect 时 GET 检查连通/认证；留空跳过
```

## 依赖

`pyproject.toml` 声明（含 SQLAlchemy 与 pyodbc），`uv run neko-plugin sync plugin_database` 同步到 `vendor/`
（宿主运行时由插件自行把 vendor 加入 sys.path）。宿主自带的 onnxruntime /
numpy / pyyaml 不在本插件依赖里。

> **克隆本仓库后 `vendor/` 是空的**（`.gitignore` 排除，其中含 `vec0.dll`、
> asyncpg 的 cp311 二进制等约 16 MB 平台相关文件）。补齐方式：
>
> ```bash
> uv run neko-plugin sync plugin_database
> ```
>
> 若 `vendor/` 缺失，插件仍能启动（`ensure_vendor_path` 只是把它加进 `sys.path`，
> 不存在就跳过），但非 SQLite 驱动与 **sqlite-vec（vec0）** 会不可用 ——
> 后者会让向量检索退化为 numpy 余弦扫描。`db_capabilities` 会如实报
> `vec0` / `fts5` 的可用性，可用来确认补齐是否成功。
>
> 注意 vec0 是 `cp311-win_amd64` 二进制：`pyproject.toml` 因此把
> `requires-python` 收紧到 `>=3.11,<3.12`，3.12+ 上 ABI 不匹配会静默失效。

## 测试

```bash
uv run neko-plugin sync plugin_database   # 先补齐 vendor/，否则见下
python -m pytest tests -q                 # 单元 + SQLite 全链路
python tests/stub_run.py                  # 桩 SDK 驱动入口层端到端
```

> **`vendor/` 缺失时测试会失败**，报 `ModuleNotFoundError: No module named
> 'aiosqlite'`（`tests/conftest.py` 把 `vendor/` 加进 `sys.path`，
> 见其文件头说明）。这不是测试本身的问题 —— SQLite 适配器依赖 `aiosqlite`，
> 而它随 `vendor/` 分发。先跑一次 `neko-plugin sync` 即可。

通用驱动的实测在本机直接跑，不需要环境变量：sqlalchemy 用临时 sqlite 文件（装了 greenlet 时
`sqlite+aiosqlite` 一起测）；odbc 用 Windows 自带的 Access Text 驱动读临时 CSV（没有该驱动则跳过）；
http 用线程里起的 `http.server` 假服务。

真实服务测试默认跳过，设环境变量启用。Mongo 测试只碰 `neko_t_*` 集合，事务部分需要副本集；
设置了 Mongo 变量时，`stub_run.py` 也会把入口层在 Mongo 上跑一遍：

```bash
NEKO_TEST_PG_DSN=... NEKO_TEST_MYSQL_DSN=... NEKO_TEST_MONGO_URI=... NEKO_TEST_MONGO_DB=... NEKO_TEST_NEO4J_URI=bolt://127.0.0.1:7687 NEKO_TEST_NEO4J_USER=... NEKO_TEST_NEO4J_PASSWORD=... NEKO_TEST_REDIS_URL=redis://127.0.0.1:6379/15
```

Neo4j / Redis 的实测只碰自己的数据：Neo4j 只用 `:NekoTest` 标签（清理也只删该标签），
Redis 只写 `neko_test:*` 键并逐个删除，从不 FLUSH——本机实例可能是别的应用共享的。

## hot_knowledge 表结构

任务书 schema + 一处显式扩展：`status TEXT NOT NULL DEFAULT 'active'`
（`'demoted'` 表示冷数据降级标记，数据保留但不参与热查询）。
PostgreSQL / MySQL / MongoDB 方言见 `hot_schema.py`。

## 已知边界

- MySQL 提供 params 时，SQL 中的字面 `%` 需写成 `%%`（PyMySQL 规则）。
- 事务：`async with await adapter.transaction():` 期间，本适配器的
  execute/fetch 经 contextvar 自动路由到事务连接（pg/mysql 从池里钉住连接）；
  sqlalchemy / odbc 同样钉住连接，mongo 钉住 client session。SQLite / MongoDB（副本集）/
  sqlalchemy(sqlite) 有实测覆盖。MongoDB 多文档事务要求副本集：单机 mongod 返回结构化 Err，
  这时原子性靠单文档原子操作或 replace upsert。
- 跨插件 IPC 传不了事务句柄，也不提供 begin/commit/rollback 三段式（调用方
  中途失联会留悬挂事务）；多条语句的原子写入用 `db_transaction` 入口一次提交。
- vec0 尽力而为：加载失败（缺 DLL / 宿主 sqlite 未编译扩展支持 / ABI 不匹配）
  只记录原因并在 `db_capabilities.vec0_error` 如实上报，不阻止启动；知识库自动
  退化 numpy 余弦扫描。仅显式 `require_vec0 = true` 时才视为致命。
- 相对 `vec0_path` 按插件目录解析（不依赖进程 CWD）。
- Redis 事务是 MULTI/EXEC：事务内 `execute` 只入队（返回 0），`fetch_*` 报错；
  Redis 没有回滚，EXEC 时单条命令出错其余命令照样生效（错误以 Err 上报）。
- 知识库插件要求 sqlite 驱动：主驱动切到其它任何驱动时，知识库都不可用。
- sqlalchemy 走 format / pyformat 的方言（pymysql、psycopg）在提供 params 时，SQL 里的字面 `%`
  要写成 `%%`。
- 通用驱动的事务外调用：sqlalchemy 每次从池里借一条连接，执行完立即提交；odbc 每次新开一条
  连接（由驱动管理器做连接池）。Access Text 这类慢驱动每次调用约 1 秒。

## 许可

本插件以 **GNU General Public License v3.0** 发布，全文见仓库根 [`LICENSE`](LICENSE)。

本仓库是独立仓库，许可**不随 N.E.K.O 宿主项目**（Apache-2.0）继承或改变。
