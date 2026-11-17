# 制品目录历史查询服务

发布值班工程师追查“误上线时制品目录实际指向哪个版本”的历史查询系统。
源 PostgreSQL 中的制品目录（制品编号 / 服务名 / 版本 / SHA-256）通过
**一致基线 + 逻辑复制**被完整捕获到本地 SQLite 仅增日志，可按任意已捕获
提交点回看整张目录，支持删除后编号重用、多行事务整体可见。

## 1. 架构与正确性设计

```
PostgreSQL 源库 (artifacts 固定四列)
   │  ① 建逻辑复制槽 test_decoding + pg_export_snapshot 导出一致性快照
   │  ② 另开连接 SET TRANSACTION SNAPSHOT 复用快照做整表基线
   │  ③ 从槽 LSN 持续消费逻辑变更流（walsender）
   ▼
捕获工作线程（FastAPI 进程内 daemon thread）
   │  每个源事务：先整体写 SQLite（行变更 + 提交点同一事务）
   │              落盘成功后才 send_feedback 推进源端槽位
   ▼
SQLite 历史库（WAL，仅增不改）  ←→  FastAPI 网页 / JSON API
```

### 关键不变量

| 需求 | 实现方式 |
| --- | --- |
| 一致基线，普通 SQL 不中断 | 建槽与 `pg_export_snapshot()` 在同一事务完成；基线用 `SET TRANSACTION SNAPSHOT` 读一致性快照，源库无需停机、普通读写照常提交 |
| 基线前开始、基线后提交的事务不漏 | 该事务对导出快照不可见（不进基线），但其提交记录的 LSN 晚于槽起始 LSN，必然出现在逻辑流中被补捕获（自测第 2 组确定性验证） |
| 多行事务整体可见 | 一个源事务的全部行变更与提交点在**同一个 SQLite 事务**落盘，不存在半条事务 |
| 删除后重用编号保留旧历史 | `artifact_history` 仅追加，每行带 `change_type` 与提交点；按编号取“最后一条 ≤ 查询点”的记录重建目录，DELETE 与后续 INSERT 各自留痕 |
| 源断连 / 强杀重启后续接 | 复制槽持久化在源库；应用重启从槽位重放，SQLite 以源事务 xid 为主键幂等去重；断线自动指数/固定退避重连 |
| 不确认尚未落盘的更改 | 严格“先 SQLite commit，后 `send_feedback(flush_lsn=...)`”；keepalive 与无关消息一律不回送反馈。自测断言槽 `confirmed_flush_lsn` 永不超前已落盘提交点 |
| 初始化未完成不开放历史 | 状态机 `INITIALIZING → STREAMING / ERROR`；未到 STREAMING 时 `/api/points`、`/api/directory`、`/api/history/*` 一律 503 |
| 固定结构的增改删 | 网页/API 只接受 `insert/update/delete` 三类操作，pydantic 判别联合按 `op` 分流，仅四列，sha256 强制 64 位十六进制；无任何 DDL/任意 SQL 通道 |
| 一次性核验结论不可变 | 核验发起时把源整目录快照与边界 LSN 整体存入 `verifications` 只增表；结论（一致/不符/未确认）一旦写库即不可改写，之后源库的任何提交都不影响本次结论 |
| 核验不锁源表、不改源数据 | 核验只用一个短只读事务（REPEATABLE READ 快照 + `pg_current_wal_lsn()` 边界），事务立即结束；全程不执行任何源库写操作 |
| 中断核验可重新发起 | 源断连或应用重启时，所有未完成核验自动转 `UNCONFIRMED`（带原因），终态结论不动；网页/API 可随时重新发起新核验 |

### SQLite 表结构（`/data/history.db`）

- `capture_state`：阶段、槽名、基线 LSN、最近提交点/消息 LSN、复制流水位、错误信息。
- `txns`：源事务 xid（主键）、提交 LSN、提交时间。
- `artifact_history`：仅增变更链（BASELINE/INSERT/UPDATE/DELETE），带 xid、
  提交 LSN 与数值化 LSN，支持按提交点过滤与编号重建。
- `verifications`：一次性核验记录（只增不改）。发起时固定源整目录快照
  （`expected_json`）与复制流边界 LSN；终态为 `CONSISTENT` / `DIVERGED` /
  `UNCONFIRMED`，结论字段（缺失、额外、字段不符）写入后永不变更。

## 1A. 一次性核验（捕获结果与源库一致性）

值班工程师排查“误上线”前，可在网页发起一次性核验，确认捕获结果与源库一致。

### 流程与正确性

1. **发起即固定边界**：后端在一个短的 `REPEATABLE READ` 只读事务里
   `pg_export_snapshot()` 固定一致性快照、读整张 `artifacts`，再取
   `pg_current_wal_lsn()` 作为复制流边界（保证边界不早于快照内任何数据的
   WAL 位置）。快照与边界一并写入 SQLite `verifications` 表后事务立即结束，
   **不在源表上持锁、不写源库任何数据**。
2. **待捕获**：复制流水位（每条流消息含 keepalive 都会推进的服务端 WAL
   位置）越过边界 LSN 之前，页面显示“待捕获”。等待期间普通 SQL 可照常
   修改目录，互不阻塞。
3. **同边界逐编号比较**：水位越过边界后，从本地仅增历史按“每个编号最后一条
   ≤ 边界 LSN 的变更”重建目录，与发起时快照逐编号比对，列出：
   - **缺失**：源库有、本地无；
   - **额外**：本地有、源库无；
   - **字段不符**：编号相同但服务名/版本/SHA-256 任一不同（逐字段列出期望值与本地值）。

   三类都为空才判定**一致**。
4. **结论不可变**：结论与快照同库存档，之后源库的任何提交都不会重新计算或
   改写本次结论（核验只增，终态 UPDATE 被存储层拒绝）。
5. **中断即未确认，可重新发起**：源断连（捕获线程进入重连）或应用重启时，
   所有未完成核验自动转 `UNCONFIRMED` 并记录原因；已形成的结论保持不变。
   未确认不阻塞新核验的发起。

### 边界语义说明

边界 LSN 取在快照读取**之后**，因此它可能略晚于快照中最后一行数据的真实
WAL 位置——这只会让“待捕获”等待稍久一点，不会漏判：水位越过该边界时，
快照内全部数据必然已落入本地。核验比较的是**发起时刻的快照**而非源库当前
值，所以等待期间目录继续变化不影响结论。

## 2. 用 Docker Compose 启动（交付方式）

```bash
docker compose up --build
# 网页：http://localhost:8000/
# 源 PostgreSQL：localhost:5432（user/pass/db = postgres/postgres/postgres）
```

- `postgres` 服务：`postgres:16-bookworm`，已开启 `wal_level=logical`，
  命名卷 `pgdata` 持久化；首次启动自动执行 `init/01_schema.sql`
  幂等建表并播种 3 行示例数据。
- `app` 服务：本仓库构建的 Python 3.12 镜像，命名卷 `historydata`
  挂载到 `/data` 持久化 SQLite；`restart: unless-stopped`，源库健康检查
  通过后才启动。
- 两侧均为命名卷，`docker compose down` 不丢数据，`down -v` 才清空。

## 3. 网页功能（单页应用，无前端构建）

1. **捕获进度**：阶段徽章、复制槽、基线/最近提交 LSN、已捕获事务数、
   历史行数、最近错误，3 秒自动刷新。
2. **成组修改源目录**：动态增删 insert/update/delete 行，整组在一个源事务
   提交；任一操作失败整组回滚。
3. **提交点回看**：列出 BASELINE 与每个源事务提交点（LSN、xid、时间、
   变更行数），点击任一行即查看该时刻整张目录。
4. **单编号完整历史**：含 BASELINE/INSERT/UPDATE/DELETE 全链，
   直观验证“删除后重用编号，旧历史保留”。
5. **一次性核验**：一键发起，卡片展示状态徽章（待捕获/一致/不符/未确认）、
   边界 LSN、发起/完成时间；完成后分区列出缺失、额外、字段不符明细，
   全部相同才显示一致；2 秒自动刷新，中断卡片显示原因并可直接重新发起。

## 4. HTTP API

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/api/progress` | 捕获状态与进度（初始化期也可用） |
| GET | `/api/source/artifacts` | 源库当前目录 |
| POST | `/api/source/operations` | 成组增改删，单源事务，失败整组回滚 |
| GET | `/api/points` | 全部可查询提交点（未就绪时 503） |
| GET | `/api/directory?point=0/16B3748` | 该提交点（含）的整目录 |
| GET | `/api/history/{artifact_no}` | 单编号完整变更链 |
| POST | `/api/verifications` | 发起一次性核验；已有待捕获核验时 409，源库不可达时 502 |
| GET | `/api/verifications` | 全部核验记录（只增，新记录在前） |
| GET | `/api/verifications/{id}` | 单次核验状态与结论明细；不存在时 404 |

请求示例：

```bash
curl -X POST localhost:8000/api/source/operations -H 'Content-Type: application/json' -d '{
  "operations": [
    {"op":"insert","artifact_no":1004,"service_name":"cart-svc","version":"3.1.0",
     "sha256":"abcd1234abcd1234abcd1234abcd1234abcd1234abcd1234abcd1234abcd1234"},
    {"op":"delete","artifact_no":1003}
  ]}'
curl 'localhost:8000/api/directory?point=0/16B3748'
```

## 5. 自测

### 5.1 解析器单元测试

```bash
python3 -m unittest tests.test_decoder -v
```

覆盖 BEGIN/COMMIT、带空格与转义引号的值、DELETE 仅主键、其他表/DDL 忽略。

### 5.2 端到端自测（30 项断言）

`tests/selftest.sh` 在本机自动拉起一个便携 PostgreSQL（不依赖 Docker）、
启停应用并完成全链路验证：

```bash
# 可选：指定本机 PostgreSQL 二进制位置
export PGBIN=/path/to/postgresql/bin PGCLIENT=/path/to/client/bin
bash tests/selftest.sh
```

覆盖场景：

1. 初始化窗口历史接口 503，完成后 STREAMING 与基线目录正确；
2. **跨基线事务**：通过应用内置测试钩子
   （`TEST_INFLIGHT_ARTIFACT_NO` 环境变量，默认关闭）确定性构造
   “基线快照点之后打开、基线落盘之后提交”的事务，断言不进基线、
   不被遗漏、在其提交点可见；
3. 多行事务只产生一个提交点、同事务插入/删除同时生效；
4. 删除后重用编号，旧历史链完整（BASELINE,DELETE,INSERT）；
5. 失败整组回滚、非法 sha256/非法 LSN 参数校验；
6. `kill -9` 强杀应用后重启：事务数不重复、最后更新仍在、
   槽 `confirmed_flush_lsn` 不超前已落盘 LSN、续接后新变更继续捕获；
7. 源 PostgreSQL 重启后应用自动重连恢复 STREAMING，恢复后变更不丢；
8. SQLite 与 PostgreSQL 数据目录均落盘持久化。

最近一次结果：**30 passed / 0 failed**。

### 5.3 一次性核验单元测试与端到端自测

```bash
python3 -m unittest tests.test_verification -v   # 状态机/比较逻辑，8 项
bash tests/selftest_verify.sh                    # 真实 PG + uvicorn 端到端，37 项
```

端到端覆盖：

1. 正常核验：发起即固定边界与整目录快照，先“待捕获”后“一致”，
   缺失/额外/字段不符均为空；
2. **结论不可变**：核验完成后继续改源目录，结论与快照行数保持不变；
3. **并发修改**：通过测试钩子 `TEST_PUMP_PAUSE_MS` 确定性制造复制积压，
   发起核验后稳定观察到“待捕获”；等待期间普通 SQL 照常毫秒级提交
   （不被核验阻塞），核验仍按发起边界得出一致结论，等待期间的提交
   不混入本次快照；
4. **应用重启**：待捕获核验重启后显示“未确认”并带原因，可重新发起；
   已形成的一致结论不受影响；
5. **源断连**：停掉源 PostgreSQL 后待捕获核验转“未确认”，源库恢复、
   应用自动重连后可重新发起并得到一致结论；
6. 待捕获期间重复发起返回 409（用 `TEST_PUMP_PAUSE_MS` 钩子确定性构造
   待捕获窗口）、不存在的核验 404、核验记录只增保留；
7. 核验全程只读：源目录行数与 `artifact_history` 行数均不变化。

最近一次结果：**全部通过**。

## 6. 配置（环境变量）

| 变量 | 默认 | 说明 |
| --- | --- | --- |
| `PG_DSN` | 本地开发 DSN | 源库 libpq 连接串，Compose 中指向 `postgres` 服务 |
| `SQLITE_PATH` | `/tmp/capture/history.db` | 历史库路径（Compose 中为 `/data/history.db`） |
| `REPLICATION_SLOT` | `artifacts_history_slot` | 逻辑复制槽名 |
| `SOURCE_TABLE` | `artifacts` | 捕获的源表（结构固定） |
| `REPL_STATUS_INTERVAL_S` | `5` | 复制协议 keepalive 间隔 |
| `REPL_RECONNECT_BACKOFF_S` | `2` | 断线重连退避秒数 |
| `CAPTURE_INIT_SLEEP_MS` | `0` | 测试钩子：拉长初始化窗口 |
| `TEST_INFLIGHT_ARTIFACT_NO` | `0`(关) | 测试钩子：跨基线事务编号 |
| `TEST_INFLIGHT_SLEEP_S` | `3` | 测试钩子：在途事务持有秒数 |
| `TEST_PUMP_PAUSE_MS` | `0`(关) | 测试钩子：每条复制连接建立后泵暂停毫秒数，确定性制造待捕获窗口 |
| `TEST_STREAM_CONNECT_DELAY_MS` | `0`(关) | 测试钩子：建立 walsender 连接前暂停毫秒数，确定性构造源断连场景 |

## 7. 已知边界 / 未完成项

- 逻辑解码使用 PostgreSQL 内置 `test_decoding`（无需安装插件），解析器
  依赖本项目固定的四列结构；表结构变更不在支持范围内（需求明确禁止 DDL）。
- 历史库为单进程 SQLite，适合值班排查类读多写少场景；如需多副本水平扩展
  需更换存储层。
- 复制槽若被人工从源库删除，应用进入 ERROR 并在 `/api/progress` 暴露，
  不会自动重建（重建会使旧 LSN 失效，需人工确认后清空历史库重新基线）。
- 沙箱环境未提供 Docker，Compose 文件未经本机 `docker compose` 实跑，
  但应用本体已用同等配置在本机 PostgreSQL 15 + uvicorn 下完成全部端到端
  验证；Compose 镜像基于官方 `python:3.12-slim` 与 `postgres:16-bookworm`。
