"""源 PostgreSQL 访问：固定结构的增/改/删、快照基线、复制槽推进。

所有写接口都只操作 artifacts 表的四列固定结构，不接受任意 SQL。
"""
from __future__ import annotations

import logging
from contextlib import contextmanager
from typing import Any, Iterator

import psycopg2
import psycopg2.extras

from .config import Settings

log = logging.getLogger("source")

FIXED_COLUMNS = ("artifact_no", "service_name", "version", "sha256")


class SourceError(Exception):
    pass


class SourceDB:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings

    @contextmanager
    def connect(self) -> Iterator[psycopg2.extensions.connection]:
        conn = psycopg2.connect(self._settings.pg_dsn)
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def ensure_schema(self) -> None:
        """幂等建表 + 空表播种（固定结构，无 DDL 变更通道）。"""
        import os

        sql_path = os.path.join(
            os.path.dirname(os.path.dirname(__file__)), "init", "01_schema.sql"
        )
        with self.connect() as conn, conn.cursor() as cur:
            with open(sql_path, "r", encoding="utf-8") as fh:
                cur.execute(fh.read())

    # ------------------------------------------------------------- 业务写入
    def apply_operations(self, operations: list[dict[str, Any]]) -> dict[str, Any]:
        """成组修改源目录：全部操作在同一个源事务里，要么全成功要么全回滚。"""
        inserted = updated = deleted = 0
        with self.connect() as conn, conn.cursor() as cur:
            for op in operations:
                kind = op["op"]
                if kind == "insert":
                    cur.execute(
                        "INSERT INTO artifacts(artifact_no, service_name, version,"
                        " sha256) VALUES (%s, %s, %s, %s)",
                        (
                            op["artifact_no"],
                            op["service_name"],
                            op["version"],
                            op["sha256"],
                        ),
                    )
                    inserted += 1
                elif kind == "update":
                    cur.execute(
                        "UPDATE artifacts SET service_name=%s, version=%s,"
                        " sha256=%s WHERE artifact_no=%s",
                        (
                            op["service_name"],
                            op["version"],
                            op["sha256"],
                            op["artifact_no"],
                        ),
                    )
                    if cur.rowcount == 0:
                        raise SourceError(
                            f"UPDATE 失败：artifact_no={op['artifact_no']} 不存在"
                        )
                    updated += 1
                elif kind == "delete":
                    cur.execute(
                        "DELETE FROM artifacts WHERE artifact_no=%s",
                        (op["artifact_no"],),
                    )
                    if cur.rowcount == 0:
                        raise SourceError(
                            f"DELETE 失败：artifact_no={op['artifact_no']} 不存在"
                        )
                    deleted += 1
                else:  # pydantic 已拦截，这里只是防御
                    raise SourceError(f"不支持的操作类型: {kind}")
        return {"inserted": inserted, "updated": updated, "deleted": deleted}

    def list_artifacts(self) -> list[dict[str, Any]]:
        with self.connect() as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT artifact_no, service_name, version, sha256"
                " FROM artifacts ORDER BY artifact_no"
            )
            return [dict(zip(FIXED_COLUMNS, row)) for row in cur.fetchall()]

    # ------------------------------------------------------------- 复制槽
    def slot_exists(self, slot_name: str) -> bool:
        with self.connect() as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT 1 FROM pg_replication_slots WHERE slot_name=%s",
                (slot_name,),
            )
            return cur.fetchone() is not None

    def create_slot_with_snapshot(
        self, slot_name: str
    ) -> tuple[str, psycopg2.extensions.connection, str]:
        """创建逻辑复制槽并返回 (consistent_point LSN, 持有快照的连接, snapshot_id)。

        在一条普通连接的事务里先建槽（拿到槽的一致性 LSN），再
        pg_export_snapshot() 导出同一时刻快照；事务在基线读取完成前保持
        打开，另一条普通连接 SET TRANSACTION SNAPSHOT 复用该快照做基线，
        保证基线与复制流起点严格一致。兼容 PG 14/15/16。
        """
        conn = psycopg2.connect(self._settings.pg_dsn)
        conn.autocommit = False
        cur = conn.cursor()
        try:
            cur.execute("BEGIN")
            cur.execute(
                "SELECT lsn::text FROM pg_create_logical_replication_slot(%s, %s)",
                (slot_name, "test_decoding"),
            )
            consistent_lsn = cur.fetchone()[0]
            cur.execute("SELECT pg_export_snapshot()")
            snapshot_id = cur.fetchone()[0]
        except Exception:
            try:
                conn.close()
            except Exception:
                pass
            raise
        return consistent_lsn, conn, snapshot_id

    def baseline_rows_using_snapshot(self, snapshot_id: str) -> list[tuple]:
        """在普通连接上复用导出快照，读取基线时刻整目录。"""
        conn = psycopg2.connect(self._settings.pg_dsn)
        try:
            conn.autocommit = False
            with conn.cursor() as cur:
                cur.execute("BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ")
                cur.execute("SET TRANSACTION SNAPSHOT %s", (snapshot_id,))
                cur.execute(
                    "SELECT artifact_no, service_name, version, sha256"
                    " FROM artifacts ORDER BY artifact_no"
                )
                rows = cur.fetchall()
            conn.rollback()  # 只读事务，回滚即可释放快照引用
            return rows
        finally:
            conn.close()

    def drop_slot(self, slot_name: str) -> None:
        with self.connect() as conn, conn.cursor() as cur:
            cur.execute("SELECT pg_drop_replication_slot(%s)", (slot_name,))

    def drop_slots_with_prefix(self, prefix: str) -> int:
        """删除指定前缀的复制槽（清理核验临时槽残留），返回删除数量。"""
        with self.connect() as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT slot_name FROM pg_replication_slots"
                " WHERE slot_name LIKE %s",
                (prefix + "%",),
            )
            names = [r[0] for r in cur.fetchall()]
            for name in names:
                cur.execute("SELECT pg_drop_replication_slot(%s)", (name,))
            return len(names)

    def emit_boundary_marker(self, tag: str) -> None:
        """提交一个空事务推动复制流前进（不改源目录）。

        一次性核验用：让主流出现一个提交 LSN 大于核验边界的事务，保证
        "边界完整落入 SQLite"的判定在源库静默时也能收敛。优先用
        pg_logical_emit_message（PG14+，需超级用户）；权限不足时降级为
        临时表空事务（catalog 变更同样产生提交记录，且提交后即删无残留）。
        """
        try:
            with self.connect() as conn, conn.cursor() as cur:
                cur.execute(
                    "SELECT pg_logical_emit_message(true, 'verify_boundary', %s)",
                    (tag,),
                )
            return
        except Exception as exc:
            log.info(
                "pg_logical_emit_message 不可用（%s），降级为临时表空事务", exc
            )
        with self.connect() as conn, conn.cursor() as cur:
            cur.execute("CREATE TEMP TABLE _verify_marker(i int) ON COMMIT DROP")
            cur.execute("INSERT INTO _verify_marker VALUES (1)")
